import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";

import {
  InMemoryTransport,
  LATEST_PROTOCOL_VERSION,
  type JSONRPCMessage,
  type RequestId,
} from "@modelcontextprotocol/server";
import { serveStdio } from "@modelcontextprotocol/server/stdio";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ReadOnly, TypeError } from "../src/errors.js";
import { JsonFloat, parseJson, stringifyJson } from "../src/jsonio.js";
import {
  LosslessStdioTransport,
  createMcpServer,
  embed,
  runMcp,
} from "../src/mcp.js";
import { Db, connect } from "../src/store.js";

const directories: string[] = [];

function temporaryDirectory(): string {
  const path = mkdtempSync(join(tmpdir(), "fgraph-mcp-"));
  directories.push(path);
  return path;
}

class TestClient {
  readonly #transport: InMemoryTransport;
  readonly #pending = new Map<
    RequestId,
    (message: { result: unknown }) => void
  >();
  #id = 0;

  constructor(transport: InMemoryTransport) {
    this.#transport = transport;
    transport.onmessage = (message) => {
      if (!("id" in message) || message.id === undefined) return;
      const resolveResponse = this.#pending.get(message.id);
      if (resolveResponse !== undefined && "result" in message) {
        this.#pending.delete(message.id);
        resolveResponse(message);
      } else if (resolveResponse !== undefined && "error" in message) {
        this.#pending.delete(message.id);
        resolveResponse({
          result: {
            rpc_error: String(message.error.message),
          },
        });
      }
    };
  }

  async start(): Promise<void> {
    await this.#transport.start();
  }

  async request(
    method: string,
    params: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const id = ++this.#id;
    const response = new Promise<{ result: unknown }>((resolveResponse) =>
      this.#pending.set(id, resolveResponse),
    );
    await this.#transport.send({
      jsonrpc: "2.0",
      id,
      method,
      params,
    } as JSONRPCMessage);
    const result = (await response).result as Record<string, unknown>;
    if (typeof result.rpc_error === "string")
      throw new Error(`MCP RPC error: ${result.rpc_error}`);
    return result;
  }

  async initialize(): Promise<Record<string, unknown>> {
    const result = await this.request("initialize", {
      protocolVersion: LATEST_PROTOCOL_VERSION,
      capabilities: {},
      clientInfo: { name: "vitest", version: "1.0.0" },
    });
    await this.#transport.send({
      jsonrpc: "2.0",
      method: "notifications/initialized",
    } as JSONRPCMessage);
    return result;
  }

  async close(): Promise<void> {
    await this.#transport.close();
  }
}

function toolEnvelope(
  result: Record<string, unknown>,
): Record<string, unknown> {
  const blocks = result.content as Array<{ type: string; text: string }>;
  expect(blocks[0]?.type).toBe("text");
  const envelope = parseJson(
    blocks[0]?.text ?? "",
    "MCP tool content",
  ) as Record<string, unknown>;
  expect(envelope).toMatchObject({ ok: true, basis_tx: expect.anything() });
  expect(stringifyJson(result.structuredContent)).toBe(stringifyJson(envelope));
  return envelope;
}

function content(result: Record<string, unknown>): unknown {
  return toolEnvelope(result).data;
}

function resourceData(result: Record<string, unknown>): unknown {
  const blocks = result.contents as Array<{ text: string }>;
  return parseJson(blocks[0]?.text ?? "", "MCP resource content");
}

function coreRows(db: ReturnType<typeof connect>): Record<string, unknown[]> {
  return {
    meta: db._connection
      .prepare("SELECT * FROM fgraph_meta ORDER BY key")
      .all(),
    ids: db._connection.prepare("SELECT * FROM fgraph_ids ORDER BY id").all(),
    facts: db._connection
      .prepare("SELECT * FROM fgraph_facts ORDER BY id")
      .all(),
    blobs: db._connection
      .prepare("SELECT * FROM fgraph_blobs ORDER BY hash")
      .all(),
    fts: db._connection
      .prepare("SELECT rowid,text FROM fgraph_fts ORDER BY rowid")
      .all(),
  };
}

afterEach(() => {
  while (directories.length > 0)
    rmSync(directories.pop() as string, { recursive: true, force: true });
});

describe("MCP server", () => {
  it("advertises and executes every normative read/write tool", async () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const server = createMcpServer(db, { write: true });
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    const client = new TestClient(clientTransport);
    await client.start();
    await server.connect(serverTransport);
    const initialized = await client.initialize();
    try {
      const listed = await client.request("tools/list", {});
      const tools = listed.tools as Array<{
        name: string;
        description: string;
        inputSchema: Record<string, unknown>;
        outputSchema?: Record<string, unknown>;
        annotations?: {
          readOnlyHint?: boolean;
          destructiveHint?: boolean;
          idempotentHint?: boolean;
          openWorldHint?: boolean;
        };
      }>;
      expect(initialized.instructions).toContain("read-only");
      expect(tools.map((tool) => tool.name).sort()).toEqual([
        "about",
        "datoms",
        "explain",
        "forget",
        "history",
        "query",
        "recall",
        "receipt",
        "remember",
        "schema",
        "undo",
        "why",
      ]);
      expect(tools.every((tool) => tool.description.includes("Example:"))).toBe(
        true,
      );
      expect(tools.every((tool) => tool.outputSchema !== undefined)).toBe(true);
      expect(
        tools.every(
          (tool) =>
            tool.annotations?.idempotentHint === true &&
            tool.annotations.openWorldHint === false,
        ),
      ).toBe(true);
      expect(
        tools.find((tool) => tool.name === "schema")?.inputSchema
          .properties as Record<string, unknown>,
      ).toHaveProperty("include_system");

      const rememberedResult = await client.request("tools/call", {
        name: "remember",
        arguments: {
          key: "memory/language",
          text: "User prefers Go",
          source: "conversation:42",
          operation_id: "remember-language",
        },
      });
      const remembered = content(rememberedResult) as {
        tx: number;
        asserted: Array<Record<string, unknown>>;
      };
      expect(remembered.tx).toBeGreaterThan(64);
      expect(toolEnvelope(rememberedResult).basis_tx).toBe(remembered.tx);
      expect(remembered.asserted).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ a: "fgraph/by", v: "mcp:vitest" }),
          expect.objectContaining({ a: "fgraph/source", v: "conversation:42" }),
        ]),
      );
      const emptySource = content(
        await client.request("tools/call", {
          name: "remember",
          arguments: {
            key: "memory/empty-source",
            text: "Empty source is still explicit provenance",
            source: "",
            operation_id: "remember-empty-source",
          },
        }),
      ) as { asserted: Array<Record<string, unknown>> };
      expect(emptySource.asserted).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ a: "fgraph/source", v: "" }),
        ]),
      );

      expect(
        content(
          await client.request("tools/call", {
            name: "about",
            arguments: { entity: "memory/language", depth: 1 },
          }),
        ),
      ).toMatchObject({
        "memory/text": "User prefers Go",
      });
      const recalled = content(
        await client.request("tools/call", {
          name: "recall",
          arguments: { query: "prefers", k: 3, expand: 0 },
        }),
      ) as {
        hits: unknown[];
      };
      expect(recalled.hits).toHaveLength(1);
      const schema = content(
        await client.request("tools/call", {
          name: "schema",
          arguments: { prefix: "memory/", include_system: false },
        }),
      ) as {
        attributes: unknown[];
      };
      expect(schema.attributes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            name: "memory/text",
            observed: expect.objectContaining({ types: ["text"] }),
          }),
        ]),
      );
      expect(
        content(
          await client.request("tools/call", {
            name: "history",
            arguments: { entity: "memory/language", attribute: "memory/text" },
          }),
        ),
      ).toMatchObject({ items: [expect.any(Object)], truncated: false });
      expect(
        content(
          await client.request("tools/call", {
            name: "why",
            arguments: { entity: "memory/language", attribute: "memory/text" },
          }),
        ),
      ).toMatchObject({ items: [expect.any(Object)], truncated: false });
      expect(
        content(
          await client.request("tools/call", {
            name: "query",
            arguments: {
              q: {
                find: ["?text"],
                where: [["memory/language", "memory/text", "?text"]],
              },
            },
          }),
        ),
      ).toEqual({ columns: ["?text"], rows: [["User prefers Go"]] });
      expect(
        content(
          await client.request("tools/call", {
            name: "explain",
            arguments: {
              q: {
                find: ["?text"],
                where: [["memory/language", "memory/text", "?text"]],
              },
            },
          }),
        ),
      ).toMatchObject({ clauses: [{ access: "eavt/ea" }] });
      expect(
        content(
          await client.request("tools/call", {
            name: "datoms",
            arguments: {
              index: "avet",
              components: ["memory/text", "User prefers Go"],
              limit: 1,
            },
          }),
        ),
      ).toMatchObject({
        items: [{ a: "memory/text", v: "User prefers Go" }],
      });
      expect(
        content(
          await client.request("tools/call", {
            name: "receipt",
            arguments: { tx: remembered.tx },
          }),
        ),
      ).toMatchObject({ tx: remembered.tx, event: expect.any(String) });

      const templates = await client.request("resources/templates/list", {});
      expect(
        (templates.resourceTemplates as Array<{ name: string }>).map(
          (template) => template.name,
        ),
      ).toEqual(["schema", "entity", "transaction", "changes", "event"]);
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: "fgraph://schema?prefix=memory%2F",
          }),
        ),
      ).toMatchObject({ attributes: expect.any(Array) });
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: "fgraph://entity/memory%2Flanguage",
          }),
        ),
      ).toMatchObject({
        items: [
          expect.objectContaining({ a: "memory/text", v: "User prefers Go" }),
        ],
      });
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: `fgraph://tx/${remembered.tx}`,
          }),
        ),
      ).toMatchObject({ tx: remembered.tx });
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: "fgraph://changes?since=64",
          }),
        ),
      ).toMatchObject({ events: expect.any(Array) });

      const forgottenResult = await client.request("tools/call", {
        name: "forget",
        arguments: {
          entity: "memory/language",
          attribute: "memory/text",
          value: "User prefers Go",
          operation_id: "forget-language",
          if_basis_tx: db._basisTx(),
        },
      });
      const forgotten = content(forgottenResult) as { tx: number };
      expect(forgotten.tx).toBeGreaterThan(remembered.tx);
      expect(toolEnvelope(forgottenResult).basis_tx).toBe(forgotten.tx);
      expect(db.receipt(forgotten.tx)).toMatchObject({ by: "mcp:vitest" });
      const undoResult = await client.request("tools/call", {
        name: "undo",
        arguments: {
          tx: forgotten.tx,
          operation_id: "undo-forget-language",
          if_basis_tx: db._basisTx(),
        },
      });
      const undone = content(undoResult) as { tx: number };
      expect(undone).toMatchObject({ tx: expect.any(Number) });
      expect(toolEnvelope(undoResult).basis_tx).toBe(undone.tx);
      expect(db.receipt(undone.tx)).toMatchObject({ by: "mcp:vitest" });
      expect(db.entity("memory/language")["memory/text"]).toBe(
        "User prefers Go",
      );

      const invalid = await client.request("tools/call", {
        name: "remember",
        arguments: {
          key: "memory/invalid",
          facts: { id: "x", "x/value": 1 },
          operation_id: "invalid-key-without-text",
        },
      });
      expect(invalid).toMatchObject({
        isError: true,
        content: [
          expect.objectContaining({
            text: expect.stringContaining("key requires text"),
          }),
        ],
      });
      expect(
        await client.request("tools/call", {
          name: "remember",
          arguments: {
            key: "",
            text: "invalid",
            operation_id: "invalid-empty-key",
          },
        }),
      ).toMatchObject({ isError: true });
      expect(
        await client.request("tools/call", {
          name: "remember",
          arguments: {
            facts: [],
            text: "  ",
            operation_id: "invalid-empty-memory",
          },
        }),
      ).toMatchObject({ isError: true });
      expect(
        content(
          await client.request("tools/call", {
            name: "remember",
            arguments: {
              facts: { id: "fact/one", "fact/value": 1 },
              operation_id: "remember-fact-one",
            },
          }),
        ),
      ).toMatchObject({ tx: expect.any(Number) });
      expect(
        content(
          await client.request("tools/call", {
            name: "remember",
            arguments: {
              facts: [
                { id: "fact/two", "fact/value": 2 },
                { id: "fact/three", "fact/value": 3 },
              ],
              operation_id: "remember-facts-two-three",
            },
          }),
        ),
      ).toMatchObject({ tx: expect.any(Number) });
      expect(
        await client.request("tools/call", {
          name: "forget",
          arguments: {
            entity: "fact/two",
            attribute: "",
            operation_id: "invalid-forget-empty-attribute",
            if_basis_tx: db._basisTx(),
          },
        }),
      ).toMatchObject({ isError: true });
      expect(db.entity("fact/two")).toMatchObject({ "fact/value": 2 });
      expect(
        content(
          await client.request("tools/call", {
            name: "remember",
            arguments: {
              facts: ["assert", "fact/one", "fact/other", 4],
              operation_id: "remember-fact-other",
            },
          }),
        ),
      ).toMatchObject({
        tx: expect.any(Number),
      });
      expect(
        content(
          await client.request("tools/call", {
            name: "remember",
            arguments: {
              facts: ["retract", "fact/one", "fact/value", 1],
              operation_id: "remember-retract-fact-one",
              if_basis_tx: db._basisTx(),
            },
          }),
        ),
      ).toMatchObject({ tx: expect.any(Number) });
      expect(
        content(
          await client.request("tools/call", {
            name: "forget",
            arguments: {
              entity: "fact/one",
              attribute: "fact/other",
              operation_id: "forget-fact-other",
              if_basis_tx: db._basisTx(),
            },
          }),
        ),
      ).toMatchObject({
        tx: expect.any(Number),
      });
      expect(
        content(
          await client.request("tools/call", {
            name: "forget",
            arguments: {
              entity: "fact/three",
              operation_id: "forget-fact-three",
              if_basis_tx: db._basisTx(),
            },
          }),
        ),
      ).toMatchObject({ tx: expect.any(Number) });
      expect(
        content(
          await client.request("tools/call", {
            name: "about",
            arguments: { entity: "fact/two" },
          }),
        ),
      ).toMatchObject({ "fact/value": 2 });
      const whySpy = vi.spyOn(Db.prototype, "why");
      const historySpy = vi.spyOn(Db.prototype, "history");
      expect(
        content(
          await client.request("tools/call", {
            name: "why",
            arguments: { entity: "fact/two" },
          }),
        ),
      ).toMatchObject({ items: [expect.any(Object)], truncated: false });
      expect(whySpy).toHaveBeenLastCalledWith("fact/two");
      expect(
        content(
          await client.request("tools/call", {
            name: "history",
            arguments: { entity: "fact/two" },
          }),
        ),
      ).toMatchObject({ items: [expect.any(Object)], truncated: false });
      expect(historySpy).toHaveBeenLastCalledWith("fact/two");
      expect(
        await client.request("tools/call", {
          name: "why",
          arguments: { entity: "fact/two", attribute: "" },
        }),
      ).toMatchObject({ isError: true });
      expect(
        await client.request("tools/call", {
          name: "history",
          arguments: { entity: "fact/two", attribute: "" },
        }),
      ).toMatchObject({ isError: true });
      expect(whySpy).toHaveBeenCalledTimes(1);
      expect(historySpy).toHaveBeenCalledTimes(1);
      expect(
        content(
          await client.request("tools/call", {
            name: "query",
            arguments: {
              q: { find: ["?v"], where: [["fact/two", "fact/value", "?v"]] },
              args: {},
            },
          }),
        ),
      ).toEqual({
        columns: ["?v"],
        rows: [[2]],
      });
      db.defineShape("fact/shape", { required: ["fact/value"] });
      const schemaPage = content(
        await client.request("tools/call", {
          name: "schema",
          arguments: { prefix: "fact/", limit: 1 },
        }),
      ) as {
        attributes: unknown[];
        shapes: unknown[];
        basis_tx: number;
        next_cursor: string;
        truncated: boolean;
      };
      expect(schemaPage).toMatchObject({
        attributes: [expect.any(Object)],
        next_cursor: expect.any(String),
        truncated: true,
      });
      expect(schemaPage.attributes.length + schemaPage.shapes.length).toBe(1);
      content(
        await client.request("tools/call", {
          name: "remember",
          arguments: {
            facts: { id: "fact/four", "fact/zz-new": 4 },
            operation_id: "remember-fact-four",
          },
        }),
      );
      const schemaNextResult = await client.request("tools/call", {
        name: "schema",
        arguments: { cursor: schemaPage.next_cursor, limit: 100 },
      });
      expect(
        schemaNextResult,
        stringifyJson(schemaNextResult),
      ).not.toMatchObject({ isError: true });
      const schemaNext = content(schemaNextResult) as {
        attributes: Array<{ name: string }>;
        shapes: Array<{ name: string }>;
        basis_tx: number;
      };
      expect(schemaNext.basis_tx).toBe(schemaPage.basis_tx);
      expect(
        schemaNext.attributes.map((attribute) => attribute.name),
      ).not.toContain("fact/zz-new");
      expect(schemaNext.shapes.map((shape) => shape.name)).toContain(
        "fact/shape",
      );
      expect(
        await client.request("tools/call", {
          name: "schema",
          arguments: { cursor: "%%%" },
        }),
      ).toMatchObject({ isError: true });
      expect(
        await client.request("tools/call", {
          name: "schema",
          arguments: { cursor: "A" },
        }),
      ).toMatchObject({ isError: true });
      const schemaCursor = (value: unknown): string =>
        Buffer.from(stringifyJson(value), "utf8").toString("base64url");
      const validCursorShape = {
        v: 1,
        basis: schemaPage.basis_tx,
        offset: 0,
        prefix: null,
        include_system: false,
        digest: "sha256:test",
      };
      for (const invalidCursor of [
        "not-an-object",
        { ...validCursorShape, v: 2 },
        { ...validCursorShape, basis: "64" },
        { ...validCursorShape, offset: "0" },
        { ...validCursorShape, offset: -1 },
        { ...validCursorShape, offset: 9_007_199_254_740_992n },
        { ...validCursorShape, prefix: 1 },
        { ...validCursorShape, include_system: "false" },
        { ...validCursorShape, digest: 1 },
      ]) {
        expect(
          await client.request("tools/call", {
            name: "schema",
            arguments: { cursor: schemaCursor(invalidCursor) },
          }),
        ).toMatchObject({ isError: true });
      }
      expect(
        await client.request("tools/call", {
          name: "schema",
          arguments: { cursor: schemaPage.next_cursor, prefix: "other/" },
        }),
      ).toMatchObject({ isError: true });
      expect(
        await client.request("tools/call", {
          name: "schema",
          arguments: {
            cursor: schemaPage.next_cursor,
            include_system: true,
          },
        }),
      ).toMatchObject({ isError: true });
      const changedDigest = parseJson(
        Buffer.from(schemaPage.next_cursor, "base64url").toString("utf8"),
        "schema cursor",
      ) as Record<string, unknown>;
      changedDigest.digest = "sha256:changed";
      expect(
        await client.request("tools/call", {
          name: "schema",
          arguments: { cursor: schemaCursor(changedDigest) },
        }),
      ).toMatchObject({ isError: true });
      expect(
        content(
          await client.request("tools/call", { name: "schema", arguments: {} }),
        ),
      ).toMatchObject({ attributes: expect.any(Array) });
      expect(
        content(
          await client.request("tools/call", { name: "datoms", arguments: {} }),
        ),
      ).toMatchObject({ items: expect.any(Array) });
      const datomPage = content(
        await client.request("tools/call", {
          name: "datoms",
          arguments: { limit: 1 },
        }),
      ) as { next_cursor: string };
      expect(
        content(
          await client.request("tools/call", {
            name: "datoms",
            arguments: { cursor: datomPage.next_cursor },
          }),
        ),
      ).toMatchObject({ items: expect.any(Array) });
      expect(
        content(
          await client.request("tools/call", {
            name: "datoms",
            arguments: { source: "history" },
          }),
        ),
      ).toMatchObject({ items: expect.any(Array) });
      const unprefixedSchema = content(
        await client.request("tools/call", {
          name: "schema",
          arguments: { include_system: true, limit: 1 },
        }),
      ) as { next_cursor: string };
      expect(
        content(
          await client.request("tools/call", {
            name: "schema",
            arguments: { cursor: unprefixedSchema.next_cursor },
          }),
        ),
      ).toMatchObject({ attributes: expect.any(Array) });
      db.transact(
        { id: "memory/oversized", "memory/text": "x".repeat(300 * 1024) },
        { operationId: "oversized-memory" },
      );
      expect(
        await client.request("tools/call", {
          name: "about",
          arguments: { entity: "memory/oversized" },
        }),
      ).toMatchObject({ isError: true });
      await expect(
        client.request("resources/read", {
          uri: "fgraph://entity/memory%2Foversized",
        }),
      ).rejects.toThrow("MCP RPC error");
      expect(
        await client.request("tools/call", {
          name: "recall",
          arguments: { query: "   " },
        }),
      ).toMatchObject({ isError: true });
      expect(
        await client.request("tools/call", {
          name: "recall",
          arguments: { query: "value", k: 0 },
        }),
      ).toMatchObject({ isError: true });
    } finally {
      await server.close();
      await client.close();
    }
  });

  it("pins read envelopes before evaluating the answer", async () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const seeded = db.transact({
      id: "pinned/entity",
      "pinned/value": "seen",
    });
    const server = createMcpServer(db);
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    const client = new TestClient(clientTransport);
    await client.start();
    await server.connect(serverTransport);
    await client.initialize();
    const originalAt = db.at.bind(db);
    vi.spyOn(db, "at").mockImplementationOnce((basis) => {
      const view = originalAt(basis);
      db.transact({ id: "pinned/entity", "pinned/new": "unseen" });
      return view;
    });
    try {
      const result = await client.request("tools/call", {
        name: "about",
        arguments: { entity: "pinned/entity" },
      });
      expect(toolEnvelope(result).basis_tx).toBe(seeded.tx);
      expect(content(result)).toEqual({ "pinned/value": "seen" });
    } finally {
      await server.close();
      await client.close();
    }
  });

  it("uses the checked basis when a mutation is elided as a no-op", async () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    vi.spyOn(db, "transact").mockReturnValueOnce({
      status: "noop",
      event: null,
      basis_tx: 64,
      tx: null,
      at: null,
      ids: {},
      asserted: [],
      retracted: [],
    });
    const server = createMcpServer(db, { write: true });
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    const client = new TestClient(clientTransport);
    await client.start();
    await server.connect(serverTransport);
    await client.initialize();
    try {
      const result = await client.request("tools/call", {
        name: "remember",
        arguments: {
          facts: { id: "noop/entity", "noop/value": true },
          operation_id: "noop-mutation",
        },
      });
      expect(toolEnvelope(result).basis_tx).toBe(64);
      expect(content(result)).toMatchObject({ status: "noop", tx: null });
    } finally {
      await server.close();
      await client.close();
    }
  });

  it("omits mutation tools in read-only mode", async () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const server = createMcpServer(db);
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    const client = new TestClient(clientTransport);
    await client.start();
    await server.connect(serverTransport);
    await client.initialize();
    try {
      const listed = await client.request("tools/list", {});
      expect(
        (listed.tools as Array<{ name: string }>)
          .map((tool) => tool.name)
          .sort(),
      ).toEqual([
        "about",
        "datoms",
        "explain",
        "history",
        "query",
        "recall",
        "receipt",
        "schema",
        "why",
      ]);
    } finally {
      await server.close();
      await client.close();
    }
    expect(() => runMcp(db)).toThrowError(ReadOnly);
    const writableHandle = runMcp(db, { write: true });
    await writableHandle.close();

    const directory = temporaryDirectory();
    const path = join(directory, "readonly.db");
    using initialized = connect(path, { clock: 1_767_225_600_000_000n });
    initialized.close();
    using readOnly = connect(path, { readOnly: true });
    expect(() => runMcp(readOnly, { write: true })).toThrowError(ReadOnly);
  });

  it("bounds and pins resource pages and rejects forged continuations", async () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const anonymous = db.transact(
      { "anonymous/resource-value": true },
      { operationId: "anonymous-resource" },
    );
    const anonymousId = anonymous.asserted.find(
      (fact) => fact.a === "anonymous/resource-value",
    )?.e as number | bigint;
    const anonymousHex = db._connection
      .prepare<[number | bigint], { gid: string }>(
        "SELECT hex(gid) AS gid FROM fgraph_ids WHERE id=?",
      )
      .get(anonymousId)
      ?.gid.toLowerCase() as string;
    const gid = `${anonymousHex.slice(0, 8)}-${anonymousHex.slice(8, 12)}-${anonymousHex.slice(12, 16)}-${anonymousHex.slice(16, 20)}-${anonymousHex.slice(20)}`;
    const facts: Record<string, unknown> = { id: "bulk/entity" };
    const transactionFacts: Record<string, unknown> = {};
    for (let index = 0; index < 105; index++)
      facts[`bulk/attribute-${String(index).padStart(3, "0")}`] = index;
    for (let index = 0; index < 105; index++)
      transactionFacts[`audit/attribute-${String(index).padStart(3, "0")}`] =
        index;
    const bulk = db.transact(facts, {
      operationId: "bulk-resource-page",
      tx: transactionFacts,
    });
    for (let index = 0; index < 101; index++)
      db.transact(
        {
          id: `change/${String(index).padStart(3, "0")}`,
          "change/value": index,
        },
        { operationId: `change-${String(index).padStart(3, "0")}` },
      );

    const identity = db._connection
      .prepare<[], { id: bigint }>(
        "SELECT id FROM fgraph_ids WHERE name='bulk/entity'",
      )
      .get();
    expect(identity).toBeDefined();

    const server = createMcpServer(db);
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    const client = new TestClient(clientTransport);
    await client.start();
    await server.connect(serverTransport);
    await client.initialize();
    try {
      const schemaFirst = resourceData(
        await client.request("resources/read", {
          uri: "fgraph://schema?prefix=",
        }),
      ) as {
        attributes: unknown[];
        next_uri: string;
      };
      expect(schemaFirst.attributes).toHaveLength(100);
      const schemaSecond = resourceData(
        await client.request("resources/read", { uri: schemaFirst.next_uri }),
      ) as { attributes: unknown[] };
      expect(schemaSecond.attributes.length).toBeGreaterThan(0);

      const schemaUrl = new URL(schemaFirst.next_uri);
      const schemaCursor = schemaUrl.searchParams.get("cursor") as string;
      const decodedSchema = parseJson(
        Buffer.from(schemaCursor, "base64url").toString("utf8"),
        "schema resource cursor",
      ) as Record<string, unknown>;
      const encodeCursor = (value: unknown): string =>
        Buffer.from(stringifyJson(value), "utf8").toString("base64url");
      const readSchemaCursor = async (cursor: string, prefix?: string) => {
        const uri = new URL("fgraph://schema?prefix=");
        uri.searchParams.set("cursor", cursor);
        if (prefix !== undefined) uri.searchParams.set("prefix", prefix);
        return client.request("resources/read", { uri: uri.href });
      };
      for (const invalid of [
        "not-an-object",
        { ...decodedSchema, v: 2 },
        { ...decodedSchema, resource: "changes" },
        { ...decodedSchema, basis: "64" },
        { ...decodedSchema, offset: "100" },
        { ...decodedSchema, offset: -1 },
        { ...decodedSchema, offset: 9_007_199_254_740_992n },
        { ...decodedSchema, digest: 1 },
      ])
        await expect(readSchemaCursor(encodeCursor(invalid))).rejects.toThrow(
          "MCP RPC error",
        );
      await expect(readSchemaCursor("%")).rejects.toThrow("MCP RPC error");
      await expect(readSchemaCursor("A")).rejects.toThrow("MCP RPC error");
      await expect(readSchemaCursor("a".repeat(4097))).rejects.toThrow(
        "MCP RPC error",
      );
      await expect(readSchemaCursor(schemaCursor, "other/")).rejects.toThrow(
        "MCP RPC error",
      );
      await expect(
        readSchemaCursor(
          encodeCursor({ ...decodedSchema, digest: "sha256:forged" }),
        ),
      ).rejects.toThrow("MCP RPC error");
      await expect(
        readSchemaCursor(encodeCursor({ ...decodedSchema, offset: 10_000 })),
      ).rejects.toThrow("MCP RPC error");

      const entityFirst = resourceData(
        await client.request("resources/read", {
          uri: `fgraph://entity/bulk%2Fentity?at=${bulk.tx}`,
        }),
      ) as { items: unknown[]; next_uri: string };
      expect(entityFirst.items).toHaveLength(100);
      expect(
        resourceData(
          await client.request("resources/read", { uri: entityFirst.next_uri }),
        ),
      ).toMatchObject({ items: expect.any(Array) });
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: `fgraph://entity/${String((identity as { id: bigint }).id)}`,
          }),
        ),
      ).toMatchObject({ items: expect.any(Array) });
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: `fgraph://entity/${gid}`,
          }),
        ),
      ).toMatchObject({ items: [expect.any(Object)] });

      expect(
        resourceData(
          await client.request("resources/read", {
            uri: `fgraph://tx/${bulk.tx}`,
          }),
        ),
      ).toMatchObject({ facts: expect.any(Array), truncated: true });

      const changesFirst = resourceData(
        await client.request("resources/read", {
          uri: `fgraph://changes?since=${bulk.tx}`,
        }),
      ) as { events: unknown[]; next_uri: string };
      expect(changesFirst.events).toHaveLength(100);
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: changesFirst.next_uri,
          }),
        ),
      ).toMatchObject({ events: [expect.any(Object)] });
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: `fgraph://changes?since=${db._basisTx()}`,
          }),
        ),
      ).toMatchObject({ events: [] });
      expect(
        resourceData(
          await client.request("resources/read", {
            uri: "fgraph://changes?unused=",
          }),
        ),
      ).toMatchObject({
        events: expect.any(Array),
        next_uri: expect.any(String),
      });
      const changesUrl = new URL(changesFirst.next_uri);
      const changesCursor = changesUrl.searchParams.get("cursor") as string;
      const decodedChanges = parseJson(
        Buffer.from(changesCursor, "base64url").toString("utf8"),
        "changes resource cursor",
      ) as Record<string, unknown>;
      await expect(
        client.request("resources/read", {
          uri: `fgraph://changes?since=${bulk.tx}&cursor=${encodeURIComponent(
            encodeCursor({ ...decodedChanges, position: "65" }),
          )}`,
        }),
      ).rejects.toThrow("MCP RPC error");
      await expect(
        client.request("resources/read", {
          uri: `fgraph://changes?since=${bulk.tx}&cursor=${encodeURIComponent(
            encodeCursor({
              ...decodedChanges,
              basis: BigInt(db._basisTx()) + 1n,
            }),
          )}`,
        }),
      ).rejects.toThrow("MCP RPC error");
      await expect(
        client.request("resources/read", { uri: "fgraph://changes?since=63" }),
      ).rejects.toThrow("MCP RPC error");
      await expect(
        client.request("resources/read", {
          uri: "fgraph://changes?since=not-a-transaction",
        }),
      ).rejects.toThrow("MCP RPC error");

      for (const limit of [null, 1.5, -1, 1001])
        expect(
          await client.request("tools/call", {
            name: "query",
            arguments: { q: { find: ["ok"], where: [], limit } },
          }),
        ).toMatchObject({ isError: true });
    } finally {
      await server.close();
      await client.close();
    }
  });

  it("embeds anonymous notes and recall queries through the configured local command", async () => {
    const directory = temporaryDirectory();
    const embedder = join(directory, "embedder.mjs");
    writeFileSync(
      embedder,
      'process.stdin.resume(); process.stdin.on("end",()=>process.stdout.write("[1.0,2.5]"));\n',
    );
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const server = createMcpServer(db, {
      write: true,
      embedCommand: JSON.stringify([process.execPath, embedder]),
    });
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    const client = new TestClient(clientTransport);
    await client.start();
    await server.connect(serverTransport);
    await client.initialize();
    try {
      expect(
        content(
          await client.request("tools/call", {
            name: "remember",
            arguments: {
              text: "anonymous vector memory",
              operation_id: "anonymous-vector-memory",
            },
          }),
        ),
      ).toMatchObject({ tx: expect.any(Number) });
      const recalled = content(
        await client.request("tools/call", {
          name: "recall",
          arguments: { query: "vector" },
        }),
      ) as { hits: unknown[] };
      expect(recalled.hits).toHaveLength(1);
    } finally {
      await server.close();
      await client.close();
    }
  });

  it("chunks an oversized change event and advances to later events", async () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const oversized = db.transact({
      id: "change/oversized",
      "change/value": "x".repeat(300_000),
    });
    const later = db.transact({ id: "change/later", "change/value": "small" });
    const expected = stringifyJson(
      db.eventRecords(64, oversized.tx as number)[0],
    );
    const server = createMcpServer(db);
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    const client = new TestClient(clientTransport);
    await client.start();
    await server.connect(serverTransport);
    await client.initialize();
    try {
      const changes = resourceData(
        await client.request("resources/read", {
          uri: "fgraph://changes?since=64",
        }),
      ) as {
        events: unknown[];
        oversized_event: {
          event: string;
          event_hash: string;
          bytes: number;
          uri: string;
        };
        next_uri: string;
      };
      expect(changes.events).toEqual([]);
      expect(changes.oversized_event).toMatchObject({
        event: oversized.event,
        event_hash: expect.stringMatching(/^[0-9a-f]{64}$/u),
        bytes: Buffer.byteLength(expected),
        uri: expect.stringMatching(/^fgraph:\/\/event\//u),
      });

      const chunks: Buffer[] = [];
      let next: string | undefined = changes.oversized_event.uri;
      while (next !== undefined) {
        const page = resourceData(
          await client.request("resources/read", { uri: next }),
        ) as {
          basis_tx: number;
          event: string;
          event_hash: string;
          offset: number;
          encoding: string;
          data: string;
          next_uri?: string;
        };
        expect(page).toMatchObject({
          basis_tx: later.tx,
          event: oversized.event,
          event_hash: changes.oversized_event.event_hash,
          encoding: "base64",
        });
        chunks.push(Buffer.from(page.data, "base64"));
        next = page.next_uri;
      }
      expect(Buffer.concat(chunks).toString("utf8")).toBe(expected);
      expect(
        resourceData(
          await client.request("resources/read", { uri: changes.next_uri }),
        ),
      ).toMatchObject({
        events: [expect.objectContaining({ event: later.event })],
      });

      const invalidChunk = new URL(changes.oversized_event.uri);
      invalidChunk.searchParams.set("offset", "-1");
      await expect(
        client.request("resources/read", { uri: invalidChunk.href }),
      ).rejects.toThrow("MCP RPC error");
      invalidChunk.searchParams.set("offset", "0");
      invalidChunk.searchParams.set("digest", "0".repeat(64));
      await expect(
        client.request("resources/read", { uri: invalidChunk.href }),
      ).rejects.toThrow("MCP RPC error");
      const laterIdentity = db._connection
        .prepare<[], { id: bigint }>(
          "SELECT id FROM fgraph_ids WHERE name='change/later'",
        )
        .get();
      expect(laterIdentity).toBeDefined();
      invalidChunk.searchParams.set(
        "digest",
        changes.oversized_event.event_hash,
      );
      invalidChunk.searchParams.set("basis", String(laterIdentity?.id));
      await expect(
        client.request("resources/read", { uri: invalidChunk.href }),
      ).rejects.toThrow("MCP RPC error");
    } finally {
      await server.close();
      await client.close();
    }
  });

  it("rolls back every database row when an embedded remember fails", async () => {
    const directory = temporaryDirectory();
    const embedder = join(directory, "atomic-embedder.mjs");
    writeFileSync(embedder, 'process.stdout.write("[1.0,2.5]")\n');
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const beforeStats = db.stats();
    const beforeRows = coreRows(db);
    const server = createMcpServer(db, {
      write: true,
      embedCommand: JSON.stringify([process.execPath, embedder]),
    });
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    const client = new TestClient(clientTransport);
    await client.start();
    await server.connect(serverTransport);
    await client.initialize();
    try {
      expect(
        await client.request("tools/call", {
          name: "remember",
          arguments: {
            facts: { id: "invalid/fact", "": true },
            text: "this transaction must fail atomically",
            operation_id: "invalid-atomic-memory",
          },
        }),
      ).toMatchObject({ isError: true });
      expect(db.stats()).toEqual(beforeStats);
      expect(coreRows(db)).toEqual(beforeRows);
    } finally {
      await server.close();
      await client.close();
    }
  });

  it("preserves max-int64 tokens across raw stdio JSON and rejects unsafe numeric request ids", async () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const highEntity = 9_007_199_254_740_993n;
    const highTransaction = 9_007_199_254_741_000n;
    db._connection
      .prepare(
        "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,NULL,64)",
      )
      .run(highEntity, "raw/high-entity");
    const inserted = db._connection
      .prepare(
        "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,2,?,4,64,NULL)",
      )
      .run(highEntity, "raw identity");
    db._connection
      .prepare("INSERT INTO fgraph_fts(rowid,text) VALUES (?,?)")
      .run(inserted.lastInsertRowid, "raw identity");
    db._connection
      .prepare("UPDATE fgraph_meta SET value=? WHERE key='next_id'")
      .run(highTransaction);
    const highReceipt = db.transact([
      ["assert", highEntity, "fgraph/source", "high transaction"],
    ]);
    expect(highReceipt.tx).toBe(highTransaction);
    const input = new PassThrough();
    const output = new PassThrough();
    const transport = new LosslessStdioTransport(input, output);
    const handle = serveStdio(() => createMcpServer(db, { write: true }), {
      transport,
    });
    let buffered = "";
    const responses = new Map<
      number,
      (value: Record<string, unknown>) => void
    >();
    output.on("data", (chunk: Buffer) => {
      buffered += chunk.toString("utf8");
      while (buffered.includes("\n")) {
        const newline = buffered.indexOf("\n");
        const line = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        const message = parseJson(line, "raw MCP response") as Record<
          string,
          unknown
        >;
        const resolveResponse =
          typeof message.id === "number"
            ? responses.get(message.id)
            : undefined;
        if (resolveResponse !== undefined) {
          responses.delete(message.id as number);
          resolveResponse(message);
        }
      }
    });
    const request = async (
      id: number,
      method: string,
      params: Record<string, unknown>,
    ): Promise<Record<string, unknown>> => {
      const response = new Promise<Record<string, unknown>>((resolveResponse) =>
        responses.set(id, resolveResponse),
      );
      input.write(`${stringifyJson({ jsonrpc: "2.0", id, method, params })}\n`);
      return response;
    };
    try {
      await request(1, "initialize", {
        protocolVersion: LATEST_PROTOCOL_VERSION,
        capabilities: {},
        clientInfo: { name: "raw-int64", version: "1.0.0" },
      });
      input.write('{"jsonrpc":"2.0","method":"notifications/initialized"}\n');
      const listed = await request(2, "tools/list", {});
      const tools = (listed.result as { tools: Array<{ name: string }> }).tools;
      expect(tools.map((tool) => tool.name).sort()).toEqual([
        "about",
        "datoms",
        "explain",
        "forget",
        "history",
        "query",
        "recall",
        "receipt",
        "remember",
        "schema",
        "undo",
        "why",
      ]);
      const raw = await request(3, "tools/call", {
        name: "remember",
        arguments: {
          facts: {
            id: "number/max",
            "number/value": 9_223_372_036_854_775_807n,
          },
          operation_id: "raw-number-max",
        },
      });
      expect(content(raw.result as Record<string, unknown>)).toMatchObject({
        tx: expect.anything(),
      });
      expect(db.entity("number/max")["number/value"]).toBe(
        9_223_372_036_854_775_807n,
      );
      for (const [id, name, argumentsValue] of [
        [4, "about", { entity: highEntity }],
        [5, "why", { entity: highEntity }],
        [6, "history", { entity: highEntity }],
      ] as const) {
        const response = await request(id, "tools/call", {
          name,
          arguments: argumentsValue,
        });
        expect(response).not.toHaveProperty("error");
        expect(response.result).not.toMatchObject({ isError: true });
      }
      const undone = await request(7, "tools/call", {
        name: "undo",
        arguments: {
          tx: highTransaction,
          operation_id: "raw-undo-high",
          if_basis_tx: db._basisTx(),
        },
      });
      expect(undone.result).not.toMatchObject({ isError: true });
      const forgotten = await request(8, "tools/call", {
        name: "forget",
        arguments: {
          entity: highEntity,
          attribute: "fgraph/by",
          value: "raw identity",
          operation_id: "raw-forget-high",
          if_basis_tx: db._basisTx(),
        },
      });
      expect(forgotten.result).not.toMatchObject({ isError: true });
    } finally {
      await handle.close();
    }

    const badInput = new PassThrough();
    const badOutput = new PassThrough();
    const badTransport = new LosslessStdioTransport(badInput, badOutput);
    const errors: Error[] = [];
    badTransport.onerror = (error) => errors.push(error);
    await badTransport.start();
    await expect(badTransport.start()).rejects.toThrow("already started");
    badInput.write(
      '{"jsonrpc":"2.0","id":9007199254740993,"method":"ping"}\r\n',
    );
    badInput.write(Buffer.from([0xff, 0x0a]));
    badInput.write('[]\n{"jsonrpc":"1.0","method":"ping"}\n');
    await new Promise((resolvePromise) => setImmediate(resolvePromise));
    badInput.write(Buffer.alloc(10 * 1024 * 1024 + 1, 0x20));
    await new Promise((resolvePromise) => setImmediate(resolvePromise));
    expect(errors.map((error) => error.message)).toEqual(
      expect.arrayContaining([
        expect.stringContaining("safe integer"),
        expect.stringContaining("encoded data"),
        expect.stringContaining("JSON-RPC 2.0"),
        expect.stringContaining("exceeds"),
      ]),
    );
    await badTransport.close();
    await expect(
      badTransport.send({ jsonrpc: "2.0", id: 1, method: "ping" }),
    ).rejects.toThrow("closed");

    const textInput = new PassThrough();
    textInput.setEncoding("utf8");
    const backpressuredOutput = new PassThrough({ highWaterMark: 1 });
    backpressuredOutput.resume();
    const textTransport = new LosslessStdioTransport(
      textInput,
      backpressuredOutput,
    );
    let delivered = false;
    textTransport.onmessage = () => {
      delivered = true;
    };
    await textTransport.start();
    textInput.write('{"jsonrpc":"2.0","method":"ping"}\n');
    await new Promise((resolvePromise) => setImmediate(resolvePromise));
    expect(delivered).toBe(true);
    await textTransport.send({ jsonrpc: "2.0", id: 1, method: "ping" });
    await textTransport.send({
      jsonrpc: "2.0",
      id: 2,
      result: { value: new JsonFloat(1.5) },
    });
    for (const value of [
      2n ** 63n,
      new JsonFloat(Number.POSITIVE_INFINITY),
      Number.POSITIVE_INFINITY,
      "\ud800",
    ])
      await expect(
        textTransport.send({ jsonrpc: "2.0", id: 3, result: { value } }),
      ).rejects.toThrow(TypeError);
    await expect(
      textTransport.send(undefined as unknown as JSONRPCMessage),
    ).rejects.toThrow("JSON-RPC object");
    const extraListener = (): void => {};
    textInput.on("data", extraListener);
    await textTransport.close();
    textInput.off("data", extraListener);

    const throwingInput = new PassThrough();
    const throwingOutput = new PassThrough();
    const throwingTransport = new LosslessStdioTransport(
      throwingInput,
      throwingOutput,
    );
    const wrappedErrors: Error[] = [];
    let errorCalls = 0;
    throwingTransport.onmessage = () => {
      throw "non-Error message failure";
    };
    throwingTransport.onerror = (error) => {
      errorCalls++;
      if (errorCalls === 1) throw "non-Error handler failure";
      wrappedErrors.push(error);
    };
    await throwingTransport.start();
    throwingInput.write('{"jsonrpc":"2.0","method":"ping"}\n');
    await new Promise((resolvePromise) => setImmediate(resolvePromise));
    expect(wrappedErrors[0]?.message).toContain("handler failure");
    await throwingTransport.close();
  });
});

describe("external embedding command", () => {
  it("executes a shell-free executable or JSON argv and validates its output", () => {
    const directory = temporaryDirectory();
    const valid = join(directory, "valid.mjs");
    const invalid = join(directory, "invalid.mjs");
    const failed = join(directory, "failed.mjs");
    const nonnumeric = join(directory, "nonnumeric.mjs");
    const huge = join(directory, "huge.mjs");
    const largeInteger = join(directory, "large-integer.mjs");
    writeFileSync(
      valid,
      'process.stdin.resume(); process.stdin.on("end",()=>process.stdout.write("[0.5,-1.0]"));\n',
    );
    writeFileSync(invalid, 'process.stdout.write("{}")\n');
    writeFileSync(failed, "process.exitCode = 7\n");
    writeFileSync(nonnumeric, 'process.stdout.write("[true]")\n');
    writeFileSync(huge, 'process.stdout.write("["+"0,".repeat(524287)+"0]")\n');
    writeFileSync(largeInteger, 'process.stdout.write("[9007199254740993]")\n');
    expect(embed(JSON.stringify([process.execPath, valid]), "query")).toEqual([
      0.5, -1,
    ]);
    expect(
      embed(JSON.stringify([process.execPath, largeInteger]), "query"),
    ).toEqual([9_007_199_254_740_992]);
    expect(() => embed(" ", "query")).toThrowError(TypeError);
    expect(() => embed("[]", "query")).toThrowError(TypeError);
    expect(() => embed(join(directory, "missing"), "query")).toThrowError(
      TypeError,
    );
    expect(() =>
      embed(JSON.stringify([process.execPath, failed]), "query"),
    ).toThrowError(TypeError);
    expect(() =>
      embed(JSON.stringify([process.execPath, invalid]), "query"),
    ).toThrowError(TypeError);
    expect(() =>
      embed(JSON.stringify([process.execPath, nonnumeric]), "query"),
    ).toThrowError(TypeError);
    expect(() =>
      embed(JSON.stringify([process.execPath, huge]), "query"),
    ).toThrowError(TypeError);
  });
});
