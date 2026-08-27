import { spawnSync } from "node:child_process";
import type { Readable, Writable } from "node:stream";

import {
  McpServer,
  ResourceTemplate,
  type JSONRPCMessage,
  type Transport,
} from "@modelcontextprotocol/server";
import {
  serveStdio,
  type StdioServerHandle,
} from "@modelcontextprotocol/server/stdio";
import { z } from "zod";

import { Conflict, ReadOnly, TooLarge, TypeError } from "./errors.js";
import { JsonFloat, parseJson, stringifyJson } from "./jsonio.js";
import type { TxReport } from "./models.js";
import type { Db, EntityRef } from "./store.js";
import { MAX_VALUE_BYTES, isRecord } from "./values.js";

const MAX_STDIO_MESSAGE_BYTES = 2 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 256 * 1024;
const JSON_WITH_RAW = JSON as typeof JSON & {
  rawJSON(text: string): unknown;
};

function stringifyMcpMessage(message: JSONRPCMessage): string {
  const rendered = JSON.stringify(message, (_key, value: unknown) => {
    if (typeof value === "bigint") {
      if (value < -(2n ** 63n) || value > 2n ** 63n - 1n)
        throw new TypeError("MCP JSON integer exceeds signed 64-bit range");
      return JSON_WITH_RAW.rawJSON(value.toString());
    }
    if (value instanceof JsonFloat) {
      if (!Number.isFinite(value.value))
        throw new TypeError("MCP JSON number must be finite");
      return value.value;
    }
    if (typeof value === "number" && !Number.isFinite(value))
      throw new TypeError("MCP JSON number must be finite");
    if (typeof value === "string" && !value.isWellFormed())
      throw new TypeError("MCP JSON strings must be valid UTF-8");
    return value;
  });
  if (rendered === undefined)
    throw new TypeError("MCP message must be a JSON-RPC object");
  return rendered;
}

/** NDJSON MCP transport whose external JSON boundary preserves signed int64 tokens. */
export class LosslessStdioTransport implements Transport {
  readonly #input: Readable;
  readonly #output: Writable;
  #buffer = Buffer.alloc(0);
  #started = false;
  #closed = false;
  onclose: (() => void) | undefined;
  onerror: ((error: Error) => void) | undefined;
  onmessage: ((message: JSONRPCMessage) => void) | undefined;

  constructor(
    input: Readable = process.stdin,
    output: Writable = process.stdout,
  ) {
    this.#input = input;
    this.#output = output;
  }

  readonly #onData = (chunk: Buffer | string): void => {
    try {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      if (this.#buffer.length + bytes.length > MAX_STDIO_MESSAGE_BYTES)
        throw new TypeError(
          `MCP stdio message exceeds ${MAX_STDIO_MESSAGE_BYTES} bytes; send a smaller request`,
        );
      this.#buffer = Buffer.concat([this.#buffer, bytes]);
      this.#process();
    } catch (error) {
      this.onerror?.(error instanceof Error ? error : new Error(String(error)));
      void this.close();
    }
  };

  readonly #onError = (error: Error): void => {
    this.onerror?.(error);
  };

  #process(): void {
    while (true) {
      const newline = this.#buffer.indexOf(0x0a);
      if (newline < 0) return;
      let line = this.#buffer.subarray(0, newline);
      this.#buffer = this.#buffer.subarray(newline + 1);
      if (line.at(-1) === 0x0d) line = line.subarray(0, -1);
      try {
        const text = new TextDecoder("utf-8", { fatal: true }).decode(line);
        const parsed = parseJson(text, "MCP stdio message");
        if (!isRecord(parsed) || parsed.jsonrpc !== "2.0")
          throw new TypeError(
            "MCP stdio message must be a JSON-RPC 2.0 object",
          );
        if (typeof parsed.id === "bigint")
          throw new TypeError(
            "MCP request id exceeds JavaScript's safe integer range; use a string id",
          );
        this.onmessage?.(parsed as unknown as JSONRPCMessage);
      } catch (error) {
        this.onerror?.(
          error instanceof Error ? error : new Error(String(error)),
        );
      }
    }
  }

  async start(): Promise<void> {
    if (this.#started)
      throw new Error("LosslessStdioTransport already started");
    this.#started = true;
    this.#input.on("data", this.#onData);
    this.#input.on("error", this.#onError);
    this.#output.on("error", this.#onError);
  }

  async send(message: JSONRPCMessage): Promise<void> {
    if (this.#closed) throw new Error("LosslessStdioTransport is closed");
    // MCP SDK response objects use ordinary JSON.stringify semantics for
    // optional fields, while database values still need lossless int64 tokens.
    const output = `${stringifyMcpMessage(message)}\n`;
    await new Promise<void>((resolvePromise, rejectPromise) => {
      const onError = (error: Error): void => {
        this.#output.off("drain", onDrain);
        rejectPromise(error);
      };
      const onDrain = (): void => {
        this.#output.off("error", onError);
        resolvePromise();
      };
      this.#output.once("error", onError);
      if (this.#output.write(output)) {
        this.#output.off("error", onError);
        resolvePromise();
      } else this.#output.once("drain", onDrain);
    });
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#input.off("data", this.#onData);
    this.#input.off("error", this.#onError);
    this.#output.off("error", this.#onError);
    if (this.#input.listenerCount("data") === 0) this.#input.pause();
    this.#buffer = Buffer.alloc(0);
    this.onclose?.();
  }
}

function toolResult(
  value: unknown,
  basis: number | bigint,
): {
  content: [{ type: "text"; text: string }];
  structuredContent: Record<string, unknown>;
} {
  const envelope = { ok: true, basis_tx: basis, data: value };
  const text = stringifyJson(envelope);
  if (Buffer.byteLength(text, "utf8") > MAX_RESPONSE_BYTES)
    throw new TooLarge(
      "MCP response exceeds 256 KiB; narrow the request or continue with a cursor",
    );
  return {
    content: [{ type: "text", text }],
    structuredContent: envelope,
  };
}

function reportBasis(report: TxReport): number | bigint {
  return report.tx ?? report.basis_tx;
}

const signedInt64 = z.custom<bigint>(
  (value) =>
    typeof value === "bigint" &&
    value >= -(2n ** 63n) &&
    value <= 2n ** 63n - 1n,
  "expected a signed 64-bit integer",
);
const losslessInteger = z.union([
  z.number().int().min(Number.MIN_SAFE_INTEGER).max(Number.MAX_SAFE_INTEGER),
  signedInt64,
]);
const losslessEntity = z.union([z.string(), losslessInteger]);
const advertisedInteger = z.number().int();
const advertisedEntity = z.union([z.string(), advertisedInteger]);

/** Keep runtime bigint validation while advertising the JSON integer on the wire. */
function advertiseAs<Runtime extends z.ZodType>(
  runtime: Runtime,
  advertised: z.ZodType,
): Runtime {
  const standard = runtime["~standard"];
  const advertisedStandard = advertised["~standard"];
  Object.defineProperty(runtime, "~standard", {
    configurable: true,
    value: { ...standard, jsonSchema: advertisedStandard.jsonSchema },
  });
  return runtime;
}

function entityRef(value: string | number | bigint): EntityRef {
  return value;
}

export function embed(command: string, text: string): number[] {
  const stripped = command.trim();
  if (stripped === "")
    throw new TypeError(
      "embed command is empty; provide an executable that reads text and returns a JSON vector",
    );
  let executable: string;
  let parameters: string[];
  if (stripped.startsWith("[")) {
    const parsed = parseJson(stripped, "embed command argv");
    if (
      !Array.isArray(parsed) ||
      parsed.length === 0 ||
      parsed.some((item) => typeof item !== "string" || item.length === 0)
    ) {
      throw new TypeError(
        "embed command argv must be a non-empty JSON array of non-empty strings",
      );
    }
    const argumentsList = parsed as string[];
    executable = argumentsList[0] as string;
    parameters = argumentsList.slice(1);
  } else {
    executable = stripped;
    parameters = [];
  }
  const completed = spawnSync(executable, parameters, {
    input: text,
    encoding: "utf8",
    maxBuffer: MAX_VALUE_BYTES + 1,
    shell: false,
    timeout: 60_000,
  });
  if (completed.error !== undefined) {
    if ((completed.error as NodeJS.ErrnoException).code === "ETIMEDOUT")
      throw new TypeError(
        "embed command timed out after 60 seconds; use a bounded local embedder",
      );
    throw new TypeError(
      `embed command executable ${JSON.stringify(executable)} could not be started; check its path and permissions`,
    );
  }
  if (completed.status !== 0)
    throw new TypeError(
      `embed command exited ${String(completed.status)}; make it read stdin and emit one JSON float array on stdout`,
    );
  if (Buffer.byteLength(completed.stdout) > MAX_VALUE_BYTES)
    throw new TypeError(
      "embed command output exceeds 1 MiB; emit one compact embedding vector",
    );
  const value = parseJson(completed.stdout, "embed command output");
  if (!Array.isArray(value) || value.length === 0)
    throw new TypeError(
      "embed command output is not a non-empty JSON array; emit float values such as [0.1,-0.2]",
    );
  const result = value.map((item) =>
    item instanceof JsonFloat
      ? item.value
      : typeof item === "bigint"
        ? Number(item)
        : item,
  );
  if (result.some((item) => typeof item !== "number" || !Number.isFinite(item)))
    throw new TypeError(
      "embed command output contains a non-number; emit only finite float values",
    );
  return result as number[];
}

export interface McpOptions {
  write?: boolean;
  embedCommand?: string;
}

export function createMcpServer(db: Db, options: McpOptions = {}): McpServer {
  const writable = options.write === true;
  const server = new McpServer(
    { name: "fgraph", version: "1.0.2" },
    {
      capabilities: { tools: {}, resources: {} },
      instructions:
        "Use fgraph as an auditable temporal fact store. Discover schema first, prefer bounded query/datoms pages, preserve returned basis_tx for follow-up reads, and supply stable operation_id plus if_basis_tx for retry-safe writes. The server is read-only unless explicitly started with write access.",
    },
  );

  const outputSchema = advertiseAs(
    z.object({
      ok: z.literal(true),
      basis_tx: losslessInteger,
      data: z.unknown(),
    }),
    z.object({
      ok: z.literal(true),
      basis_tx: advertisedInteger,
      data: z.unknown(),
    }),
  );
  const readAnnotations = {
    readOnlyHint: true,
    destructiveHint: false,
    idempotentHint: true,
    openWorldHint: false,
  } as const;
  const pinnedView = (): { view: Db; basis: number | bigint } => {
    const basis = db._basisTx();
    return { view: db.at(basis), basis };
  };
  const requestClient = (context: {
    mcpReq: { envelope?: unknown };
  }): string => {
    const legacyClient = server.server.getClientVersion()?.name;
    const envelope = context.mcpReq.envelope as
      Record<string, unknown> | undefined;
    const modernInfo = envelope?.["io.modelcontextprotocol/clientInfo"];
    const modernClient =
      modernInfo !== null && typeof modernInfo === "object"
        ? (modernInfo as Record<string, unknown>).name
        : undefined;
    return typeof modernClient === "string"
      ? modernClient
      : (legacyClient ?? "unknown");
  };

  if (writable) {
    server.registerTool(
      "remember",
      {
        description:
          "Remember structured facts and/or a searchable text note with provenance. A key updates one stable note while retaining history. Example: remember({key:'preference/language', text:'User prefers Go', source:'conversation:42'}).",
        inputSchema: advertiseAs(
          z.object({
            facts: z.unknown().optional(),
            text: z.string().optional(),
            source: z.string().optional(),
            key: z.string().optional(),
            operation_id: z.string().min(1).max(512),
            if_basis_tx: losslessInteger.optional(),
          }),
          z.object({
            facts: z.unknown().optional(),
            text: z.string().optional(),
            source: z.string().optional(),
            key: z.string().optional(),
            operation_id: z.string().min(1).max(512),
            if_basis_tx: advertisedInteger.optional(),
          }),
        ),
        outputSchema,
        annotations: {
          readOnlyHint: false,
          destructiveHint: false,
          idempotentHint: true,
          openWorldHint: false,
        },
      },
      async (
        { facts, text, source, key, operation_id, if_basis_tx },
        context,
      ) => {
        const hasFacts =
          facts !== undefined && !(Array.isArray(facts) && facts.length === 0);
        const hasText = text !== undefined && text.trim() !== "";
        if (key !== undefined && !hasText)
          throw new TypeError(
            "remember key requires text; provide a non-blank text note for the stable key",
          );
        if (key === "")
          throw new TypeError(
            "remember key is empty; provide a stable non-empty entity name",
          );
        if (!hasFacts && !hasText)
          throw new TypeError(
            "remember needs facts or text; provide at least one memory payload",
          );
        const data: unknown[] = [];
        if (hasFacts) {
          if (
            Array.isArray(facts) &&
            !(
              typeof facts[0] === "string" &&
              (facts[0] === "assert" || facts[0] === "retract")
            )
          )
            data.push(...facts);
          else data.push(facts);
        }
        if (hasText) {
          const note: Record<string, unknown> = { "memory/text": text };
          if (key !== undefined) note.id = key;
          if (options.embedCommand !== undefined) {
            note["memory/embedding"] = {
              vector: embed(options.embedCommand, text as string),
            };
          }
          data.push(note);
        }
        const report = db.transact(data, {
          ...(source === undefined ? {} : { source }),
          by: `mcp:${requestClient(context)}`,
          operationId: operation_id,
          ...(if_basis_tx === undefined ? {} : { ifBasisTx: if_basis_tx }),
        });
        return toolResult(report, reportBasis(report));
      },
    );

    server.registerTool(
      "forget",
      {
        description:
          "Retract a fact, attribute, or whole entity while preserving history. Example: forget({entity:'user', attribute:'user/editor', value:'vim'}).",
        inputSchema: advertiseAs(
          z.object({
            entity: losslessEntity,
            attribute: z.string().min(1).optional(),
            value: z.unknown().optional(),
            operation_id: z.string().min(1).max(512),
            if_basis_tx: losslessInteger,
          }),
          z.object({
            entity: advertisedEntity,
            attribute: z.string().min(1).optional(),
            value: z.unknown().optional(),
            operation_id: z.string().min(1).max(512),
            if_basis_tx: advertisedInteger,
          }),
        ),
        outputSchema,
        annotations: {
          readOnlyHint: false,
          destructiveHint: true,
          idempotentHint: true,
          openWorldHint: false,
        },
      },
      async (
        { entity, attribute, value, operation_id, if_basis_tx },
        context,
      ) => {
        const report = db.transact(
          value === undefined
            ? attribute === undefined
              ? ["retract", entityRef(entity)]
              : ["retract", entityRef(entity), attribute]
            : ["retract", entityRef(entity), attribute, value],
          {
            by: `mcp:${requestClient(context)}`,
            operationId: operation_id,
            ifBasisTx: if_basis_tx,
          },
        );
        return toolResult(report, reportBasis(report));
      },
    );

    server.registerTool(
      "undo",
      {
        description:
          "Undo a transaction by audited compensation. Example: undo({tx:70}) restores what transaction 70 changed.",
        inputSchema: advertiseAs(
          z.object({
            tx: losslessInteger,
            operation_id: z.string().min(1).max(512),
            if_basis_tx: losslessInteger,
          }),
          z.object({
            tx: advertisedInteger,
            operation_id: z.string().min(1).max(512),
            if_basis_tx: advertisedInteger,
          }),
        ),
        outputSchema,
        annotations: {
          readOnlyHint: false,
          destructiveHint: true,
          idempotentHint: true,
          openWorldHint: false,
        },
      },
      async ({ tx, operation_id, if_basis_tx }, context) => {
        const report = db.undo(tx, {
          by: `mcp:${requestClient(context)}`,
          operationId: operation_id,
          ifBasisTx: if_basis_tx,
        });
        return toolResult(report, reportBasis(report));
      },
    );
  }

  server.registerTool(
    "recall",
    {
      description:
        "Recall ranked text memories. Example: recall({query:'preferred editor', k:5, expand:1}).",
      inputSchema: z.object({
        query: z.string(),
        k: z.number().int().min(1).max(20).optional(),
        expand: z.number().int().min(0).max(2).optional(),
      }),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ query, k, expand }) => {
      if (query.trim() === "")
        throw new TypeError("recall query is blank; provide text to search");
      const vector =
        options.embedCommand === undefined
          ? undefined
          : embed(options.embedCommand, query);
      const searchOptions: {
        text: string;
        vector?: number[];
        k?: number;
        expand?: number;
        vectorAttribute?: string;
      } = { text: query };
      if (vector !== undefined) {
        searchOptions.vector = vector;
        searchOptions.vectorAttribute = "memory/embedding";
      }
      if (k !== undefined) searchOptions.k = k;
      if (expand !== undefined) searchOptions.expand = expand;
      const result = db.search(searchOptions);
      return toolResult(result, result.basis_tx);
    },
  );

  server.registerTool(
    "about",
    {
      description:
        "Pull current facts about an entity. Example: about({entity:'ada', depth:2}).",
      inputSchema: advertiseAs(
        z.object({
          entity: losslessEntity,
          depth: z.number().int().min(0).max(2).optional(),
        }),
        z.object({
          entity: advertisedEntity,
          depth: z.number().int().min(0).max(2).optional(),
        }),
      ),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ entity, depth }) => {
      const { view, basis } = pinnedView();
      return toolResult(view.entity(entityRef(entity), depth), basis);
    },
  );

  server.registerTool(
    "why",
    {
      description:
        "Explain current facts with full provenance. Example: why({entity:'ada', attribute:'person/city'}).",
      inputSchema: advertiseAs(
        z.object({
          entity: losslessEntity,
          attribute: z.string().min(1).optional(),
          limit: z.number().int().min(1).max(100).optional(),
        }),
        z.object({
          entity: advertisedEntity,
          attribute: z.string().min(1).optional(),
          limit: z.number().int().min(1).max(100).optional(),
        }),
      ),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ entity, attribute, limit }) => {
      const { view, basis } = pinnedView();
      const ref = entityRef(entity);
      const rows =
        attribute === undefined ? view.why(ref) : view.why(ref, attribute);
      const pageSize = limit ?? 100;
      return toolResult(
        {
          items: rows.slice(0, pageSize),
          truncated: rows.length > pageSize,
        },
        basis,
      );
    },
  );

  server.registerTool(
    "history",
    {
      description:
        "Read a fact timeline. Example: history({entity:'ada', attribute:'person/city'}).",
      inputSchema: advertiseAs(
        z.object({
          entity: losslessEntity,
          attribute: z.string().min(1).optional(),
          limit: z.number().int().min(1).max(100).optional(),
        }),
        z.object({
          entity: advertisedEntity,
          attribute: z.string().min(1).optional(),
          limit: z.number().int().min(1).max(100).optional(),
        }),
      ),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ entity, attribute, limit }) => {
      const { view, basis } = pinnedView();
      const ref = entityRef(entity);
      const rows =
        attribute === undefined
          ? view.history(ref)
          : view.history(ref, attribute);
      const pageSize = limit ?? 100;
      return toolResult(
        {
          items: rows.slice(0, pageSize),
          truncated: rows.length > pageSize,
        },
        basis,
      );
    },
  );

  server.registerTool(
    "query",
    {
      description:
        "Run canonical JSON Datalog. Example: query({q:{find:['?n'],where:[['?e','person/name','?n']]}}).",
      inputSchema: z.object({
        q: z.record(z.string(), z.unknown()),
        args: z.record(z.string(), z.unknown()).optional(),
      }),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ q, args }, context) => {
      const requestedLimit = q.limit ?? 100;
      if (
        typeof requestedLimit !== "number" ||
        !Number.isSafeInteger(requestedLimit) ||
        requestedLimit < 0 ||
        requestedLimit > 1000
      )
        throw new TypeError(
          "MCP query limit must be an integer from zero through 1000",
        );
      const bounded = { ...q, limit: requestedLimit };
      const { view, basis } = pinnedView();
      return toolResult(
        view.q(bounded, args, { signal: context.mcpReq.signal }),
        basis,
      );
    },
  );

  server.registerTool(
    "datoms",
    {
      description:
        "Page the EAVT, AVET, or VAET datom index at a stable basis. Example: datoms({index:'avet', components:['person/name'], limit:50}).",
      inputSchema: z.object({
        index: z.enum(["eavt", "avet", "vaet"]).optional(),
        source: z.enum(["current", "history"]).optional(),
        components: z.array(z.unknown()).max(5).optional(),
        limit: z.number().int().min(1).max(100).optional(),
        cursor: z.string().min(1).max(4096).optional(),
      }),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ index, source, components, limit, cursor }) => {
      const page = db.datoms(index ?? "eavt", {
        ...(source === undefined ? {} : { source }),
        ...(components === undefined ? {} : { components }),
        ...(limit === undefined ? {} : { limit }),
        ...(cursor === undefined ? {} : { cursor }),
      });
      return toolResult(page, page.basis_tx);
    },
  );

  server.registerTool(
    "receipt",
    {
      description:
        "Read a stable transaction and operation receipt. Example: receipt({tx:70}).",
      inputSchema: advertiseAs(
        z.object({ tx: losslessInteger }),
        z.object({ tx: advertisedInteger }),
      ),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ tx }) => {
      const { view, basis } = pinnedView();
      return toolResult(view.receipt(tx), basis);
    },
  );

  server.registerTool(
    "explain",
    {
      description:
        "Explain the deterministic indexed plan for canonical JSON Datalog without evaluating it. Example: explain({q:{find:['?n'],where:[['?e','person/name','?n']]}}).",
      inputSchema: z.object({
        q: z.record(z.string(), z.unknown()),
        args: z.record(z.string(), z.unknown()).optional(),
      }),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ q, args }) => {
      const { view, basis } = pinnedView();
      return toolResult(view.explain(q, args), basis);
    },
  );

  server.registerTool(
    "schema",
    {
      description:
        "Discover known attributes, observed types, and effective schema behavior. Example: schema({prefix:'person/', include_system:false}).",
      inputSchema: z.object({
        prefix: z.string().optional(),
        include_system: z.boolean().optional(),
        limit: z.number().int().min(1).max(100).optional(),
        cursor: z.string().min(1).max(4096).optional(),
      }),
      outputSchema,
      annotations: readAnnotations,
    },
    async ({ prefix, include_system, limit, cursor }) => {
      const pageSize = limit ?? 100;
      let basis = db._basisTx();
      let offset = 0;
      let effectivePrefix = prefix;
      let includeSystem = include_system ?? false;
      let expectedDigest: string | null = null;
      if (cursor !== undefined) {
        if (!/^[A-Za-z0-9_-]+$/u.test(cursor))
          throw new TypeError("schema cursor is malformed; restart pagination");
        const bytes = Buffer.from(cursor, "base64url");
        if (bytes.toString("base64url") !== cursor)
          throw new TypeError("schema cursor is malformed; restart pagination");
        const decoded = parseJson(bytes.toString("utf8"), "schema cursor");
        if (
          !isRecord(decoded) ||
          (decoded.v !== 1 && decoded.v !== 1n) ||
          (typeof decoded.basis !== "number" &&
            typeof decoded.basis !== "bigint") ||
          (typeof decoded.offset !== "number" &&
            typeof decoded.offset !== "bigint") ||
          BigInt(decoded.offset) < 0n ||
          BigInt(decoded.offset) > BigInt(Number.MAX_SAFE_INTEGER) ||
          (decoded.prefix !== null && typeof decoded.prefix !== "string") ||
          typeof decoded.include_system !== "boolean" ||
          typeof decoded.digest !== "string"
        )
          throw new TypeError("schema cursor is invalid; restart pagination");
        if (
          (prefix !== undefined && prefix !== decoded.prefix) ||
          (include_system !== undefined &&
            include_system !== decoded.include_system)
        )
          throw new TypeError(
            "schema cursor does not match prefix/include_system; restart pagination",
          );
        basis = decoded.basis;
        offset = Number(decoded.offset);
        effectivePrefix = decoded.prefix ?? undefined;
        includeSystem = decoded.include_system;
        expectedDigest = decoded.digest;
      }
      const target = cursor === undefined ? db : db.at(basis);
      const snapshot = target.schema(effectivePrefix, {
        includeSystem,
      });
      if (expectedDigest !== null && snapshot.digest !== expectedDigest)
        throw new TypeError(
          "schema cursor digest no longer matches its basis; restart pagination",
        );
      const total = snapshot.attributes.length + snapshot.shapes.length;
      if (offset > total)
        throw new TypeError(
          "schema cursor is outside this snapshot; restart pagination",
        );
      let remaining = pageSize;
      const attributeOffset = Math.min(offset, snapshot.attributes.length);
      const attributes = snapshot.attributes.slice(
        attributeOffset,
        attributeOffset + remaining,
      );
      remaining -= attributes.length;
      const shapeOffset = Math.max(0, offset - snapshot.attributes.length);
      const shapes = snapshot.shapes.slice(
        shapeOffset,
        shapeOffset + remaining,
      );
      const nextOffset = offset + attributes.length + shapes.length;
      const nextCursor =
        nextOffset < total
          ? Buffer.from(
              stringifyJson({
                v: 1,
                basis: snapshot.basis_tx,
                offset: nextOffset,
                prefix: effectivePrefix ?? null,
                include_system: includeSystem,
                digest: snapshot.digest,
              }),
              "utf8",
            ).toString("base64url")
          : null;
      return toolResult(
        {
          ...snapshot,
          attributes,
          shapes,
          next_cursor: nextCursor,
          truncated: nextCursor !== null,
        },
        snapshot.basis_tx,
      );
    },
  );

  const resource = (uri: URL, value: unknown) => {
    const text = stringifyJson(value);
    if (Buffer.byteLength(text, "utf8") > MAX_RESPONSE_BYTES)
      throw new TooLarge(
        "MCP resource exceeds 256 KiB; request a narrower page",
      );
    return {
      contents: [{ uri: uri.href, mimeType: "application/json", text }],
    };
  };
  const resourceVariable = (value: unknown): string => {
    try {
      return decodeURIComponent(String(value));
    } catch {
      throw new TypeError("MCP resource identifier is not valid URI encoding");
    }
  };
  const resourceEntity = (value: unknown): EntityRef => {
    const text = resourceVariable(value);
    if (/^-?[0-9]+$/u.test(text)) return BigInt(text);
    if (
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(
        text,
      )
    ) {
      const identity = db._connection
        .prepare<[Buffer], { id: bigint }>(
          "SELECT id FROM fgraph_ids WHERE gid=?",
        )
        .get(Buffer.from(text.replaceAll("-", ""), "hex"));
      if (identity !== undefined) return identity.id;
    }
    return text;
  };
  const encodeResourceCursor = (value: Record<string, unknown>): string =>
    Buffer.from(stringifyJson(value), "utf8").toString("base64url");
  const decodeResourceCursor = (
    raw: string,
    resourceName: string,
    argument: string,
  ): Record<string, unknown> => {
    if (raw.length > 4096)
      throw new TooLarge(
        `${resourceName} resource cursor is too large; restart without a cursor`,
      );
    if (!/^[A-Za-z0-9_-]+$/u.test(raw))
      throw new TypeError(
        `${resourceName} resource cursor is malformed; restart without a cursor`,
      );
    const bytes = Buffer.from(raw, "base64url");
    if (bytes.toString("base64url") !== raw)
      throw new TypeError(
        `${resourceName} resource cursor is malformed; restart without a cursor`,
      );
    const value = parseJson(
      bytes.toString("utf8"),
      `${resourceName} resource cursor`,
    );
    if (
      !isRecord(value) ||
      (value.v !== 1 && value.v !== 1n) ||
      value.resource !== resourceName ||
      value.argument !== argument ||
      (typeof value.basis !== "number" && typeof value.basis !== "bigint")
    )
      throw new Conflict(
        `${resourceName} resource cursor belongs to another request; restart without a cursor`,
      );
    return value;
  };
  const continuedUri = (uri: URL, cursor: string): string => {
    const next = new URL(uri.href);
    next.searchParams.set("cursor", cursor);
    return next.href;
  };
  server.registerResource(
    "schema",
    // The installed SDK requires every RFC 6570 query variable to be present.
    // A reserved suffix keeps prefix-only and prefix+cursor URIs routable.
    new ResourceTemplate("fgraph://schema{+query}", {
      list: undefined,
    }),
    {
      description: "Bounded, basis-pinned schema snapshot pages",
      mimeType: "application/json",
    },
    async (uri) => {
      const prefix = uri.searchParams.get("prefix") ?? undefined;
      const argument = prefix ?? "";
      const rawCursor = uri.searchParams.get("cursor");
      let offset = 0;
      let expectedDigest: string | undefined;
      let target = db;
      if (rawCursor !== null) {
        const cursor = decodeResourceCursor(rawCursor, "schema", argument);
        if (
          (typeof cursor.offset !== "number" &&
            typeof cursor.offset !== "bigint") ||
          BigInt(cursor.offset) < 0n ||
          BigInt(cursor.offset) > BigInt(Number.MAX_SAFE_INTEGER) ||
          typeof cursor.digest !== "string"
        )
          throw new TypeError(
            "schema resource cursor is invalid; restart without a cursor",
          );
        offset = Number(cursor.offset);
        expectedDigest = cursor.digest;
        target = db.at(cursor.basis);
      }
      const snapshot = target.schema(prefix);
      if (expectedDigest !== undefined && expectedDigest !== snapshot.digest)
        throw new Conflict(
          "schema resource cursor digest changed; restart without a cursor",
        );
      const total = snapshot.attributes.length + snapshot.shapes.length;
      if (offset > total)
        throw new Conflict(
          "schema resource cursor is outside this snapshot; restart without a cursor",
        );
      let remaining = 100;
      const attributeOffset = Math.min(offset, snapshot.attributes.length);
      const attributes = snapshot.attributes.slice(
        attributeOffset,
        attributeOffset + remaining,
      );
      remaining -= attributes.length;
      const shapeOffset = Math.max(0, offset - snapshot.attributes.length);
      const shapes = snapshot.shapes.slice(
        shapeOffset,
        shapeOffset + remaining,
      );
      const nextOffset = offset + attributes.length + shapes.length;
      const value: Record<string, unknown> = {
        basis_tx: snapshot.basis_tx,
        digest: snapshot.digest,
        attributes,
        shapes,
      };
      if (nextOffset < total)
        value.next_uri = continuedUri(
          uri,
          encodeResourceCursor({
            v: 1,
            resource: "schema",
            argument,
            basis: snapshot.basis_tx,
            offset: nextOffset,
            digest: snapshot.digest,
          }),
        );
      return resource(uri, value);
    },
  );
  server.registerResource(
    "entity",
    new ResourceTemplate("fgraph://entity/{+selector}", {
      list: undefined,
    }),
    {
      description: "Current entity datoms, bounded to 100 per cursor",
      mimeType: "application/json",
    },
    async (uri) => {
      const at = uri.searchParams.get("at");
      const target = at === null ? db : db.at(BigInt(at));
      const selector = uri.pathname.replace(/^\//u, "");
      const page = target.datoms("eavt", {
        components: [resourceEntity(selector)],
        limit: 100,
        ...(uri.searchParams.get("cursor") === null
          ? {}
          : { cursor: uri.searchParams.get("cursor") as string }),
      });
      const value: Record<string, unknown> = {
        basis_tx: page.basis_tx,
        items: page.items,
      };
      if (page.next_cursor !== null)
        value.next_uri = continuedUri(uri, page.next_cursor);
      return resource(uri, value);
    },
  );
  server.registerResource(
    "transaction",
    new ResourceTemplate("fgraph://tx/{tx}", { list: undefined }),
    {
      description: "One immutable transaction receipt with bounded evidence",
      mimeType: "application/json",
    },
    async (uri, variables) => {
      const { view } = pinnedView();
      const transaction = view._resolveRead(resourceEntity(variables.tx));
      const receipt = view.receipt(transaction as bigint);
      const facts = receipt.facts as unknown[];
      return resource(uri, {
        ...receipt,
        facts: facts.slice(0, 100),
        truncated: facts.length > 100,
      });
    },
  );
  server.registerResource(
    "changes",
    new ResourceTemplate("fgraph://changes{+query}", {
      list: undefined,
    }),
    {
      description: "Portable committed events after a pinned transaction",
      mimeType: "application/json",
    },
    async (uri) => {
      const sinceText = uri.searchParams.get("since") ?? "64";
      const visibleBasis = BigInt(db._basisTx());
      let basis = visibleBasis;
      let position: bigint;
      try {
        position = BigInt(sinceText);
      } catch {
        throw new TypeError(
          "changes since must be a transaction at or after genesis",
        );
      }
      const rawCursor = uri.searchParams.get("cursor");
      if (rawCursor !== null) {
        const cursor = decodeResourceCursor(rawCursor, "changes", sinceText);
        if (
          typeof cursor.position !== "number" &&
          typeof cursor.position !== "bigint"
        )
          throw new TypeError(
            "changes resource cursor is invalid; restart without a cursor",
          );
        basis = BigInt(cursor.basis as number | bigint);
        position = BigInt(cursor.position);
      }
      if (
        basis < 64n ||
        basis > visibleBasis ||
        position < 64n ||
        position > basis
      )
        throw new TypeError(
          "changes cursor basis or position is not visible; restart without a cursor",
        );
      const rows = db._connection
        .prepare<[bigint, bigint], { tx: bigint }>(
          "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx LIMIT 101",
        )
        .all(position, basis);
      const selected = rows.slice(0, 100);
      const end = selected.at(-1)?.tx;
      const events = end === undefined ? [] : db.eventRecords(position, end);
      const value: Record<string, unknown> = { basis_tx: basis, events };
      if (rows.length > 100 && end !== undefined)
        value.next_uri = continuedUri(
          uri,
          encodeResourceCursor({
            v: 1,
            resource: "changes",
            argument: sinceText,
            basis,
            position: end,
          }),
        );
      return resource(uri, value);
    },
  );

  return server;
}

export function runMcp(db: Db, options: McpOptions = {}): StdioServerHandle {
  if (options.write !== true && !db._isReadOnly())
    throw new ReadOnly(
      "MCP defaults to read-only and requires a read-only SQLite handle; reopen read-only or pass write=true explicitly",
    );
  if (options.write === true && db._isReadOnly())
    throw new ReadOnly(
      "MCP write mode requires a writable SQLite handle; reopen writable before passing write=true",
    );
  return serveStdio(() => createMcpServer(db, options), {
    transport: new LosslessStdioTransport(),
  });
}
