import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { main } from "../src/cli.js";
import { parseJson } from "../src/jsonio.js";
import { runMcp } from "../src/mcp.js";

vi.mock("../src/mcp.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/mcp.js")>();
  return { ...actual, runMcp: vi.fn() };
});

const directories: string[] = [];

function temporaryDirectory(): string {
  const path = mkdtempSync(join(tmpdir(), "fgraph-cli-"));
  directories.push(path);
  return path;
}

interface Invocation {
  code: number;
  stdout: string;
  stderr: string;
}

async function invoke(...args: string[]): Promise<Invocation> {
  let stdout = "";
  let stderr = "";
  const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
    chunk: string | Uint8Array,
  ) => {
    stdout += chunk.toString();
    return true;
  }) as typeof process.stdout.write);
  const stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(((
    chunk: string | Uint8Array,
  ) => {
    stderr += chunk.toString();
    return true;
  }) as typeof process.stderr.write);
  try {
    return { code: await main(args), stdout, stderr };
  } finally {
    stdoutSpy.mockRestore();
    stderrSpy.mockRestore();
  }
}

function decoded(invocation: Invocation): unknown {
  return parseJson(invocation.stdout.trim(), "CLI output");
}

afterEach(() => {
  while (directories.length > 0)
    rmSync(directories.pop() as string, { recursive: true, force: true });
  delete process.env.FGRAPH_DB;
  delete process.env.FGRAPH_CLOCK;
  delete process.env.FGRAPH_QUERY_BUDGET;
  vi.clearAllMocks();
});

describe("CLI", () => {
  it("executes the complete bounded command surface", async () => {
    const directory = temporaryDirectory();
    const database = join(directory, "graph.db");
    const backup = join(directory, "backup.db");
    const restored = join(directory, "restored.db");
    const applied = join(directory, "applied.db");
    process.env.FGRAPH_CLOCK = "1767225600000000";

    const initialized = await invoke("init", "--db", database);
    expect(initialized.code).toBe(0);
    expect(initialized.stdout).toContain('"format_version": 2');

    expect(
      (
        await invoke(
          "declare",
          "person/tags",
          "--type",
          "text",
          "--many",
          "--unique",
          "--history",
          "--doc",
          "Tags",
          "--operation-id",
          "declare-person-tags",
          "--if-basis-tx",
          "64",
          "--db",
          database,
          "--json",
        )
      ).code,
    ).toBe(0);
    const schemaManifest = decoded(
      await invoke("schema-export", "--db", database, "--json"),
    );
    expect(schemaManifest).toMatchObject({ fgraph: "schema/1" });
    expect(
      decoded(
        await invoke(
          "schema-check",
          JSON.stringify(schemaManifest),
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ valid: true, changes: [] });
    expect(
      decoded(
        await invoke(
          "schema-apply",
          JSON.stringify(schemaManifest),
          "--operation-id",
          "schema:apply",
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ status: expect.stringMatching(/^(applied|noop)$/u) });
    expect(
      (
        await invoke(
          "declare",
          "person/vector",
          "--type=vector",
          "--dims",
          "2",
          "--nohistory",
          "--vector-model",
          "local/test-v1",
          "--db",
          database,
          "--json",
        )
      ).code,
    ).toBe(0);
    const added = await invoke(
      "add",
      '{"id":"person/ada","person/name":"Ada needle","person/tags":["math","code"],"person/vector":{"vector":[1.0,0.0]}}',
      "--db",
      database,
      "--json",
    );
    expect(added.code).toBe(0);
    const addReport = decoded(added) as { tx: number; event: string };
    expect(addReport.tx).toBeGreaterThan(64);

    const entity = await invoke(
      "get",
      "person/ada",
      "--depth",
      "0",
      "--db",
      database,
      "--json",
    );
    expect(decoded(entity)).toMatchObject({ "person/name": "Ada needle" });
    expect(
      decoded(
        await invoke("get", String(addReport.tx), "--db", database, "--json"),
      ),
    ).toHaveProperty("fgraph/at");
    expect(
      decoded(
        await invoke(
          "get",
          "person/ada",
          "--at",
          String(addReport.tx),
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ "person/name": "Ada needle" });
    const query = await invoke(
      "q",
      '{"find":["?name"],"in":["?wanted"],"where":[["?e","person/name","?name"],["=","?name","?wanted"]]}',
      "--args",
      '{"?wanted":"Ada needle"}',
      "--query-budget",
      "10",
      "--db",
      database,
      "--json",
    );
    expect(decoded(query)).toEqual({
      columns: ["?name"],
      rows: [["Ada needle"]],
    });
    expect(
      decoded(
        await invoke(
          "q",
          '{"find":["?name"],"where":[["?e","person/name","?name"]]}',
          "--at",
          String(addReport.tx),
          "--db",
          database,
          "--json",
        ),
      ),
    ).toEqual({ columns: ["?name"], rows: [["Ada needle"]] });
    expect(
      decoded(
        await invoke(
          "explain",
          '{"find":["?name"],"where":[["?e","person/name","?name"]]}',
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ source: "current", clauses: [{ access: "avet/a" }] });
    const datomPage = decoded(
      await invoke("datoms", "--limit", "1", "--db", database, "--json"),
    ) as { next_cursor: string };
    expect(datomPage.next_cursor).toEqual(expect.any(String));
    expect(
      decoded(
        await invoke(
          "datoms",
          "eavt",
          "--cursor",
          datomPage.next_cursor,
          "--limit",
          "1",
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ items: expect.any(Array) });
    expect(
      decoded(
        await invoke(
          "datoms",
          "eavt",
          "--source",
          "history",
          "--limit",
          "1",
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ items: expect.any(Array) });
    expect(
      decoded(
        await invoke(
          "datoms",
          "avet",
          "--components",
          '["person/name"]',
          "--limit",
          "1",
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ items: [{ a: "person/name", v: "Ada needle" }] });
    expect(
      decoded(
        await invoke("tx", String(addReport.tx), "--db", database, "--json"),
      ),
    ).toMatchObject({
      tx: addReport.tx,
      event: addReport.event,
      event_hash: expect.stringMatching(/^sha256:[0-9a-f]{64}$/u),
    });

    const keyword = await invoke(
      "search",
      "needle",
      "--filter",
      '["person/tags","math"]',
      '--filter=["person/tags","code"]',
      "--k",
      "2",
      "--expand",
      "0",
      "--text-attribute",
      "person/name",
      "--db",
      database,
      "--json",
    );
    expect((decoded(keyword) as { hits: unknown[] }).hits).toHaveLength(1);
    const semantic = await invoke(
      "search",
      "--vector",
      "[1.0,0.0]",
      "--vector-attribute",
      "person/vector",
      "--db",
      database,
      "--json",
    );
    expect((decoded(semantic) as { hits: unknown[] }).hits).toHaveLength(1);
    expect(
      (
        decoded(
          await invoke(
            "search",
            "--vector",
            "[9007199254740992,0]",
            "--vector-attribute",
            "person/vector",
            "--db",
            database,
            "--json",
          ),
        ) as { hits: unknown[] }
      ).hits,
    ).toHaveLength(1);

    expect(
      (
        decoded(
          await invoke(
            "history",
            "person/ada",
            "person/name",
            "--db",
            database,
            "--json",
          ),
        ) as unknown[]
      ).length,
    ).toBeGreaterThan(0);
    expect(
      decoded(
        await invoke(
          "why",
          "person/ada",
          "person/name",
          "--db",
          database,
          "--json",
        ),
      ) as unknown[],
    ).toHaveLength(1);
    const schema = decoded(
      await invoke("schema", "person/", "--system", "--db", database, "--json"),
    ) as { attributes: unknown[]; digest: string };
    expect(schema.attributes).toHaveLength(4);
    expect(schema.digest).toMatch(/^sha256:[0-9a-f]{64}$/u);

    expect(
      (
        await invoke(
          "shape",
          "shape/person",
          "--required",
          "person/name",
          "--allowed",
          "person/name",
          "--allowed",
          "person/tags",
          "--allowed",
          "person/vector",
          "--closed",
          "--operation-id",
          "define-person-shape",
          "--if-basis-tx",
          String(addReport.tx),
          "--db",
          database,
          "--json",
        )
      ).code,
    ).toBe(0);
    expect(
      (
        await invoke(
          "shape",
          "shape/open",
          "--open",
          "--db",
          database,
          "--json",
        )
      ).code,
    ).toBe(0);
    expect(
      (
        await invoke(
          "add",
          '{"id":"person/ada","fgraph/shape":{"ref":"shape/person"}}',
          "--db",
          database,
          "--json",
        )
      ).code,
    ).toBe(0);
    expect(
      decoded(
        await invoke("validate", "person/ada", "--db", database, "--json"),
      ),
    ).toMatchObject({ valid: true, violations: [] });

    const snapshot = await invoke("snapshot", "--db", database);
    expect(snapshot.code).toBe(0);
    const snapshotPath = join(directory, "snapshot.ndjson");
    writeFileSync(snapshotPath, snapshot.stdout);
    expect(
      decoded(
        await invoke("restore", snapshotPath, "--db", restored, "--json"),
      ),
    ).toMatchObject({ ok: true });
    expect(
      (
        decoded(
          await invoke("get", "person/ada", "--db", restored, "--json"),
        ) as Record<string, unknown>
      )["person/name"],
    ).toBe("Ada needle");

    const tail = await invoke("tail", "--since", "64", "--db", database);
    expect(tail.code).toBe(0);
    const eventsPath = join(directory, "events.ndjson");
    writeFileSync(eventsPath, tail.stdout.trimEnd().split("\n").join("\r\n"));
    expect(
      decoded(await invoke("apply", eventsPath, "--db", applied, "--json")),
    ).toMatchObject({ applied: expect.any(Number), already_applied: 0 });
    expect(
      decoded(await invoke("get", "person/ada", "--db", applied, "--json")),
    ).toMatchObject({ "person/name": "Ada needle" });

    const update = decoded(
      await invoke(
        "add",
        '{"id":"person/ada","person/name":"Augusta"}',
        "--db",
        database,
        "--json",
      ),
    ) as { tx: number };
    expect(
      (
        await invoke(
          "undo",
          String(update.tx),
          "--operation-id",
          "undo-name-update",
          "--if-basis-tx",
          String(update.tx),
          "--db",
          database,
          "--json",
        )
      ).code,
    ).toBe(0);
    expect(
      (
        decoded(
          await invoke("get", "person/ada", "--db", database, "--json"),
        ) as Record<string, unknown>
      )["person/name"],
    ).toBe("Ada needle");
    expect(
      (
        decoded(
          await invoke(
            "diff",
            String(addReport.tx),
            String(update.tx),
            "--db",
            database,
            "--json",
          ),
        ) as { asserted: unknown[] }
      ).asserted.length,
    ).toBeGreaterThan(0);
    expect(tail.stdout.trim().split("\n").length).toBeGreaterThan(1);

    expect(
      decoded(await invoke("doctor", "--db", database, "--json")),
    ).toMatchObject({ ok: true, repaired: false });
    expect(
      decoded(await invoke("doctor", "--repair", "--db", database, "--json")),
    ).toMatchObject({ ok: true, repaired: true });
    const backupResult = await invoke(
      "backup",
      backup,
      "--db",
      database,
      "--json",
    );
    expect(backupResult).toMatchObject({ code: 0, stderr: "" });
    expect(decoded(backupResult)).toEqual({ path: backup });
    expect(
      decoded(await invoke("info", "--db", backup, "--json")),
    ).toMatchObject({ application_id: 1_718_055_521, format_version: 2 });

    const secret = decoded(
      await invoke(
        "add",
        '{"id":"secret/item","secret/value":"erase me"}',
        "--db",
        database,
        "--json",
      ),
    ) as { tx: number };
    const excised = decoded(
      await invoke(
        "excise",
        "secret/item",
        "--operation-id",
        "cli-excise-secret",
        "--if-basis-tx",
        String(secret.tx),
        "--db",
        database,
        "--json",
      ),
    ) as { status: string; tx: number };
    expect(excised).toMatchObject({
      status: "applied",
      tx: expect.any(Number),
    });
    expect(
      decoded(await invoke("get", "secret/item", "--db", database, "--json")),
    ).toEqual({});
    expect(
      decoded(
        await invoke(
          "excise",
          "secret/item",
          "--operation-id",
          "cli-excise-secret",
          "--if-basis-tx",
          String(secret.tx),
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ status: "already_applied", tx: excised.tx });

    expect(
      (
        await invoke(
          "retract",
          "person/ada",
          "person/tags",
          '"math"',
          "--db",
          database,
          "--json",
        )
      ).code,
    ).toBe(0);
    const retracted = await invoke(
      "retract",
      "person/ada",
      "--db",
      database,
      "--json",
    );
    expect(retracted).toMatchObject({ code: 0, stderr: "" });
  });

  it("accepts NDJSON/@file inputs, environment defaults, and embedding argv", async () => {
    const directory = temporaryDirectory();
    const database = join(directory, "environment.db");
    const payloadPath = join(directory, "payload.ndjson");
    const embedderPath = join(directory, "embedder.mjs");
    writeFileSync(
      payloadPath,
      '{"id":"note/one","note/text":"alpha"}\n{"id":"note/two","note/text":"beta"}\n',
    );
    writeFileSync(
      embedderPath,
      'process.stdin.resume(); process.stdin.on("end",()=>process.stdout.write("[1.0,0.0]"));\n',
    );
    process.env.FGRAPH_DB = database;
    process.env.FGRAPH_CLOCK = "1767225600000000";
    process.env.FGRAPH_QUERY_BUDGET = "100";
    expect(
      (decoded(await invoke("add", `@${payloadPath}`, "--json")) as unknown[])
        .length,
    ).toBe(2);
    expect(
      (decoded(await invoke("info", "--json")) as Record<string, unknown>)
        .transactions,
    ).toBe(3);
    expect(
      (
        await invoke(
          "declare",
          "note/vector",
          "--type",
          "vector",
          "--dims",
          "2",
          "--json",
        )
      ).code,
    ).toBe(0);
    expect(
      (
        await invoke(
          "add",
          '{"id":"note/one","note/vector":{"vector":[1.0,0.0]}}',
          "--json",
        )
      ).code,
    ).toBe(0);
    const command = JSON.stringify([process.execPath, embedderPath]);
    expect(
      (
        decoded(
          await invoke(
            "search",
            "alpha",
            "--embed-cmd",
            command,
            "--vector-attribute",
            "note/vector",
            "--json",
          ),
        ) as { hits: unknown[] }
      ).hits,
    ).toHaveLength(1);
  });

  it("maps help, usage, and typed failures to stable exit codes", async () => {
    const directory = temporaryDirectory();
    const database = join(directory, "errors.db");
    expect(await invoke("--version")).toMatchObject({
      code: 0,
      stdout: "1.0.2\n",
    });
    expect(await invoke("version")).toMatchObject({
      code: 0,
      stdout: "1.0.2\n",
    });
    expect(await invoke("--help")).toMatchObject({ code: 0 });
    expect(await invoke("-h")).toMatchObject({ code: 0 });
    expect(await invoke()).toMatchObject({ code: 2 });
    expect(await invoke("unknown")).toMatchObject({ code: 2 });
    expect(await invoke("version", "extra")).toMatchObject({ code: 2 });
    expect(await invoke("init", "extra", "--db", database)).toMatchObject({
      code: 2,
    });
    expect(
      await invoke("declare", "x/a", "--many", "--one", "--db", database),
    ).toMatchObject({ code: 2 });
    expect(await invoke("q", "[]", "--db", database)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("TypeError:"),
    });
    expect(await invoke("get", "missing", "--db", database)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("NotFound:"),
    });
    expect(
      await invoke("info", "--db", join(directory, "missing.db")),
    ).toMatchObject({ code: 1, stderr: expect.stringContaining("NotFound:") });
    expect(
      await invoke("info", "--query-budget", "0", "--db", database),
    ).toMatchObject({ code: 2 });
  });

  it("adapts MCP options and reports adapter failures without escaping main", async () => {
    const directory = temporaryDirectory();
    const database = join(directory, "mcp.db");
    expect((await invoke("init", "--db", database)).code).toBe(0);
    const mockedRunMcp = vi.mocked(runMcp);
    const stopImmediately: typeof runMcp = (db) => {
      db.close();
      return { close: async () => undefined };
    };
    mockedRunMcp
      .mockImplementationOnce(stopImmediately)
      .mockImplementationOnce(stopImmediately);

    expect(await invoke("mcp", "--db", database)).toMatchObject({ code: 0 });
    expect(mockedRunMcp).toHaveBeenLastCalledWith(expect.anything(), {
      write: false,
    });
    expect(
      await invoke(
        "mcp",
        "--write",
        "--embed-cmd",
        '["node","embed.mjs"]',
        "--db",
        database,
      ),
    ).toMatchObject({ code: 0 });
    expect(mockedRunMcp).toHaveBeenLastCalledWith(expect.anything(), {
      write: true,
      embedCommand: '["node","embed.mjs"]',
    });
    expect(await invoke("mcp", "extra", "--db", database)).toMatchObject({
      code: 2,
    });

    let failedDb: Parameters<typeof runMcp>[0] | undefined;
    mockedRunMcp.mockImplementationOnce((db) => {
      failedDb = db;
      throw new Error("adapter failed");
    });
    expect(await invoke("mcp", "--write", "--db", database)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("Error: adapter failed"),
    });
    expect(failedDb?._connection.open).toBe(false);
    mockedRunMcp.mockImplementationOnce((db) => {
      failedDb = db;
      throw "non-error adapter failure";
    });
    expect(await invoke("mcp", "--write", "--db", database)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("Error: non-error adapter failure"),
    });
    expect(failedDb?._connection.open).toBe(false);
  });

  it("validates every command arity and option boundary", async () => {
    const directory = temporaryDirectory();
    const database = join(directory, "edges.db");
    const empty = join(directory, "empty.json");
    const batch = join(directory, "batch.ndjson");
    const invalidUtf8 = join(directory, "invalid.ndjson");
    writeFileSync(empty, "   \n");
    writeFileSync(batch, '{"id":"one"}\n{"id":"two"}\n');
    writeFileSync(invalidUtf8, Buffer.from([0xff]));
    expect((await invoke("init", "--db", database)).code).toBe(0);

    const usageCases = [
      ["info", "--db"],
      ["add", "--db", database],
      ["add", "{}", "extra", "--db", database],
      ["retract", "--db", database],
      ["retract", "a", "b", "c", "d", "--db", database],
      ["get", "--db", database],
      ["tx", "--db", database],
      ["q", "--db", database],
      ["explain", "--db", database],
      ["datoms", "eavt", "avet", "--db", database],
      ["datoms", "--source", "bad", "--db", database],
      ["datoms", "--components", "{}", "--db", database],
      ["search", "needle", "--unknown", "--db", database],
      ["search", "needle", "--filter", "--db", database],
      ["history", "--db", database],
      ["why", "a", "b", "c", "--db", database],
      ["diff", "64", "--db", database],
      ["diff", "nope", "65", "--db", database],
      ["declare", "--many", "--db", database],
      ["shape", "--closed", "--open", "shape/test", "--db", database],
      ["shape", "--db", database],
      ["validate", "--db", database],
      ["schema", "a", "b", "--db", database],
      ["schema-export", "extra", "--db", database],
      ["schema-check", "--db", database],
      ["schema-apply", "--db", database],
      ["snapshot", "extra", "--db", database],
      ["restore", "a", "b", "--db", database],
      ["apply", "a", "b", "--db", database],
      ["undo", "--db", database],
      ["excise", "entity", "--db", database],
      ["tail", "extra", "--db", database],
      ["backup", "--db", database],
      ["doctor", "extra", "--db", database],
    ];
    for (const args of usageCases) expect((await invoke(...args)).code).toBe(2);

    expect(await invoke("explain", "[]", "--db", database)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("TypeError:"),
    });
    expect(
      await invoke("explain", "{}", "--args", "[]", "--db", database),
    ).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("TypeError:"),
    });

    expect(
      await invoke(
        "add",
        `@${join(directory, "missing.json")}`,
        "--db",
        database,
      ),
    ).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("FormatError:"),
    });
    expect(await invoke("add", `@${empty}`, "--db", database)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("is empty"),
    });
    expect(await invoke("add", "{", "--db", database)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("TypeError:"),
    });
    expect(
      await invoke(
        "add",
        `@${batch}`,
        "--operation-id",
        "batch-operation",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke("add", `@${batch}`, "--if-basis-tx", "64", "--db", database),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke(
        "add",
        `@${batch}`,
        "--batch-size",
        "1",
        "--operation-id",
        "batch-operation",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke(
        "add",
        `@${batch}`,
        "--batch-size",
        "10001",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke(
        "add",
        `@${batch}`,
        "--batch-size",
        "1",
        "--operation-id",
        "batch-operation",
        "--operation-id-prefix",
        "batch-prefix",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke(
        "add",
        `@${batch}`,
        "--operation-id-prefix",
        "batch-prefix",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke(
        "add",
        `@${batch}`,
        "--batch-size",
        "1",
        "--if-basis-tx",
        "64",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    const batchedArguments = [
      "add",
      `@${batch}`,
      "--batch-size",
      "1",
      "--operation-id-prefix",
      "import:batch",
      "--db",
      database,
      "--json",
    ];
    const firstBatch = decoded(await invoke(...batchedArguments)) as Record<
      string,
      unknown
    >;
    expect(firstBatch).toMatchObject({
      batches: 2,
      items: 2,
      applied: 2,
      already_applied: 0,
      noop: 0,
    });
    expect(firstBatch.basis_tx).toBe(firstBatch.tx);
    expect(decoded(await invoke(...batchedArguments))).toMatchObject({
      batches: 2,
      items: 2,
      applied: 0,
      already_applied: 2,
      noop: 0,
      basis_tx: firstBatch.tx,
    });
    expect(
      decoded(
        await invoke(
          "add",
          '{"id":"inline/one"}',
          "--batch-size",
          "1",
          "--operation-id",
          "inline:one",
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ batches: 1, applied: 1 });
    expect(
      decoded(
        await invoke(
          "add",
          '{"id":"inline/one"}',
          "--batch-size",
          "1",
          "--db",
          database,
          "--json",
        ),
      ),
    ).toMatchObject({ batches: 1, noop: 1 });
    expect(
      await invoke("add", "   ", "--batch-size", "1", "--db", database),
    ).toMatchObject({ code: 1, stderr: expect.stringContaining("is empty") });
    expect(
      await invoke("add", `@${empty}`, "--batch-size", "1", "--db", database),
    ).toMatchObject({ code: 1, stderr: expect.stringContaining("is empty") });
    expect(
      await invoke(
        "add",
        `@${join(directory, "missing.ndjson")}`,
        "--batch-size",
        "1",
        "--db",
        database,
      ),
    ).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("FormatError:"),
    });
    expect(
      await invoke("search", "--vector", "[]", "--db", database),
    ).toMatchObject({ code: 1, stderr: expect.stringContaining("non-empty") });
    expect(
      await invoke("search", "--vector", '["bad"]', "--db", database),
    ).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("finite numbers"),
    });
    expect(
      await invoke("search", "needle", "--k", "0", "--db", database),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke(
        "apply",
        join(directory, "missing.ndjson"),
        "--db",
        database,
      ),
    ).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("cannot be opened"),
    });
    expect(await invoke("apply", invalidUtf8, "--db", database)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("cannot be opened as UTF-8"),
    });
    expect(
      (await invoke("tail", "--since", "9223372036854775807", "--db", database))
        .code,
    ).toBe(0);
    expect(
      await invoke("tail", "--since", "9223372036854775808", "--db", database),
    ).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("signed 64-bit"),
    });
    expect(await invoke("undo", "64", "--db", database)).toMatchObject({
      code: 1,
    });
    expect(
      await invoke(
        "excise",
        "--operation-id",
        "missing-entity",
        "--if-basis-tx",
        "64",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke(
        "declare",
        "edge/unique",
        "--unique",
        "--not-unique",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    expect(
      await invoke(
        "declare",
        "edge/history",
        "--history",
        "--nohistory",
        "--db",
        database,
      ),
    ).toMatchObject({ code: 2 });
    expect(
      (
        await invoke(
          "declare",
          "edge/value",
          "--ref",
          "--not-unique",
          "--one",
          "--nohistory",
          "--db",
          database,
          "--json",
        )
      ).code,
    ).toBe(0);
  });

  it("resumes streamed batches after a malformed later line", async () => {
    const directory = temporaryDirectory();
    const database = join(directory, "partial.db");
    const input = join(directory, "partial.ndjson");
    const arguments_ = [
      "add",
      `@${input}`,
      "--batch-size",
      "1",
      "--operation-id-prefix",
      "import:partial",
      "--db",
      database,
      "--json",
    ];
    writeFileSync(input, '{"id":"partial/0"}\n{\n');
    expect(await invoke(...arguments_)).toMatchObject({
      code: 1,
      stderr: expect.stringContaining("TypeError:"),
    });

    writeFileSync(input, '{"id":"partial/0"}\n{"id":"partial/1"}\n');
    expect(decoded(await invoke(...arguments_))).toMatchObject({
      batches: 2,
      items: 2,
      applied: 1,
      already_applied: 1,
    });
  });
});
