import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import Database from "better-sqlite3";
import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";

import {
  FormatError,
  TooLarge,
  TypeError as FGraphTypeError,
} from "../src/errors.js";
import {
  MAX_JSON_DEPTH,
  MAX_JSON_DOCUMENT_DEPTH,
  canonicalValueJson,
  parseJson,
  parseJsonValue,
} from "../src/jsonio.js";
import type { RenderedFact } from "../src/models.js";
import { encode } from "../src/values.js";
import { connect } from "../src/store.js";

const directories: string[] = [];

function temporaryDirectory(): string {
  const path = mkdtempSync(join(tmpdir(), "fgraph-release-hardening-"));
  directories.push(path);
  return path;
}

async function waitForFiles(paths: string[]): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (!paths.every((path) => existsSync(path))) {
    if (Date.now() >= deadline)
      throw new Error(`timed out waiting for ${paths.join(", ")}`);
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 10));
  }
}

function initializerProcess(
  database: string,
  reached: string,
): { child: ChildProcess; completion: Promise<string> } {
  const source = String.raw`
    import { writeFileSync } from "node:fs";
    import Database from "better-sqlite3";
    import { createServer } from "vite";

    const server = await createServer({
      root: process.cwd(),
      appType: "custom",
      logLevel: "silent",
      server: { middlewareMode: true },
    });
    try {
      const originalExec = Database.prototype.exec;
      Database.prototype.exec = function (sql) {
        if (sql === "BEGIN IMMEDIATE") writeFileSync(process.env.REACHED, "ready");
        return originalExec.call(this, sql);
      };
      const { connect } = await server.ssrLoadModule("/src/store.ts");
      const db = connect(process.env.DATABASE, { clock: 1767225600000000n });
      try {
        if (db.doctor().ok !== true) throw new Error("initialized database is invalid");
      } finally {
        db.close();
      }
    } finally {
      await server.close();
    }
  `;
  const child = spawn(process.execPath, ["--input-type=module", "-e", source], {
    cwd: resolve("."),
    env: {
      ...process.env,
      DATABASE: database,
      REACHED: reached,
    },
    stdio: ["ignore", "ignore", "pipe"],
  });
  const completion = new Promise<string>((resolvePromise, reject) => {
    let stderr = "";
    child.stderr?.setEncoding("utf8");
    child.stderr?.on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolvePromise(stderr);
      else
        reject(
          new Error(
            `initializer exited with code ${String(code)} signal ${String(signal)}: ${stderr}`,
          ),
        );
    });
  });
  return { child, completion };
}

afterEach(() => {
  while (directories.length > 0)
    rmSync(directories.pop() as string, { recursive: true, force: true });
});

describe("public search evidence types", () => {
  it("types bounded value and snippet fields", () => {
    const fact = {} as RenderedFact;
    expectTypeOf(fact.snippet).toEqualTypeOf<string | undefined>();
    expectTypeOf(fact.snippet_truncated).toEqualTypeOf<boolean | undefined>();
    expectTypeOf(fact.value_truncated).toEqualTypeOf<boolean | undefined>();
  });
});

describe("coherent read operations", () => {
  it("pins search basis before filters, candidates, pulls, and expansion", () => {
    const directory = temporaryDirectory();
    const path = join(directory, "search.db");
    using writer = connect(path, { clock: 1_767_225_600_000_000n });
    writer.declare("doc/vector", { type: "vector", dims: 2 });
    writer.declare("doc/link", { ref: true });
    writer.transact([
      { id: "neighbor/old", "doc/title": "neighbor before" },
      {
        id: "doc/old",
        "doc/text": "needle",
        "doc/kind": "keep",
        "doc/title": "before",
        "doc/vector": { vector: [1, 0] },
        "doc/link": { ref: "neighbor/old" },
      },
    ]);
    const oldBasis = writer._basisTx();
    using reader = connect(path, { readOnly: true });

    const prepare = reader._connection.prepare.bind(reader._connection);
    let coordinated = false;
    const spy = vi.spyOn(reader._connection, "prepare").mockImplementation(((
      source: string,
    ) => {
      if (
        source.includes("FROM fgraph_fts JOIN fgraph_facts") &&
        !coordinated
      ) {
        coordinated = true;
        writer.transact([
          { id: "neighbor/old", "doc/title": "neighbor after" },
          { id: "neighbor/new", "doc/title": "new neighbor" },
          { id: "doc/old", "doc/title": "after" },
          {
            id: "doc/new",
            "doc/text": "needle",
            "doc/kind": "keep",
            "doc/title": "new",
            "doc/vector": { vector: [1, 0] },
            "doc/link": { ref: "neighbor/new" },
          },
        ]);
      }
      return prepare(source);
    }) as typeof reader._connection.prepare);
    let result;
    try {
      result = reader.search({
        text: "needle",
        vector: [1, 0],
        vectorAttribute: "doc/vector",
        filters: [["doc/kind", "keep"]],
        k: 10,
        expand: 1,
      });
    } finally {
      spy.mockRestore();
    }
    expect(coordinated).toBe(true);
    expect(writer._basisTx()).not.toBe(oldBasis);
    expect(result.basis_tx).toBe(oldBasis);
    expect(result.hits.map((hit) => hit.entity)).toEqual(["doc/old"]);
    expect(result.hits[0]?.pull["doc/title"]).toBe("before");
    expect(result.expanded.map((item) => item.entity)).toEqual([
      "neighbor/old",
    ]);
    expect(result.expanded[0]?.pull["doc/title"]).toBe("neighbor before");
  });

  for (const mutation of ["nohistory", "excise"] as const) {
    it(`keeps snapshotLines coherent when ${mutation} commits after the header`, () => {
      const directory = temporaryDirectory();
      const path = join(directory, `${mutation}.db`);
      using writer = connect(path, { clock: 1_767_225_600_000_000n });
      writer.declare("private/value", { type: "text", nohistory: true });
      writer.transact({ id: "private/item", "private/value": "before" });
      using reader = connect(path, { readOnly: true });

      const lines = reader.snapshotLines();
      const header = lines.next();
      expect(header.done).toBe(false);
      if (mutation === "nohistory")
        writer.transact({ id: "private/item", "private/value": "after" });
      else writer.excise("private/item");

      const snapshot = [header.value, ...lines].join("");
      using restored = connect(":memory:", {
        clock: 1_767_225_600_000_000n,
      });
      restored.restore(snapshot);
      expect(restored.entity("private/item")).toEqual({
        "private/value": "before",
      });
    });
  }

  it("reuses caller-owned transactions without committing or rolling them back", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact({ id: "doc/one", "doc/text": "needle" });
    db._connection.exec("BEGIN");
    try {
      expect(db.search({ text: "needle" }).hits).toHaveLength(1);
      expect(db._connection.inTransaction).toBe(true);
      expect([...db.snapshotLines()].join("")).toContain('"fgraph":"end"');
      expect(db._connection.inTransaction).toBe(true);
    } finally {
      db._connection.exec("ROLLBACK");
    }
  });

  it("rolls back an owned read transaction when its basis is corrupt", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db._connection.exec("DELETE FROM fgraph_events");
    expect(() => db.search({ text: "needle" })).toThrowError(FormatError);
    expect(db._connection.inTransaction).toBe(false);
  });
});

describe("bounded JSON", () => {
  it("preserves __proto__ as JSON data and still rejects duplicate keys", () => {
    const parsed = parseJsonValue('{"__proto__":{"safe":true},"ordinary":1}');
    expect(Object.hasOwn(parsed as object, "__proto__")).toBe(true);
    expect(Object.getPrototypeOf(parsed)).toBe(Object.prototype);
    expect(canonicalValueJson(parsed)).toBe(
      '{"__proto__":{"safe":true},"ordinary":1}',
    );
    expect(() => parseJsonValue('{"__proto__":1,"__proto__":2}')).toThrowError(
      FGraphTypeError,
    );
  });

  it("rejects exotic, sparse, and otherwise non-JSON in-memory values", () => {
    const sparse: unknown[] = [];
    sparse.length = 2;
    sparse[1] = "present";
    const custom = Object.create({ inherited: true }) as Record<
      string,
      unknown
    >;
    custom.own = true;
    const symbolObject = { visible: true } as Record<PropertyKey, unknown>;
    symbolObject[Symbol("hidden")] = true;
    const accessorObject: Record<string, unknown> = {};
    Object.defineProperty(accessorObject, "computed", {
      enumerable: true,
      get: () => "value",
    });
    const accessorArray = ["value"];
    Object.defineProperty(accessorArray, "0", {
      enumerable: true,
      get: () => "value",
    });

    for (const value of [
      new Date(0),
      new Map([["key", "value"]]),
      new Set(["value"]),
      new Uint8Array([1, 2]),
      sparse,
      [undefined],
      { missing: undefined },
      custom,
      symbolObject,
      accessorObject,
      accessorArray,
    ]) {
      expect(() => canonicalValueJson(value)).toThrowError(FGraphTypeError);
      expect(() => encode({ json: value })).toThrowError(FGraphTypeError);
    }
  });

  it("accepts 64 containers and rejects deeper or cyclic JSON before stack overflow", () => {
    const documentAtLimit = `${"[".repeat(MAX_JSON_DOCUMENT_DEPTH)}0${"]".repeat(MAX_JSON_DOCUMENT_DEPTH)}`;
    expect(parseJson(documentAtLimit)).toBeDefined();
    expect(() => parseJson(`[${documentAtLimit}]`)).toThrowError(TooLarge);

    let value: unknown = 0;
    for (let depth = 0; depth < MAX_JSON_DEPTH; depth++) value = [value];
    const valueAtLimit = `${"[".repeat(MAX_JSON_DEPTH)}0${"]".repeat(MAX_JSON_DEPTH)}`;
    expect(canonicalValueJson(value)).toBe(valueAtLimit);
    expect(encode({ json: value })).toMatchObject({ stored: valueAtLimit });
    expect(() => encode({ json: [value] })).toThrowError(TooLarge);

    const cyclic: unknown[] = [];
    cyclic.push(cyclic);
    expect(() => encode({ json: cyclic })).toThrowError(TooLarge);
  });
});

describe("bounded search candidates", () => {
  it("probes beyond the exact keyword cap and bounds multibyte snippets", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact(
      Array.from({ length: 50 }, (_unused, index) => ({
        id: `exact/${String(index).padStart(2, "0")}`,
        "doc/text": "needle",
      })),
    );
    expect(db.search({ text: "needle", k: 1 }).truncated).toBe(false);
    db.transact({ id: "exact/50", "doc/text": "needle" });
    expect(db.search({ text: "needle", k: 1 }).truncated).toBe(true);

    db.transact({
      id: "long/snippet",
      "doc/text": `unique ${"界".repeat(1_000)}`,
    });
    const matched = db.search({ text: "unique", k: 1 }).hits[0]?.matched[0];
    expect(matched).toBeDefined();
    for (const field of [matched?.v, matched?.snippet]) {
      expect(typeof field).toBe("string");
      expect(Buffer.byteLength(field as string, "utf8")).toBeLessThanOrEqual(
        2048,
      );
      expect(field).toMatch(/…$/u);
    }
    expect(matched?.value_truncated).toBe(true);
    expect(matched?.snippet_truncated).toBe(true);
  });

  it("streams entity-ordered vector candidates from one joined query", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("doc/vector", { type: "vector", dims: 2 });
    db.transact(
      Array.from({ length: 8 }, (_unused, index) => ({
        id: `vector/${index}`,
        "doc/vector": { vector: [1, index / 10] },
      })),
    );
    const statements: string[] = [];
    const prepare = db._connection.prepare.bind(db._connection);
    const spy = vi.spyOn(db._connection, "prepare").mockImplementation(((
      source: string,
    ) => {
      statements.push(source);
      return prepare(source);
    }) as typeof db._connection.prepare);
    try {
      expect(
        db.search({
          vector: [1, 0],
          vectorAttribute: "doc/vector",
          k: 8,
        }).hits,
      ).toHaveLength(8);
    } finally {
      spy.mockRestore();
    }
    expect(
      statements.some(
        (statement) =>
          statement.includes("JOIN fgraph_blobs") &&
          statement.includes("b.data") &&
          statement.includes("ORDER BY f.e,f.id"),
      ),
    ).toBe(true);
    expect(
      statements.filter((statement) =>
        statement.includes("SELECT data FROM fgraph_blobs WHERE hash=?"),
      ),
    ).toHaveLength(0);
  });

  it("counts vector candidates exactly and preserves fact-id tie ordering", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("doc/vector", { type: "vector", dims: 2 });
    db.transact(
      Array.from({ length: 50 }, (_unused, index) => ({
        id: `${index === 0 ? "z-first" : "vector"}/${String(index).padStart(2, "0")}`,
        "doc/vector": { vector: [1, index === 0 ? 0 : index / 100] },
      })),
    );
    const exact = db.search({
      vector: [1, 0],
      vectorAttribute: "doc/vector",
      k: 1,
    });
    expect(exact).toMatchObject({
      truncated: false,
      work_used: 50,
      hits: [{ entity: "z-first/00" }],
    });

    db.transact({
      id: "a-second/50",
      "doc/vector": { vector: [1, 0] },
    });
    const overflow = db.search({
      vector: [1, 0],
      vectorAttribute: "doc/vector",
      k: 2,
      explain: true,
    });
    expect(overflow).toMatchObject({ truncated: true, work_used: 51 });
    expect(overflow.hits.slice(0, 2).map((hit) => hit.entity)).toEqual([
      "z-first/00",
      "a-second/50",
    ]);
    expect(overflow.hits[0]?.matched[0]?.v).toEqual({ vector_dims: 2 });

    db.transact({
      id: "worse/51",
      "doc/vector": { vector: [0, 1] },
    });
    const withDiscardedTail = db.search({
      vector: [1, 0],
      vectorAttribute: "doc/vector",
      k: 2,
    });
    expect(withDiscardedTail).toMatchObject({ truncated: true, work_used: 52 });
    expect(withDiscardedTail.hits.map((hit) => hit.entity)).toEqual([
      "z-first/00",
      "a-second/50",
    ]);
  });
});

describe("file initialization and backup", () => {
  it("accepts a valid initializer that wins between inspection and locking", () => {
    const directory = temporaryDirectory();
    const database = join(directory, "interleaved.db");
    const originalExec = Database.prototype.exec;
    let initialized = false;
    const spy = vi
      .spyOn(Database.prototype, "exec")
      .mockImplementation(function (this: Database.Database, sql: string) {
        if (
          !initialized &&
          sql === "BEGIN IMMEDIATE" &&
          this.name === database
        ) {
          initialized = true;
          connect(database, { clock: 1_767_225_600_000_000n }).close();
        }
        return originalExec.call(this, sql);
      });
    try {
      using accepted = connect(database, {
        clock: 1_767_225_600_000_001n,
      });
      expect(accepted.doctor()).toMatchObject({ ok: true });
    } finally {
      spy.mockRestore();
    }
  });

  it("rejects a pristine-looking SQLite file with an application table", () => {
    const directory = temporaryDirectory();
    const database = join(directory, "application.db");
    const sqlite = new Database(database);
    sqlite.exec("CREATE TABLE application_data(value TEXT)");
    sqlite.close();
    expect(() => connect(database)).toThrowError(FormatError);
  });

  it("accepts a valid initializer that wins after both processes see a pristine file", async () => {
    const directory = temporaryDirectory();
    const database = join(directory, "concurrent.db");
    const reached = [
      join(directory, "reached-a"),
      join(directory, "reached-b"),
    ];
    const locker = new Database(database);
    locker.pragma("journal_mode = WAL");
    locker.exec("BEGIN IMMEDIATE");
    const workers = reached.map((marker) =>
      initializerProcess(database, marker),
    );
    try {
      await waitForFiles(reached);
      locker.exec("COMMIT");
      await Promise.all(workers.map((worker) => worker.completion));
    } finally {
      if (locker.inTransaction) locker.exec("ROLLBACK");
      locker.close();
      workers.forEach((worker) => worker.child.kill());
    }
    using verified = connect(database, { readOnly: true });
    expect(verified.doctor()).toMatchObject({ ok: true });
  }, 20_000);

  it("uses online backup while another handle commits and publishes once", async () => {
    const directory = temporaryDirectory();
    const sourcePath = join(directory, "source.db");
    const backupPath = join(directory, "backup.db");
    using source = connect(sourcePath, { clock: 1_767_225_600_000_000n });
    source.transact({ id: "backup/before", "item/value": "before" });
    using writer = connect(sourcePath, { clock: 1_767_225_600_000_001n });

    const rawBackup = source._connection.backup.bind(source._connection);
    let wroteDuringTransfer = false;
    const backupSpy = vi
      .spyOn(source._connection, "backup")
      .mockImplementation((destination) =>
        rawBackup(destination, {
          progress(progress) {
            if (
              !wroteDuringTransfer &&
              progress.remainingPages < progress.totalPages
            ) {
              wroteDuringTransfer = true;
              writer.transact({
                id: "backup/during",
                "item/value": "during",
              });
            }
            // One page per turn guarantees an observable mid-copy boundary.
            return 1;
          },
        }),
      );
    try {
      const pending = source.backup(backupPath);
      expect(pending).toBeInstanceOf(Promise);
      await pending;
    } finally {
      backupSpy.mockRestore();
    }

    expect(wroteDuringTransfer).toBe(true);
    using backup = connect(backupPath, { readOnly: true });
    expect(backup.doctor()).toMatchObject({ ok: true });
    expect(backup.entity("backup/before")).toEqual({ "item/value": "before" });
    expect(backup.entity("backup/during")).toEqual({ "item/value": "during" });
    await expect(source.backup(backupPath)).rejects.toBeInstanceOf(Error);
  });
});
