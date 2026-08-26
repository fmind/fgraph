import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { afterEach, describe, expect, it } from "vitest";

import {
  Conflict,
  FormatError,
  NotFound,
  QueryError,
  ReadOnly,
  SchemaError,
  TypeError,
  Unsupported,
} from "../src/errors.js";
import { canonicalJson } from "../src/jsonio.js";
import { GENESIS_TX, connect } from "../src/store.js";

const directories: string[] = [];

function temporaryDirectory(): string {
  const path = mkdtempSync(join(tmpdir(), "fgraph-store-"));
  directories.push(path);
  return path;
}

afterEach(() => {
  while (directories.length > 0)
    rmSync(directories.pop() as string, { recursive: true, force: true });
  delete process.env.FGRAPH_CLOCK;
});

describe("schema manifests", () => {
  it("round-trips declarations and shapes and rejects invalid input atomically", () => {
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.declare("person/name", { type: "text", doc: "Display name" });
    source.defineShape("person/shape", {
      required: ["person/name"],
      closed: true,
    });
    const manifest = source.schemaManifest();
    expect(source.checkSchemaManifest(manifest).valid).toBe(true);

    using target = connect(":memory:", { clock: 1_767_225_600_000_000n });
    expect(
      target.applySchemaManifest(manifest, { operationId: "schema:v1" }).status,
    ).toBe("applied");
    expect(target.schemaManifest()).toEqual(manifest);
    const removal = target.checkSchemaManifest({
      fgraph: "schema/1",
      attributes: [],
      shapes: [],
    });
    expect(removal.changes.every((change) => change.after === null)).toBe(true);
    expect(
      target.applySchemaManifest({
        fgraph: "schema/1",
        attributes: [],
        shapes: [
          {
            name: "shape/empty",
            required: [],
            allowed: [],
            closed: false,
          },
        ],
      }).status,
    ).toBe("applied");
    const before = target.snapshot();
    expect(() =>
      target.applySchemaManifest({
        ...manifest,
        attributes: [{ name: "person/name", declared: { dims: 0 } }],
      }),
    ).toThrowError(SchemaError);
    expect(target.snapshot()).toBe(before);
  });

  it("holds the writer lock while discovering a full replacement", () => {
    const directory = temporaryDirectory();
    const path = join(directory, "schema-race.db");
    using target = connect(path, { clock: 1_767_225_600_000_000n });
    target.declare("stale/attribute", { type: "text" });
    using concurrent = connect(path, { clock: 1_767_225_601_000_000n });
    concurrent._connection.pragma("busy_timeout = 1");

    const schemaManifest = target.schemaManifest.bind(target);
    let concurrentWriterBlocked = false;
    target.schemaManifest = () => {
      const snapshot = schemaManifest();
      try {
        concurrent.declare("race/new", { type: "text" });
      } catch (error) {
        expect(error).toBeInstanceOf(Conflict);
        concurrentWriterBlocked = true;
      }
      return snapshot;
    };

    expect(
      target.applySchemaManifest({
        fgraph: "schema/1",
        attributes: [],
        shapes: [],
      }).status,
    ).toBe("applied");
    expect(concurrentWriterBlocked).toBe(true);
    expect(schemaManifest().attributes).toEqual([]);
  });

  it("rejects malformed control-plane documents at the boundary", () => {
    using db = connect(":memory:");
    const malformed: unknown[] = [
      null,
      { fgraph: "schema/2" },
      { fgraph: "schema/1", extra: true },
      { fgraph: "schema/1", attributes: {}, shapes: [] },
      { fgraph: "schema/1", attributes: [], shapes: {} },
      { fgraph: "schema/1", attributes: ["bad"] },
      { fgraph: "schema/1", attributes: [{ name: 1, declared: {} }] },
      { fgraph: "schema/1", attributes: [{ name: "item/id", declared: [] }] },
      {
        fgraph: "schema/1",
        attributes: [
          { name: "item/id", declared: { type: "text" } },
          { name: "item/id", declared: { type: "text" } },
        ],
      },
      {
        fgraph: "schema/1",
        attributes: [{ name: "item/id", declared: { other: 1 } }],
      },
      {
        fgraph: "schema/1",
        attributes: [{ name: "item/id", declared: { type: "unknown" } }],
      },
      {
        fgraph: "schema/1",
        attributes: [{ name: "item/id", declared: { many: 1 } }],
      },
      {
        fgraph: "schema/1",
        attributes: [{ name: "item/id", declared: { dims: true } }],
      },
      {
        fgraph: "schema/1",
        attributes: [{ name: "item/id", declared: { doc: 1 } }],
      },
      {
        fgraph: "schema/1",
        attributes: [{ name: "item/id", declared: { vector_model: " " } }],
      },
      { fgraph: "schema/1", shapes: ["bad"] },
      {
        fgraph: "schema/1",
        shapes: [{ name: 1, required: [], allowed: [], closed: false }],
      },
      {
        fgraph: "schema/1",
        shapes: [
          {
            name: "shape/item",
            required: [],
            allowed: [],
            closed: false,
          },
          {
            name: "shape/item",
            required: [],
            allowed: [],
            closed: false,
          },
        ],
      },
      {
        fgraph: "schema/1",
        shapes: [
          {
            name: "shape/item",
            required: "item/id",
            allowed: [],
            closed: false,
          },
        ],
      },
      {
        fgraph: "schema/1",
        shapes: [
          {
            name: "shape/item",
            required: [],
            allowed: [1],
            closed: false,
          },
        ],
      },
    ];
    malformed.forEach((manifest) =>
      expect(() => db.checkSchemaManifest(manifest)).toThrowError(SchemaError),
    );
  });

  it("normalizes ordering and reports deterministic drift", () => {
    using db = connect(":memory:");
    const check = db.checkSchemaManifest({
      fgraph: "schema/1",
      attributes: [
        { name: "item/tags", declared: { many: true, type: "text" } },
        { name: "item/id", declared: {} },
      ],
      shapes: [
        {
          name: "shape/item",
          required: ["item/id", "item/id"],
          allowed: ["item/tags"],
          closed: true,
        },
      ],
    });
    expect(check.valid).toBe(false);
    expect(check.changes).toEqual([
      {
        kind: "attribute",
        name: "item/tags",
        before: null,
        after: { many: true, type: "text" },
      },
      {
        kind: "shape",
        name: "shape/item",
        before: null,
        after: {
          name: "shape/item",
          required: ["item/id"],
          allowed: ["item/id", "item/tags"],
          closed: true,
        },
      },
    ]);
  });

  it("rejects an invalid internal request digest before mutation", () => {
    using db = connect(":memory:");
    expect(() =>
      db.transact([], { _requestHashOverride: Buffer.alloc(31) }),
    ).toThrowError(TypeError);
    expect(db.stats().transactions).toBe(1);
  });
});

describe("file lifecycle", () => {
  it("rejects missing, uninitialized, marked, partial, corrupt, and incomplete files", () => {
    const directory = temporaryDirectory();
    expect(() => connect(":memory:", { readOnly: true })).toThrowError(
      ReadOnly,
    );
    expect(() =>
      connect(join(directory, "missing.db"), { readOnly: true }),
    ).toThrowError(NotFound);

    const empty = join(directory, "empty.db");
    new Database(empty).close();
    expect(() => connect(empty, { readOnly: true })).toThrowError(FormatError);

    const marked = join(directory, "marked.db");
    const markedSqlite = new Database(marked);
    markedSqlite.pragma("application_id = 7");
    markedSqlite.close();
    expect(() => connect(marked)).toThrowError(FormatError);

    const partial = join(directory, "partial.db");
    const partialSqlite = new Database(partial);
    partialSqlite.exec("CREATE TABLE fgraph_partial(value INTEGER)");
    partialSqlite.close();
    expect(() => connect(partial)).toThrowError(FormatError);

    const garbage = join(directory, "garbage.db");
    writeFileSync(garbage, "not sqlite");
    expect(() => connect(garbage)).toThrowError(FormatError);

    const mismatched = join(directory, "mismatched.db");
    connect(mismatched, { clock: 1_767_225_600_000_000n }).close();
    const mismatchSqlite = new Database(mismatched);
    mismatchSqlite.pragma("user_version = 1");
    mismatchSqlite.close();
    expect(() => connect(mismatched)).toThrowError(FormatError);

    const incomplete = join(directory, "incomplete.db");
    connect(incomplete, { clock: 1_767_225_600_000_000n }).close();
    const incompleteSqlite = new Database(incomplete);
    incompleteSqlite.exec("DROP VIEW fgraph_now");
    incompleteSqlite.close();
    expect(() => connect(incomplete)).toThrowError(FormatError);
  });

  it("uses clocks/env, read-only files, cache refresh, and idempotent close", () => {
    const directory = temporaryDirectory();
    const path = join(directory, "graph.db");
    process.env.FGRAPH_CLOCK = "1767225600000000";
    const writer = connect(path);
    writer.add({ id: "entity/one", "entity/value": 1 });
    using reader = connect(path, { readOnly: true });
    expect(reader.entity("entity/one")).toEqual({ "entity/value": 1 });
    expect(() =>
      reader.transact({ id: "entity/two", "entity/value": 2 }),
    ).toThrowError(ReadOnly);
    writer.transact({ id: "entity/two", "entity/value": 2 });
    expect(reader.entity("entity/two")).toEqual({ "entity/value": 2 });
    writer.close();
    writer.close();
    expect(() => writer.stats()).toThrowError(FormatError);

    process.env.FGRAPH_CLOCK = "bad";
    expect(() => connect(":memory:")).toThrowError(TypeError);
    delete process.env.FGRAPH_CLOCK;
    using dynamic = connect(":memory:", {
      clock: () => 1_767_225_600_000_000n,
    });
    expect(dynamic.stats().transactions).toBe(1);
  });
});

describe("transaction boundary and schema", () => {
  it("returns ordinary JavaScript numbers inside stored JSON values", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.add({
      id: "json/entity",
      "json/value": { json: { nested: [1.5, { value: -2.25 }] } },
    });
    const stored = db.entity("json/entity")["json/value"];
    expect(stored).toEqual({
      json: { nested: [1.5, { value: -2.25 }] },
    });
    expect(JSON.stringify(stored)).toBe(
      '{"json":{"nested":[1.5,{"value":-2.25}]}}',
    );
  });

  it("treats COMMIT as the success boundary and publishes names lazily", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const connection = db._connection;
    const originalPrepare = connection.prepare.bind(connection);
    Object.defineProperty(connection, "prepare", {
      configurable: true,
      value: (source: string) => {
        if (
          source === "SELECT id, name FROM fgraph_ids" &&
          !connection.inTransaction
        )
          throw new Error("injected post-commit cache failure");
        return originalPrepare(source);
      },
    });
    let report;
    try {
      report = db.transact({ id: "committed", "item/value": 1 });
    } finally {
      Reflect.deleteProperty(connection, "prepare");
    }

    expect(report.tx).not.toBeNull();
    expect(db.entity("committed")).toEqual({ "item/value": 1 });
  });

  it("does not rescan the identity registry after its own writes", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const connection = db._connection;
    const originalPrepare = connection.prepare.bind(connection);
    let registryScans = 0;
    Object.defineProperty(connection, "prepare", {
      configurable: true,
      value: (source: string) => {
        if (source.startsWith("SELECT id,name,gid,created_tx FROM fgraph_ids"))
          registryScans++;
        return originalPrepare(source);
      },
    });
    try {
      for (let index = 0; index < 10; index++)
        db.transact({ id: `cached/${index}`, "item/value": index });
    } finally {
      Reflect.deleteProperty(connection, "prepare");
    }
    expect(registryScans).toBe(0);
  });

  it("applies many events without rescanning every prior identity", () => {
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    for (let index = 0; index < 10; index++)
      source.transact({ id: `event/${index}`, "item/value": index });
    const lines = (source.tail() as string).trim().split("\n");

    using target = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const connection = target._connection;
    const originalPrepare = connection.prepare.bind(connection);
    let registryScans = 0;
    let preparedStatements = 0;
    Object.defineProperty(connection, "prepare", {
      configurable: true,
      value: (statement: string) => {
        preparedStatements++;
        if (
          statement.startsWith("SELECT id,name,gid,created_tx FROM fgraph_ids")
        )
          registryScans++;
        return originalPrepare(statement);
      },
    });
    let summary: ReturnType<typeof target.applySummary>;
    try {
      summary = target.applySummary(lines);
    } finally {
      Reflect.deleteProperty(connection, "prepare");
    }
    expect(summary).toMatchObject({ events: 10, applied: 10 });
    expect(registryScans).toBe(0);
    expect(preparedStatements).toBeLessThan(300);
  });

  it("loads touched entities in bounded pages during large transactions", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const connection = db._connection;
    const originalPrepare = connection.prepare.bind(connection);
    let pairLoads = 0;
    let batchedLoads = 0;
    Object.defineProperty(connection, "prepare", {
      configurable: true,
      value: (source: string) => {
        if (
          source ===
          "SELECT * FROM fgraph_facts WHERE e=? AND a=? AND rx IS NULL ORDER BY id"
        )
          pairLoads++;
        if (
          source.startsWith(
            "SELECT * FROM fgraph_facts WHERE rx IS NULL AND e IN (",
          )
        )
          batchedLoads++;
        return originalPrepare(source);
      },
    });
    try {
      db.transact(
        Array.from({ length: 500 }, (_, index) => ({
          id: `bulk/${index}`,
          "bulk/value": index,
        })),
      );
    } finally {
      Reflect.deleteProperty(connection, "prepare");
    }
    expect(pairLoads).toBe(0);
    expect(batchedLoads).toBe(2);
  });

  it("validates names, attributes, selectors, operations, and provenance", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    for (const data of [
      { id: "", "x/value": 1 },
      { id: "x".repeat(513), "x/value": 1 },
      { id: "bad\nname", "x/value": 1 },
      { id: "\uD800", "x/value": 1 },
      { id: "fgraph/private", "x/value": 1 },
      { id: "ok", invalid: 1 },
      { id: "ok", "a/b/c": 1 },
      { id: "ok", "fgraph/private": 1 },
      { id: true, "x/value": 1 },
      { id: { tmp: "" }, "x/value": 1 },
      { id: { bad: 1 }, "x/value": 1 },
      null,
      [1],
      ["assert"],
      ["retract"],
      ["retract", "ok", "x/value", 1, 2],
      ["assert", "ok", 1, 2],
      ["assert", true, "x/value", 1],
    ] as unknown[])
      expect(() => db.transact(data)).toThrowError();
    expect(() =>
      db.transact({ id: "ok", "x/value": 1 }, { by: 1 as never }),
    ).toThrowError(TypeError);
    expect(() =>
      db.transact({ id: "ok", "x/value": 1 }, { source: 1 as never }),
    ).toThrowError(TypeError);
    expect(() => db.retract("ok", undefined, 1)).toThrowError(TypeError);
    expect(db.retract("unknown").tx).toBeNull();
    expect(db.transact(["retract", "unknown", "unknown/value"]).tx).toBeNull();
    expect(db.transact({}).tx).toBeNull();
    expect(db.transact({ id: "factless" })).toMatchObject({
      status: "applied",
      tx: expect.any(Number),
    });
    expect(db.transact({ id: "factless" }).tx).toBeNull();
  });

  it("supports lookups, nested refs, ids, tempids, and rejects conflicting identity", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("person/email", { type: "text", unique: true });
    db.declare("person/friend", { ref: true });
    const report = db.transact([
      { id: { tmp: "ada" }, "person/email": "ada@example.test" },
      {
        id: "person/grace",
        "person/email": "grace@example.test",
        "person/friend": { id: { tmp: "ada" }, "person/name": "Ada" },
      },
    ]);
    expect(report.ids.ada).toBeDefined();
    expect(db.entity(["person/email", "ada@example.test"])).toMatchObject({
      "person/email": "ada@example.test",
      "person/name": "Ada",
    });
    expect(db.entity(report.ids.ada as number)).toMatchObject({
      "person/name": "Ada",
    });
    expect(
      db.pull("person/grace", [{ "person/friend": ["person/name"] }]),
    ).toEqual({ "person/friend": { "person/name": "Ada" } });
    expect(() => db.entity(true as never)).toThrowError(TypeError);
    expect(() => db.entity({ bad: true } as never)).toThrowError(TypeError);
    expect(() => db.entity(Number.MAX_SAFE_INTEGER + 1)).toThrowError(
      TypeError,
    );
    expect(() => db.entity([1 as never, "x"])).toThrowError(SchemaError);
    expect(() => db.entity(["missing/value", "x"])).toThrowError(NotFound);
    expect(() =>
      db.entity(["person/friend", { ref: "person/grace" }]),
    ).toThrowError(SchemaError);
    expect(() => db.entity(["person/email", "missing"])).toThrowError(NotFound);
    expect(() => db.transact(["assert", 999, "x/value", 1])).toThrowError(
      NotFound,
    );
    expect(() =>
      db.transact(["assert", ["person/email", "missing"], "x/value", 1]),
    ).toThrowError(NotFound);
    expect(() =>
      db.transact([
        { id: "person/one", "person/email": "ada@example.test" },
        { id: "person/two", "person/email": "grace@example.test" },
      ]),
    ).toThrowError(Conflict);
    expect(() =>
      db.transact({ id: { tmp: "same" }, "person/email": "ada@example.test" }),
    ).not.toThrow();
    expect(() =>
      db.transact([
        { id: { tmp: "same" }, "person/email": "ada@example.test" },
        { id: { tmp: "same" }, "person/email": "grace@example.test" },
      ]),
    ).toThrowError(Conflict);
  });

  it("validates declaration patches against live data", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    for (const operation of [
      () => db.declare("bad", { type: "text" }),
      () => db.declare("x/value"),
      () => db.declare("x/value", { ref: true, type: "text" }),
      () => db.declare("x/value", { type: "unknown" }),
      () => db.declare("x/value", { type: "text", dims: 2 }),
      () => db.declare("x/value", { type: "vector", dims: 0 }),
      () => db.declare("x/value", { unique: true }),
      () => db.transact({ id: "x/value", "fgraph/type": "unknown" }),
    ])
      expect(operation).toThrowError(SchemaError);

    db.transact({ id: "typed/one", "typed/value": "text" });
    expect(() => db.declare("typed/value", { type: "int" })).toThrowError(
      SchemaError,
    );
    db.declare("many/value", { many: true });
    db.transact({ id: "many/one", "many/value": [1, 2] });
    expect(() => db.declare("many/value", { many: false })).toThrowError(
      SchemaError,
    );
    db.transact([
      { id: "unique/one", "unique/value": "same" },
      { id: "unique/two", "unique/value": "same" },
    ]);
    db.declare("unique/value", { type: "text" });
    expect(() => db.declare("unique/value", { unique: true })).toThrowError(
      SchemaError,
    );
    expect(() =>
      db.declare("json/value", { type: "json", unique: true }),
    ).toThrowError(SchemaError);
    expect(() =>
      db.declare("vector/unique", { type: "vector", unique: true }),
    ).toThrowError(SchemaError);
    db.transact({ id: "vector/one", "vector/value": { vector: [1, 2] } });
    expect(() => db.declare("vector/value", { dims: 3 })).toThrowError(
      SchemaError,
    );
    expect(() =>
      db.transact({ id: "vector/two", "vector/value": { vector: [1, 2, 3] } }),
    ).toThrowError(TypeError);

    db.declare("documented/vector", { doc: "An inferred embedding" });
    db.transact({
      id: "documented/one",
      "documented/vector": { vector: [1, 2] },
    });
    expect(db.entity("documented/vector")).toMatchObject({
      "fgraph/type": "vector",
      "fgraph/dims": 2,
      "fgraph/doc": "An inferred embedding",
    });
    expect(() =>
      db.transact({
        id: "documented/two",
        "documented/vector": { vector: [1, 2, 3] },
      }),
    ).toThrowError(TypeError);
    expect(() =>
      db.transact({
        id: "nested",
        "nested/value": { id: "child", "child/value": 1 },
      }),
    ).toThrowError(TypeError);
  });

  it("validates transaction-entity facts and preserves explicit metadata null", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("audit/tag", { many: true });
    const report = db.transact(
      { id: "thing", "thing/value": 1 },
      {
        by: "tester",
        source: "unit",
        meta: null,
        tx: { "audit/tag": ["a", "b"] },
      },
    );
    expect(db.entity(report.tx as number)).toMatchObject({
      "fgraph/by": "tester",
      "fgraph/source": "unit",
      "fgraph/meta": { json: null },
      "audit/tag": ["a", "b"],
    });
    expect(() => db.transact([], { tx: { id: "bad" } })).toThrowError(
      SchemaError,
    );
    expect(() => db.transact([], { tx: { "fgraph/at": 1 } })).toThrowError(
      SchemaError,
    );
    expect(() =>
      db.transact([], { tx: { "single/value": [1, 2] } }),
    ).toThrowError(Conflict);
    expect(() =>
      db.transact([], { tx: { "nested/value": { id: "child" } } }),
    ).toThrowError(TypeError);
    db.declare("audit/id", { type: "text", unique: true });
    db.transact({ id: "owner", "audit/id": "taken" });
    expect(() => db.transact([], { tx: { "audit/id": "taken" } })).toThrowError(
      Conflict,
    );
    expect(() =>
      db.transact(["assert", report.tx, "thing/value", 2]),
    ).toThrowError(Unsupported);
    expect(() => db.retract(1)).toThrowError(Unsupported);
  });
});

describe("pull and temporal maintenance", () => {
  it("validates pull patterns and traverses cycles and reverse refs", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("node/next", { ref: true });
    db.transact([
      { id: "node/a", "node/name": "A", "node/next": { ref: "node/b" } },
      { id: "node/b", "node/name": "B", "node/next": { ref: "node/a" } },
    ]);
    expect(db.entity("node/a", 2)).toMatchObject({
      "node/next": { "node/name": "B", "node/next": { ref: "node/a" } },
    });
    expect(db.pull("node/a", ["node/_next"])).toEqual({
      "node/_next": [{ ref: "node/b" }],
    });
    expect(db.pull("node/a", ["missing/_next"])).toEqual({
      "missing/_next": [],
    });
    expect(
      db._pullEntity(
        db._resolveRead("node/a") as bigint,
        ["node/_next"],
        2,
        new Set(),
      ),
    ).toMatchObject({
      "node/_next": [expect.objectContaining({ "node/name": "B" })],
    });
    for (const pattern of [
      null,
      [1],
      ["bad"],
      [{}],
      [{ "node/_next": ["*"] }],
      [{ bad: ["*"] }],
      [{ "missing/ref": ["*"] }],
      [{ "node/name": ["*"] }],
    ] as unknown[]) {
      expect(() => db.pull("node/a", pattern as unknown[])).toThrowError(
        QueryError,
      );
    }
    expect(() => db.entity("node/a", -1)).toThrowError(QueryError);
    expect(() => db.entity("node/a", 1.5)).toThrowError(QueryError);
  });

  it("supports changes, speculation, follow, undo no-op, and historical guards", async () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const first = db.transact(
      { id: "thing", "thing/value": 1 },
      { source: "first" },
    );
    const second = db.transact(
      { id: "thing", "thing/value": 2 },
      { source: "second" },
    );
    expect(db.changes(first.tx as number)).toEqual(
      db.diff(first.tx as number, second.tx as number),
    );
    expect(
      db.speculate((candidate) => {
        candidate.transact({ id: "thing", "thing/value": 3 });
        expect(candidate.entity("thing")["thing/value"]).toBe(3);
        return "rolled back";
      }),
    ).toBe("rolled back");
    expect(db.entity("thing")["thing/value"]).toBe(2);
    expect(() =>
      db.speculate((candidate) => candidate.speculate(() => 1)),
    ).toThrowError(Unsupported);
    expect(() =>
      db.speculate(() => {
        throw new Error("boom");
      }),
    ).toThrow("boom");
    expect(db.entity("thing")["thing/value"]).toBe(2);

    const metadataOnly = db.transact([], { source: "audit" });
    const metadataUndo = db.undo(metadataOnly.tx as number, {
      by: "mcp:test-client",
    });
    expect(metadataUndo).toMatchObject({
      status: "applied",
      tx: expect.any(Number),
    });
    expect(db.receipt(metadataUndo.tx as number)).toMatchObject({
      by: "mcp:test-client",
    });
    expect(() => db.undo(GENESIS_TX)).toThrowError(Unsupported);
    expect(() => db.undo(999_999)).toThrowError(NotFound);

    const controller = new AbortController();
    const follower = db.follow(GENESIS_TX, {
      interval: 1,
      signal: controller.signal,
    });
    expect((await follower.next()).value).toMatchObject({
      fgraph: "event/1",
      event: first.event,
    });
    controller.abort();
    expect((await follower.next()).done).toBe(true);
    const invalidFollower = db.follow(GENESIS_TX, { interval: 0 });
    await expect(invalidFollower.next()).rejects.toThrowError(TypeError);
    const historicalFollower = db.at(first.tx).follow(first.tx);
    await expect(historicalFollower.next()).rejects.toThrowError(Unsupported);
    expect(() => db.at({ instant: 0 })).toThrowError(NotFound);
    expect(() => db.at({ bad: 1 })).toThrowError(TypeError);
    expect(() =>
      db.at(second.tx).transact({ id: "x", "x/value": 1 }),
    ).toThrowError(ReadOnly);
  });

  it("plans undo after a concurrent reassertion", () => {
    const directory = temporaryDirectory();
    const path = join(directory, "undo-race.db");
    using target = connect(path, { clock: 1_767_225_600_000_000n });
    using concurrent = connect(path, { clock: 1_767_225_601_000_000n });
    const original = target.transact({
      id: "item",
      "undo/value": "kept",
    });
    const transact = target.transact.bind(target);
    let interleaved = false;
    target.transact = (data, options = {}) => {
      if (!interleaved) {
        interleaved = true;
        concurrent.retract("item", "undo/value", "kept");
        concurrent.transact({ id: "item", "undo/value": "kept" });
      }
      return transact(data, options);
    };

    const undone = target.undo(original.tx as number);

    expect(interleaved).toBe(true);
    expect(undone.status).toBe("applied");
    expect(target.entity("item")).toEqual({ "undo/value": "kept" });
  });

  it("excises subjects and inbound references while protecting system/transaction entities", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("node/link", { ref: true });
    const created = db.transact([
      { id: "subject", "subject/text": "sensitive".repeat(40) },
      { id: "owner", "node/link": { ref: "subject" } },
    ]);
    const report = db.excise("subject", {
      operationId: "erase-subject",
      ifBasisTx: created.tx as number,
    });
    expect(report.retracted.length).toBeGreaterThanOrEqual(2);
    expect(db.entity("subject")).toEqual({});
    expect(db.entity("owner")).toEqual({});
    db.transact({ id: "later", "later/value": true });
    expect(
      db.excise("subject", {
        operationId: "erase-subject",
        ifBasisTx: created.tx as number,
      }),
    ).toMatchObject({
      status: "already_applied",
      event: report.event,
      tx: report.tx,
      basis_tx: created.tx,
    });
    expect(db.receipt(report.tx as number)).toMatchObject({
      operation_id: "erase-subject",
      request_hash: expect.stringMatching(/^sha256:/u),
    });
    const events = (db.tail() as string)
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(events.find((event) => event.event === created.event)).toMatchObject(
      { redacted: true, event_hash: expect.any(String) },
    );
    expect(events.find((event) => event.event === report.event)).toMatchObject({
      redacted: true,
      redacts: [created.event],
    });
    expect(db.doctor()).toMatchObject({
      ok: true,
      unverifiable_event_hashes: 1,
    });

    using restored = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
    });
    restored.restore(db.snapshot() as string);
    expect(restored.entity("subject")).toEqual({});
    expect(restored.entity("owner")).toEqual({});
    expect(restored.doctor()).toMatchObject({ ok: true });
    expect(() =>
      db.excise("subject", {
        operationId: "erase-subject-again",
        ifBasisTx: db._basisTx(),
      }),
    ).toThrowError(Conflict);
    expect(db.doctor()).toMatchObject({ ok: true });
    expect(() =>
      db.excise("owner", {
        operationId: "erase-owner-stale",
        ifBasisTx: created.tx as number,
      }),
    ).toThrowError(Conflict);
    expect(() => db.excise("missing")).toThrowError(NotFound);
    expect(() => db.excise(1)).toThrowError(Unsupported);
    expect(() => db.excise(created.tx as number)).toThrowError(Unsupported);
  });

  it("purges payload-only nohistory values when excising their subject", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("secret/value", { type: "text", nohistory: true });
    const first = db.transact({ id: "secret/subject", "secret/value": "old" });
    const second = db.transact({ id: "secret/subject", "secret/value": "new" });

    const before = db._connection
      .prepare<[], { event_data: string }>(
        "SELECT event_data FROM fgraph_events WHERE event_data IS NOT NULL",
      )
      .all()
      .map((row) => row.event_data)
      .join("\n");
    expect(before).toContain('"old"');
    expect(before).toContain('"new"');

    const excision = db.excise("secret/subject");
    const after = db._connection
      .prepare<[], { event_data: string }>(
        "SELECT event_data FROM fgraph_events WHERE event_data IS NOT NULL",
      )
      .all()
      .map((row) => row.event_data)
      .join("\n");
    expect(after).not.toContain('"old"');
    expect(after).not.toContain('"new"');
    expect(db.tail()).toContain(
      canonicalJson({
        fgraph: "event/1",
        event: excision.event,
        at: excision.at,
        created: [],
        asserted: [],
        retracted: [],
        redacted: true,
        redacts: [first.event, second.event].sort(),
      }),
    );
    expect(db.doctor()).toMatchObject({
      ok: true,
      unverifiable_event_hashes: 2,
    });
  });

  it("excises user attributes and transaction-fact references as identities", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const declaration = db.declare("private/value", { type: "text" });
    const value = db.transact({
      id: "private/holder",
      "private/value": "secret",
    });
    const attribute = db._attributeId("private/value") as bigint;
    const erasedAttribute = db.excise("private/value");
    expect(db.entity("private/holder")).toEqual({});
    expect(
      db._connection
        .prepare<[bigint], { count: bigint }>(
          "SELECT count(*) count FROM fgraph_facts WHERE a=?",
        )
        .get(attribute)?.count,
    ).toBe(0n);
    expect(db.receipt(erasedAttribute.tx as number)).toMatchObject({
      tx: erasedAttribute.tx,
    });
    expect(db.tail()).not.toContain('"secret"');
    expect(db.doctor()).toMatchObject({
      ok: true,
      unverifiable_event_hashes: 2,
    });
    expect([declaration.event, value.event]).toHaveLength(2);

    db.declare("audit/subject", { ref: true });
    db.transact({ id: "audit/target", "audit/value": true });
    const txFact = db.transact([], {
      tx: { "audit/subject": { ref: "audit/target" } },
    });
    db.excise("audit/target");
    const retained = db._connection
      .prepare<[number], { event_data: string | null }>(
        "SELECT event_data FROM fgraph_events WHERE tx=?",
      )
      .get(txFact.tx as number);
    expect(retained?.event_data).toBeNull();
    expect(db.doctor()).toMatchObject({ ok: true });

    const anonymous = db.transact({ "anonymous/value": "private" });
    const anonymousId = anonymous.asserted.find(
      (fact) => fact.a === "anonymous/value",
    )?.e as number | bigint;
    db.excise(anonymousId);
    expect(db.entity(anonymousId)).toEqual({});
    expect(db.tail()).not.toContain('"private"');
    expect(db.doctor()).toMatchObject({ ok: true });
  });
});

describe("backup, discovery, and doctor", () => {
  it("handles safe backup destinations", async () => {
    const directory = temporaryDirectory();
    const path = join(directory, "graph.db");
    using db = connect(path, { clock: 1_767_225_600_000_000n });
    db.transact({ id: "one", "thing/value": 1 });
    db.transact({ id: "two", "thing/value": 2 });
    await expect(db.backup(path)).rejects.toThrowError(Conflict);
    const nonempty = join(directory, "nonempty.db");
    writeFileSync(nonempty, "occupied");
    await expect(db.backup(nonempty)).rejects.toThrowError(Conflict);
    const empty = join(directory, "empty.db");
    writeFileSync(empty, "");
    await expect(db.backup(empty)).rejects.toThrowError(Conflict);
    await expect(
      db.backup(join(directory, "missing", "backup.db")),
    ).rejects.toThrowError(FormatError);
  });

  it("discovers attributes and repairs orphaned derived data", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("doc/text", {
      type: "text",
      many: true,
      unique: false,
      nohistory: true,
      doc: "Words",
    });
    db.transact({ id: "doc/one", "doc/text": ["one", "two"] });
    expect(db.attributes("doc/")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "doc/text",
          types: ["text"],
          facts: 2,
          many: true,
          nohistory: true,
          doc: "Words",
        }),
      ]),
    );
    expect(
      db
        .attributes(undefined, { includeSystem: true })
        .some((item) => item.name === "fgraph/type"),
    ).toBe(true);
    expect(() =>
      db.attributes(undefined, { includeSystem: "yes" as never }),
    ).toThrowError(TypeError);
    expect(() => db.doctor({ repair: "yes" as never })).toThrowError(TypeError);
    db._connection
      .prepare("INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)")
      .run(Buffer.alloc(32, 7), Buffer.from("orphan"));
    expect(db.doctor()).toMatchObject({
      ok: false,
      repair_needed: true,
      orphaned_blobs: 1,
      repaired: false,
    });
    expect(db.doctor({ repair: true })).toMatchObject({
      ok: true,
      orphaned_blobs: 0,
      orphaned_blobs_removed: 1,
      repaired: true,
    });
  });

  it.each(["next_id", "created_at", "dangling", "missing_blob", "interval"])(
    "detects fatal doctor corruption: %s",
    (kind) => {
      using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
      const report = db.transact({ id: "doc", "doc/text": "x".repeat(257) });
      if (kind === "next_id")
        db._connection
          .prepare("UPDATE fgraph_meta SET value=1 WHERE key='next_id'")
          .run();
      else if (kind === "created_at")
        db._connection
          .prepare("UPDATE fgraph_meta SET value=1 WHERE key='created_at'")
          .run();
      else if (kind === "dangling")
        db._connection
          .prepare(
            "UPDATE fgraph_facts SET a=999999 WHERE id=(SELECT max(id) FROM fgraph_facts)",
          )
          .run();
      else if (kind === "missing_blob")
        db._connection.exec("DELETE FROM fgraph_blobs");
      else {
        db._connection.pragma("ignore_check_constraints = ON");
        db._connection
          .prepare(
            "UPDATE fgraph_facts SET rx=tx WHERE id=(SELECT max(id) FROM fgraph_facts WHERE e<>?)",
          )
          .run(report.tx);
        db._connection.pragma("ignore_check_constraints = OFF");
      }
      expect(db.doctor()).toMatchObject({
        ok: false,
        repaired: false,
        problems: expect.any(Array),
      });
      expect(() => db.doctor({ repair: true })).toThrowError(FormatError);
    },
  );
});
