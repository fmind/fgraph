import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  Conflict,
  FormatError,
  NotFound,
  QueryError,
  SchemaError,
  TooLarge,
  TypeError,
} from "../src/errors.js";
import { JsonFloat, canonicalJson, parseJson } from "../src/jsonio.js";
import { evaluate } from "../src/query.js";
import { GENESIS_TX, MAX_EVENT_BYTES, connect } from "../src/store.js";
import { INT64_MAX } from "../src/values.js";

const directories: string[] = [];

function temporaryDirectory(): string {
  const path = mkdtempSync(join(tmpdir(), "fgraph-edge-"));
  directories.push(path);
  return path;
}

afterEach(() => {
  while (directories.length > 0)
    rmSync(directories.pop() as string, { recursive: true, force: true });
  delete process.env.FGRAPH_EVENT_SEED;
});

describe("query decision boundaries", () => {
  it("covers lossless equality, every numeric pairing, text order, and duplicate branches", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("item/value", { many: true });
    db.transact([
      {
        id: "item/a",
        "item/bytes": { bytes: "YQ==" },
        "item/vector": { vector: [1, 2] },
        "item/text": "beta",
        "item/value": [
          2,
          new JsonFloat(2),
          new JsonFloat(2.5),
          9_223_372_036_854_775_807n,
        ],
      },
      {
        id: "item/b",
        "item/bytes": { bytes: "Yg==" },
        "item/vector": { vector: [2, 1] },
        "item/text": "alpha",
        "item/value": [1, new JsonFloat(3)],
      },
    ]);

    const equal = (attribute: string, value: unknown): unknown[][] =>
      db.q({
        find: ["?e"],
        where: [
          ["?e", attribute, "?v"],
          ["=", "?v", value],
        ],
        order: [["?e", "asc"]],
      }).rows;
    expect(equal("item/bytes", { bytes: "YQ==" })).toEqual([
      [{ ref: "item/a" }],
    ]);
    expect(equal("item/bytes", { bytes: "eg==" })).toEqual([]);
    expect(equal("item/vector", { vector: [1, 2] })).toEqual([
      [{ ref: "item/a" }],
    ]);
    expect(equal("item/vector", { vector: [1, 3] })).toEqual([]);

    for (const [left, operator, right, count] of [
      [9_223_372_036_854_775_807n, ">", 9_223_372_036_854_775_806n, 1],
      [9_223_372_036_854_775_807n, "=", 9_223_372_036_854_775_807n, 1],
      [1, "<", 2n, 1],
      [new JsonFloat(2), "=", 2n, 1],
      [new JsonFloat(2), "<", 3n, 1],
      [2n, "=", new JsonFloat(2), 1],
      [2n, "<", new JsonFloat(2.5), 1],
      [new JsonFloat(2.5), ">", 2n, 1],
      [new JsonFloat(2.5), "<", 3n, 1],
      [new JsonFloat(3), ">", new JsonFloat(2), 1],
    ] as Array<[unknown, string, unknown, number]>) {
      expect(
        db.q(
          {
            find: [["count", "?left"]],
            in: ["?left", "?right"],
            where: [[operator, "?left", "?right"]],
          },
          { "?left": left, "?right": right },
        ).rows,
      ).toEqual([[count]]);
    }

    for (const [operator, expected] of [
      ["<", ["alpha"]],
      ["<=", ["alpha", "beta"]],
      [">", []],
      [">=", ["beta"]],
    ] as Array<[string, string[]]>) {
      expect(
        db
          .q({
            find: ["?text"],
            where: [
              ["?e", "item/text", "?text"],
              [operator, "?text", "beta"],
            ],
            order: [["?text", "asc"]],
          })
          .rows.flat(),
      ).toEqual(expected);
    }
    expect(
      db.q({
        find: ["?e"],
        where: [
          {
            or: [
              [["?e", "item/text", "alpha"]],
              [["?e", "item/text", "alpha"]],
            ],
          },
        ],
      }).rows,
    ).toEqual([[{ ref: "item/b" }]]);
  });

  it("validates late clause, rule, aggregate, and work-budget paths", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact({ id: "item/a", "item/name": "A" });
    expect(() =>
      evaluate(
        db,
        { find: ["?e"], where: [["?e", "item/name", "_"]] },
        {},
        { budget: 0, signal: undefined },
      ),
    ).toThrowError(QueryError);

    const invalidWhere = [
      [
        ["?e", "item/name", "?name"],
        ["=", "?name"],
      ],
      [["?e", "item/name", "?name"], { not: "bad" }],
      [["?e", "item/name", "?name"], { or: "bad" }],
      [["?e", "item/name", "?name"], { rule: "bad" }],
      [["?e", "item/name", "?name"], { unknown: [] }],
      [["?e", "item/name", "?name"], 1],
    ];
    for (const where of invalidWhere)
      expect(() => db.q({ find: ["?e"], where, rules: [] })).toThrowError(
        QueryError,
      );

    for (const rules of [
      "bad",
      [{}],
      [{ head: [1, "?x"], body: [] }],
      [{ head: ["r", "x"], body: [] }],
      [
        { head: ["r", "?x"], body: [] },
        { head: ["r", "?x", "?y"], body: [] },
      ],
      [{ head: ["r", "?x"], body: [{ rule: "bad" }] }],
      [{ head: ["r", "?x"], body: [{ rule: ["missing", "?x"] }] }],
      [{ head: ["r", "?x"], body: [{ rule: ["r"] }] }],
      [
        { head: ["a", "?x"], body: [{ rule: ["b", "?x"] }] },
        { head: ["b", "?x"], body: [{ rule: ["c", "?x"] }] },
        { head: ["c", "?x"], body: [{ rule: ["a", "?x"] }] },
      ],
    ]) {
      expect(() =>
        db.q({ find: ["?x"], in: ["?x"], where: [], rules }, { "?x": 1 }),
      ).toThrowError(QueryError);
    }
    expect(() => db.q({ find: [["count"]], where: [] })).toThrowError(
      QueryError,
    );
  });

  it("computes recursive rules, rejects unbound heads, grouping, min/max, and float sums", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("edge/to", { ref: true, many: true });
    db.transact([
      {
        id: "node/a",
        "edge/to": { ref: "node/b" },
        "group/name": "x",
        "score/value": 3,
      },
      {
        id: "node/b",
        "edge/to": { ref: "node/c" },
        "group/name": "x",
        "score/value": 1,
      },
      { id: "node/c", "group/name": "y", "score/value": 2 },
      { id: "float/a", "float/value": new JsonFloat(1.5) },
      { id: "float/b", "float/value": new JsonFloat(2.5) },
    ]);
    const rules = [
      { head: ["reach", "?from", "?to"], body: [["?from", "edge/to", "?to"]] },
      {
        head: ["reach", "?from", "?to"],
        body: [
          ["?from", "edge/to", "?middle"],
          { rule: ["reach", "?middle", "?to"] },
        ],
      },
    ];
    expect(
      db.q({
        find: ["?to"],
        where: [{ rule: ["reach", "node/a", "?to"] }],
        rules,
        order: [["?to", "asc"]],
      }).rows,
    ).toEqual([[{ ref: "node/b" }], [{ ref: "node/c" }]]);
    expect(
      db.q({
        find: ["?node"],
        where: [{ rule: ["top", "?node"] }],
        rules: [
          {
            head: ["base", "?node"],
            body: [["?node", "group/name", "x"]],
          },
          {
            head: ["middle", "?node"],
            body: [{ rule: ["base", "?node"] }],
          },
          {
            head: ["top", "?node"],
            body: [{ rule: ["middle", "?node"] }],
          },
        ],
        order: [["?node", "asc"]],
      }).rows,
    ).toEqual([[{ ref: "node/a" }], [{ ref: "node/b" }]]);
    expect(() =>
      db.q({
        find: [["count", "?x"]],
        where: [{ rule: ["never", "?x"] }],
        rules: [
          ...rules,
          {
            head: ["never", "?missing"],
            body: [["?e", "group/name", "x"]],
          },
        ],
      }),
    ).toThrowError(QueryError);
    expect(
      db.q({
        find: [
          "?group",
          ["count", "?score"],
          ["min", "?score"],
          ["max", "?score"],
        ],
        where: [
          ["?e", "group/name", "?group"],
          ["?e", "score/value", "?score"],
        ],
        order: [["?group", "asc"]],
      }).rows,
    ).toEqual([
      ["x", 2, 1, 3],
      ["y", 1, 2, 2],
    ]);
    expect(
      db.q({
        find: [
          ["sum", "?value"],
          ["avg", "?value"],
        ],
        where: [["?e", "float/value", "?value"]],
      }).rows,
    ).toEqual([[4, 2]]);
  });

  it("rejects every pattern mismatch stage and evaluates empty and mixed projections", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("item/value", { many: true });
    db.transact({
      id: "item/one",
      "item/value": [1, new JsonFloat(2.5)],
    });

    expect(
      db.q(
        {
          find: ["?value"],
          in: ["?entity"],
          where: [["?entity", "_", "?value"]],
        },
        { "?entity": "not-an-entity-reference" },
      ).rows,
    ).toEqual([]);
    for (const [clause, count] of [
      [["?same", "?same", "_"], 2],
      [["?same", "item/value", "?same"], 0],
      [["?same", "item/value", "_", "?same"], 0],
      [["?same", "item/value", "_", "_", "?same"], 0],
    ] as Array<[unknown[], number]>) {
      expect(
        db.q({ find: [["count", "?same"]], where: [clause] }).rows,
      ).toEqual([[count]]);
    }
    expect(
      db.q({
        find: ["?entity", "?tx", "?added"],
        where: [["?entity", "item/value", "_", "?tx", "?added"]],
      }).rows,
    ).toHaveLength(1);
    expect(
      db.q({ find: ["?input"], in: ["?input"] }, { "?input": 7 }).rows,
    ).toEqual([[7]]);
    expect(() =>
      db.q(
        { find: ["?input"], in: ["?input"] },
        { "?input": { ref: "missing/entity" } },
      ),
    ).toThrowError(QueryError);
    expect(() =>
      db.q({ find: ["?entity"], where: [["?entity", "item/value"]] }),
    ).toThrowError(QueryError);
    expect(() =>
      db.q({ find: ["?entity"], where: [["?entity", "bad", "_"]] }),
    ).toThrowError(QueryError);

    for (const [left, operator, right, expected] of [
      [new JsonFloat(2), "=", new JsonFloat(2), 1],
      [new JsonFloat(2), "<", new JsonFloat(3), 1],
      [3n, ">", new JsonFloat(2.5), 1],
      [new JsonFloat(2.5), "<", 3n, 1],
      [new JsonFloat(2), ">=", new JsonFloat(3), 0],
    ] as Array<[unknown, string, unknown, number]>) {
      expect(
        db.q(
          {
            find: [["count", "?left"]],
            in: ["?left", "?right"],
            where: [[operator, "?left", "?right"]],
          },
          { "?left": left, "?right": right },
        ).rows,
      ).toEqual([[expected]]);
    }

    db.declare("sort/value", { many: true });
    db.transact({
      id: "sort/entity",
      "sort/value": [true, 1, "a", { json: { rank: 1 } }],
    });
    expect(
      db.q({
        find: ["?value"],
        where: [["sort/entity", "sort/value", "?value"]],
        order: [["?value", "asc"]],
      }).rows,
    ).toEqual([[true], [1], ["a"], [{ json: { rank: 1 } }]]);

    expect(
      db.q({
        find: [["sum", "?value"]],
        where: [["missing/entity", "item/value", "?value"]],
      }).rows,
    ).toEqual([[null]]);
    expect(() =>
      db.q({
        find: [["sum", "?value"]],
        where: [["sort/entity", "sort/value", "?value"]],
      }),
    ).toThrowError(QueryError);
  });
});

describe("store defensive and temporal paths", () => {
  it("covers lookup missing modes, selector forms, schema clearing, and pending compaction", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("identity/email", { type: "text", unique: true });
    const created = db.transact({
      id: "person/a",
      "identity/email": "a@example.test",
    });
    const id = db._resolveRead("person/a") as bigint;
    expect(db._resolveRead(["missing/value", 1], true)).toBeNull();
    expect(db._resolveRead(["identity/email", "missing"], true)).toBeNull();
    expect(() => db._resolveRead([1, "x"])).toThrowError(SchemaError);
    expect(() => db._attributeId("bad")).toThrowError(QueryError);
    expect(db.transact({ id }).tx).toBeNull();
    expect(
      db.transact({ id: ["identity/email", "a@example.test"] }).tx,
    ).toBeNull();
    expect(db.transact({ id: { tmp: "factless" } })).toMatchObject({
      tx: null,
      ids: {},
    });
    expect(() => db.transact({ id: 999_999 })).toThrowError(NotFound);
    expect(() =>
      db.transact({ id: ["identity/email", "missing"] }),
    ).toThrowError(NotFound);

    db.declare("schema/all", {
      type: "text",
      many: true,
      unique: true,
      nohistory: true,
      doc: "all",
    });
    db.transact([
      ["retract", "schema/all", "fgraph/many", true],
      ["retract", "schema/all", "fgraph/unique", true],
      ["retract", "schema/all", "fgraph/nohistory", true],
      ["retract", "schema/all", "fgraph/type", "text"],
      ["retract", "schema/all", "fgraph/doc", "all"],
    ]);
    expect(db.attributes("schema/")[0]).toMatchObject({
      many: false,
      unique: false,
      nohistory: false,
    });
    expect(
      db.transact([
        ["assert", { tmp: "drop" }, "pending/value", 1],
        ["retract", { tmp: "drop" }],
      ]),
    ).toMatchObject({
      status: "applied",
      tx: expect.any(Number),
      retracted: [],
    });
    expect(created.tx).toBeGreaterThan(GENESIS_TX);
  });

  it("applies pending schema facts and clears every declaration field before later values", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact([
      ["assert", "dynamic/value", "fgraph/type", "text"],
      ["assert", "dynamic/entity", "dynamic/value", "text"],
    ]);
    expect(db.entity("dynamic/entity")["dynamic/value"]).toBe("text");

    db.declare("schema/all", {
      type: "text",
      many: true,
      unique: true,
      nohistory: true,
      doc: "all",
    });
    db.transact([
      ["retract", "schema/all", "fgraph/many", true],
      ["retract", "schema/all", "fgraph/unique", true],
      ["retract", "schema/all", "fgraph/nohistory", true],
      ["retract", "schema/all", "fgraph/type", "text"],
      ["retract", "schema/all", "fgraph/doc"],
      ["assert", "schema/entity", "schema/all", 42],
    ]);
    expect(db.entity("schema/entity")["schema/all"]).toBe(42);

    db.declare("schema/vector", { type: "vector", dims: 2 });
    db.transact([
      ["retract", "schema/vector", "fgraph/dims", 2],
      ["retract", "schema/vector", "fgraph/type", "vector"],
      ["assert", "schema/vector-entity", "schema/vector", "now-untyped"],
    ]);
    expect(db.entity("schema/vector-entity")["schema/vector"]).toBe(
      "now-untyped",
    );

    db.declare("schema/reset", {
      type: "text",
      doc: "Reset in the same transaction",
    });
    db.transact([
      ["retract", "schema/reset"],
      ["assert", "schema/reset-target", "schema/reset", 42],
    ]);
    expect(db.entity("schema/reset-target")["schema/reset"]).toBe(42);
  });

  it("enforces every compare-and-swap boundary and supports deletion", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("item/tags", { many: true });
    db.transact({
      id: "cas/item",
      "item/state": "new",
      "item/tags": ["a", "b"],
      "item/optional": "present",
    });
    expect(() =>
      db.transact(["cas", "cas/item", "item/state", "new"]),
    ).toThrowError(TypeError);
    expect(() =>
      db.transact(["cas", "cas/item", "item/tags", "a", "c"]),
    ).toThrowError(SchemaError);
    expect(() =>
      db.transact([
        ["cas", "cas/item", "item/state", "new", "ready"],
        ["cas", "cas/item", "item/state", "new", "other"],
      ]),
    ).toThrowError(Conflict);
    db.transact([
      "cas",
      "cas/item",
      "item/optional",
      "present",
      { missing: true },
    ]);
    expect(db.entity("cas/item")).not.toHaveProperty("item/optional");

    const entity = db._resolveRead("cas/item") as bigint;
    const attribute = db._attributeId("item/state") as bigint;
    const tx = BigInt(db._basisTx());
    db._connection
      .prepare(
        "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,?,?,4,?,NULL)",
      )
      .run(entity, attribute, "duplicate", tx);
    expect(() =>
      db.transact(["cas", "cas/item", "item/state", "new", "ready"]),
    ).toThrowError(FormatError);
  });

  it("validates vector declarations and open, closed, and malformed shapes", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const vectorDeclaration = db.declare("embedding/value", {
      type: "vector",
      dims: 2,
      doc: "Embedding",
      vectorModel: "model/test",
      operationId: "declare-embedding",
      ifBasisTx: GENESIS_TX,
    });
    expect(vectorDeclaration.status).toBe("applied");
    expect(db.schema("embedding/").attributes[0]).toMatchObject({
      declared: {
        type: "vector",
        dims: 2,
        doc: "Embedding",
        vector_model: "model/test",
      },
      effective: {
        type: "vector",
        dims: 2,
        doc: "Embedding",
        vector_model: "model/test",
      },
    });
    db.transact([
      ["retract", "embedding/value", "fgraph/dims", 2],
      ["retract", "embedding/value", "fgraph/doc", "Embedding"],
      ["retract", "embedding/value", "fgraph/vector-model", "model/test"],
    ]);
    expect(db.schema("embedding/").attributes[0]).toMatchObject({
      effective: { dims: null, doc: null, vector_model: null },
    });
    const embedding = db._attributeId("embedding/value") as bigint;
    expect(db._vectorDims(embedding)).toBeNull();
    db.transact({
      id: "embedding/item",
      "embedding/value": { vector: [1, 2] },
    });
    expect(db._vectorDims(embedding)).toBe(2);

    for (const data of [
      { id: "bad/dims", "fgraph/dims": 2 },
      { id: "bad/model", "fgraph/vector-model": "model/test" },
      {
        id: "bad/vector-dims",
        "fgraph/type": "vector",
        "fgraph/dims": 0,
      },
    ])
      expect(() => db.transact(data)).toThrowError(SchemaError);
    expect(() =>
      db.declare("bad/ref", { ref: true, type: "text" }),
    ).toThrowError(SchemaError);
    expect(() => db.declare("bad/type", { type: "unknown" })).toThrowError(
      SchemaError,
    );
    expect(() =>
      db.declare("bad/nonvector", { type: "text", dims: 2 }),
    ).toThrowError(SchemaError);
    expect(() => db.declare("bad/empty")).toThrowError(SchemaError);

    db.declare("person/name", { type: "text" });
    db.defineShape("shape/open");
    expect(db.schema().shapes).toContainEqual(
      expect.objectContaining({
        name: "shape/open",
        required: [],
        allowed: [],
        closed: false,
      }),
    );
    db.defineShape("shape/required", { required: ["person/name"] });
    expect(() =>
      db.transact({
        id: "person/missing",
        "fgraph/shape": { ref: "shape/required" },
      }),
    ).toThrowError(SchemaError);
    expect(() =>
      db.transact([
        [
          "assert",
          "shape/invalid-closed",
          "fgraph/shape-required",
          { ref: "person/name" },
        ],
        ["assert", "shape/invalid-closed", "fgraph/shape-closed", true],
        [
          "assert",
          "shape/member",
          "fgraph/shape",
          { ref: "shape/invalid-closed" },
        ],
      ]),
    ).toThrowError(SchemaError);
    for (const options of [
      { required: "person/name" },
      { allowed: [1] },
      { closed: "yes" },
    ])
      expect(() =>
        db.defineShape("shape/bad-options", options as never),
      ).toThrowError(SchemaError);
  });

  it("rejects malformed operation identities and oversized events atomically", () => {
    using invalidEvent = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
      eventId: () => "not-a-uuid",
    });
    expect(() =>
      invalidEvent.transact({ id: "event/invalid", "event/value": 1 }),
    ).toThrowError(TypeError);
    expect(invalidEvent._basisTx()).toBe(Number(GENESIS_TX));

    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    for (const operationId of ["", "line\nbreak", "x".repeat(513), "\ud800"])
      expect(() =>
        db.transact(
          { id: "operation/invalid", "operation/value": 1 },
          { operationId },
        ),
      ).toThrowError(TypeError);
    expect(() =>
      db.transact({ id: "option/undefined", "option/value": 1 }, {
        by: undefined,
      } as never),
    ).toThrowError(TypeError);
    expect(() => db.apply("x".repeat(MAX_EVENT_BYTES + 1))).toThrowError(
      TooLarge,
    );

    process.env.FGRAPH_EVENT_SEED = "edge-coverage";
    using seeded = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const seededEvent = seeded.transact({
      id: "event/seeded",
      "event/value": 1,
    }).event;
    expect(seededEvent).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u,
    );
    delete process.env.FGRAPH_EVENT_SEED;

    const basis = db._basisTx();
    const huge: Record<string, unknown> = { id: "event/too-large" };
    for (let index = 0; index < 9; index++)
      huge[`event/value-${index}`] = "x".repeat(1_048_576);
    expect(() => db.transact(huge)).toThrowError(TooLarge);
    expect(db._basisTx()).toBe(basis);
  });

  it("checks receipt boundaries, retained event integrity, and event writers", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const first = db.transact({ id: "receipt/one", "receipt/value": 1 });
    const second = db.transact({ id: "receipt/two", "receipt/value": 2 });
    expect(() => db.receipt((second.tx as number) + 1)).toThrowError(NotFound);
    expect(() =>
      db.receipt(db._resolveRead("receipt/one") as bigint),
    ).toThrowError(NotFound);

    const writes: string[] = [];
    expect(
      db.tail(GENESIS_TX, {
        write(value: string): void {
          writes.push(value);
        },
      } as never),
    ).toBeUndefined();
    expect(writes).toHaveLength(2);
    expect(writes.join("")).toContain('"fgraph":"event/1"');

    const firstTx = first.tx as number;
    db._connection
      .prepare("UPDATE fgraph_events SET event_data=? WHERE tx=?")
      .run("{}", firstTx);
    expect(() => db.eventRecords(GENESIS_TX)).toThrowError(FormatError);

    const secondTx = second.tx as number;
    db._connection.pragma("ignore_check_constraints = ON");
    db._connection
      .prepare("UPDATE fgraph_ids SET gid=? WHERE id=?")
      .run(Buffer.from([1, 2, 3]), secondTx);
    db._connection.pragma("ignore_check_constraints = OFF");
    expect(() => db.receipt(secondTx)).toThrowError(FormatError);
  });

  it("composes iterable event and snapshot streams without buffering APIs", () => {
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.transact({ id: "stream/item", "stream/value": 1 });
    const eventLines = (source.tail() as string).trim().split("\n");
    using applied = connect(":memory:", { clock: 1_767_225_600_000_000n });
    expect(applied.apply(eventLines)).toHaveLength(1);
    expect(applied.entity("stream/item")["stream/value"]).toBe(1);

    using atomicTarget = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
    });
    function* oversizedLaterLine(): Generator<string> {
      yield eventLines[0] as string;
      yield "x".repeat(MAX_EVENT_BYTES + 1);
      throw new Error("apply consumed input after the oversized line");
    }
    expect(() => atomicTarget.apply(oversizedLaterLine())).toThrowError(
      TooLarge,
    );
    expect(atomicTarget._basisTx()).toBe(Number(GENESIS_TX));
    expect(() => atomicTarget.entity("stream/item")).toThrowError(NotFound);

    const snapshotWrites: string[] = [];
    expect(
      source.snapshot({
        write(value: string): void {
          snapshotWrites.push(value);
        },
      }),
    ).toBeUndefined();
    using restored = connect(":memory:", { clock: 1_767_225_600_000_000n });
    restored.restore(snapshotWrites.join("").split("\n"));
    expect(restored.entity("stream/item")["stream/value"]).toBe(1);

    const emptyWrites: string[] = [];
    restored.tail(restored._basisTx(), {
      write(value: string): void {
        emptyWrites.push(value);
      },
    });
    expect(emptyWrites).toEqual([""]);
  });

  it("explains barrier clauses and rejects invalid query and pull surfaces", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("node/link", { ref: true });
    db.transact([
      { id: "cycle/a", "node/link": { ref: "cycle/b" } },
      { id: "cycle/b", "node/link": { ref: "cycle/a" } },
    ]);
    expect(db.entity("cycle/a", 3)).toMatchObject({
      "node/link": { "node/link": { ref: "cycle/a" } },
    });
    expect(
      db.pull("cycle/a", [{ "node/link": [{ "node/link": ["*"] }] }]),
    ).toMatchObject({ "node/link": { "node/link": { ref: "cycle/a" } } });

    expect(() => db.q({ source: "invalid", find: [], where: [] })).toThrowError(
      QueryError,
    );
    expect(() => db.explain({ find: [] })).toThrowError(QueryError);
    expect(() =>
      db.explain({ source: "invalid", find: [], where: [] }),
    ).toThrowError(QueryError);
    expect(
      db.explain(
        {
          find: ["?e"],
          in: ["?bound", 1],
          where: [["=", 1, 1], {}, 7],
        },
        { "?bound": true },
      ).clauses,
    ).toEqual([
      expect.objectContaining({ operator: "=" }),
      expect.objectContaining({ operator: "invalid" }),
      expect.objectContaining({ operator: "invalid" }),
    ]);
    expect(
      db.explain({
        find: ["?left", "?right"],
        where: [
          ["?left", "?left-attribute", "?left-value"],
          ["?right", "?right-attribute", "?right-value"],
        ],
      }).warnings,
    ).toEqual(["unbound datom scan"]);
    for (const pattern of [["invalid"], [{ invalid: ["*"] }]])
      expect(() => db.pull("cycle/a", pattern)).toThrowError(QueryError);
  });

  it("covers reference selectors, anonymous upserts, conflicting owners, and operation validation", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("identity/email", { type: "text", unique: true });
    db.declare("identity/code", { type: "text", unique: true });
    db.declare("node/link", { ref: true, many: true });
    db.transact([
      { id: "person/a", "identity/email": "a@example.test" },
      { id: "person/b", "identity/code": "B" },
    ]);
    expect(() =>
      db.transact({ "identity/email": "a@example.test", "identity/code": "B" }),
    ).toThrowError(Conflict);
    expect(
      db
        .transact({ "anonymous/value": 1 })
        .asserted.some((fact) => fact.a === "anonymous/value"),
    ).toBe(true);
    expect(
      db
        .transact({ "identity/email": "a@example.test", "person/name": "Ada" })
        .asserted.some((fact) => fact.e === "person/a"),
    ).toBe(true);
    const personId = db._resolveRead("person/a") as bigint;
    db.transact({ id: personId, "person/number": 1 });
    db.transact({
      id: ["identity/email", "a@example.test"],
      "person/city": "London",
    });
    db.transact({
      id: "source/lookup",
      "node/link": { ref: ["identity/email", "a@example.test"] },
    });
    db.transact([
      ["assert", { tmp: "target" }, "person/name", "Target"],
      ["assert", { tmp: "source" }, "node/link", { ref: { tmp: "target" } }],
    ]);
    expect(() =>
      db.transact({ id: "source/bad", "node/link": { ref: true } }),
    ).toThrowError(TypeError);
    expect(() => db.transact([[]])).toThrowError(TypeError);
    expect(() => db.transact([["unknown", "person/a"]])).toThrowError(
      TypeError,
    );
    expect(() =>
      db.transact(["assert", { tmp: "" }, "person/name", "bad"]),
    ).toThrowError(TypeError);
    expect(() => db.transact(["retract", "person/a", 1])).toThrowError(
      SchemaError,
    );
    expect(db._visibility(null, "fact")).toEqual({
      sql: "fact.rx IS NULL",
      params: [],
    });
  });

  it("rejects missing or non-integer allocator metadata and covers transaction-fact dedupe/conflict", () => {
    using missing = connect(":memory:", { clock: 1_767_225_600_000_000n });
    missing._connection.exec("DELETE FROM fgraph_meta WHERE key='next_id'");
    expect(() => missing.transact({ id: "x", "x/value": 1 })).toThrowError(
      FormatError,
    );

    using malformed = connect(":memory:", { clock: 1_767_225_600_000_000n });
    malformed._connection
      .prepare("UPDATE fgraph_meta SET value=? WHERE key='next_id'")
      .run("bad");
    expect(() => malformed.transact({ id: "x", "x/value": 1 })).toThrowError(
      FormatError,
    );

    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("audit/tag", { many: true });
    expect(
      db
        .transact([], { tx: { "audit/tag": ["same", "same"] } })
        .asserted.filter((fact) => fact.a === "audit/tag"),
    ).toHaveLength(1);
    expect(() =>
      db.transact([], {
        _extraTxFacts: [
          ["audit/single", 1],
          ["audit/single", 2],
        ],
        _force: true,
      }),
    ).toThrowError(Conflict);
  });

  it("rejects allocator exhaustion atomically and diagnoses an exhausted maximum", () => {
    using exhausted = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
    });
    exhausted.transact({ id: "existing/entity", "existing/value": 1 });
    exhausted._connection
      .prepare("UPDATE fgraph_meta SET value=? WHERE key='next_id'")
      .run(INT64_MAX);
    const before = exhausted._connection.serialize();
    expect(() =>
      exhausted.transact({ id: "new/entity", "new/value": 1 }),
    ).toThrowError(TooLarge);
    expect(exhausted._connection.serialize()).toEqual(before);
    expect(() => exhausted.transact([], { source: "exhausted" })).toThrowError(
      TooLarge,
    );
    expect(exhausted._connection.serialize()).toEqual(before);

    using corrupt = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
    });
    corrupt._connection
      .prepare(
        "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?, ?, NULL, 64)",
      )
      .run(INT64_MAX, "exhausted/id");
    corrupt._connection
      .prepare("UPDATE fgraph_meta SET value=? WHERE key='next_id'")
      .run(INT64_MAX);
    expect(corrupt.doctor()).toMatchObject({
      ok: false,
      problems: expect.arrayContaining([
        expect.stringContaining("identifier space exhausted"),
      ]),
    });
    expect(() => corrupt.doctor({ repair: true })).toThrowError(FormatError);
  });

  it("covers auto vector dimensions, nested rollback, restore-in-place, and nohistory deletion", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    expect(() =>
      db.transact([
        { id: "vector/a", "vector/value": { vector: [1, 2] } },
        { id: "vector/b", "vector/value": { vector: [1, 2, 3] } },
      ]),
    ).toThrowError(TypeError);
    db.transact({ id: "thing", "thing/value": 1 });
    const before = db.history("thing", "thing/value");
    expect(
      db.transact([
        ["retract", "thing", "thing/value", 1],
        ["assert", "thing", "thing/value", 1],
      ]).tx,
    ).toBeNull();
    expect(db.history("thing", "thing/value")).toEqual(before);
    db.speculate((candidate) => {
      expect(() =>
        candidate.transact({ id: "bad", invalid: 1 }),
      ).toThrowError();
      candidate.transact({ id: "inside", "inside/value": 1 });
    });
    expect(() => db.entity("inside")).toThrowError(NotFound);
    db.declare("secret/value", { type: "text", nohistory: true });
    db.transact({ id: "secret", "secret/value": "first" });
    db.transact({ id: "secret", "secret/value": "second" });
    expect(db.history("secret", "secret/value")).toHaveLength(1);
  });

  it("maps writer locks and indirect-value corruption to typed errors", () => {
    const directory = temporaryDirectory();
    const path = join(directory, "lock.db");
    using first = connect(path, { clock: 1_767_225_600_000_000n });
    using second = connect(path, { clock: 1_767_225_600_000_000n });
    second._connection.pragma("busy_timeout = 1");
    first._connection.exec("BEGIN IMMEDIATE");
    try {
      expect(() =>
        second.transact({ id: "locked", "locked/value": 1 }),
      ).toThrowError(Conflict);
    } finally {
      first._connection.exec("ROLLBACK");
    }

    using beginFailure = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
    });
    const sqliteError = vi
      .spyOn(beginFailure._connection, "exec")
      .mockImplementationOnce(() => {
        throw new Error("injected SQLite failure");
      });
    expect(() =>
      beginFailure.transact({ id: "failed", "failed/value": 1 }),
    ).toThrowError(FormatError);
    sqliteError.mockRestore();
    const throwValue = (value: unknown): never => {
      throw value;
    };
    const nonError = vi
      .spyOn(beginFailure._connection, "exec")
      .mockImplementationOnce(() => throwValue("injected non-Error failure"));
    expect(() =>
      beginFailure.transact({ id: "failed", "failed/value": 1 }),
    ).toThrowError(FormatError);
    nonError.mockRestore();

    expect(() =>
      beginFailure.transact({
        id: "invalid-ref",
        "invalid-ref/value": { ref: { invalid: true } },
      }),
    ).toThrowError(TypeError);

    using missingBlob = connect(":memory:", { clock: 1_767_225_600_000_000n });
    missingBlob.transact({ id: "large", "large/text": "x".repeat(300) });
    missingBlob._connection.exec("DELETE FROM fgraph_blobs");
    expect(() => missingBlob.entity("large")).toThrowError(FormatError);

    using invalidVector = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
    });
    invalidVector.transact({
      id: "vector",
      "vector/value": { vector: [1, 2] },
    });
    invalidVector._connection
      .prepare("UPDATE fgraph_blobs SET data=?")
      .run(Buffer.from([1, 2, 3]));
    expect(() => invalidVector.entity("vector")).toThrowError(FormatError);
  });

  it("resolves every time form, clamps historical views, emits metadata, and waits while following", async () => {
    let clock = 1_767_225_600_000_000n;
    using db = connect(":memory:", { clock: () => (clock += 1_000_000n) });
    const first = db.transact(
      { id: "thing", "thing/value": 1 },
      {
        by: "tester",
        source: "edge",
        meta: { ticket: 1 },
        tx: { "audit/value": 7 },
      },
    );
    const second = db.transact({ id: "thing", "thing/value": 2 });
    expect(db.at("2026-01-01T00:00:02Z").entity("thing")["thing/value"]).toBe(
      1,
    );
    expect(db.at({ instant: first.at }).entity("thing")["thing/value"]).toBe(1);
    expect(db.at(first.at).entity("thing")["thing/value"]).toBe(1);
    using clamped = db.at(first.tx).at(second.tx);
    expect(clamped.entity("thing")["thing/value"]).toBe(1);
    expect(() => db.diff(second.tx, first.tx)).toThrowError(QueryError);
    expect(db.tail() as string).toContain('"meta":{"ticket":1}');
    expect(db.tail() as string).toContain('"tx_facts"');

    const controller = new AbortController();
    const iterator = db.follow(second.tx, {
      interval: 1,
      signal: controller.signal,
    });
    const pending = iterator.next();
    setTimeout(() => controller.abort(), 5);
    expect((await pending).done).toBe(true);
    const stopped = new AbortController();
    stopped.abort();
    expect(
      (await db.follow(second.tx, { signal: stopped.signal }).next()).done,
    ).toBe(true);
    expect(() => db.history("thing", "missing/value")).toThrowError(NotFound);
    expect(() => db.why("thing", "missing/value")).toThrowError(NotFound);
    expect(db.at(first.tx).eventRecords()).toHaveLength(1);
    expect(
      db.at(first.tx).eventRecords(GENESIS_TX, second.tx as number),
    ).toHaveLength(1);
    expect(() => db.eventRecords(GENESIS_TX - 1n)).toThrowError(TypeError);
    expect(() => db.eventRecords(GENESIS_TX, GENESIS_TX - 1n)).toThrowError(
      TypeError,
    );
  });

  it("detects a malformed genesis receipt independently of repairable drift", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db._connection
      .prepare("UPDATE fgraph_facts SET rx=? WHERE e=? AND a=1")
      .run(65, GENESIS_TX);
    expect(db.doctor()).toMatchObject({
      ok: false,
      repaired: false,
      problems: expect.arrayContaining([
        expect.stringContaining("genesis receipt"),
      ]),
    });
    expect(() => db.doctor({ repair: true })).toThrowError(FormatError);
  });

  it("diagnoses malformed retained event payloads and unaudited redactions", () => {
    type TestDb = ReturnType<typeof connect>;
    const setPayload = (
      db: TestDb,
      transaction: number | bigint,
      value: unknown,
    ): void => {
      const data = typeof value === "string" ? value : canonicalJson(value);
      const hash = createHash("sha256").update(data, "utf8").digest();
      db._connection
        .prepare(
          "UPDATE fgraph_events SET event_data=?,event_hash=? WHERE tx=?",
        )
        .run(data, hash, transaction);
    };
    const eventProblems = (
      corrupt: (db: TestDb, tx: number, event: string) => void,
    ): string[] => {
      const db = connect(":memory:", { clock: 1_767_225_600_000_000n });
      try {
        const report = db.transact({ id: "event/item", "event/value": 1 });
        corrupt(db, report.tx as number, report.event as string);
        return db.doctor().problems as string[];
      } finally {
        db.close();
      }
    };

    for (const corrupt of [
      (db: TestDb, tx: number): void => setPayload(db, tx, []),
      (db: TestDb, tx: number, event: string): void =>
        setPayload(db, tx, {
          fgraph: "wrong/1",
          event,
          at: 1,
          created: [],
          asserted: [],
          retracted: [],
        }),
      (db: TestDb, tx: number): void => {
        const row = db._connection
          .prepare<[number], { event_data: string }>(
            "SELECT event_data FROM fgraph_events WHERE tx=?",
          )
          .get(tx) as { event_data: string };
        setPayload(db, tx, `${row.event_data} `);
      },
      (db: TestDb, tx: number): void =>
        setPayload(db, tx, " ".repeat(MAX_EVENT_BYTES + 1)),
      (db: TestDb, tx: number): void => {
        db._connection
          .prepare("UPDATE fgraph_events SET event_data=NULL WHERE tx=?")
          .run(tx);
      },
      (db: TestDb, tx: number, event: string): void =>
        setPayload(db, tx, {
          fgraph: "event/1",
          event,
          at: 1,
          created: [],
          asserted: [],
          retracted: [],
          redacted: false,
        }),
      (db: TestDb, tx: number, event: string): void =>
        setPayload(db, tx, {
          fgraph: "event/1",
          event,
          at: 1,
          created: [],
          asserted: [],
          retracted: [],
          redacted: true,
          redacts: [],
        }),
    ]) {
      expect(
        eventProblems(corrupt).some((problem) => problem.includes("event")),
      ).toBe(true);
    }

    const excisionProblems = (
      corrupt: (
        db: TestDb,
        excisionTx: number,
        excisionEvent: string,
        originalPayloads: Map<number, string>,
      ) => void,
    ): string[] => {
      const db = connect(":memory:", { clock: 1_767_225_600_000_000n });
      try {
        const first = db.transact({ id: "redact/item", "redact/value": 1 });
        const second = db.transact({ id: "redact/item", "redact/value": 2 });
        const originalPayloads = new Map<number, string>();
        for (const tx of [first.tx, second.tx] as number[]) {
          const row = db._connection
            .prepare<[number], { event_data: string }>(
              "SELECT event_data FROM fgraph_events WHERE tx=?",
            )
            .get(tx) as { event_data: string };
          originalPayloads.set(tx, row.event_data);
        }
        const excised = db.excise("redact/item", {
          operationId: "edge-redaction",
          ifBasisTx: second.tx as number,
        });
        corrupt(
          db,
          excised.tx as number,
          excised.event as string,
          originalPayloads,
        );
        return db.doctor().problems as string[];
      } finally {
        db.close();
      }
    };
    const changeRedaction = (
      db: TestDb,
      tx: number,
      mutate: (record: Record<string, unknown>) => void,
    ): void => {
      const row = db._connection
        .prepare<[number], { event_data: string }>(
          "SELECT event_data FROM fgraph_events WHERE tx=?",
        )
        .get(tx) as { event_data: string };
      const record = parseJson(row.event_data, "excision event") as Record<
        string,
        unknown
      >;
      mutate(record);
      setPayload(db, tx, record);
    };

    const invalidRedactions = [
      (
        db: TestDb,
        _tx: number,
        _event: string,
        originals: Map<number, string>,
      ): void => {
        const [target, data] = originals.entries().next().value as [
          number,
          string,
        ];
        setPayload(db, target, data);
      },
      (db: TestDb, tx: number): void =>
        changeRedaction(db, tx, (record) => {
          record.redacts = ["00000000-0000-4000-8000-999999999999"];
        }),
      (db: TestDb, tx: number): void =>
        changeRedaction(db, tx, (record) => {
          record.redacts = [...(record.redacts as unknown[])].reverse();
        }),
      (db: TestDb, tx: number): void =>
        changeRedaction(db, tx, (record) => {
          const first = (record.redacts as unknown[])[0];
          record.redacts = [first, first];
        }),
      (db: TestDb, tx: number): void =>
        changeRedaction(db, tx, (record) => {
          record.redacts = [1];
        }),
      (db: TestDb, tx: number): void => {
        db._connection
          .prepare("DELETE FROM fgraph_facts WHERE e=? AND a=11 AND tx=e")
          .run(tx);
      },
    ];
    for (const corrupt of invalidRedactions)
      expect(
        excisionProblems(corrupt).some((problem) => problem.includes("event")),
      ).toBe(true);
  });
});

describe("search graph decisions", () => {
  it("deduplicates cycles during expansion and rejects mismatching filter values", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("node/link", { ref: true, many: true });
    db.declare("node/kind", { type: "text" });
    db.transact([
      {
        id: "node/a",
        "node/text": "root needle",
        "node/kind": "root",
        "node/link": [{ ref: "node/b" }, { ref: "node/c" }],
      },
      { id: "node/b", "node/text": "other", "node/link": { ref: "node/c" } },
      { id: "node/c", "node/text": "other", "node/link": { ref: "node/a" } },
    ]);
    const result = db.search({
      text: "needle",
      expand: 3,
      filters: [["node/kind", "root"]],
    });
    expect(result.expanded.map((item) => item.entity).sort()).toEqual([
      "node/b",
      "node/c",
    ]);
    expect(
      db.search({ text: "needle", filters: [["node/kind", "other"]] }).hits,
    ).toEqual([]);
    expect(() =>
      db.search({ text: "needle", filters: [["node/kind", { vector: [1] }]] }),
    ).toThrowError(TypeError);
  });
});
