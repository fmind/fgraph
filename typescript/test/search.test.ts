import { describe, expect, it, vi } from "vitest";

import { NotFound, TypeError, Unsupported } from "../src/errors.js";
import { connect } from "../src/store.js";

function searchable() {
  const db = connect(":memory:", { clock: 1_767_225_600_000_000n });
  db.declare("doc/vector", { type: "vector", dims: 2 });
  db.declare("doc/link", { ref: true });
  db.transact(
    [
      {
        id: "doc/a",
        "doc/text": "alpha common",
        "doc/kind": "keep",
        "doc/vector": { vector: [1, 0] },
        "doc/link": { ref: "doc/c" },
      },
      {
        id: "doc/b",
        "doc/text": "beta common",
        "doc/kind": "drop",
        "doc/vector": { vector: [0, 1] },
      },
      { id: "doc/c", "doc/text": "neighbor", "doc/vector": { vector: [0, 0] } },
    ],
    { by: "searcher", source: "unit" },
  );
  return db;
}

describe("hybrid search", () => {
  it("bounds compact pull attributes, values, decoding, and SQL shape", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("a/many", { many: true });
    db.transact({
      id: "compact/target",
      ...Object.fromEntries(
        Array.from({ length: 31 }, (_unused, index) => [
          `a/${String(index).padStart(2, "0")}`,
          index,
        ]),
      ),
      "a/many": Array.from({ length: 40 }, (_unused, index) => index),
      "search/text": "compact window needle",
      "z/hidden": "outside the compact pull",
    });

    const statements: string[] = [];
    const prepare = db._connection.prepare.bind(db._connection);
    const prepareSpy = vi.spyOn(db._connection, "prepare").mockImplementation(((
      source: string,
    ) => {
      statements.push(source);
      return prepare(source);
    }) as typeof db._connection.prepare);
    const wireSpy = vi.spyOn(db, "_wire");
    let result;
    let wireCalls = 0;
    try {
      result = db.search({ text: "compact window needle", k: 1 });
    } finally {
      wireCalls = wireSpy.mock.calls.length;
      prepareSpy.mockRestore();
      wireSpy.mockRestore();
    }

    const pull = result.hits[0]?.pull;
    expect(Object.keys(pull ?? {})).toEqual([
      ...Array.from(
        { length: 31 },
        (_unused, index) => `a/${String(index).padStart(2, "0")}`,
      ),
      "a/many",
    ]);
    expect(pull?.["a/many"]).toEqual(
      Array.from({ length: 32 }, (_unused, index) => index),
    );
    expect(wireCalls).toBe(63);
    expect(
      statements.some((statement) =>
        statement.includes(
          "GROUP BY f.a,i.name ORDER BY i.name COLLATE BINARY LIMIT ?",
        ),
      ),
    ).toBe(true);
    expect(
      statements.filter(
        (statement) =>
          statement.includes("FROM fgraph_facts f") &&
          statement.includes("f.e=? AND f.a=?") &&
          statement.includes("ORDER BY f.id LIMIT ?"),
      ),
    ).toHaveLength(32);
  });

  it("fuses keyword/vector candidates, filters, tie-breaks, and expands refs", () => {
    using db = searchable();
    const hybrid = db.search({
      text: "common",
      vector: [1, 0],
      vectorAttribute: "doc/vector",
      k: 2,
      expand: 1,
      filters: [["doc/kind", "keep"]],
      explain: true,
    });
    expect(hybrid.hits).toHaveLength(1);
    expect(hybrid.hits[0]).toMatchObject({
      entity: "doc/a",
      matched: expect.any(Array),
      ranks: { keyword: 1, vector: 1 },
    });
    expect(
      hybrid.hits[0]?.matched.every(
        (fact) =>
          fact.at !== undefined &&
          fact.by === "searcher" &&
          fact.source === "unit",
      ),
    ).toBe(true);
    expect(hybrid.expanded).toEqual([
      expect.objectContaining({
        entity: "doc/c",
        via: [expect.objectContaining({ a: "doc/link" })],
      }),
    ]);
    expect(
      db.search({ text: "common", filters: [["missing/value", 1]] }).hits,
    ).toEqual([]);
    expect(db.search({ text: "!!!" }).hits).toEqual([]);
    expect(
      db.search({ text: "alpha", textAttributes: ["doc/text"] }).hits,
    ).toHaveLength(1);
    expect(() =>
      db.search({ text: "alpha", textAttributes: ["missing/text"] }),
    ).toThrowError(NotFound);
    expect(db.search({ text: "common", k: 1 }).hits).toHaveLength(1);
    expect(
      db.search({ vector: [1, 0], vectorAttribute: "doc/vector" }).hits[0]
        ?.entity,
    ).toBe("doc/a");

    db.declare("multi/text", { many: true });
    db.transact({
      id: "multi/a",
      "multi/text": ["repeated token one", "repeated token two"],
    });
    const repeated = db.search({ text: "repeated" });
    expect(repeated.hits).toHaveLength(1);
    expect(repeated.hits[0]?.matched).toHaveLength(1);
  });

  it("handles implicit vector schema, zero/mismatched candidates, and selected attributes", () => {
    using db = searchable();
    db.transact({ id: "implicit/a", "implicit/vector": { vector: [1, 2, 3] } });
    expect(
      db.search({ vector: [1, 2, 3], vectorAttribute: "implicit/vector" })
        .hits[0]?.entity,
    ).toBe("implicit/a");
    expect(
      db
        .search({ vector: [1, 0, 0], vectorAttribute: "implicit/vector" })
        .hits.every((hit) => hit.entity !== "doc/c"),
    ).toBe(true);
    expect(() =>
      db.search({ vector: [0, 0], vectorAttribute: "doc/vector" }),
    ).toThrowError(TypeError);
    expect(() =>
      db.search({ vector: [1], vectorAttribute: "doc/vector" }),
    ).toThrowError(TypeError);
    expect(() =>
      db.search({ vector: [1, 0], vectorAttribute: "missing/vector" }),
    ).toThrowError(NotFound);
    expect(() =>
      db.search({ vector: [1, 0], vectorAttribute: "doc/text" }),
    ).toThrowError(TypeError);
    db.declare("empty/text", { type: "text" });
    expect(() =>
      db.search({ vector: [1], vectorAttribute: "empty/text" }),
    ).toThrowError(TypeError);
    db.transact({ id: "empty/vector", "empty/value": "not-vector" });
    expect(() =>
      db.search({ vector: [1], vectorAttribute: "empty/value" }),
    ).toThrowError(TypeError);
    expect(() => db.search({ vector: [1, 0] })).toThrowError(TypeError);
  });

  it("rejects malformed retrieval options and historical search", () => {
    using db = searchable();
    for (const options of [
      null,
      [],
      {},
      { text: "   " },
      { text: 1 },
      { vector: "bad" },
      { text: "x", k: 0 },
      { text: "x", k: 1.5 },
      { text: "x", k: 101 },
      { text: "x", expand: -1 },
      { text: "x", expand: 1.5 },
      { text: "x", expand: 4 },
      { text: "x", attribute: 1 },
      { text: "x", vectorAttribute: "doc/vector" },
      { vector: [1, 0], vectorAttribute: "" },
      { text: "x", textAttributes: "bad" },
      { text: "x", textAttributes: [1] },
      {
        vector: [1, 0],
        vectorAttribute: "doc/vector",
        textAttributes: ["doc/text"],
      },
      { text: "x", explain: "yes" },
      { text: "x", workBudget: 0 },
      { text: "x", filters: "bad" },
      { text: "x", filters: [[]] },
      { text: "x", filters: [[1, 2]] },
      { text: "x", filters: [["doc/kind", "keep", 3]] },
      {
        text: "x",
        filters: Array.from({ length: 17 }, () => ["doc/kind", "keep"]),
      },
      {
        text: "x",
        textAttributes: Array.from({ length: 17 }, () => "doc/text"),
      },
    ] as Array<Record<string, unknown>>)
      expect(() => db.search(options as never)).toThrowError(TypeError);
    expect(() => db.at(GENESIS_TX).search({ text: "alpha" })).toThrowError(
      Unsupported,
    );
  });

  it("intersects filters, ranks repeated vectors, and isolates selected text fields", () => {
    using db = searchable();
    db.transact({ id: "doc/a", "doc/status": "active" });
    expect(
      db
        .search({
          text: "common",
          filters: [
            ["doc/kind", "keep"],
            ["doc/status", "active"],
          ],
        })
        .hits.map((hit) => hit.entity),
    ).toEqual(["doc/a"]);

    db.transact([
      { id: "selected/doc", "doc/text": "selected token" },
      { id: "selected/other", "other/text": "selected token" },
    ]);
    expect(
      db
        .search({
          text: "selected",
          textAttributes: ["doc/text"],
        })
        .hits.map((hit) => hit.entity),
    ).toEqual(["selected/doc"]);

    db.declare("multi/vector", { type: "vector", dims: 2, many: true });
    db.transact([
      {
        id: "vector/multi",
        "multi/vector": [
          { vector: [0, 1] },
          { vector: [1, 0] },
          { vector: [0.5, 0.5] },
        ],
      },
      { id: "vector/tie", "multi/vector": { vector: [1, 0] } },
    ]);
    const semantic = db.search({
      vector: [1, 0],
      vectorAttribute: "multi/vector",
      k: 2,
      explain: true,
    });
    expect(semantic.hits.map((hit) => hit.entity)).toEqual([
      "vector/multi",
      "vector/tie",
    ]);
    expect(semantic.hits.every((hit) => hit.ranks?.vector !== undefined)).toBe(
      true,
    );
    const keyword = db.search({ text: "alpha", explain: true });
    expect(keyword.hits[0]?.ranks).toEqual({ keyword: 1 });

    db.transact({
      id: "vector/mismatched-physical-row",
      "other/vector": { vector: [1, 2, 3] },
    });
    const targetAttribute = db._attributeId("doc/vector") as bigint;
    const otherAttribute = db._attributeId("other/vector") as bigint;
    db._connection
      .prepare("UPDATE fgraph_facts SET a=? WHERE a=? AND t=7")
      .run(targetAttribute, otherAttribute);
    expect(
      db.search({ vector: [1, 0], vectorAttribute: "doc/vector" }).hits[0]
        ?.entity,
    ).toBe("doc/a");
  });

  it("caps graph expansion and trims oversized provenance from results", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("node/link", { ref: true });
    db.transact(
      [
        { id: "node/root", "node/text": "bounded needle" },
        ...Array.from({ length: 101 }, (_unused, index) => ({
          id: `node/${String(index).padStart(3, "0")}`,
          "node/link": { ref: "node/root" },
        })),
      ],
      { source: "x".repeat(1_048_000) },
    );
    const result = db.search({ text: "needle", expand: 1 });
    expect(result.truncated).toBe(true);
    expect(
      Buffer.byteLength(JSON.stringify(result), "utf8"),
    ).toBeLessThanOrEqual(1024 * 1024);
    expect(result.expanded).toEqual([]);
  });

  it("drops every matched fact before trimming hits at the result cap", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact(
      { id: "doc/first", "doc/text": "oversized evidence" },
      { source: "a".repeat(600_000) },
    );
    db.transact(
      { id: "doc/second", "doc/text": "oversized evidence" },
      { source: "b".repeat(600_000) },
    );

    const result = db.search({ text: "oversized", k: 2 });
    expect(result.truncated).toBe(true);
    expect(result.hits).toHaveLength(2);
    expect(result.hits.every((hit) => hit.matched.length === 0)).toBe(true);
    expect(
      Buffer.byteLength(JSON.stringify(result), "utf8"),
    ).toBeLessThanOrEqual(1024 * 1024);
  });
});

const GENESIS_TX = 64;
