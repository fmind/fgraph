import { describe, expect, it } from "vitest";

import { QueryError } from "../src/errors.js";
import { JsonFloat } from "../src/jsonio.js";
import { planPattern } from "../src/query.js";
import { connect } from "../src/store.js";

function seeded() {
  const db = connect(":memory:", { clock: 1_767_225_600_000_000n });
  db.declare("item/value", { many: true });
  db.transact([
    {
      id: "item/a",
      "item/name": "Alpha",
      "item/number": 1,
      "item/value": [1, new JsonFloat(1), "one", true, { bytes: "YQ==" }],
      "item/vector": { vector: [1, 0] },
    },
    {
      id: "item/b",
      "item/name": "Beta",
      "item/number": new JsonFloat(2.5),
      "item/value": [2, new JsonFloat(2.5), "two", false, { bytes: "Yg==" }],
      "item/vector": { vector: [0, 1] },
    },
    {
      id: "item/c",
      "item/name": "Gamma",
      "item/number": 3,
      "item/value": [3],
      "item/link": { ref: "item/a" },
    },
  ]);
  return db;
}

function queryError(
  db: ReturnType<typeof seeded>,
  query: Record<string, unknown>,
  args: Record<string, unknown> = {},
): void {
  expect(() => db.q(query, args), JSON.stringify(query)).toThrowError(
    QueryError,
  );
}

describe("query validation", () => {
  it("rejects malformed top-level structures, inputs, clauses, and pagination", () => {
    using db = seeded();
    for (const query of [
      { find: ["?x"], where: [], typo: true },
      { where: [] },
      { find: "bad", where: [] },
      { find: [], where: [] },
      { find: ["?x"], where: "bad" },
      { find: ["?x"], where: [], in: "bad" },
      { find: ["?x"], where: [], in: ["x"] },
      { find: ["?x"], where: [], in: ["?x"] },
      { find: ["?missing"], where: [] },
      { find: ["?x"], where: [null] },
      { find: ["?x"], where: [{}] },
      { find: ["?x"], where: [{ not: [], or: [] }] },
      { find: ["?x"], where: [{ not: "bad" }] },
      { find: ["?x"], where: [{ not: [["?x", "item/name", "?name"]] }] },
      { find: ["?x"], where: [{ or: [] }] },
      { find: ["?x"], where: [{ or: ["bad"] }] },
      {
        find: ["?x"],
        where: [
          { or: [[["?x", "item/name", "?name"]], [["?x", "item/name", "?"]]] },
        ],
      },
      { find: ["?x"], where: [{ rule: "bad" }] },
      { find: ["?x"], where: [["?x", "item/name"]] },
      { find: ["?x"], where: [["?x", 1, "?v"]] },
      { find: ["?x"], where: [["contains", "?x", "x"]] },
      { find: ["?x"], where: [["?x", "item/name", "?name"]], order: "bad" },
      { find: ["?x"], where: [["?x", "item/name", "?name"]], order: ["bad"] },
      { find: ["?x"], where: [["?x", "item/name", "?name"]], order: [["?x"]] },
      {
        find: ["?x"],
        where: [["?x", "item/name", "?name"]],
        order: [["unknown", "asc"]],
      },
      {
        find: ["?x"],
        where: [["?x", "item/name", "?name"]],
        order: [["?missing", "asc"]],
      },
      { find: ["?x"], where: [["?x", "item/name", "?name"]], offset: -1 },
      { find: ["?x"], where: [["?x", "item/name", "?name"]], offset: "0" },
      { find: ["?x"], where: [["?x", "item/name", "?name"]], offset: 1.5 },
      { find: ["?x"], where: [["?x", "item/name", "?name"]], limit: "one" },
      { find: ["?x"], where: [["?x", "item/name", "?name"]], limit: -1 },
    ] as Array<Record<string, unknown>>)
      queryError(db, query);
    queryError(
      db,
      { find: ["?input"], in: ["?input"], where: [] },
      { "?input": null },
    );
    queryError(
      db,
      { find: ["?input"], in: ["?input"], where: [] },
      { "?input": { ref: "missing/input" } },
    );
    expect(
      db.q(
        { find: ["?input"], in: ["?input"], rules: null, limit: 0 },
        { "?input": 1 },
      ).rows,
    ).toEqual([]);
  });

  it("rejects malformed and unsafe rules", () => {
    using db = seeded();
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
      [{ head: ["r", "?x"], body: [{ rule: [1, "?x"] }] }],
      [{ head: ["r", "?x"], body: [{ rule: ["missing", "?x"] }] }],
      [{ head: ["r", "?x"], body: [{ rule: ["r"] }] }],
      [
        { head: ["left", "?x"], body: [{ rule: ["right", "?x"] }] },
        { head: ["right", "?x"], body: [{ rule: ["left", "?x"] }] },
      ],
    ] as unknown[]) {
      queryError(
        db,
        { find: ["?x"], in: ["?x"], where: [], rules },
        { "?x": 1 },
      );
    }
    queryError(db, {
      find: ["?x"],
      where: [{ rule: ["missing", "?x"] }],
      rules: [],
    });
    queryError(db, { find: ["?x"], where: [{ rule: [] }], rules: [] });
    queryError(db, {
      find: ["literal"],
      where: [],
      rules: [{ head: ["r", "?x"], body: [], typo: true }],
    });
  });
});

describe("query evaluation", () => {
  it("selects every indexed pattern access shape", () => {
    expect(planPattern(["entity", "item/name", "?v"], new Set()).rank).toBe(0);
    expect(planPattern(["?e", "item/name", "Alpha"], new Set()).rank).toBe(1);
    expect(planPattern(["entity", "?a", "?v"], new Set()).rank).toBe(2);
    expect(planPattern(["?e", "item/name", "?v"], new Set()).rank).toBe(3);
    expect(planPattern(["?e", "?a", "Alpha"], new Set()).rank).toBe(4);
    expect(planPattern(["?e", "?a", "?v"], new Set()).rank).toBe(5);
    expect(planPattern(["?e", "?a", "?v"], new Set(["?e", "?a"])).access).toBe(
      "eavt/batch",
    );
  });

  it("pushes temporal pattern constants and preserves wildcard semantics", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("item/score", { type: "float" });
    const first = db.transact({
      id: "item/one",
      "item/score": new JsonFloat(1.5),
    });
    const second = db.transact({
      id: "item/one",
      "item/score": new JsonFloat(2.5),
    });

    expect(
      db.q({
        find: ["?e"],
        where: [["?e", "item/score", new JsonFloat(2.5), second.tx, true]],
      }).rows,
    ).toEqual([[{ ref: "item/one" }]]);
    expect(
      db.q({
        find: ["?e"],
        where: [["?e", "item/score", new JsonFloat(2.5), "_", false]],
      }).rows,
    ).toEqual([]);
    expect(
      db.q({
        source: "history",
        find: ["?tx", "?added"],
        where: [
          ["item/one", "item/score", new JsonFloat(1.5), "?tx", "?added"],
        ],
        order: [["?tx", "asc"]],
      }).rows,
    ).toEqual([
      [{ ref: first.tx }, true],
      [{ ref: second.tx }, false],
    ]);
    expect(
      db.q({
        source: "history",
        find: ["?v"],
        where: [["item/one", "item/score", "?v", second.tx, false]],
      }).rows,
    ).toEqual([[1.5]]);
    expect(
      db.q({
        find: ["?v"],
        where: [["_", "item/score", "?v", "_", "_"]],
        order: [["?v", "asc"]],
      }).rows,
    ).toEqual([[2.5]]);
    expect(
      db.q(
        {
          find: ["?v"],
          in: ["?e"],
          where: [["?e", "item/score", "?v"]],
        },
        { "?e": { ref: "item/one" } },
      ).rows,
    ).toEqual([[2.5]]);
  });

  it("handles constants, wildcards, deduplication, negation, disjunction, and all predicates", () => {
    using db = seeded();
    expect(
      db.q({ find: ["?e"], where: [["?e", "missing/attribute", "_"]] }).rows,
    ).toEqual([]);
    expect(
      db.q({ find: [["count", "?e"]], where: [["missing", "item/name", "?e"]] })
        .rows,
    ).toEqual([[0]]);
    queryError(db, {
      find: [["count", "?e"]],
      where: [[{ bad: true }, "item/name", "?e"]],
    });
    queryError(db, {
      find: [["count", "?e"]],
      where: [["?e", "item/name", { unknown: true }]],
    });
    expect(
      db.q({
        find: ["?e"],
        where: [["?e", "item/name", "_"]],
        order: [["?e", "desc"]],
        offset: 1,
        limit: 1,
      }).rows,
    ).toEqual([[{ ref: "item/b" }]]);
    expect(
      db.q({ find: ["?e"], where: [["?e", "item/name", "?e"]] }).rows,
    ).toEqual([]);
    expect(
      db.q({
        find: ["?name"],
        where: [
          ["?e", "item/name", "?name"],
          { not: [["?e", "item/name", "Gamma"]] },
          {
            or: [
              [["starts-with", "?name", "A"]],
              [["contains", "?name", "et"]],
            ],
          },
        ],
        order: [["?name", "asc"]],
      }).rows,
    ).toEqual([["Alpha"], ["Beta"]]);
    for (const [operator, expected] of [
      ["=", [1]],
      ["!=", [1, 2.5]],
      ["<", [1, 2.5]],
      ["<=", [1, 2.5, 3]],
      [">", []],
      [">=", [3]],
    ] as Array<[string, number[]]>) {
      const comparison =
        operator === "="
          ? ["=", "?v", new JsonFloat(1)]
          : operator === "!="
            ? ["!=", "?v", 3]
            : [operator, "?v", 3];
      const rows = db
        .q({
          find: ["?v"],
          where: [["?e", "item/number", "?v"], comparison],
          order: [["?v", "asc"]],
        })
        .rows.flat();
      expect(
        rows.filter(
          (value) => typeof value === "number" || typeof value === "bigint",
        ),
      ).toEqual(expected);
    }
    queryError(db, {
      find: ["?name"],
      where: [
        ["?e", "item/name", "?name"],
        ["contains", "?name", 1],
      ],
    });
    queryError(db, {
      find: ["?value"],
      where: [
        ["item/a", "item/value", "?value"],
        ["<", "?value", "z"],
      ],
    });
    queryError(db, {
      find: ["?value"],
      where: [
        ["item/a", "item/value", "?value"],
        ["<", "?value", { bytes: "eg==" }],
      ],
    });
    queryError(db, {
      find: ["?name"],
      where: [
        ["?e", "item/name", "?name"],
        ["=", "?name", { unknown: 1 }],
      ],
    });
  });

  it("matches typed constants and both mixed numeric operand directions", () => {
    using db = seeded();
    expect(
      db.q({
        find: ["?name"],
        where: [[{ ref: "item/a" }, "item/name", "?name"]],
      }).rows,
    ).toEqual([["Alpha"]]);
    expect(
      db.q({
        find: [["count", "?value"]],
        where: [["item/a", "_", "?value"]],
      }).rows,
    ).toEqual([[7]]);
    expect(
      db.q({
        find: ["?name", "?number"],
        where: [
          ["?entity", "item/name", "?name"],
          ["?entity", "item/number", "?number"],
        ],
        order: [["?name", "asc"]],
      }).rows,
    ).toEqual([
      ["Alpha", 1],
      ["Beta", 2.5],
      ["Gamma", 3],
    ]);
    expect(
      db.q({
        find: ["?value"],
        where: [
          ["item/a", "item/value", "?value"],
          ["=", "?value", { bytes: "YQ==" }],
        ],
      }).rows,
    ).toEqual([[{ bytes: "YQ==" }]]);
    expect(
      db.q({
        find: ["?value"],
        where: [
          ["item/a", "item/vector", "?value"],
          ["=", "?value", { vector: [1, 0] }],
        ],
      }).rows,
    ).toEqual([[{ vector: [1, 0] }]]);
    for (const predicate of [
      ["<", "?value", new JsonFloat(1.5)],
      [">", new JsonFloat(1.5), "?value"],
      ["=", new JsonFloat(1), "?value"],
    ]) {
      expect(
        db.q({
          find: ["?value"],
          where: [["?e", "item/number", "?value"], predicate],
          order: [["?value", "asc"]],
        }).rows,
      ).toEqual([[1]]);
    }
    expect(
      db.q(
        {
          find: [["count", "?value"]],
          in: ["?transaction"],
          where: [["item/a", "item/value", "?value", "?transaction"]],
        },
        { "?transaction": 1 },
      ).rows,
    ).toEqual([[0]]);
    expect(
      db.q({
        find: ["?name"],
        where: [
          ["item/a", "item/name", "?text"],
          ["?text", "item/name", "?name"],
        ],
      }).rows,
    ).toEqual([]);
    expect(
      db.q({
        find: ["?value"],
        where: [["item/a", "item/value", "?value", "_", "not-a-bool"]],
      }).rows,
    ).toEqual([]);
    queryError(db, {
      find: ["?name"],
      where: [
        ["?entity", "item/name", "?name"],
        ["=", "?name", { ref: "missing/entity" }],
      ],
    });
    queryError(db, {
      find: ["?value"],
      where: [
        ["item/a", "item/value", "?value"],
        ["<", { bytes: "YQ==" }, { bytes: "Yg==" }],
      ],
    });
  });

  it("projects pulls, ordering categories, aggregates, and empty aggregate identities", () => {
    using db = seeded();
    expect(
      db.q({
        find: [["pull", "?e", ["item/name"]]],
        where: [["?e", "item/name", "Alpha"]],
      }).rows,
    ).toEqual([[{ "item/name": "Alpha" }]]);
    queryError(db, {
      find: [["pull", "?name", ["item/name"]]],
      where: [["?e", "item/name", "?name"]],
    });
    queryError(db, {
      find: [["pull", "?e", "bad"]],
      where: [["?e", "item/name", "?name"]],
    });
    queryError(db, { find: [{ bad: true }], where: [] });
    queryError(db, {
      find: [["bad", "?v"]],
      where: [["?e", "item/value", "?v"]],
    });
    queryError(db, {
      find: [
        ["count", "?e"],
        ["pull", "?e", ["*"]],
      ],
      where: [["?e", "item/name", "?name"]],
    });
    queryError(db, {
      find: [["sum", "?name"]],
      where: [["?e", "item/name", "?name"]],
    });

    const aggregate = db.q({
      find: [
        ["count", "?v"],
        ["count-distinct", "?v"],
        ["sum", "?v"],
        ["min", "?v"],
        ["max", "?v"],
        ["avg", "?v"],
      ],
      where: [["item/c", "item/value", "?v"]],
    });
    expect(aggregate.rows).toEqual([[1, 1, 3, 3, 3, 3]]);
    expect(
      db.q({
        find: [
          ["count", "?v"],
          ["count-distinct", "?v"],
          ["sum", "?v"],
          ["min", "?v"],
          ["max", "?v"],
          ["avg", "?v"],
        ],
        where: [["missing", "item/value", "?v"]],
      }).rows,
    ).toEqual([[0, 0, null, null, null, null]]);
    db.transact([
      { id: "sum/a", "sum/value": INT64_MAX },
      { id: "sum/b", "sum/value": 1 },
      { id: "float/a", "float/value": new JsonFloat(1e308) },
      { id: "float/b", "float/value": new JsonFloat(1e308) },
    ]);
    queryError(db, {
      find: [["sum", "?v"]],
      where: [["?e", "sum/value", "?v"]],
    });
    queryError(db, {
      find: [["sum", "?v"]],
      where: [["?e", "float/value", "?v"]],
    });

    const ordered = db
      .q({
        find: ["?v"],
        where: [
          {
            or: [
              [["?item", "item/value", "?v"]],
              [["?item", "item/vector", "?v"]],
            ],
          },
        ],
        order: [["?v", "asc"]],
      })
      .rows.flat();
    expect(ordered.slice(0, 2)).toEqual([false, true]);
    expect(ordered.at(-1)).toEqual({ vector: [1, 0] });
  });

  it("selects replacing and retained integer and float extrema", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact([
      { id: "min/three", "score/min": 3 },
      { id: "min/one", "score/min": 1 },
      { id: "min/two", "score/min": 2 },
      { id: "max/one", "score/max": 1 },
      { id: "max/three", "score/max": 3 },
      { id: "max/two", "score/max": 2 },
      { id: "float/three", "score/float": new JsonFloat(3.5) },
      { id: "float/one", "score/float": new JsonFloat(1.5) },
      { id: "float/two", "score/float": new JsonFloat(2.5) },
    ]);
    expect(
      db.q({
        find: [["min", "?value"]],
        where: [["?entity", "score/min", "?value"]],
      }).rows,
    ).toEqual([[1]]);
    expect(
      db.q({
        find: [["max", "?value"]],
        where: [["?entity", "score/max", "?value"]],
      }).rows,
    ).toEqual([[3]]);
    expect(
      db.q({
        find: [
          ["min", "?value"],
          ["max", "?value"],
          ["sum", "?value"],
          ["avg", "?value"],
        ],
        where: [["?entity", "score/float", "?value"]],
      }).rows,
    ).toEqual([[1.5, 3.5, 7.5, 2.5]]);
  });

  it("evaluates multiple rule definitions and constants with set semantics", () => {
    using db = seeded();
    const result = db.q({
      find: ["?e"],
      where: [{ rule: ["selected", "?e"] }],
      rules: [
        { head: ["selected", "?e"], body: [["?e", "item/name", "Alpha"]] },
        { head: ["selected", "?e"], body: [["?e", "item/name", "Beta"]] },
      ],
      order: [["?e", "asc"]],
    });
    expect(result.rows).toEqual([[{ ref: "item/a" }], [{ ref: "item/b" }]]);
  });

  it("evaluates a self-recursive rule to a deterministic fixed point", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("edge/to", { ref: true, many: true });
    db.transact([
      { id: "node/c", "node/name": "C" },
      { id: "node/b", "edge/to": { ref: "node/c" } },
      { id: "node/a", "edge/to": { ref: "node/b" } },
    ]);
    const rules = [
      {
        head: ["reachable", "?from", "?to"],
        body: [["?from", "edge/to", "?to"]],
      },
      {
        head: ["reachable", "?from", "?to"],
        body: [
          ["?from", "edge/to", "?middle"],
          { rule: ["reachable", "?middle", "?to"] },
        ],
      },
    ];
    expect(
      db.q({
        find: ["?to"],
        where: [{ rule: ["reachable", "node/a", "?to"] }],
        rules,
        order: [["?to", "asc"]],
      }).rows,
    ).toEqual([[{ ref: "node/b" }], [{ ref: "node/c" }]]);
    expect(
      db.q({
        find: [["count", "?to"]],
        where: [{ rule: ["reachable", "missing/node", "?to"] }],
        rules,
      }).rows,
    ).toEqual([[0]]);
  });

  it("orders projected and hidden refs by their rendered names", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("holder/target", { ref: true });
    db.transact([
      { id: "target/z", "target/name": "Z" },
      { id: "target/a", "target/name": "A" },
      { id: "holder/one", "holder/target": { ref: "target/z" } },
      { id: "holder/two", "holder/target": { ref: "target/a" } },
    ]);

    expect(
      db.q({
        find: ["?target"],
        where: [["?holder", "holder/target", "?target"]],
        order: [["?target", "asc"]],
      }).rows,
    ).toEqual([[{ ref: "target/a" }], [{ ref: "target/z" }]]);
    expect(
      db.q({
        find: ["?holder"],
        where: [["?holder", "holder/target", "?target"]],
        order: [["?target", "asc"]],
      }).rows,
    ).toEqual([[{ ref: "holder/two" }], [{ ref: "holder/one" }]]);
  });

  it("pushes undeclared attribute constants into the value index", () => {
    using db = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
      queryBudget: 3,
    });
    db.transact(
      Array.from({ length: 100 }, (_, index) => ({
        id: `item/${index}`,
        "item/group": index,
      })),
    );

    expect(
      db.q({
        find: ["?entity"],
        where: [["?entity", "item/group", 77]],
      }).rows,
    ).toEqual([[{ ref: "item/77" }]]);
  });
});

const INT64_MAX = 9_223_372_036_854_775_807n;
