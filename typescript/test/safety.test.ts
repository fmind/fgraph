import { describe, expect, it } from "vitest";

import { QueryError, TooLarge, TypeError } from "../src/errors.js";
import {
  JsonFloat,
  canonicalJson,
  parseJson,
  stringifyJson,
} from "../src/jsonio.js";
import { DEFAULT_QUERY_BUDGET, connect } from "../src/store.js";

describe("v1 resource-safety contract", () => {
  it("counts exact pattern and predicate work units and does not let limit bypass evaluation", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact([
      { id: "item/one", "item/value": "x" },
      { id: "item/two", "item/value": "y" },
    ]);
    const pattern = {
      find: ["?value"],
      where: [["?entity", "item/value", "?value"]],
      order: [["?value", "asc"]],
    };
    expect(db.q(pattern, {}, { budget: 2 }).rows).toEqual([["x"], ["y"]]);
    expect(() => db.q(pattern, {}, { budget: 1 })).toThrowError(TooLarge);
    expect(() =>
      db.q({ ...pattern, limit: 1 }, {}, { budget: 1 }),
    ).toThrowError(TooLarge);

    const withPredicate = {
      find: ["?value"],
      where: [
        ["?entity", "item/value", "?value"],
        ["=", "?value", "x"],
      ],
    };
    expect(db.q(withPredicate, {}, { budget: 4 }).rows).toEqual([["x"]]);
    expect(() => db.q(withPredicate, {}, { budget: 3 })).toThrowError(TooLarge);
  });

  it("streams query candidates instead of materializing the full match set", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact(
      Array.from({ length: 100 }, (_value, index) => ({
        id: `item/${index}`,
        "item/value": index,
      })),
    );

    const candidates = db._queryDatoms(null, null);
    expect(Array.isArray(candidates)).toBe(false);
    const iterator = candidates[Symbol.iterator]();
    expect(iterator.next().done).toBe(false);
    iterator.return(undefined);
  });

  it("spends batched query work while rows are streamed", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact(
      Array.from({ length: 3 }, (_value, index) => ({
        id: `batch/${index}`,
        "batch/value": index,
      })),
    );
    const entities = Array.from({ length: 3 }, (_value, index) =>
      db._resolveRead(`batch/${index}`),
    ) as bigint[];
    const attribute = db._resolveRead("batch/value") as bigint;
    let spent = 0;

    expect(() =>
      db._queryDatomsForEntities(entities, attribute, () => {
        spent++;
        if (spent === 2)
          throw new TooLarge("test budget exhausted during iteration");
      }),
    ).toThrowError(TooLarge);
    expect(spent).toBe(2);
  });

  it("charges every duplicate binding that reuses a batched row", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("item/tag", { many: true });
    db.transact({
      id: "item/one",
      "item/name": "One",
      "item/tag": ["a", "b"],
    });
    const query = {
      find: ["?tag", "?name"],
      where: [
        ["?entity", "item/tag", "?tag"],
        ["?entity", "item/name", "?name"],
      ],
    };

    expect(() => db.q(query, {}, { budget: 3 })).toThrowError(TooLarge);
    expect(db.q(query, {}, { budget: 4 }).rows).toEqual([
      ["a", "One"],
      ["b", "One"],
    ]);
  });

  it("charges projected pull facts to the active query work budget", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact({
      id: "pull/target",
      "pull/name": "target",
      "pull/enabled": true,
    });
    const query = {
      find: [["pull", "?entity", ["*"]]],
      where: [["?entity", "pull/name", "target"]],
    };

    expect(db.q(query, {}, { budget: 3 }).rows).toEqual([
      [{ "pull/enabled": true, "pull/name": "target" }],
    ]);
    expect(() => db.q(query, {}, { budget: 2 })).toThrowError(TooLarge);
  });

  it("deduplicates wildcard bindings between patterns without changing work accounting", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("item/tag", { many: true });
    db.transact({ id: "item/one", "item/tag": ["a", "b"], "item/name": "One" });
    const query = {
      find: ["?name"],
      where: [
        ["?entity", "item/tag", "_"],
        ["?entity", "item/name", "?name"],
      ],
    };
    expect(db.q(query, {}, { budget: 3 }).rows).toEqual([["One"]]);
    expect(() => db.q(query, {}, { budget: 2 })).toThrowError(TooLarge);
  });

  it("deduplicates wildcard rule bindings with the same deterministic work accounting", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.declare("item/tag", { many: true });
    db.transact({ id: "item/one", "item/tag": ["a", "b"], "item/name": "One" });
    const query = {
      find: ["?name"],
      where: [
        { rule: ["has-tag", "?entity", "_"] },
        ["?entity", "item/name", "?name"],
      ],
      rules: [
        {
          head: ["has-tag", "?entity", "?tag"],
          body: [["?entity", "item/tag", "?tag"]],
        },
      ],
    };
    expect(db.q(query, {}, { budget: 7 }).rows).toEqual([["One"]]);
    expect(() => db.q(query, {}, { budget: 6 })).toThrowError(TooLarge);
  });

  it("materializes rule dependencies before dependents regardless of declaration order", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact({ id: "item/one", "item/name": "One" });
    const query = {
      find: ["?entity"],
      where: [{ rule: ["derived", "?entity"] }],
      rules: [
        { head: ["derived", "?entity"], body: [{ rule: ["base", "?entity"] }] },
        { head: ["base", "?entity"], body: [["?entity", "item/name", "_"]] },
      ],
    };
    expect(db.q(query, {}, { budget: 5 }).rows).toEqual([
      [{ ref: "item/one" }],
    ]);
    expect(() => db.q(query, {}, { budget: 4 })).toThrowError(TooLarge);
  });

  it("pre-resolves impossible constants, normalizes signed zero sets, and keeps refs distinct from ints", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact({
      id: "item/one",
      "item/value": 1,
      "item/name": "One",
      "item/zero": new JsonFloat(-0),
    });
    expect(
      db.q(
        {
          find: [["count", "?value"]],
          where: [["missing/entity", "item/value", "?value"]],
        },
        {},
        { budget: 1 },
      ).rows,
    ).toEqual([[0]]);
    expect(
      db.q(
        {
          find: [["count", "?entity"]],
          where: [["?entity", "item/value", { ref: "missing/entity" }]],
        },
        {},
        { budget: 1 },
      ).rows,
    ).toEqual([[0]]);
    expect(() =>
      db.q(
        { find: ["?negative"], in: ["?negative"], where: [{ or: [] }] },
        { "?negative": new JsonFloat(-0) },
      ),
    ).toThrowError(QueryError);
    const zeros = db.q(
      {
        find: ["?name"],
        where: [
          {
            or: [
              [["?entity", "item/zero", new JsonFloat(-0)]],
              [["?entity", "item/zero", new JsonFloat(0)]],
            ],
          },
          ["?entity", "item/name", "?name"],
        ],
      },
      {},
      { budget: 3 },
    );
    expect(zeros.rows).toEqual([["One"]]);
    expect(
      db.q(
        {
          find: [["count", "?entity"]],
          in: ["?id"],
          where: [
            ["?entity", "item/value", "?value"],
            ["=", "?entity", "?id"],
          ],
        },
        { "?id": 65 },
      ).rows,
    ).toEqual([[0]]);
    expect(() =>
      db.q({
        find: ["?entity"],
        where: [
          ["?entity", "item/value", "?value"],
          ["<", "?entity", { ref: "item/one" }],
        ],
      }),
    ).toThrowError(QueryError);
  });

  it("uses the configured/default budget and honors AbortSignal cancellation", () => {
    using configured = connect(":memory:", {
      clock: 1_767_225_600_000_000n,
      queryBudget: 1,
    });
    configured.transact([
      { id: "item/one", "item/value": 1 },
      { id: "item/two", "item/value": 2 },
    ]);
    expect(configured.queryBudget).toBe(1);
    expect(() =>
      configured.q({
        find: ["?value"],
        where: [["?entity", "item/value", "?value"]],
      }),
    ).toThrowError(TooLarge);
    expect(() =>
      configured.q(
        { find: ["?value"], where: [["?entity", "item/value", "?value"]] },
        {},
        { budget: 100 },
      ),
    ).toThrowError(TooLarge);

    using defaults = connect(":memory:", { clock: 1_767_225_600_000_000n });
    expect(defaults.queryBudget).toBe(DEFAULT_QUERY_BUDGET);
    defaults.transact({ id: "item/one", "item/value": 1 });
    const controller = new AbortController();
    controller.abort();
    expect(() =>
      defaults.q(
        { find: ["?value"], where: [["?entity", "item/value", "?value"]] },
        {},
        { signal: controller.signal },
      ),
    ).toThrowError(QueryError);
    expect(() =>
      defaults.q(
        { find: ["?value"], where: [] },
        {},
        { budget: Number.POSITIVE_INFINITY },
      ),
    ).toThrowError(QueryError);
    expect(() =>
      defaults.q({ find: ["?value"], where: [] }, {}, { budget: 0 }),
    ).toThrowError(QueryError);
    expect(() => connect(":memory:", { queryBudget: 0 })).toThrowError(
      TypeError,
    );
  });
});

describe("v1 durability and retrieval boundaries", () => {
  it("keeps check-only doctor non-mutating and repairs derived drift only when requested", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact({ id: "note/one", "note/text": "doctor needle" });
    db._connection.exec("DELETE FROM fgraph_fts");
    const checked = db.doctor();
    expect(checked).toMatchObject({
      ok: false,
      repair_needed: true,
      repaired: false,
      fts_rows: 0,
      expected_fts_rows: 37,
      fts_rows_rebuilt: 0,
    });
    expect(
      db._connection.prepare("SELECT count(*) count FROM fgraph_fts").get(),
    ).toEqual({ count: 0n });

    const repaired = db.doctor({ repair: true });
    expect(repaired).toMatchObject({
      ok: true,
      repair_needed: false,
      repaired: true,
      fts_rows: 37,
      expected_fts_rows: 37,
      fts_rows_rebuilt: 37,
    });
    expect(db.search({ text: "needle" }).hits).toHaveLength(1);
  });

  it("searches only application text and attaches asserting provenance to matches", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact(
      { id: "note/domain", "note/text": "private needle" },
      { by: "agent:test", source: "conversation:7" },
    );
    db.transact(
      { id: "note/other", "note/text": "unrelated" },
      { source: "needle system metadata must not rank" },
    );

    const result = db.search({ text: "needle" });
    expect(result.hits).toHaveLength(1);
    expect(result.hits[0]?.entity).toBe("note/domain");
    expect(result.hits[0]?.matched).toHaveLength(1);
    expect(result.hits[0]?.matched[0]).toMatchObject({
      a: "note/text",
      v: "private needle",
      at: 1_767_225_601_000_000,
      by: "agent:test",
      source: "conversation:7",
    });
    expect(result.hits[0]?.matched[0]?.snippet).toContain("[needle]");
    expect(
      result.hits[0]?.matched.every((fact) => !fact.a.startsWith("fgraph/")),
    ).toBe(true);
  });
});

describe("lossless JSON boundary", () => {
  it("round-trips the complete signed-int64 range and rejects overflow", () => {
    expect(() => parseJson('"\\q"')).toThrowError(TypeError);
    expect(stringifyJson({ escaped: 'a\\"b' }, true)).toContain('a\\\\\\"b');
    const value = parseJson(
      '{"max":9223372036854775807,"min":-9223372036854775808}',
    ) as Record<string, unknown>;
    expect(value).toEqual({
      max: 9_223_372_036_854_775_807n,
      min: -9_223_372_036_854_775_808n,
    });
    expect(canonicalJson(value)).toBe(
      '{"max":9223372036854775807,"min":-9223372036854775808}',
    );
    expect(parseJson("9223372036854775808")).toBe(9_223_372_036_854_775_808n);
    expect(() => canonicalJson(9_223_372_036_854_775_808n)).toThrowError(
      TypeError,
    );
  });

  it("returns unsafe application integers as bigint instead of rounding them", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    db.transact({
      id: "number/extreme",
      "number/value": 9_223_372_036_854_775_807n,
    });
    expect(db.entity("number/extreme")["number/value"]).toBe(
      9_223_372_036_854_775_807n,
    );
  });
});
