import { describe, expect, it } from "vitest";

import { FormatError } from "../src/errors.js";
import { type Db, connect } from "../src/store.js";
import {
  BOOL,
  BYTES,
  FLOAT,
  INSTANT,
  INSTANT_MAX,
  INSTANT_MIN,
  INT,
  JSON_TAG,
  REF,
  TEXT,
  type Stored,
} from "../src/values.js";

const CLOCK = 1_767_225_600_000_000n;

function factId(db: Db): bigint {
  const report = db.transact({ id: "subject", "item/value": 42 });
  const fact = report.asserted.find((item) => item.a === "item/value");
  if (fact === undefined) throw new Error("transaction has no item/value fact");
  return BigInt(fact.id);
}

function problems(db: Db): string[] {
  return db.doctor().problems as string[];
}

describe("doctor physical value validation", () => {
  it.each([
    ["non-integer tag", Number.NaN, 1n],
    ["negative tag", -1, 1n],
    ["future tag", 11, 1n],
    ["bool storage", BOOL, "1"],
    ["bool domain", BOOL, 2n],
    ["ref storage", REF, "1"],
    ["ref domain", REF, 0n],
    ["int storage", INT, 1.5],
    ["float storage", FLOAT, 1n],
    ["float domain", FLOAT, Number.POSITIVE_INFINITY],
    ["text storage", TEXT, Buffer.from("x")],
    ["text UTF-8", TEXT, "\uD800"],
    ["text bound", TEXT, "x".repeat(257)],
    ["instant storage", INSTANT, "1"],
    ["instant minimum", INSTANT, INSTANT_MIN - 1n],
    ["instant maximum", INSTANT, INSTANT_MAX + 1n],
    ["bytes storage", BYTES, "x"],
    ["bytes bound", BYTES, Buffer.alloc(257)],
    ["JSON storage", JSON_TAG, Buffer.from("{}")],
    ["JSON canonical form", JSON_TAG, '{"b":2, "a":1}'],
    ["JSON syntax", JSON_TAG, "{"],
  ] as Array<[string, number, Stored]>)(
    "rejects corrupt %s during a normal read",
    (_name, tag, stored) => {
      using db = connect(":memory:", { clock: CLOCK });

      expect(() => db._logical(tag, stored)).toThrowError(FormatError);
    },
  );

  it("rejects an unknown physical value tag", () => {
    using db = connect(":memory:", { clock: CLOCK });
    const id = factId(db);
    db._connection.pragma("ignore_check_constraints = ON");
    db._connection.prepare("UPDATE fgraph_facts SET t=99 WHERE id=?").run(id);
    db._connection.pragma("ignore_check_constraints = OFF");

    expect(problems(db)).toContain("invalid value tags: 1");
    expect(() => db.entity("subject")).toThrowError(FormatError);
    expect(() => db.doctor({ repair: true })).toThrowError(FormatError);
  });

  it("rejects a renamed system identity without mutation", () => {
    using db = connect(":memory:", { clock: CLOCK });
    db._connection
      .prepare("UPDATE fgraph_ids SET name='corrupt/at' WHERE id=1")
      .run();
    const before = db._connection.serialize();

    expect(db.doctor()).toMatchObject({
      ok: false,
      problems: expect.arrayContaining(["invalid system identities: 1"]),
    });
    expect(db._connection.serialize()).toEqual(before);
    expect(() => db.doctor({ repair: true })).toThrowError(FormatError);
    expect(db._connection.serialize()).toEqual(before);
  });

  it("rejects a mutated genesis fact without mutation", () => {
    using db = connect(":memory:", { clock: CLOCK });
    db._connection.prepare("UPDATE fgraph_facts SET e=2 WHERE id=2").run();
    const before = db._connection.serialize();

    expect(db.doctor()).toMatchObject({
      ok: false,
      problems: expect.arrayContaining(["invalid genesis facts: 1"]),
    });
    expect(db._connection.serialize()).toEqual(before);
    expect(() => db.doctor({ repair: true })).toThrowError(FormatError);
    expect(db._connection.serialize()).toEqual(before);
  });

  it.each([
    ["ref storage", "UPDATE fgraph_facts SET t=0,v='65' WHERE id=?", undefined],
    ["bool domain", "UPDATE fgraph_facts SET t=1,v=2 WHERE id=?", undefined],
    [
      "non-finite float",
      "UPDATE fgraph_facts SET t=3,v=? WHERE id=?",
      Infinity,
    ],
    [
      "inline text bound",
      "UPDATE fgraph_facts SET t=4,v=? WHERE id=?",
      "x".repeat(257),
    ],
    [
      "instant domain",
      "UPDATE fgraph_facts SET t=5,v=253402300800000000 WHERE id=?",
      undefined,
    ],
    [
      "inline bytes bound",
      "UPDATE fgraph_facts SET t=6,v=? WHERE id=?",
      Buffer.alloc(257),
    ],
    [
      "canonical JSON",
      "UPDATE fgraph_facts SET t=10,v=? WHERE id=?",
      '{"b":2, "a":1}',
    ],
  ] as const)("rejects invalid %s", (_name, sql, value) => {
    using db = connect(":memory:", { clock: CLOCK });
    const id = factId(db);
    const statement = db._connection.prepare(sql);
    if (value === undefined) statement.run(id);
    else statement.run(value, id);

    expect(problems(db)).toContain("invalid physical values: 1");
    expect(() => db.entity("subject")).toThrowError(FormatError);
    expect(() => db.doctor({ repair: true })).toThrowError(FormatError);
  });
});
