import { describe, expect, it } from "vitest";

import { FormatError } from "../src/errors.js";
import { type Db, connect } from "../src/store.js";
import { JSON_TAG } from "../src/values.js";

const CLOCK = 1_767_225_600_000_000n;

function doctorProblems(db: Db): string[] {
  return db.doctor().problems as string[];
}

function factCount(db: Db): bigint {
  const row = db._connection
    .prepare<[], { value: bigint }>("SELECT count(*) value FROM fgraph_facts")
    .get();
  if (row === undefined) throw new Error("count query returned no row");
  return row.value;
}

describe("indirect value hardening", () => {
  it("rejects a corrupted allocator before writing into reserved ids", () => {
    using db = connect(":memory:", { clock: CLOCK });
    const before = factCount(db);
    db._connection
      .prepare("UPDATE fgraph_meta SET value=0 WHERE key='next_id'")
      .run();

    expect(() => db.transact({ "item/value": 2 })).toThrowError(FormatError);
    expect(factCount(db)).toBe(before);
  });

  it("decodes canonical JSON cells without changing their stored text", () => {
    using db = connect(":memory:", { clock: CLOCK });

    expect(db._cell(JSON_TAG, '{"value":1}')).toEqual({
      tag: JSON_TAG,
      value: '{"value":1}',
    });
  });

  it("round-trips equal long text and bytes without sharing a blob", () => {
    using db = connect(":memory:", { clock: CLOCK });
    const text = "a".repeat(257);
    const bytes = Buffer.from(text);

    db.transact({
      id: "collision",
      "collision/text": text,
      "collision/bytes": bytes,
    });

    expect(db.entity("collision")).toEqual({
      "collision/bytes": { bytes: bytes.toString("base64") },
      "collision/text": text,
    });
    const hashes = db._connection
      .prepare<[], { t: bigint; v: Buffer }>(
        "SELECT t,v FROM fgraph_facts WHERE t IN (8,9) ORDER BY t",
      )
      .all();
    expect(hashes).toHaveLength(2);
    expect(hashes.map((row) => row.v.toString("hex"))).toEqual([
      "2a99e8adb5dd1bd09413a1df378c45c14a50b13521a741ac35aa521094bdf7ef",
      "d49b9f26c550f91ee6dd4a64820876f79f7610439a460fc4bc4ed03b77d3a6fe",
    ]);
  });

  it("rejects malformed indirect keys before looking up a blob", () => {
    using db = connect(":memory:", { clock: CLOCK });
    db.transact({ id: "value", "value/data": "x".repeat(257) });
    const malformed = Buffer.alloc(31);
    db._connection
      .prepare("INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)")
      .run(malformed, "x".repeat(257));
    db._connection
      .prepare(
        "UPDATE fgraph_facts SET v=? WHERE t=8 AND e=(SELECT id FROM fgraph_ids WHERE name='value')",
      )
      .run(malformed);

    expect(() => db.entity("value")).toThrowError(FormatError);
    expect(doctorProblems(db)).toContain("invalid indirect blobs: 1");
  });

  it.each([
    ["text storage", "x".repeat(257), Buffer.alloc(257)],
    ["text length", "x".repeat(257), "short"],
    ["bytes storage", Buffer.alloc(257, 1), "x".repeat(257)],
    ["bytes length", Buffer.alloc(257, 1), Buffer.alloc(256, 1)],
    ["empty vector", { vector: [1] }, Buffer.alloc(0)],
    ["unaligned vector", { vector: [1] }, Buffer.alloc(3)],
  ])("rejects invalid %s on read", (_name, value, corrupted) => {
    using db = connect(":memory:", { clock: CLOCK });
    db.transact({ id: "value", "value/data": value });
    db._connection.prepare("UPDATE fgraph_blobs SET data=?").run(corrupted);

    expect(() => db.entity("value")).toThrowError(FormatError);
  });

  it.each([
    [
      "invalid identity ids",
      (db: Db) => {
        db._connection
          .prepare(
            "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (19,'invalid/id',NULL,64)",
          )
          .run();
      },
    ],
    [
      "invalid fact identifiers",
      (db: Db) => {
        db._connection
          .prepare(
            "INSERT INTO fgraph_facts(id,e,a,v,t,tx,rx) VALUES (-1,63,1,1,2,64,NULL)",
          )
          .run();
      },
    ],
    [
      "named identities overlap transaction receipts",
      (db: Db) => {
        db._connection
          .prepare("UPDATE fgraph_ids SET name='named/tx',gid=NULL WHERE id=64")
          .run();
      },
    ],
    [
      "facts reference missing asserting transactions",
      (db: Db) => {
        const report = db.transact({ id: "value", "value/data": 1 });
        db._connection
          .prepare("DELETE FROM fgraph_facts WHERE e=? AND a=1 AND tx=e")
          .run(report.tx);
      },
    ],
    [
      "facts reference missing retracting transactions",
      (db: Db) => {
        db.transact({ id: "value", "value/data": 1 });
        db._connection
          .prepare(
            "UPDATE fgraph_facts SET rx=1000 WHERE a=(SELECT id FROM fgraph_ids WHERE name='value/data')",
          )
          .run();
        db._connection
          .prepare("UPDATE fgraph_meta SET value=1001 WHERE key='next_id'")
          .run();
      },
    ],
  ] as const)("doctor detects %s", (problem, corrupt) => {
    using db = connect(":memory:", { clock: CLOCK });
    corrupt(db);

    expect(doctorProblems(db)).toContain(`${problem}: 1`);
    expect(() => db.doctor({ repair: true })).toThrowError(FormatError);
  });

  it.each([
    ["content hash", "y".repeat(257)],
    ["storage class", Buffer.alloc(257)],
    ["length", "short"],
  ])("doctor detects invalid indirect blob %s", (_name, corrupted) => {
    using db = connect(":memory:", { clock: CLOCK });
    db.transact({ id: "value", "value/data": "x".repeat(257) });
    db._connection.prepare("UPDATE fgraph_blobs SET data=?").run(corrupted);

    expect(doctorProblems(db)).toContain("invalid indirect blobs: 1");
    expect(() => db.entity("value")).toThrowError(FormatError);
    expect(() => db.doctor({ repair: true })).toThrowError(FormatError);
  });
});
