import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import { Conflict, FormatError, TooLarge, TypeError } from "../src/errors.js";
import { canonicalJson, parseJson } from "../src/jsonio.js";
import { MAX_EVENT_BYTES, connect } from "../src/store.js";

const MAX_SNAPSHOT_LINE_BYTES = 2 * MAX_EVENT_BYTES + 64 * 1024;

function snapshotRecords(snapshot: string): Array<Record<string, unknown>> {
  return snapshot
    .trimEnd()
    .split("\n")
    .map(
      (line) => parseJson(line, "snapshot fixture") as Record<string, unknown>,
    );
}

function seal(records: Array<Record<string, unknown>>): string {
  const footer = records.at(-1) as Record<string, unknown>;
  footer.sha256 = createHash("sha256")
    .update(
      `${records
        .slice(0, -1)
        .map((record) => canonicalJson(record))
        .join("\n")}\n`,
      "utf8",
    )
    .digest("hex");
  return `${records.map((record) => canonicalJson(record)).join("\n")}\n`;
}

describe("streaming snapshot restore", () => {
  it("stops a one-shot iterable at the first malformed record", () => {
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.transact({ id: "stream/item", "stream/value": 1 });
    const lines = (source.snapshot() as string).trimEnd().split("\n");

    function* malformed(): Generator<string> {
      yield lines[0] as string;
      yield "{not-json";
      throw new Error("restore consumed input after the malformed record");
    }

    using target = connect(":memory:", { clock: 1_800_000_000_000_000n });
    expect(() => target.restore(malformed())).toThrowError(TypeError);
  });

  it("rolls back rows applied before a late malformed record", () => {
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.transact({ id: "stream/item", "stream/value": 1 });
    const lines = (source.snapshot() as string).trimEnd().split("\n");

    using target = connect(":memory:", { clock: 1_800_000_000_000_000n });
    const before = target.snapshot();
    let observedIncrementalWrites = false;
    function* malformedLate(): Generator<string> {
      for (const line of lines.slice(0, -1)) yield line;
      const events = target._connection
        .prepare<[], { count: bigint }>(
          "SELECT count(*) count FROM fgraph_events",
        )
        .get()?.count;
      const facts = target._connection
        .prepare<[], { count: bigint }>(
          "SELECT count(*) count FROM fgraph_facts WHERE tx>64",
        )
        .get()?.count;
      expect(events).toBe(2n);
      expect(facts).toBeGreaterThan(0n);
      observedIncrementalWrites = true;
      yield "{not-json";
      throw new Error("restore consumed input after the malformed record");
    }

    expect(() => target.restore(malformedLate())).toThrowError(TypeError);
    expect(observedIncrementalWrites).toBe(true);
    expect(target.snapshot()).toBe(before);
  });

  it("round-trips one-shot and CRLF snapshots without Unicode line splitting", () => {
    const separators = "\u0085\u2028\u2029";
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.transact({
      id: "stream/item",
      "stream/text": `before${separators}after`,
    });
    const snapshot = source.snapshot() as string;
    expect(snapshot).toContain(separators);

    let iteratorRequests = 0;
    const oneShot: Iterable<string> = {
      [Symbol.iterator](): Iterator<string> {
        iteratorRequests++;
        if (iteratorRequests > 1) throw new Error("snapshot iterator reused");
        return snapshot.trimEnd().split("\n")[Symbol.iterator]();
      },
    };
    using iterableTarget = connect(":memory:", {
      clock: 1_800_000_000_000_000n,
    });
    iterableTarget.restore(oneShot);
    expect(iteratorRequests).toBe(1);
    expect(iterableTarget.snapshot()).toBe(snapshot);

    using stringTarget = connect(":memory:", {
      clock: 1_800_000_000_000_000n,
    });
    stringTarget.restore(snapshot.replaceAll("\n", "\r\n"));
    expect(stringTarget.snapshot()).toBe(snapshot);
  });

  it("bounds snapshot records without counting their LF delimiter", () => {
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.transact({ id: "stream/item", "stream/value": 1 });
    const snapshot = source.snapshot() as string;
    const newline = snapshot.indexOf("\n");
    const header = snapshot.slice(0, newline);
    const remainder = snapshot.slice(newline + 1);
    const padding = " ".repeat(
      MAX_SNAPSHOT_LINE_BYTES - Buffer.byteLength(header, "utf8"),
    );

    using exact = connect(":memory:", { clock: 1_800_000_000_000_000n });
    exact.restore(`${header}${padding}\n${remainder}`);
    expect(exact.snapshot()).toBe(snapshot);

    using oversized = connect(":memory:", {
      clock: 1_800_000_000_000_000n,
    });
    expect(() =>
      oversized.restore(`${header}${padding} \n${remainder}`),
    ).toThrowError(TooLarge);

    using oversizedCRLF = connect(":memory:", {
      clock: 1_800_000_000_000_000n,
    });
    expect(() =>
      oversizedCRLF.restore(`${header}${padding}\r\n${remainder}`),
    ).toThrowError(TooLarge);

    source._connection
      .prepare("UPDATE fgraph_ids SET name=? WHERE name='stream/item'")
      .run("x".repeat(MAX_SNAPSHOT_LINE_BYTES));
    expect(() => source.snapshot()).toThrowError(TooLarge);
  });

  it("validates streamed ordering, receipt domains, and footer domains atomically", () => {
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.transact({
      id: "stream/item",
      "stream/float": 1.5,
      "stream/value": 1,
    });
    const snapshot = source.snapshot() as string;
    const reject = (
      mutate: (records: Array<Record<string, unknown>>) => void,
      error: new (...args: never[]) => Error = TypeError,
      reseal = true,
    ): void => {
      const records = snapshotRecords(snapshot);
      mutate(records);
      const candidate = reseal
        ? seal(records)
        : `${records.map((record) => canonicalJson(record)).join("\n")}\n`;
      using target = connect(":memory:", {
        clock: 1_800_000_000_000_000n,
      });
      const before = target.snapshot();
      expect(() => target.restore(candidate)).toThrowError(error);
      expect(target.snapshot()).toBe(before);
    };
    const receipt = (records: Array<Record<string, unknown>>) =>
      records.find((record) => Object.hasOwn(record, "receipt")) as {
        receipt: Record<string, unknown>;
      };
    const fact = (records: Array<Record<string, unknown>>) =>
      records.find((record) => Object.hasOwn(record, "fact")) as {
        fact: unknown[];
      };

    using empty = connect(":memory:");
    expect(() => empty.restore("")).toThrowError(TypeError);
    using afterFooter = connect(":memory:");
    expect(() =>
      afterFooter.restore(`${snapshot}{"unexpected":true}\n`),
    ).toThrowError(TypeError);
    reject((records) => {
      const receiptRecord = receipt(records) as unknown as Record<
        string,
        unknown
      >;
      const receiptIndex = records.indexOf(receiptRecord);
      const factRecord = fact(records) as unknown as Record<string, unknown>;
      const factIndex = records.indexOf(factRecord);
      records[receiptIndex] = factRecord;
      records[factIndex] = receiptRecord;
    });
    reject((records) => {
      const wrapper = fact(records) as unknown as Record<string, unknown>;
      wrapper.receipt = {};
    });
    reject(
      (records) => {
        (records.at(-1) as Record<string, unknown>).sha256 = 7;
      },
      TypeError,
      false,
    );
    reject((records) => {
      (records.at(-1) as Record<string, unknown>).receipts =
        9_007_199_254_740_992n;
    });
    reject((records) => {
      (records.at(-1) as Record<string, unknown>).receipts =
        -9_007_199_254_740_992n;
    });
    reject((records) => {
      receipt(records).receipt.at = "invalid";
    });
    reject((records) => {
      const restoredReceipt = receipt(records).receipt;
      restoredReceipt.at = BigInt(restoredReceipt.at as number) + 1n;
    }, Conflict);
    reject((records) => {
      receipt(records).receipt.origin_at = "invalid";
    });
    reject((records) => {
      receipt(records).receipt.event_data = 7;
    });
    reject((records) => {
      receipt(records).receipt.event_hash = "0".repeat(64);
    }, Conflict);
    reject((records) => {
      receipt(records).receipt.operation_id = 7;
    });
    reject((records) => {
      receipt(records).receipt.request_hash = 7;
    });
    reject((records) => {
      const created = receipt(records).receipt.created as unknown[];
      created.push(created[0]);
    }, Conflict);
    reject((records) => {
      fact(records).fact[4] = 7;
    });
    reject((records) => {
      fact(records).fact[5] = 7;
    });
  });

  it("rolls back a doctor failure after consuming the footer", () => {
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.transact({ id: "stream/item", "stream/value": 1 });
    const lines = (source.snapshot() as string).trimEnd().split("\n");
    using target = connect(":memory:", { clock: 1_800_000_000_000_000n });
    const before = target.snapshot();
    function* corruptedAfterFooter(): Generator<string> {
      yield* lines;
      target._connection.prepare("DELETE FROM fgraph_ids WHERE id=1").run();
    }
    expect(() => target.restore(corruptedAfterFooter())).toThrowError(
      FormatError,
    );
    expect(target.snapshot()).toBe(before);
  });
});
