import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";

import { Conflict, NotFound, QueryError, TypeError } from "../src/errors.js";
import { canonicalJson, parseJson } from "../src/jsonio.js";
import { FORMAT_VERSION, GENESIS_TX, connect } from "../src/store.js";

function clock(): () => bigint {
  let value = 1_767_225_600_000_000n;
  return () => value++;
}

function eventIds(): () => string {
  let value = 1000;
  return () => `00000000-0000-4000-8000-${String(value++).padStart(12, "0")}`;
}

describe("v1 format and transaction contract", () => {
  it("exposes only the canonical event and snapshot portability APIs", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });

    expect("export" in db).toBe(false);
    expect("import" in db).toBe(false);
    expect(typeof db.tail).toBe("function");
    expect(typeof db.apply).toBe("function");
    expect(typeof db.snapshot).toBe("function");
    expect(typeof db.restore).toBe("function");
  });

  it("uses the durable dedicated format-v2 identity and event registries", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });

    expect(FORMAT_VERSION).toBe(2);
    expect(db._connection.pragma("user_version", { simple: true })).toBe(2n);
    expect(db._connection.pragma("synchronous", { simple: true })).toBe(2n);
    expect(db._connection.pragma("trusted_schema", { simple: true })).toBe(0n);
    expect(
      db._connection
        .prepare(
          "SELECT lower(hex(gid)) gid,created_tx FROM fgraph_ids WHERE id=?",
        )
        .get(GENESIS_TX),
    ).toEqual({
      gid: "00000000000040008000000000000040",
      created_tx: GENESIS_TX,
    });
    expect(
      db._connection.prepare("SELECT count(*) count FROM fgraph_events").get(),
    ).toEqual({ count: 1n });
  });

  it("temporalizes names, attributes, anonymous ids, and logical stats", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });
    db.transact([
      { id: "future/entity", "future/value": 1 },
      { "anonymous/value": 2 },
    ]);
    const old = db.at(GENESIS_TX);

    expect(() => old.entity("future/entity")).toThrowError(NotFound);
    expect(old.attributes("future/")).toEqual([]);
    expect(old.stats()).toEqual({
      application_id: 0x66677261,
      format_version: 2,
      entities: 18,
      attributes: 18,
      facts: 39,
      live_facts: 39,
      transactions: 1,
      blobs: 0,
      size: 0,
    });
    expect(db.stats()).toEqual({
      application_id: 0x66677261,
      format_version: 2,
      entities: 22,
      attributes: 21,
      facts: 42,
      live_facts: 42,
      transactions: 2,
      blobs: 0,
      size: 0,
    });
    expect(
      db._connection
        .prepare(
          "SELECT count(*) count FROM fgraph_ids WHERE gid IS NOT NULL AND id<>?",
        )
        .get(GENESIS_TX),
    ).toEqual({ count: 2n });
  });

  it("makes identity-only creation observable and operation retries idempotent", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });
    const identity = db.transact(
      { id: "factless" },
      { operationId: "create-1" },
    );
    expect(identity).toMatchObject({ status: "applied" });
    expect(identity.tx).not.toBeNull();

    const first = db.transact(
      { id: "counter", "counter/value": 1 },
      { operationId: "counter-1", ifBasisTx: identity.tx as number },
    );
    db.transact({ id: "later", "later/value": true });
    const retried = db.transact(
      { id: "counter", "counter/value": 1 },
      { operationId: "counter-1", ifBasisTx: identity.tx as number },
    );
    expect(retried).toMatchObject({
      status: "already_applied",
      event: first.event,
      tx: first.tx,
      at: first.at,
      basis_tx: identity.tx,
    });
    expect(() =>
      db.transact(
        { id: "counter", "counter/value": 2 },
        { operationId: "counter-1" },
      ),
    ).toThrowError(Conflict);
  });

  it("checks the basis and supports cardinality-one compare-and-swap", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });
    db.declare("item/optional", { type: "text" });
    const initial = db.transact({ id: "item", "item/state": "new" });
    expect(() =>
      db.transact(["cas", "item", "item/state", "new", "ready"], {
        ifBasisTx: GENESIS_TX,
      }),
    ).toThrowError(Conflict);
    const changed = db.transact(["cas", "item", "item/state", "new", "ready"], {
      ifBasisTx: initial.tx as number,
    });
    expect(changed.basis_tx).toBe(initial.tx);
    expect(db.entity("item")["item/state"]).toBe("ready");
    expect(() =>
      db.transact(["cas", "item", "item/state", "new", "stale"]),
    ).toThrowError(Conflict);
    db.transact(["cas", "item", "item/optional", { missing: true }, "present"]);
    expect(db.entity("item")["item/optional"]).toBe("present");
  });
});

describe("v1 query, schema, and search contract", () => {
  it("queries current and history datoms with variable attributes", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });
    const first = db.transact({ id: "ada", "person/name": "Ada" });
    const second = db.transact({ id: "ada", "person/name": "Ada L." });

    expect(
      db.q({
        find: ["?a", "?v", "?tx", "?added"],
        where: [["ada", "?a", "?v", "?tx", "?added"]],
      }).rows,
    ).toEqual([[{ ref: "person/name" }, "Ada L.", { ref: second.tx }, true]]);
    expect(
      db.q({
        source: "history",
        find: ["?v", "?tx", "?added"],
        where: [["ada", "person/name", "?v", "?tx", "?added"]],
        order: [["?tx", "asc"]],
      }).rows,
    ).toEqual([
      ["Ada", { ref: first.tx }, true],
      ["Ada", { ref: second.tx }, false],
      ["Ada L.", { ref: second.tx }, true],
    ]);
  });

  it("seeks datom indexes with basis-pinned opaque cursors", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });
    db.declare("node/link", { ref: true });
    const first = db.transact([
      { id: "node/b", "node/name": "B" },
      {
        id: "node/a",
        "node/name": "A",
        "node/link": { ref: "node/b" },
      },
    ]);
    const page1 = db.datoms("eavt", {
      components: ["node/a"],
      limit: 1,
    });
    expect(page1).toMatchObject({
      basis_tx: first.tx,
      items: [expect.objectContaining({ e: "node/a" })],
      next_cursor: expect.any(String),
    });
    db.transact({ id: "node/a", "node/later": true });
    const page2 = db.datoms("eavt", {
      components: ["node/a"],
      limit: 10,
      cursor: page1.next_cursor as string,
    });
    expect(page2.basis_tx).toBe(first.tx);
    expect(page2.items.every((item) => item.a !== "node/later")).toBe(true);
    expect(() =>
      db.datoms("eavt", {
        components: ["node/b"],
        cursor: page1.next_cursor as string,
      }),
    ).toThrowError();
    expect(db.datoms("avet", { components: ["node/name", "A"] }).items).toEqual(
      [expect.objectContaining({ e: "node/a", v: "A" })],
    );
    expect(
      db.datoms("vaet", { components: [{ ref: "node/b" }] }).items,
    ).toEqual([
      expect.objectContaining({ e: "node/a", a: "node/link", added: true }),
    ]);
    expect(
      db.datoms("eavt", {
        components: ["node/a", "node/name", "A", first.tx, true],
      }).items,
    ).toEqual([expect.objectContaining({ v: "A", tx: first.tx, added: true })]);
    expect(
      db.datoms("eavt", {
        components: ["node/a", "node/name", "A", first.tx, false],
      }).items,
    ).toEqual([]);
    expect(
      db.datoms("vaet", {
        components: [{ ref: "node/b" }, "node/link", "node/a", first.tx, true],
      }).items,
    ).toHaveLength(1);
    const removed = db.retract("node/a", "node/name", "A");
    expect(
      db
        .datoms("avet", {
          source: "history",
          components: ["node/name", "A", "node/a"],
        })
        .items.map((item) => item.added),
    ).toEqual([true, false]);
    expect(
      db.datoms("avet", {
        source: "history",
        components: ["node/name", "A", "node/a", removed.tx, false],
      }).items,
    ).toEqual([expect.objectContaining({ added: false, tx: removed.tx })]);

    for (const [index, options] of [
      ["bad", {}],
      ["eavt", { source: "bad" }],
      ["eavt", { components: "bad" }],
      ["eavt", { components: [1, 2, 3, 4, 5, 6] }],
      ["eavt", { limit: 0 }],
      ["eavt", { limit: 1.5 }],
      ["eavt", { limit: 1001 }],
      ["eavt", { components: ["missing/entity"] }],
      ["eavt", { components: ["node/a", 1] }],
      ["eavt", { components: ["node/a", "missing/attribute"] }],
      ["eavt", { components: ["node/a", "node/name", "A", first.tx, "yes"] }],
      ["vaet", { components: ["node/b"] }],
      ["vaet", { components: [{ ref: "missing/entity" }] }],
      ["eavt", { cursor: "!!!" }],
    ] as const)
      expect(() => db.datoms(index as never, options as never)).toThrowError(
        QueryError,
      );

    const cursorPayload = parseJson(
      Buffer.from(page1.next_cursor as string, "base64url").toString("utf8"),
      "cursor",
    ) as Record<string, unknown>;
    cursorPayload.last = [
      { bad: true },
      ...(cursorPayload.last as unknown[]).slice(1),
    ];
    const invalidSeek = Buffer.from(
      canonicalJson(cursorPayload),
      "utf8",
    ).toString("base64url");
    expect(() =>
      db.datoms("eavt", { cursor: invalidSeek, components: ["node/a"] }),
    ).toThrowError(QueryError);

    const futurePayload = parseJson(
      Buffer.from(page1.next_cursor as string, "base64url").toString("utf8"),
      "cursor",
    ) as Record<string, unknown>;
    futurePayload.basis = BigInt(db._basisTx()) + 1n;
    const futureBasis = Buffer.from(
      canonicalJson(futurePayload),
      "utf8",
    ).toString("base64url");
    expect(() =>
      db.datoms("eavt", { cursor: futureBasis, components: ["node/a"] }),
    ).toThrowError(QueryError);
    expect(() => db.datoms("eavt", { cursor: "a".repeat(4097) })).toThrowError(
      QueryError,
    );

    const latestPage = db.datoms("eavt", { components: ["node/a"], limit: 1 });
    expect(latestPage.next_cursor).toEqual(expect.any(String));
    expect(() =>
      db.at(first.tx).datoms("eavt", {
        components: ["node/a"],
        cursor: latestPage.next_cursor as string,
      }),
    ).toThrowError(QueryError);
  });

  it("reports declared, effective, and observed schema without future leakage", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });
    const before = db.transact({ id: "before", "mixed/value": 1 });
    db.declare("mixed/value", { many: false, nohistory: false, doc: "Value" });
    const snapshot = db.schema("mixed/");
    expect(snapshot).toMatchObject({
      basis_tx: expect.anything(),
      digest: expect.stringMatching(/^sha256:/u),
      attributes: [
        {
          name: "mixed/value",
          declared: { many: false, nohistory: false, doc: "Value" },
          effective: { many: false, nohistory: false, doc: "Value" },
          observed: { types: ["int"], live_facts: 1, entities: 1 },
        },
      ],
    });
    expect(db.at(before.tx).schema("mixed/").attributes).toEqual([
      expect.objectContaining({ name: "mixed/value", declared: {} }),
    ]);
  });

  it("enforces opt-in required and closed shapes against final transaction state", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });
    db.declare("person/name", { type: "text" });
    db.declare("person/age", { type: "int" });
    db.defineShape("shape/person", {
      required: ["person/name"],
      allowed: ["person/age"],
      closed: true,
    });
    db.transact({
      id: "ada",
      "fgraph/shape": { ref: "shape/person" },
      "person/name": "Ada",
      "person/age": 37,
    });
    expect(db.validate("ada")).toMatchObject({ valid: true, violations: [] });
    expect(() =>
      db.transact({ id: "ada", "person/extra": true }),
    ).toThrowError();
    expect(() => db.retract("ada", "person/name")).toThrowError();
    expect(db.schema("person/").shapes).toEqual([
      expect.objectContaining({
        name: "shape/person",
        required: ["person/name"],
        allowed: ["person/age", "person/name"],
        closed: true,
      }),
    ]);
  });

  it("filters before candidate truncation and isolates vector spaces", () => {
    using db = connect(":memory:", { clock: clock(), eventId: eventIds() });
    db.declare("doc/tag", { type: "text" });
    for (let index = 0; index < 60; index++) {
      db.transact({
        id: `doc/${String(index).padStart(2, "0")}`,
        "doc/text": "same searchable phrase",
        "doc/tag": index === 59 ? "wanted" : "other",
      });
    }
    expect(
      db
        .search({
          text: "same searchable phrase",
          filters: [["doc/tag", "wanted"]],
          k: 1,
        })
        .hits.map((hit) => hit.entity),
    ).toEqual(["doc/59"]);
    const capped = db.search({ text: "same searchable phrase", k: 1 });
    expect(capped).toMatchObject({ truncated: true });
    expect(() =>
      db.search({
        text: "same searchable phrase",
        filters: [["doc/tag", "other"]],
        workBudget: 10,
      }),
    ).toThrowError();

    db.declare("embed/a", {
      type: "vector",
      dims: 2,
      vectorModel: "model-a",
    });
    db.declare("embed/b", {
      type: "vector",
      dims: 2,
      vectorModel: "model-b",
    });
    db.transact([
      { id: "vector/a", "embed/a": { vector: [1, 0] } },
      { id: "vector/b", "embed/b": { vector: [1, 0] } },
    ]);
    expect(() => db.search({ vector: [1, 0] })).toThrowError(TypeError);
    expect(
      db.search({ vector: [1, 0], vectorAttribute: "embed/a" }).hits,
    ).toEqual([
      expect.objectContaining({
        entity: "vector/a",
        matched: [
          expect.objectContaining({
            v: { vector_dims: 2 },
            value_truncated: true,
          }),
        ],
      }),
    ]);
    db.transact({ id: "large/text", "doc/text": "needle ".repeat(1000) });
    const bounded = db.search({ text: "needle", k: 1 });
    expect(
      Buffer.byteLength(canonicalJson(bounded), "utf8"),
    ).toBeLessThanOrEqual(1024 * 1024);
    expect(bounded.hits[0]?.matched[0]).toMatchObject({
      value_truncated: true,
    });
  });
});

describe("v1 event delivery", () => {
  it("tails stable event identities and applies a complete stream idempotently", () => {
    using source = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    const applied = source.transact(
      { id: "shared/note", "note/text": "portable" },
      { operationId: "share-note" },
    );
    source.transact({ id: "shared/other", "note/text": "second" });
    expect(source.receipt(applied.tx as number)).toMatchObject({
      basis_tx: Number(GENESIS_TX),
      tx: applied.tx,
      event: applied.event,
      operation_id: "share-note",
      event_hash: expect.stringMatching(/^sha256:[0-9a-f]{64}$/u),
      request_hash: expect.stringMatching(/^sha256:[0-9a-f]{64}$/u),
    });
    const stream = source.tail() as string;
    expect(stream).toContain('"fgraph":"event/1"');

    using target = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    const first = target.apply(stream);
    const second = target.apply(stream);
    expect(first).toHaveLength(2);
    expect(second).toEqual(
      first.map((report) => ({
        status: "already_applied",
        event: report.event,
        basis_tx: report.basis_tx,
        tx: report.tx,
        at: report.at,
        ids: {},
        asserted: [],
        retracted: [],
      })),
    );
    expect(target.entity("shared/note")).toEqual({
      "note/text": "portable",
    });
    expect(target.tail()).toBe(stream);

    const changed = stream.replace("portable", "tampered");
    expect(() => target.apply(changed)).toThrowError(Conflict);
  });

  it("retains replayable event payloads when nohistory removes old facts", () => {
    using source = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    source.declare("embedding/value", {
      type: "vector",
      dims: 2,
      nohistory: true,
      vectorModel: "test/model",
    });
    source.transact({
      id: "document/one",
      "embedding/value": { vector: [1, 0] },
    });
    source.transact({
      id: "document/one",
      "embedding/value": { vector: [0, 1] },
    });

    const stream = source.tail() as string;
    expect(stream).not.toContain('"redacted":true');
    expect(source.doctor()).toMatchObject({
      ok: true,
      unverifiable_event_hashes: 0,
    });

    using replayed = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    replayed.apply(stream);
    expect(replayed.entity("document/one")).toEqual({
      "embedding/value": { vector: [0, 1] },
    });
    expect(replayed.doctor()).toMatchObject({ ok: true });

    const snapshot = source.snapshot() as string;
    using restored = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    restored.restore(snapshot);
    expect(restored.entity("document/one")).toEqual({
      "embedding/value": { vector: [0, 1] },
    });
    expect(restored.doctor()).toMatchObject({ ok: true });
  });

  it("restores an atomic retained-state snapshot only into a pristine store", () => {
    using source = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    const first = source.transact({ id: "snapshot/item", "item/value": 1 });
    source.transact({ id: "snapshot/item", "item/value": 2 });
    const snapshot = source.snapshot() as string;
    const historicalSnapshot = source.at(first.tx).snapshot() as string;

    using target = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    target.restore(snapshot);
    expect(target.entity("snapshot/item")).toEqual({ "item/value": 2 });
    expect(target.history("snapshot/item", "item/value")).toHaveLength(2);
    expect(target.snapshot()).toBe(snapshot);
    expect(() => target.restore(snapshot)).toThrowError(Conflict);

    using historical = connect(":memory:", {
      clock: 1_900_000_000_000_000n,
      eventId: eventIds(),
    });
    historical.restore(historicalSnapshot);
    expect(historical.entity("snapshot/item")).toEqual({ "item/value": 1 });
    expect(historical.doctor()).toMatchObject({ ok: true });

    using truncated = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    const before = truncated.stats();
    expect(() =>
      truncated.restore(snapshot.split("\n").slice(0, -2).join("\n")),
    ).toThrowError();
    expect(truncated.stats()).toEqual(before);
  });

  it("rejects malformed event and snapshot streams without partial state", () => {
    using source = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    const declaration = source.declare("node/link", { ref: true });
    const first = source.transact(
      [
        { id: { tmp: "child" }, "node/value": 1.5 },
        {
          id: "node/parent",
          "node/link": { ref: { tmp: "child" } },
          "node/text": "portable",
        },
      ],
      {
        by: "stream-test",
        source: "unit",
        meta: { purpose: "coverage" },
        tx: { "audit/ref": { ref: "node/parent" } },
      },
    );
    source.transact({ id: "node/parent", "node/text": "updated" });
    const event = source.eventRecords(
      declaration.tx as number,
      first.tx as number,
    )[0] as Record<string, unknown>;

    using applied = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    expect(applied.apply([canonicalJson(event)])).toHaveLength(1);
    expect(applied.entity("node/parent")).toMatchObject({
      "node/text": "portable",
      "node/link": { ref: expect.any(Number) },
    });
    expect(applied.receipt(applied._basisTx())).toMatchObject({
      by: "stream-test",
      source: "unit",
      meta: { purpose: "coverage" },
      facts: [expect.objectContaining({ a: "audit/ref" })],
    });

    const invalidEvents: unknown[] = [
      [],
      { ...event, unknown: true },
      { ...event, event: 1 },
      { ...event, at: "bad" },
      { ...event, created: "bad" },
      { ...event, created: [null] },
      { ...event, asserted: [["node/parent"]] },
      {
        ...event,
        asserted: [["node/parent", "node/link", "bad", "ref"]],
      },
      {
        ...event,
        asserted: [["node/parent", "node/text", "portable", 7]],
      },
      {
        ...event,
        asserted: [["node/parent", "node/text", "portable", "unknown"]],
      },
      {
        ...event,
        asserted: [["node/parent", "node/score", "not-a-number", "float"]],
      },
      {
        ...event,
        asserted: [["node/parent", "node/count", "one", "int"]],
      },
      { ...event, tx_facts: "bad" },
      { ...event, tx_facts: [["audit/ref"]] },
      { ...event, by: 1 },
      { ...event, source: 1 },
    ];
    for (const invalid of invalidEvents) {
      const target = connect(":memory:", {
        clock: clock(),
        eventId: eventIds(),
      });
      try {
        const before = target.stats();
        expect(() => target.apply(canonicalJson(invalid))).toThrowError();
        expect(target.stats()).toEqual(before);
      } finally {
        target.close();
      }
    }

    const snapshot = source.snapshot() as string;
    const seal = (records: Array<Record<string, unknown>>): string => {
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
    };
    const rejectSnapshot = (
      mutate: (records: Array<Record<string, unknown>>) => void,
      reseal = true,
    ): void => {
      const records = snapshot
        .trim()
        .split("\n")
        .map(
          (line) =>
            parseJson(line, "snapshot mutation") as Record<string, unknown>,
        );
      mutate(records);
      const candidate = reseal
        ? seal(records)
        : `${records.map((record) => canonicalJson(record)).join("\n")}\n`;
      const target = connect(":memory:", {
        clock: clock(),
        eventId: eventIds(),
      });
      try {
        const before = target.stats();
        expect(() => target.restore(candidate)).toThrowError();
        expect(target.stats()).toEqual(before);
      } finally {
        target.close();
      }
    };
    const receipts = (records: Array<Record<string, unknown>>) =>
      records.filter((record) => Object.hasOwn(record, "receipt"));
    const facts = (records: Array<Record<string, unknown>>) =>
      records.filter((record) => Object.hasOwn(record, "fact"));

    rejectSnapshot((records) => {
      records[0] = { fgraph: "snapshot/1", format: 1 };
    });
    rejectSnapshot((records) => {
      (records.at(-1) as Record<string, unknown>).sha256 = "0".repeat(64);
    }, false);
    rejectSnapshot((records) => {
      const footer = records.at(-1) as Record<string, unknown>;
      footer.receipts = Number(footer.receipts) + 1;
    });
    rejectSnapshot((records) => {
      records[0] = {
        ...(records[0] as object),
        basis: "00000000-0000-4000-8000-000000000040",
      };
    });
    rejectSnapshot((records) => {
      const wrapper = receipts(records)[0] as Record<string, unknown>;
      wrapper.receipt = {};
    });
    rejectSnapshot((records) => {
      const receipt = (
        receipts(records)[0] as { receipt: Record<string, unknown> }
      ).receipt;
      receipt.event_hash = "bad";
    });
    rejectSnapshot((records) => {
      const receipt = (
        receipts(records)[0] as { receipt: Record<string, unknown> }
      ).receipt;
      receipt.operation_id = "unpaired";
    });
    rejectSnapshot((records) => {
      const receipt = (
        receipts(records)[0] as { receipt: Record<string, unknown> }
      ).receipt;
      receipt.operation_id = "\u0080";
      receipt.request_hash = "0".repeat(64);
    });
    rejectSnapshot((records) => {
      const receipt = (
        receipts(records)[0] as { receipt: Record<string, unknown> }
      ).receipt;
      receipt.origin_at = BigInt(receipt.origin_at as bigint) + 1n;
    });
    rejectSnapshot((records) => {
      const receipt = (
        receipts(records)[0] as {
          receipt: { created: unknown[] };
        }
      ).receipt;
      receipt.created.push("receipt-only/ghost");
    });
    rejectSnapshot((records) => {
      const firstReceipt = (
        receipts(records)[0] as { receipt: Record<string, unknown> }
      ).receipt;
      const secondReceipt = (
        receipts(records)[1] as { receipt: Record<string, unknown> }
      ).receipt;
      secondReceipt.event = firstReceipt.event;
      (records[0] as Record<string, unknown>).basis = firstReceipt.event;
    });
    rejectSnapshot((records) => {
      const firstReceipt = (
        receipts(records)[0] as { receipt: { created: unknown[] } }
      ).receipt;
      const secondReceipt = (
        receipts(records)[1] as { receipt: { created: unknown[] } }
      ).receipt;
      secondReceipt.created.push(firstReceipt.created[0]);
    });
    rejectSnapshot((records) => {
      (facts(records)[0] as Record<string, unknown>).fact = ["bad"];
    });
    rejectSnapshot((records) => {
      const fact = (facts(records)[0] as { fact: unknown[] }).fact;
      fact[0] = "missing/identity";
    });
    rejectSnapshot((records) => {
      const anonymous = receipts(records)
        .flatMap(
          (wrapper) =>
            (wrapper as { receipt: { created: unknown[] } }).receipt.created,
        )
        .find((selector) => typeof selector === "object");
      if (anonymous === undefined)
        throw new Error("snapshot has no anonymous selector to corrupt");
      const fact = (facts(records)[0] as { fact: unknown[] }).fact;
      fact[1] = anonymous;
    });
    rejectSnapshot((records) => {
      const fact = (facts(records)[0] as { fact: unknown[] }).fact;
      fact[4] = "00000000-0000-4000-8000-999999999999";
    });
    rejectSnapshot((records) => {
      const fact = facts(records)
        .map((wrapper) => (wrapper as { fact: unknown[] }).fact)
        .find((tuple) => tuple[3] === "ref") as unknown[];
      fact[2] = "bad";
    });
    rejectSnapshot((records) => {
      const fact = (facts(records)[0] as { fact: unknown[] }).fact;
      fact[3] = "bytes";
    });
    rejectSnapshot((records) => {
      const fact = (facts(records)[0] as { fact: unknown[] }).fact;
      fact[5] = "00000000-0000-4000-8000-999999999999";
    });
  });

  it("preserves every cardinality-many transaction fact during event replay", () => {
    using source = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    source.declare("audit/tag", { type: "text", many: true });
    source.transact(
      { id: "portable/subject", "portable/value": 1 },
      { tx: { "audit/tag": ["alpha", "beta"] } },
    );
    const events = source.tail() as string;

    using target = connect(":memory:", {
      clock: clock(),
      eventId: eventIds(),
    });
    target.apply(events);
    expect(target.entity(target._basisTx())).toMatchObject({
      "audit/tag": ["alpha", "beta"],
    });
  });

  it("publishes verified exact backups without overwriting any destination", async () => {
    const directory = mkdtempSync(join(tmpdir(), "fgraph-v1-backup-"));
    try {
      const sourcePath = join(directory, "source.db");
      const backupPath = join(directory, "backup.db");
      using source = connect(sourcePath, {
        clock: clock(),
        eventId: eventIds(),
      });
      source.transact({ id: "backup/item", "item/value": "durable" });
      await source.backup(backupPath);
      using backup = connect(backupPath, { readOnly: true });
      expect(backup.entity("backup/item")).toEqual({
        "item/value": "durable",
      });
      expect(backup.doctor()).toMatchObject({ ok: true });
      await expect(source.backup(backupPath)).rejects.toThrowError(Conflict);

      const emptyPath = join(directory, "already-exists.db");
      closeSync(openSync(emptyPath, "wx"));
      await expect(source.backup(emptyPath)).rejects.toThrowError(Conflict);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
import { closeSync, mkdtempSync, openSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
