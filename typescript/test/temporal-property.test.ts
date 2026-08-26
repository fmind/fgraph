import { describe, expect, it } from "vitest";

import { canonicalJson } from "../src/jsonio.js";
import type { RenderedFact, WireInteger } from "../src/models.js";
import { GENESIS_TX, connect } from "../src/store.js";

interface State {
  value?: number;
  tags: Set<string>;
}

interface Lifetime {
  entity: string;
  attribute: string;
  value: unknown;
  tx: number;
  rx: number | null;
}

function clone(states: Map<string, State>): Map<string, State> {
  return new Map(
    [...states].map(([entity, state]) => {
      const copy: State = { tags: new Set(state.tags) };
      if (state.value !== undefined) copy.value = state.value;
      return [entity, copy];
    }),
  );
}

function facts(
  states: Map<string, State>,
): Map<string, [string, string, unknown]> {
  const result = new Map<string, [string, string, unknown]>();
  for (const [entity, state] of states) {
    if (state.value !== undefined)
      result.set(`${entity}\0prop/value\0${canonicalJson(state.value)}`, [
        entity,
        "prop/value",
        state.value,
      ]);
    for (const tag of state.tags)
      result.set(`${entity}\0prop/tag\0${canonicalJson(tag)}`, [
        entity,
        "prop/tag",
        tag,
      ]);
  }
  return result;
}

function visibleEntity(state: State): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  if (state.tags.size > 0) result["prop/tag"] = [...state.tags];
  if (state.value !== undefined) result["prop/value"] = state.value;
  return result;
}

function renderedKey(fact: RenderedFact): string {
  return `${String(fact.e)}\0${fact.a}\0${canonicalJson(fact.v)}`;
}

describe("seeded temporal reference model", () => {
  it("matches current state, at/history, and diff after mixed assert/retract/declare sequences", () => {
    let now = 1_767_225_600_000_000n;
    using db = connect(":memory:", { clock: () => (now += 1_000_000n) });
    db.declare("prop/tag", { type: "text", many: true, doc: "seeded tags" });
    db.declare("prop/value", { type: "int" });
    const names = Array.from({ length: 5 }, (_, index) => `entity/${index}`);
    db.transact(names.map((id) => ({ id })));
    const model = new Map(
      names.map((name) => [name, { tags: new Set<string>() }]),
    );
    const lifetimes = new Map<string, Lifetime>();
    const completed: Lifetime[] = [];
    let previousTx: WireInteger = db._latestTx();
    let previous = clone(model);
    let seed = 0x5eed_1234;
    const random = (maximum: number): number => {
      seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
      return seed % maximum;
    };

    for (let step = 0; step < 120; step++) {
      const entity = names[random(names.length)] as string;
      const state = model.get(entity) as State;
      const before = facts(model);
      const action = random(6);
      let report;
      if (action === 0) {
        const value = random(8);
        report = db.transact({ id: entity, "prop/value": value });
        state.value = value;
      } else if (action === 1) {
        const tag = `tag-${random(5)}`;
        report = db.transact(["assert", entity, "prop/tag", tag]);
        state.tags.add(tag);
      } else if (action === 2) {
        const tag = `tag-${random(5)}`;
        report = db.retract(entity, "prop/tag", tag);
        state.tags.delete(tag);
      } else if (action === 3) {
        report = db.retract(entity, "prop/value");
        delete state.value;
      } else if (action === 4) {
        report = db.retract(entity);
        delete state.value;
        state.tags.clear();
      } else {
        report = db.declare("prop/tag", { doc: `seed-${random(4)}` });
      }

      for (const [name, expected] of model)
        expect(db.entity(name)).toEqual(visibleEntity(expected));
      if (report.tx === null) {
        expect(facts(model)).toEqual(before);
        continue;
      }
      const transaction = report.tx as number;
      const after = facts(model);
      for (const [key] of before) {
        if (after.has(key)) continue;
        const lifetime = lifetimes.get(key);
        if (lifetime !== undefined) {
          lifetime.rx = transaction;
          completed.push(lifetime);
          lifetimes.delete(key);
        }
      }
      for (const [key, [factEntity, attribute, value]] of after) {
        if (!before.has(key))
          lifetimes.set(key, {
            entity: factEntity,
            attribute,
            value,
            tx: transaction,
            rx: null,
          });
      }

      const delta = db.diff(previousTx, transaction);
      const actualAsserted = new Set(
        delta.asserted
          .filter(
            (fact) =>
              fact.a.startsWith("prop/") && names.includes(String(fact.e)),
          )
          .map(renderedKey),
      );
      const actualRetracted = new Set(
        delta.retracted
          .filter(
            (fact) =>
              fact.a.startsWith("prop/") && names.includes(String(fact.e)),
          )
          .map(renderedKey),
      );
      const expectedAsserted = new Set(
        [...after.keys()].filter((key) => !facts(previous).has(key)),
      );
      const expectedRetracted = new Set(
        [...facts(previous).keys()].filter((key) => !after.has(key)),
      );
      expect(actualAsserted).toEqual(expectedAsserted);
      expect(actualRetracted).toEqual(expectedRetracted);
      using historical = db.at(transaction);
      for (const [name, expected] of model)
        expect(historical.entity(name)).toEqual(visibleEntity(expected));
      previousTx = transaction;
      previous = clone(model);
    }

    completed.push(...lifetimes.values());
    for (const entity of names) {
      const actual = db
        .history(entity)
        .filter((fact) => String(fact.a).startsWith("prop/"))
        .map((fact) => ({
          entity: String(fact.e),
          attribute: String(fact.a),
          value: fact.v,
          tx: fact.tx,
          rx: fact.rx,
        }))
        .sort((left, right) =>
          canonicalJson(left).localeCompare(canonicalJson(right)),
        );
      const expected = completed
        .filter((fact) => fact.entity === entity)
        .map((fact) => ({
          entity: fact.entity,
          attribute: fact.attribute,
          value: fact.value,
          tx: fact.tx,
          rx: fact.rx,
        }))
        .sort((left, right) =>
          canonicalJson(left).localeCompare(canonicalJson(right)),
        );
      expect(actual).toEqual(expected);
    }
    expect(previousTx).toBeGreaterThan(GENESIS_TX);
  });
});
