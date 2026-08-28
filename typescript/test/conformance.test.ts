import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import { FGraphError } from "../src/errors.js";
import { JsonFloat, canonicalJson, parseJson } from "../src/jsonio.js";
import type { Db, RawRow } from "../src/store.js";
import { connect } from "../src/store.js";

const here = dirname(fileURLToPath(import.meta.url));
const casesRoot = resolve(here, "../../conformance/cases");
const portableBoundaryPath = resolve(
  here,
  "../../conformance/portable-boundaries.json",
);

class Clock {
  value = 1_767_225_600_000_000n;

  tick = (): bigint => {
    const result = this.value;
    this.value += 1_000_000n;
    return result;
  };
}

interface Step extends Record<string, unknown> {
  expect?: unknown;
  error?: unknown;
}

function matches(
  actual: unknown,
  expected: unknown,
  path: Array<string | number> = [],
  unordered = new Set<string>(),
): boolean {
  if (expected instanceof JsonFloat) {
    const actualNumber = actual instanceof JsonFloat ? actual.value : actual;
    return (
      typeof actualNumber === "number" &&
      Object.is(actualNumber, expected.value)
    );
  }
  if (
    expected !== null &&
    typeof expected === "object" &&
    !Array.isArray(expected)
  ) {
    if (actual === null || typeof actual !== "object" || Array.isArray(actual))
      return false;
    const expectedRecord = expected as Record<string, unknown>;
    const actualRecord = actual as Record<string, unknown>;
    const allowExtra = expectedRecord["..."] === true;
    const expectedKeys = Object.keys(expectedRecord).filter(
      (key) => key !== "...",
    );
    if (
      !allowExtra &&
      Object.keys(actualRecord).sort().join("\0") !==
        expectedKeys.sort().join("\0")
    )
      return false;
    return expectedKeys.every(
      (key) =>
        Object.hasOwn(actualRecord, key) &&
        matches(
          actualRecord[key],
          expectedRecord[key],
          [...path, key],
          unordered,
        ),
    );
  }
  if (Array.isArray(expected)) {
    if (!Array.isArray(actual) || actual.length !== expected.length)
      return false;
    if (unordered.has(path.join("/"))) {
      const remaining = [...actual];
      for (const wanted of expected) {
        const index = remaining.findIndex((candidate) =>
          matches(candidate, wanted, [...path, "*"], unordered),
        );
        if (index === -1) return false;
        remaining.splice(index, 1);
      }
      return true;
    }
    return expected.every((wanted, index) =>
      matches(actual[index], wanted, [...path, index], unordered),
    );
  }
  return Object.is(actual, expected);
}

function argumentsOf(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [value];
}

function actual(db: Db, step: Step): unknown {
  if (Object.hasOwn(step, "stats")) return db.stats();
  if (Object.hasOwn(step, "tx")) {
    const options = { ...((step.options ?? {}) as Record<string, unknown>) };
    if (Object.hasOwn(options, "operation_id")) {
      options.operationId = options.operation_id;
      delete options.operation_id;
    }
    if (Object.hasOwn(options, "if_basis_tx")) {
      options.ifBasisTx = options.if_basis_tx;
      delete options.if_basis_tx;
    }
    return db.transact(step.tx, options as Record<string, never>);
  }
  if (Object.hasOwn(step, "undo")) {
    const options = { ...(step.undo as Record<string, unknown>) };
    const target = options.target;
    delete options.target;
    if (Object.hasOwn(options, "operation_id")) {
      options.operationId = options.operation_id;
      delete options.operation_id;
    }
    if (Object.hasOwn(options, "if_basis_tx")) {
      options.ifBasisTx = options.if_basis_tx;
      delete options.if_basis_tx;
    }
    return db.undo(target as number | bigint, options);
  }
  if (Object.hasOwn(step, "declare")) {
    const declaration = { ...(step.declare as Record<string, unknown>) };
    const attribute = declaration.attr;
    delete declaration.attr;
    if (Object.hasOwn(declaration, "vector_model")) {
      declaration.vectorModel = declaration.vector_model;
      delete declaration.vector_model;
    }
    if (Object.hasOwn(declaration, "operation_id")) {
      declaration.operationId = declaration.operation_id;
      delete declaration.operation_id;
    }
    if (Object.hasOwn(declaration, "if_basis_tx")) {
      declaration.ifBasisTx = declaration.if_basis_tx;
      delete declaration.if_basis_tx;
    }
    return db.declare(attribute as string, declaration);
  }
  if (Object.hasOwn(step, "shape")) {
    const definition = { ...(step.shape as Record<string, unknown>) };
    const name = definition.name;
    delete definition.name;
    if (Object.hasOwn(definition, "operation_id")) {
      definition.operationId = definition.operation_id;
      delete definition.operation_id;
    }
    if (Object.hasOwn(definition, "if_basis_tx")) {
      definition.ifBasisTx = definition.if_basis_tx;
      delete definition.if_basis_tx;
    }
    return db.defineShape(name as string, definition);
  }
  if (Object.hasOwn(step, "q"))
    return db.q(
      step.q as Record<string, unknown>,
      (step.args ?? {}) as Record<string, unknown>,
    );
  if (Object.hasOwn(step, "entity")) return db.entity(step.entity as never);
  if (Object.hasOwn(step, "history")) {
    const [entity, attribute] = argumentsOf(step.history);
    return db.history(entity as never, attribute as string | undefined);
  }
  if (Object.hasOwn(step, "diff")) {
    const [first, second] = step.diff as unknown[];
    return db.diff(first, second);
  }
  if (Object.hasOwn(step, "why")) {
    const [entity, attribute] = argumentsOf(step.why);
    return db.why(entity as never, attribute as string | undefined);
  }
  if (Object.hasOwn(step, "receipt")) return db.receipt(step.receipt as never);
  if (Object.hasOwn(step, "schema")) {
    const options = step.schema as {
      prefix?: string;
      include_system?: boolean;
    };
    return db.schema(options.prefix, {
      includeSystem: options.include_system ?? false,
    });
  }
  if (Object.hasOwn(step, "schema_manifest")) return db.schemaManifest();
  if (Object.hasOwn(step, "schema_check"))
    return db.checkSchemaManifest(step.schema_check);
  if (Object.hasOwn(step, "schema_apply")) {
    const application = {
      ...(step.schema_apply as Record<string, unknown>),
    };
    const manifest = application.manifest;
    delete application.manifest;
    if (Object.hasOwn(application, "operation_id")) {
      application.operationId = application.operation_id;
      delete application.operation_id;
    }
    if (Object.hasOwn(application, "if_basis_tx")) {
      application.ifBasisTx = application.if_basis_tx;
      delete application.if_basis_tx;
    }
    return db.applySchemaManifest(manifest, application);
  }
  if (Object.hasOwn(step, "validate"))
    return db.validate(step.validate as never);
  if (Object.hasOwn(step, "datoms")) {
    const options = { ...(step.datoms as Record<string, unknown>) };
    const index = (options.index ?? "eavt") as "eavt" | "avet" | "vaet";
    delete options.index;
    return db.datoms(index, options);
  }
  if (Object.hasOwn(step, "explain"))
    return db.explain(
      step.explain as Record<string, unknown>,
      (step.args ?? {}) as Record<string, unknown>,
    );
  if (Object.hasOwn(step, "search")) {
    const search = { ...(step.search as Record<string, unknown>) };
    if (Object.hasOwn(search, "vector_attribute")) {
      search.vectorAttribute = search.vector_attribute;
      delete search.vector_attribute;
    }
    if (Object.hasOwn(search, "text_attributes")) {
      search.textAttributes = search.text_attributes;
      delete search.text_attributes;
    }
    return db.search(search as never);
  }
  if (Object.hasOwn(step, "attributes")) {
    const options = step.attributes as {
      prefix?: string;
      includeSystem?: boolean;
      include_system?: boolean;
    };
    const includeSystem = options.includeSystem ?? options.include_system;
    return includeSystem === undefined
      ? db.attributes(options.prefix)
      : db.attributes(options.prefix, { includeSystem });
  }
  if (Object.hasOwn(step, "facts")) {
    return db._connection
      .prepare<[], RawRow>(
        "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE id>39 ORDER BY id",
      )
      .all()
      .map((row) => [
        row.id,
        row.e,
        row.a,
        Buffer.isBuffer(row.v) ? { hex: row.v.toString("hex") } : row.v,
        row.t,
        row.tx,
        row.rx,
      ])
      .map((row) =>
        row.map((value) =>
          typeof value === "bigint" &&
          value <= BigInt(Number.MAX_SAFE_INTEGER) &&
          value >= BigInt(Number.MIN_SAFE_INTEGER)
            ? Number(value)
            : value,
        ),
      );
  }
  throw new Error(`unknown conformance step: ${canonicalJson(step)}`);
}

function runStep(db: Db, step: Step): void {
  if (step.error !== undefined) {
    try {
      if (Object.hasOwn(step, "at")) db.at(step.at);
      else actual(db, step);
      expect.fail(`expected ${String(step.error)}`);
    } catch (error) {
      expect(error).toBeInstanceOf(FGraphError);
      expect((error as Error).name).toBe(step.error);
    }
    return;
  }
  if (Object.hasOwn(step, "at")) {
    const view = db.at(step.at);
    for (const nested of step.steps as Step[]) runStep(view, nested);
    return;
  }
  const result = actual(db, step);
  if (Object.hasOwn(step, "expect")) {
    const query = step.q;
    const order =
      query !== null && typeof query === "object" && !Array.isArray(query)
        ? (query as Record<string, unknown>).order
        : undefined;
    const unordered =
      query !== null &&
      typeof query === "object" &&
      !Array.isArray(query) &&
      (!Array.isArray(order) || order.length === 0)
        ? new Set(["rows"])
        : new Set<string>();
    expect(
      matches(result, step.expect, [], unordered),
      `actual=${canonicalJson(result)}\nexpected=${canonicalJson(step.expect)}`,
    ).toBe(true);
  }
}

function caseFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = resolve(directory, entry.name);
    return entry.isDirectory()
      ? caseFiles(path)
      : entry.isFile() && entry.name.endsWith(".json")
        ? [path]
        : [];
  });
}

const files = caseFiles(casesRoot).sort();

describe("shared conformance", () => {
  for (const path of files) {
    it(relative(casesRoot, path), () => {
      const testCase = parseJson(readFileSync(path, "utf8"), path) as {
        steps: Step[];
      };
      const clock = new Clock();
      using db = connect(":memory:", { clock: clock.tick });
      for (const step of testCase.steps) runStep(db, step);
    });
  }

  it("portable-boundaries.json", () => {
    const boundaries = parseJson(
      readFileSync(portableBoundaryPath, "utf8"),
      portableBoundaryPath,
    ) as {
      unicode_value: string;
      invalid_json: Array<{ name: string; wire: string; error: string }>;
      snapshot_mutations: Array<{ name: string; error: string }>;
    };
    for (const invalid of boundaries.invalid_json) {
      using db = connect(":memory:", { clock: new Clock().tick });
      try {
        db.transact(parseJson(invalid.wire, invalid.name));
        expect.fail(`expected ${invalid.error} for ${invalid.name}`);
      } catch (error) {
        expect(error).toBeInstanceOf(FGraphError);
        expect((error as Error).name).toBe(invalid.error);
      }
    }

    using source = connect(":memory:", { clock: new Clock().tick });
    source.transact([
      {
        id: "portable/unicode",
        "portable/value": boundaries.unicode_value,
      },
      { "portable/anonymous": true },
    ]);
    const snapshot = source.snapshot() as string;
    const events = source.tail() as string;
    for (const [stream, restore] of [
      [snapshot, true],
      [events, false],
    ] as const) {
      using target = connect(":memory:", { clock: new Clock().tick });
      if (restore) target.restore(stream);
      else target.apply(stream);
      expect(target.entity("portable/unicode")).toEqual({
        "portable/value": boundaries.unicode_value,
      });
    }

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
    for (const mutation of boundaries.snapshot_mutations) {
      const records = snapshot
        .trim()
        .split("\n")
        .map(
          (line) => parseJson(line, mutation.name) as Record<string, unknown>,
        );
      const receipts = records.filter((record) =>
        Object.hasOwn(record, "receipt"),
      ) as Array<{ receipt: Record<string, unknown> }>;
      const facts = records.filter((record) =>
        Object.hasOwn(record, "fact"),
      ) as Array<{ fact: unknown[] }>;
      const receipt = receipts[0]?.receipt;
      if (receipt === undefined) throw new Error("snapshot receipt missing");
      if (mutation.name === "receipt_created_mismatch")
        (receipt.created as unknown[]).push("receipt-only/ghost");
      else if (mutation.name === "anonymous_attribute") {
        const anonymous = receipts
          .flatMap((wrapper) => wrapper.receipt.created as unknown[])
          .find((selector) => typeof selector === "object");
        if (anonymous === undefined || facts[0] === undefined)
          throw new Error("snapshot anonymous selector or fact missing");
        facts[0].fact[1] = anonymous;
      } else if (mutation.name === "operation_id_control") {
        receipt.operation_id = "\u0080";
        receipt.request_hash = "0".repeat(64);
      } else throw new Error(`unknown snapshot mutation ${mutation.name}`);

      using target = connect(":memory:", { clock: new Clock().tick });
      const before = target.stats();
      try {
        target.restore(seal(records));
        expect.fail(`expected ${mutation.error} for ${mutation.name}`);
      } catch (error) {
        expect(error).toBeInstanceOf(FGraphError);
        expect((error as Error).name).toBe(mutation.error);
      }
      expect(target.stats()).toEqual(before);
    }
  });
});

describe("matcher", () => {
  it("supports explicit object elision and unordered rows", () => {
    expect(matches({ value: 1, extra: 2 }, { value: 1, "...": true })).toBe(
      true,
    );
    expect(
      matches(
        { rows: [[1], [2]] },
        { rows: [[2], [1]] },
        [],
        new Set(["rows"]),
      ),
    ).toBe(true);
  });
});
