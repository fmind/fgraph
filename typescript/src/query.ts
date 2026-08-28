import { NotFound, QueryError, TooLarge } from "./errors.js";
import { canonicalJson, compareUnicode } from "./jsonio.js";
import type { Result } from "./models.js";
import type { Db } from "./store.js";
import {
  ATTRIBUTE_PATTERN,
  BOOL,
  FLOAT,
  INT,
  INT64_MAX,
  INT64_MIN,
  REF,
  type Cell,
  isRecord,
  publicInteger,
  wireValue,
} from "./values.js";

type Binding = Map<string, Cell>;
type Relation = Map<string, Cell[]>;
type Relations = Map<string, Relation>;
type RuleDefinition = { head: unknown[]; body: unknown[] };

const PREDICATES = new Set([
  "=",
  "!=",
  "<",
  "<=",
  ">",
  ">=",
  "contains",
  "starts-with",
]);
const AGGREGATES = new Set([
  "count",
  "count-distinct",
  "sum",
  "min",
  "max",
  "avg",
]);
const QUERY_KEYS = new Set([
  "find",
  "where",
  "in",
  "order",
  "limit",
  "offset",
  "rules",
  "source",
]);

export interface PatternPlan {
  access:
    | "eavt/exact"
    | "eavt/ea"
    | "eavt/batch"
    | "avet"
    | "eavt/e"
    | "avet/a"
    | "value-scan"
    | "scan";
  bound: { e: boolean; a: boolean; v: boolean };
  rank: number;
}

export function isPatternClause(clause: unknown): clause is unknown[] {
  return (
    Array.isArray(clause) &&
    !(clause.length > 0 && PREDICATES.has(String(clause[0])))
  );
}

export function planPattern(
  clause: unknown[],
  bound: ReadonlySet<string>,
): PatternPlan {
  const fixed = (term: unknown): boolean =>
    term !== "_" && (!isVariable(term) || bound.has(term));
  const e = fixed(clause[0]);
  const a = fixed(clause[1]);
  const v = fixed(clause[2]);
  if (e && a && v) return { access: "eavt/exact", bound: { e, a, v }, rank: 0 };
  if (e && a) {
    const batched =
      isVariable(clause[0]) && bound.has(clause[0]) && clause.length === 3;
    return {
      access: batched ? "eavt/batch" : "eavt/ea",
      bound: { e, a, v },
      rank: 0,
    };
  }
  if (a && v) return { access: "avet", bound: { e, a, v }, rank: 1 };
  if (e) return { access: "eavt/e", bound: { e, a, v }, rank: 2 };
  if (a) return { access: "avet/a", bound: { e, a, v }, rank: 3 };
  if (v) return { access: "value-scan", bound: { e, a, v }, rank: 4 };
  return { access: "scan", bound: { e, a, v }, rank: 5 };
}

interface WorkOptions {
  budget: number;
  signal: AbortSignal | undefined;
}

class WorkBudget {
  #remaining: number;
  readonly #signal: AbortSignal | undefined;

  constructor(options: WorkOptions) {
    if (!Number.isSafeInteger(options.budget) || options.budget <= 0)
      throw new QueryError(
        `query budget ${String(options.budget)} is invalid; use a positive safe integer`,
      );
    this.#remaining = options.budget;
    this.#signal = options.signal;
  }

  spend(): void {
    if (this.#signal?.aborted)
      throw new QueryError(
        "query was cancelled; retry without aborting its signal",
      );
    if (this.#remaining === 0)
      throw new TooLarge(
        "query exhausted its work budget; narrow the clauses or open with a larger queryBudget",
      );
    this.#remaining--;
  }
}

function isVariable(term: unknown): term is string {
  return typeof term === "string" && term.startsWith("?");
}

function valueKey(value: unknown): string {
  if (typeof value === "bigint") return `i:${value}`;
  if (Buffer.isBuffer(value)) return `b:${value.toString("hex")}`;
  if (Array.isArray(value)) return `a:${value.map(valueKey).join("|")}`;
  return `${typeof value}:${String(value)}`;
}

function cellKey(cell: Cell): string {
  return `${cell.tag}:${valueKey(cell.value)}`;
}

function bindingKey(binding: Binding): string {
  return [...binding]
    .sort(([left], [right]) => compareUnicode(left, right))
    .map(([name, cell]) => `${name.length}:${name}:${cellKey(cell)}`)
    .join(";");
}

function dedupe(bindings: Binding[]): Binding[] {
  const seen = new Set<string>();
  return bindings.filter((binding) => {
    const key = bindingKey(binding);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function cellsEqual(left: Cell, right: Cell): boolean {
  if (left.tag === right.tag) {
    if (Buffer.isBuffer(left.value) && Buffer.isBuffer(right.value))
      return left.value.equals(right.value);
    if (Array.isArray(left.value) && Array.isArray(right.value))
      return canonicalJson(left.value) === canonicalJson(right.value);
    return left.value === right.value;
  }
  return (
    numeric(left) &&
    numeric(right) &&
    compareNumeric(left.value, right.value) === 0
  );
}

function constant(db: Db, value: unknown, entity = false): Cell | null {
  if (entity) {
    const candidate =
      isRecord(value) &&
      Object.keys(value).length === 1 &&
      Object.hasOwn(value, "ref")
        ? value.ref
        : value;
    try {
      const resolved = db._resolveRead(candidate, true);
      return resolved === null ? null : { tag: REF, value: resolved };
    } catch (error) {
      if (error instanceof NotFound) return null;
      throw new QueryError(
        `invalid entity query constant: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  try {
    const encoded = db._encodeReadValue(value);
    return { tag: encoded.tag, value: encoded.logical };
  } catch (error) {
    if (error instanceof NotFound) return null;
    throw new QueryError(
      `invalid query constant: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

function unify(
  binding: Binding,
  term: unknown,
  cell: Cell,
  fixed: Cell | null,
): Binding | null {
  if (term === "_") return binding;
  if (isVariable(term)) {
    const known = binding.get(term);
    if (known !== undefined) return cellsEqual(known, cell) ? binding : null;
    const result = new Map(binding);
    result.set(term, cell);
    return result;
  }
  return fixed !== null && cellsEqual(fixed, cell) ? binding : null;
}

function pattern(
  db: Db,
  clause: unknown[],
  bindings: Binding[],
  work: WorkBudget,
): Binding[] {
  // safeNegation validates pattern arity and attribute syntax before execution.
  const entityFixed =
    isVariable(clause[0]) || clause[0] === "_"
      ? null
      : constant(db, clause[0], true);
  const attributeFixed =
    isVariable(clause[1]) || clause[1] === "_"
      ? null
      : constant(db, clause[1], true);
  const valueFixed =
    isVariable(clause[2]) || clause[2] === "_" ? null : constant(db, clause[2]);
  const transactionFixed =
    clause.length < 4 || isVariable(clause[3]) || clause[3] === "_"
      ? null
      : constant(db, clause[3], true);
  const addedFixed =
    clause.length < 5 || isVariable(clause[4]) || clause[4] === "_"
      ? null
      : constant(db, clause[4]);
  if (
    (clause[0] !== "_" && !isVariable(clause[0]) && entityFixed === null) ||
    (clause[1] !== "_" && !isVariable(clause[1]) && attributeFixed === null) ||
    (clause[2] !== "_" && !isVariable(clause[2]) && valueFixed === null) ||
    (clause.length >= 4 &&
      clause[3] !== "_" &&
      !isVariable(clause[3]) &&
      transactionFixed === null) ||
    (clause.length >= 5 &&
      clause[4] !== "_" &&
      !isVariable(clause[4]) &&
      addedFixed === null)
  )
    return [];
  const entityVariable = isVariable(clause[0]) ? clause[0] : null;
  const valueUnbound =
    clause[2] === "_" ||
    (isVariable(clause[2]) &&
      bindings.every((binding) => !binding.has(clause[2] as string)));
  if (
    clause.length === 3 &&
    entityVariable !== null &&
    attributeFixed?.tag === REF &&
    valueUnbound &&
    bindings.length > 1 &&
    bindings.every((binding) => binding.get(entityVariable)?.tag === REF)
  ) {
    const pages = db._queryDatomsForEntities(
      bindings.map((binding) => binding.get(entityVariable)?.value as bigint),
      attributeFixed.value as bigint,
      () => work.spend(),
    );
    if (pages !== null) {
      const result: Binding[] = [];
      for (const binding of bindings) {
        const entity = binding.get(entityVariable)?.value as bigint;
        for (const datom of pages.get(entity) ?? []) {
          const row = datom.row;
          let current = unify(
            binding,
            clause[0],
            { tag: REF, value: row.e },
            entityFixed,
          );
          if (current !== null)
            current = unify(
              current,
              clause[1],
              { tag: REF, value: row.a },
              attributeFixed,
            );
          if (current !== null)
            current = unify(
              current,
              clause[2],
              db._cell(Number(row.t), row.v),
              valueFixed,
            );
          if (current !== null) result.push(current);
        }
      }
      return dedupe(result);
    }
  }
  const result: Binding[] = [];
  for (const binding of bindings) {
    const reference = (term: unknown, fixed: Cell | null): bigint | null => {
      if (isVariable(term)) {
        const known = binding.get(term);
        return known?.tag === REF ? (known.value as bigint) : null;
      }
      return fixed?.tag === REF ? (fixed.value as bigint) : null;
    };
    const entity = clause[0] === "_" ? null : reference(clause[0], entityFixed);
    const attribute =
      clause[1] === "_" ? null : reference(clause[1], attributeFixed);
    const boundCell = (term: unknown, fixed: Cell | null): Cell | null =>
      isVariable(term) ? (binding.get(term) ?? null) : fixed;
    const value = clause[2] === "_" ? null : boundCell(clause[2], valueFixed);
    const transactionCell =
      clause.length < 4 || clause[3] === "_"
        ? null
        : boundCell(clause[3], transactionFixed);
    const addedCell =
      clause.length < 5 || clause[4] === "_"
        ? null
        : boundCell(clause[4], addedFixed);
    const transaction =
      transactionCell?.tag === REF ? (transactionCell.value as bigint) : null;
    const added = addedCell?.tag === BOOL ? (addedCell.value as boolean) : null;
    for (const datom of db._queryDatoms(
      entity,
      attribute,
      value,
      transaction,
      added,
    )) {
      work.spend();
      const row = datom.row;
      let current = unify(
        binding,
        clause[0],
        { tag: REF, value: row.e },
        entityFixed,
      );
      if (current === null) continue;
      current = unify(
        current,
        clause[1],
        { tag: REF, value: row.a },
        attributeFixed,
      );
      if (current === null) continue;
      current = unify(
        current,
        clause[2],
        db._cell(Number(row.t), row.v),
        valueFixed,
      );
      if (current === null) continue;
      if (clause.length >= 4)
        current = unify(
          current,
          clause[3],
          { tag: REF, value: datom.eventTx },
          transactionFixed,
        );
      if (current === null) continue;
      if (clause.length >= 5)
        current = unify(
          current,
          clause[4],
          { tag: BOOL, value: datom.added },
          addedFixed,
        );
      if (current !== null) result.push(current);
    }
  }
  return dedupe(result);
}

function operand(db: Db, term: unknown, binding: Binding): Cell {
  if (isVariable(term)) {
    const result = binding.get(term);
    if (result === undefined)
      throw new QueryError(
        `predicate variable ${term} is unbound; place a binding pattern before the predicate`,
      );
    return result;
  }
  const result = constant(db, term);
  if (result === null)
    throw new QueryError(
      `predicate constant ${String(term)} cannot be resolved; use a scalar or existing ref`,
    );
  return result;
}

function numeric(cell: Cell): cell is Cell & { value: number | bigint } {
  return cell.tag === INT || cell.tag === FLOAT;
}

function compareNumeric(left: number | bigint, right: number | bigint): number {
  if (typeof left === "bigint" && typeof right === "bigint")
    return left === right ? 0 : left < right ? -1 : 1;
  if (typeof left === "number" && typeof right === "number")
    return left === right ? 0 : left < right ? -1 : 1;
  if (typeof left === "bigint") {
    if (Number.isInteger(right)) {
      const asRight = BigInt(right);
      return left === asRight ? 0 : left < asRight ? -1 : 1;
    }
    // A non-integral number cannot equal an integer value.
    return Number(left) < right ? -1 : 1;
  }
  if (Number.isInteger(left)) {
    const asLeft = BigInt(left);
    return asLeft === right ? 0 : asLeft < right ? -1 : 1;
  }
  // This branch has a non-integral number on the left and an integer on the right.
  return left < Number(right) ? -1 : 1;
}

function compare(operator: string, left: Cell, right: Cell): boolean {
  if (operator === "contains" || operator === "starts-with") {
    if (typeof left.value !== "string" || typeof right.value !== "string")
      throw new QueryError(
        `predicate ${operator} requires text operands; bind or pass strings`,
      );
    return operator === "contains"
      ? left.value.includes(right.value)
      : left.value.startsWith(right.value);
  }
  if (operator === "=" || operator === "!=") {
    const equal = cellsEqual(left, right);
    return operator === "=" ? equal : !equal;
  }
  let order: number;
  if (numeric(left) && numeric(right))
    order = compareNumeric(left.value, right.value);
  else {
    if (left.tag !== right.tag)
      throw new QueryError(
        `predicate ${operator} cannot order unlike types; compare values of the same type`,
      );
    if (typeof left.value !== "string" || typeof right.value !== "string")
      throw new QueryError(
        `predicate ${operator} cannot compare these values; use orderable scalars`,
      );
    order = compareUnicode(left.value, right.value);
  }
  if (operator === "<") return order < 0;
  if (operator === "<=") return order <= 0;
  if (operator === ">") return order > 0;
  // safeNegation restricts this final case to >= before evaluation.
  return order >= 0;
}

function predicate(
  db: Db,
  clause: unknown[],
  bindings: Binding[],
  work: WorkBudget,
): Binding[] {
  // safeNegation validates the operator and exact operand count first.
  return bindings.filter((binding) => {
    work.spend();
    return compare(
      clause[0] as string,
      operand(db, clause[1], binding),
      operand(db, clause[2], binding),
    );
  });
}

function clauseVariables(clause: unknown): Set<string> {
  if (Array.isArray(clause)) {
    const result = new Set<string>();
    clause.forEach((item) =>
      clauseVariables(item).forEach((variable) => result.add(variable)),
    );
    return result;
  }
  if (isRecord(clause)) {
    const result = new Set<string>();
    Object.values(clause).forEach((item) =>
      clauseVariables(item).forEach((variable) => result.add(variable)),
    );
    return result;
  }
  return isVariable(clause) ? new Set([clause]) : new Set();
}

function safeNegation(
  clauses: unknown[],
  initiallyBound: Set<string>,
): Set<string> {
  const bound = new Set(initiallyBound);
  for (const clause of clauses) {
    if (
      isRecord(clause) &&
      Object.keys(clause).length === 1 &&
      Object.hasOwn(clause, "not")
    ) {
      if (!Array.isArray(clause.not))
        throw new QueryError("not clause must contain an array of clauses");
      if (
        ![...clauseVariables(clause.not)].some((variable) =>
          bound.has(variable),
        )
      )
        throw new QueryError(
          "negation is uncorrelated; bind at least one of its variables before the not clause",
        );
      safeNegation(clause.not, bound);
    } else if (
      isRecord(clause) &&
      Object.keys(clause).length === 1 &&
      Object.hasOwn(clause, "or")
    ) {
      if (
        !Array.isArray(clause.or) ||
        clause.or.length === 0 ||
        !clause.or.every(Array.isArray)
      )
        throw new QueryError(
          "or clause must contain one or more arrays of clauses",
        );
      const branchBounds = clause.or.map((branch) =>
        safeNegation(branch as unknown[], bound),
      );
      const outward = branchBounds.map(
        (branch) =>
          new Set([...branch].filter((variable) => !bound.has(variable))),
      );
      const expected = [...(outward[0] as Set<string>)].sort(compareUnicode);
      if (
        outward.some(
          (branch) =>
            [...branch].sort(compareUnicode).join("\0") !== expected.join("\0"),
        )
      )
        throw new QueryError(
          "every or branch must bind the same outward variables",
        );
      expected.forEach((variable) => bound.add(variable));
    } else if (
      isRecord(clause) &&
      Object.keys(clause).length === 1 &&
      Object.hasOwn(clause, "rule")
    ) {
      if (
        !Array.isArray(clause.rule) ||
        clause.rule.length === 0 ||
        typeof clause.rule[0] !== "string"
      )
        throw new QueryError(
          "rule invocation must be a non-empty [name, ...arguments] array",
        );
      clauseVariables(clause.rule).forEach((variable) => bound.add(variable));
    } else if (
      Array.isArray(clause) &&
      clause.length > 0 &&
      typeof clause[0] === "string" &&
      PREDICATES.has(clause[0])
    ) {
      if (clause.length !== 3)
        throw new QueryError(
          "invalid predicate; use an operator with exactly two operands",
        );
      const missing = [...clauseVariables(clause.slice(1))].filter(
        (variable) => !bound.has(variable),
      );
      if (missing.length > 0)
        throw new QueryError(
          `predicate variables ${missing.sort().join(", ")} are unbound; place binding patterns before the predicate`,
        );
    } else if (Array.isArray(clause)) {
      if (
        ![3, 4, 5].includes(clause.length) ||
        (!isVariable(clause[1]) &&
          clause[1] !== "_" &&
          (typeof clause[1] !== "string" || !ATTRIBUTE_PATTERN.test(clause[1])))
      )
        throw new QueryError(
          "invalid pattern; use [e,a,v], [e,a,v,tx], or [e,a,v,tx,added]",
        );
      clauseVariables(clause).forEach((variable) => bound.add(variable));
    } else if (isRecord(clause))
      throw new QueryError(
        "invalid clause object; use exactly one of not, or, or rule",
      );
    else
      throw new QueryError(
        "invalid clause; use a pattern, predicate, not, or, or rule",
      );
  }
  return bound;
}

function ruleInvocation(
  db: Db,
  invocation: unknown[],
  bindings: Binding[],
  relations: Relations,
  work: WorkBudget,
): Binding[] {
  // validateInvocations establishes the name, definition, and arity first.
  const name = invocation[0] as string;
  const relation = relations.get(name) as Relation;
  const result: Binding[] = [];
  for (const binding of bindings) {
    for (const row of relation.values()) {
      work.spend();
      let current: Binding | null = binding;
      for (let index = 0; index < row.length && current !== null; index++) {
        const term = invocation[index + 1];
        const cell = row[index] as Cell;
        current = unify(
          current,
          term,
          cell,
          isVariable(term) ? null : constant(db, term, cell.tag === REF),
        );
      }
      if (current !== null) result.push(current);
    }
  }
  return dedupe(result);
}

function clauses(
  db: Db,
  sequence: unknown[],
  bindings: Binding[],
  relations: Relations,
  work: WorkBudget,
): Binding[] {
  let current = [...bindings];
  const commonBindings = (): Set<string> => {
    const first = current[0];
    if (first === undefined) return new Set();
    return new Set(
      [...first.keys()].filter((name) =>
        current.every((binding) => binding.has(name)),
      ),
    );
  };
  for (let position = 0; position < sequence.length;) {
    if (isPatternClause(sequence[position])) {
      const block: Array<{ clause: unknown[]; position: number }> = [];
      while (
        position < sequence.length &&
        isPatternClause(sequence[position])
      ) {
        block.push({
          clause: sequence[position] as unknown[],
          position: position++,
        });
      }
      while (block.length > 0 && current.length > 0) {
        const bound = commonBindings();
        block.sort(
          (left, right) =>
            planPattern(left.clause, bound).rank -
              planPattern(right.clause, bound).rank ||
            left.position - right.position,
        );
        const next = block.shift() as { clause: unknown[] };
        current = pattern(db, next.clause, current, work);
      }
      if (current.length === 0) break;
      continue;
    }
    const clause = sequence[position++];
    if (isRecord(clause)) {
      const keys = Object.keys(clause);
      if (keys.length === 1 && keys[0] === "not") {
        current = current.filter(
          (binding) =>
            clauses(db, clause.not as unknown[], [binding], relations, work)
              .length === 0,
        );
      } else if (keys.length === 1 && keys[0] === "or") {
        if (
          !Array.isArray(clause.or) ||
          clause.or.length === 0 ||
          !clause.or.every(Array.isArray)
        )
          throw new QueryError(
            "or clause has no branches; provide one or more clause lists",
          );
        current = dedupe(
          clause.or.flatMap((branch) =>
            clauses(db, branch as unknown[], current, relations, work),
          ),
        );
      } else if (keys.length === 1 && keys[0] === "rule") {
        current = ruleInvocation(
          db,
          clause.rule as unknown[],
          current,
          relations,
          work,
        );
      } else
        throw new QueryError("unknown clause object; use not, or, or rule");
    } else if (Array.isArray(clause))
      current = predicate(db, clause, current, work);
    else
      throw new QueryError(
        "invalid clause; use a pattern, predicate, not, or, or rule",
      );
    if (current.length === 0) break;
  }
  return current;
}

function ruleDefinitions(raw: unknown): RuleDefinition[] {
  if (raw === undefined || raw === null) return [];
  if (
    !Array.isArray(raw) ||
    !raw.every(
      (item) =>
        isRecord(item) &&
        Object.keys(item).sort().join() === "body,head" &&
        Array.isArray(item.head) &&
        Array.isArray(item.body),
    )
  ) {
    throw new QueryError(
      "rules must be an array of objects with exactly head and body",
    );
  }
  return raw as RuleDefinition[];
}

function ruleArities(definitions: RuleDefinition[]): Map<string, number> {
  const arities = new Map<string, number>();
  for (const definition of definitions) {
    const [name, ...parameters] = definition.head;
    if (
      typeof name !== "string" ||
      parameters.some((parameter) => !isVariable(parameter))
    )
      throw new QueryError("invalid rule head; use ['name', '?arg', ...]");
    const prior = arities.get(name);
    if (prior !== undefined && prior !== parameters.length)
      throw new QueryError(
        `all definitions of rule ${name} must have the same arity`,
      );
    arities.set(name, parameters.length);
  }
  return arities;
}

function ruleCalls(value: unknown): Set<string> {
  if (isRecord(value)) {
    if (Object.keys(value).length === 1 && Object.hasOwn(value, "rule"))
      return Array.isArray(value.rule) && typeof value.rule[0] === "string"
        ? new Set([value.rule[0]])
        : new Set();
    const result = new Set<string>();
    Object.values(value).forEach((item) =>
      ruleCalls(item).forEach((name) => result.add(name)),
    );
    return result;
  }
  if (Array.isArray(value)) {
    const result = new Set<string>();
    value.forEach((item) =>
      ruleCalls(item).forEach((name) => result.add(name)),
    );
    return result;
  }
  return new Set();
}

function validateInvocations(
  value: unknown,
  arities: Map<string, number>,
): void {
  if (isRecord(value)) {
    if (Object.keys(value).length === 1 && Object.hasOwn(value, "rule")) {
      if (!Array.isArray(value.rule) || typeof value.rule[0] !== "string")
        throw new QueryError(
          "invalid rule invocation; use ['rule-name', arguments...]",
        );
      const arity = arities.get(value.rule[0]);
      if (arity === undefined)
        throw new QueryError(
          `rule ${value.rule[0]} is not defined; add a matching head under rules`,
        );
      if (value.rule.length - 1 !== arity)
        throw new QueryError(
          `rule ${value.rule[0]} expects ${arity} arguments, got ${value.rule.length - 1}`,
        );
      return;
    }
    Object.values(value).forEach((item) => validateInvocations(item, arities));
  } else if (Array.isArray(value))
    value.forEach((item) => validateInvocations(item, arities));
}

function ruleDependencies(
  definitions: RuleDefinition[],
  arities: Map<string, number>,
): Map<string, Set<string>> {
  const dependencies = new Map<string, Set<string>>();
  for (const definition of definitions) {
    const name = String(definition.head[0]);
    const calls = dependencies.get(name) ?? new Set<string>();
    ruleCalls(definition.body).forEach((call) => calls.add(call));
    dependencies.set(name, calls);
    validateInvocations(definition.body, arities);
  }
  const reaches = (
    start: string,
    target: string,
    seen: Set<string>,
  ): boolean => {
    for (const dependency of dependencies.get(start) as Set<string>) {
      if (dependency === target) return true;
      if (
        !seen.has(dependency) &&
        reaches(dependency, target, new Set([...seen, dependency]))
      )
        return true;
    }
    return false;
  };
  for (const [name, direct] of dependencies) {
    for (const dependency of direct)
      if (
        dependency !== name &&
        reaches(dependency, name, new Set([dependency]))
      )
        throw new QueryError(
          `rules ${name} and ${dependency} are mutually recursive; query language v1 supports self-recursion only`,
        );
  }
  return dependencies;
}

function relations(
  db: Db,
  definitions: RuleDefinition[],
  work: WorkBudget,
): Relations {
  const arities = ruleArities(definitions);
  const dependencies = ruleDependencies(definitions, arities);
  const result: Relations = new Map();
  definitions.forEach((definition) =>
    result.set(String(definition.head[0]), new Map()),
  );
  const ordered: string[] = [];
  const visited = new Set<string>();
  const visit = (name: string): void => {
    if (visited.has(name)) return;
    visited.add(name);
    for (const dependency of [...(dependencies.get(name) as Set<string>)].sort(
      compareUnicode,
    ))
      if (dependency !== name) visit(dependency);
    ordered.push(name);
  };
  for (const name of [...result.keys()].sort(compareUnicode)) visit(name);
  for (const ruleName of ordered) {
    let changed = true;
    while (changed) {
      changed = false;
      for (const definition of definitions.filter(
        (candidate) => candidate.head[0] === ruleName,
      )) {
        const [name, ...parameters] = definition.head;
        const relation = result.get(String(name)) as Relation;
        for (const binding of clauses(
          db,
          definition.body,
          [new Map()],
          result,
          work,
        )) {
          const cells: Cell[] = [];
          let valid = true;
          for (const term of parameters) {
            const cell = binding.get(term as string);
            if (cell === undefined) {
              valid = false;
              break;
            }
            cells.push(cell);
          }
          if (!valid) continue;
          const key = cells.map(cellKey).join("|");
          if (!relation.has(key)) {
            relation.set(key, cells);
            changed = true;
          }
        }
      }
    }
  }
  return result;
}

function column(item: unknown): string {
  if (typeof item === "string") return item;
  if (Array.isArray(item) && item.length >= 2)
    return `${item[0] === "pull" ? "pull" : String(item[0])}(${String(item[1])})`;
  throw new QueryError("invalid find item; use a variable, aggregate, or pull");
}

function validateFind(db: Db, find: unknown[]): void {
  let hasAggregate = false;
  let hasPull = false;
  for (const item of find) {
    if (isVariable(item)) continue;
    if (
      Array.isArray(item) &&
      item.length === 2 &&
      typeof item[0] === "string" &&
      AGGREGATES.has(item[0]) &&
      isVariable(item[1])
    ) {
      hasAggregate = true;
      continue;
    }
    if (
      Array.isArray(item) &&
      item.length === 3 &&
      item[0] === "pull" &&
      isVariable(item[1]) &&
      Array.isArray(item[2])
    ) {
      db._validatePullPattern(item[2]);
      hasPull = true;
      continue;
    }
    throw new QueryError(
      "invalid find item; use a variable, aggregate, or ['pull', '?entity', pattern]",
    );
  }
  if (hasAggregate && hasPull)
    throw new QueryError(
      "pull cannot be mixed with aggregate find items in query language v1; run a separate pull query",
    );
}

function renderCell(db: Db, cell: Cell): unknown {
  return wireValue(cell.tag, cell.value, (id) => db._nameOrId(id));
}

function sortCompare(left: unknown, right: unknown): number {
  const category = (value: unknown): number =>
    typeof value === "boolean"
      ? 0
      : typeof value === "number" || typeof value === "bigint"
        ? 1
        : typeof value === "string"
          ? 2
          : 3;
  const leftCategory = category(left);
  const rightCategory = category(right);
  if (leftCategory !== rightCategory) return leftCategory - rightCategory;
  if (typeof left === "number" || typeof left === "bigint")
    return compareNumeric(left, right as number | bigint);
  const a = typeof left === "string" ? left : canonicalJson(left);
  const b = typeof right === "string" ? right : canonicalJson(right);
  return compareUnicode(a, b);
}

function findValue(
  db: Db,
  item: unknown,
  binding: Binding,
  work: WorkBudget,
): unknown {
  if (isVariable(item)) {
    const cell = binding.get(item);
    if (cell === undefined)
      throw new QueryError(
        `find variable ${item} is unbound; add a matching where clause`,
      );
    return renderCell(db, cell);
  }
  if (
    Array.isArray(item) &&
    item.length === 3 &&
    item[0] === "pull" &&
    isVariable(item[1])
  ) {
    const cell = binding.get(item[1]);
    if (cell === undefined || cell.tag !== REF)
      throw new QueryError(
        `pull variable ${item[1]} is not an entity; bind it in an entity pattern position`,
      );
    const entity = cell.value as bigint;
    return db._pullEntity(entity, item[2], 1, new Set([entity]), () =>
      work.spend(),
    );
  }
  throw new QueryError(
    "invalid non-aggregate find item; use a bound variable or ['pull', '?e', pattern]",
  );
}

function aggregate(item: unknown[], rows: Binding[]): unknown {
  // validateFind guarantees an aggregate name and one variable argument.
  const cells = rows
    .map((row) => row.get(item[1] as string))
    .filter((cell): cell is Cell => cell !== undefined);
  if (item[0] === "count") return cells.length;
  if (item[0] === "count-distinct") {
    return new Set(cells.map(cellKey)).size;
  }
  if (cells.length === 0) return null;
  if (!cells.every(numeric))
    throw new QueryError(
      `aggregate ${item[0]} requires numeric values; bind an int or float attribute`,
    );
  const numericCells = cells as Array<Cell & { value: number | bigint }>;
  if (item[0] === "min" || item[0] === "max") {
    const selected = numericCells.slice(1).reduce(
      (current, cell) => {
        const comparison = compareNumeric(cell.value, current.value);
        return item[0] === "min"
          ? comparison < 0
            ? cell
            : current
          : comparison > 0
            ? cell
            : current;
      },
      numericCells[0] as Cell & { value: number | bigint },
    ).value;
    return typeof selected === "bigint" ? publicInteger(selected) : selected;
  }
  const values = numericCells.map((cell) => cell.value);
  if (item[0] === "sum" && values.every((value) => typeof value === "bigint")) {
    const total = (values as bigint[]).reduce((sum, value) => sum + value, 0n);
    if (total < INT64_MIN || total > INT64_MAX)
      throw new QueryError(
        "integer sum exceeds signed 64-bit range; aggregate smaller groups",
      );
    return publicInteger(total);
  }
  const total = values.reduce<number>((sum, value) => sum + Number(value), 0);
  if (!Number.isFinite(total))
    throw new QueryError(
      "floating-point sum is non-finite; aggregate smaller finite values",
    );
  if (item[0] === "sum") return total;
  const average = total / values.length;
  return average;
}

interface Projected {
  values: unknown[];
  binding: Binding;
}

function project(
  db: Db,
  find: unknown[],
  bindings: Binding[],
  work: WorkBudget,
): Projected[] {
  const aggregatePositions = new Set(
    find.flatMap((item, index) =>
      Array.isArray(item) &&
      typeof item[0] === "string" &&
      AGGREGATES.has(item[0])
        ? [index]
        : [],
    ),
  );
  if (aggregatePositions.size === 0)
    return bindings.map((binding) => ({
      values: find.map((item) => findValue(db, item, binding, work)),
      binding,
    }));
  // validateFind rejects pull/aggregate mixtures before projection.
  const nonAggregate = find
    .map((_item, index) => index)
    .filter((index) => !aggregatePositions.has(index));
  const groups = new Map<string, Binding[]>();
  for (const binding of bindings) {
    const keyCells = nonAggregate.map((index) =>
      typeof find[index] === "string"
        ? binding.get(find[index] as string)
        : undefined,
    );
    const key = keyCells
      .map((cell) => (cell === undefined ? "missing" : cellKey(cell)))
      .join("|");
    const rows = groups.get(key) ?? [];
    rows.push(binding);
    groups.set(key, rows);
  }
  if (bindings.length === 0 && nonAggregate.length === 0) groups.set("", []);
  return [...groups.values()].map((rows) => {
    const representative = rows[0] ?? new Map<string, Cell>();
    return {
      values: find.map((item, index) =>
        aggregatePositions.has(index)
          ? aggregate(item as unknown[], rows)
          : findValue(db, item, representative, work),
      ),
      binding: representative,
    };
  });
}

export function evaluate(
  db: Db,
  query: Record<string, unknown>,
  args: Record<string, unknown>,
  options: WorkOptions,
): Result {
  const unknown = Object.keys(query).filter((key) => !QUERY_KEYS.has(key));
  if (unknown.length > 0)
    throw new QueryError(
      `unknown query keys ${unknown.sort().join(", ")}; use find/where/in/order/limit/offset/rules`,
    );
  const find = query.find;
  const where = query.where ?? [];
  if (!Array.isArray(find) || find.length === 0)
    throw new QueryError(
      "query find must be a non-empty array of variables, aggregates, or pulls",
    );
  if (!Array.isArray(where))
    throw new QueryError("query where must be an array of clauses");
  validateFind(db, find);
  const inputs = query.in ?? [];
  if (!Array.isArray(inputs) || !inputs.every(isVariable))
    throw new QueryError(
      "query in must be an array of variables such as ['?min']",
    );
  const missing = inputs.filter((name) => !Object.hasOwn(args, name));
  if (missing.length > 0)
    throw new QueryError(
      `query inputs ${missing.join(", ")} are missing from args; bind every variable listed under in`,
    );
  const potentiallyBound = safeNegation(where, new Set(inputs));
  const findVariables = new Set<string>();
  for (const item of find) {
    const candidate =
      typeof item === "string"
        ? item
        : Array.isArray(item)
          ? item[1]
          : undefined;
    if (isVariable(candidate)) findVariables.add(candidate);
  }
  const unbound = [...findVariables].filter(
    (variable) => !potentiallyBound.has(variable),
  );
  if (unbound.length > 0)
    throw new QueryError(
      `find variables ${unbound.sort().join(", ")} are not bound by where/in; add a pattern or input for each variable`,
    );
  const initial: Binding = new Map();
  for (const name of inputs) {
    const value = constant(db, args[name]);
    if (value === null)
      throw new QueryError(
        `query input ${name} cannot resolve its value; bind a scalar or existing ref`,
      );
    initial.set(name, value);
  }
  const definitions = ruleDefinitions(query.rules);
  const arities = ruleArities(definitions);
  validateInvocations(where, arities);
  definitions.forEach((definition) => {
    const bodyBound = safeNegation(definition.body, new Set());
    const missingHead = definition.head
      .slice(1)
      .filter((variable) => !bodyBound.has(String(variable)));
    if (missingHead.length > 0)
      throw new QueryError(
        `rule ${String(definition.head[0])} head variables ${missingHead.join(", ")} are not bound by its body`,
      );
  });
  const work = new WorkBudget(options);
  const relationValues = relations(db, definitions, work);
  const bindings = clauses(db, where, [initial], relationValues, work);
  const projected = project(db, find, bindings, work);
  const seen = new Set<string>();
  const distinct = projected.filter((row) => {
    const key = canonicalJson(row.values);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  const order = query.order ?? [];
  if (!Array.isArray(order))
    throw new QueryError(
      "query order must be an array such as [['?name','asc']]",
    );
  const columns = find.map(column);
  for (const specification of [...order].reverse()) {
    if (
      !Array.isArray(specification) ||
      specification.length !== 2 ||
      (specification[1] !== "asc" && specification[1] !== "desc")
    )
      throw new QueryError(
        "invalid order; use [column-or-variable, 'asc'|'desc']",
      );
    const keyName = specification[0];
    const direction = specification[1] === "desc" ? -1 : 1;
    if (typeof keyName === "string" && columns.includes(keyName)) {
      const index = columns.indexOf(keyName);
      distinct.sort(
        (left, right) =>
          direction * sortCompare(left.values[index], right.values[index]),
      );
    } else if (isVariable(keyName)) {
      if (!potentiallyBound.has(keyName))
        throw new QueryError(
          `order variable ${keyName} is not bound by where/in`,
        );
      if (distinct.some((row) => !row.binding.has(keyName)))
        throw new QueryError(
          `order variable ${keyName} is not bound in every result row`,
        );
      distinct.sort(
        (left, right) =>
          direction *
          sortCompare(
            renderCell(db, left.binding.get(keyName) as Cell),
            renderCell(db, right.binding.get(keyName) as Cell),
          ),
      );
    } else
      throw new QueryError(
        `order key ${String(keyName)} is not a result column or bound variable`,
      );
  }
  const offset = query.offset ?? 0;
  const limit = query.limit;
  if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0)
    throw new QueryError(
      `query offset ${String(offset)} is invalid; use a non-negative integer`,
    );
  if (
    limit !== undefined &&
    (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 0)
  )
    throw new QueryError(
      `query limit ${String(limit)} is invalid; use a non-negative integer`,
    );
  const sliced = distinct.slice(
    offset,
    limit === undefined ? undefined : offset + limit,
  );
  return { columns, rows: sliced.map((row) => row.values) };
}
