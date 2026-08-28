"""Deterministic in-process evaluation of format-v2 JSON Datalog."""

from __future__ import annotations

import math
import operator
from collections.abc import Mapping, Sequence
from typing import Any

from fgraph.errors import FGraphError, NotFound, QueryError, SchemaError, TooLarge
from fgraph.models import Result
from fgraph.values import (
    ATTRIBUTE_PATTERN,
    BOOL,
    FLOAT,
    INT,
    INT64_MAX,
    INT64_MIN,
    REF,
    Cell,
    _canonical_json_document,
    encode,
    wire_value,
)

type Binding = dict[str, Cell]
type Relations = dict[str, list[tuple[Cell, ...]]]

PREDICATES = {
    "=": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}
AGGREGATES = frozenset({"count", "count-distinct", "sum", "min", "max", "avg"})


class _WorkBudget:
    """Deterministic guard against agent-generated binding explosions."""

    __slots__ = ("remaining",)

    def __init__(self, limit: int) -> None:
        self.remaining = limit

    def spend(self, amount: int = 1) -> None:
        if amount > self.remaining:
            raise TooLarge("query exhausted its work budget; narrow the clauses or open with a larger query_budget")
        self.remaining -= amount


def _is_variable(term: Any) -> bool:
    return isinstance(term, str) and term.startswith("?")


def _is_pattern_clause(clause: Any) -> bool:
    return (
        isinstance(clause, Sequence)
        and not isinstance(clause, (str, bytes))
        and not (clause and isinstance(clause[0], str) and clause[0] in {*PREDICATES, "contains", "starts-with"})
    )


def _pattern_plan(clause: Sequence[Any], bound: set[str]) -> tuple[int, str]:
    def fixed(term: Any) -> bool:
        return term != "_" and (not _is_variable(term) or term in bound)

    entity, attribute, value = (fixed(term) for term in clause[:3])
    if entity and attribute:
        if value:
            access = "eavt/exact"
        elif _is_variable(clause[0]) and clause[0] in bound and len(clause) == 3:
            access = "eavt/batch"
        else:
            access = "eavt/ea"
        return 0, access
    if attribute and value:
        return 1, "avet"
    if entity:
        return 2, "eavt/e"
    if attribute:
        return 3, "avet/a"
    if value:
        return 4, "value-scan"
    return 5, "scan"


def _binding_key(binding: Binding) -> tuple[tuple[str, int, str], ...]:
    def value_key(cell: Cell) -> str:
        if isinstance(cell.value, bytes):
            return cell.value.hex()
        return _canonical_json_document(cell.value)

    return tuple(sorted((name, cell.tag, value_key(cell)) for name, cell in binding.items()))


def _dedupe(bindings: Sequence[Binding]) -> list[Binding]:
    seen: set[tuple[tuple[str, int, str], ...]] = set()
    result: list[Binding] = []
    for binding in bindings:
        key = _binding_key(binding)
        if key not in seen:
            seen.add(key)
            result.append(binding)
    return result


def _constant(db: Any, value: Any, *, entity: bool = False) -> Cell | None:
    if entity:
        if isinstance(value, Mapping) and set(value) == {"ref"}:
            value = value["ref"]
        try:
            resolved = db._resolve_read(value, missing_ok=True)  # noqa: SLF001
        except NotFound:
            return None
        except FGraphError as exc:
            raise QueryError(f"invalid entity query constant {value!r}: {exc}") from exc
        return None if resolved is None else Cell(REF, resolved)
    try:
        encoded = db._encode_read_value(value)  # noqa: SLF001
    except NotFound:
        return None
    except FGraphError as exc:
        raise QueryError(f"invalid query constant {value!r}: {exc}") from exc
    return Cell(encoded.tag, encoded.logical)


def _unify(binding: Binding, term: Any, cell: Cell, constant: Cell | None) -> Binding | None:
    if term == "_":
        return binding
    if _is_variable(term):
        known = binding.get(term)
        if known is not None:
            return binding if known == cell else None
        result = dict(binding)
        result[term] = cell
        return result
    return binding if constant == cell else None


def _pattern(
    db: Any,
    clause: Sequence[Any],
    bindings: Sequence[Binding],
    work: _WorkBudget,
    source: str,
) -> list[Binding]:
    if len(clause) not in (3, 4, 5):
        raise QueryError(f"invalid datom pattern {clause!r}; use [e,a,v], [e,a,v,tx], or [e,a,v,tx,added]")
    if not _is_variable(clause[1]) and clause[1] != "_":
        if not isinstance(clause[1], str):
            raise QueryError(f"invalid attribute term {clause[1]!r}; use a namespace/name, variable, or underscore")
        try:
            db._validate_attribute(clause[1])  # noqa: SLF001
        except SchemaError as exc:
            raise QueryError(
                f"invalid pattern {clause!r}; use a valid namespace/attribute in the second position"
            ) from exc

    constants: list[Cell | None] = []
    for index, term in enumerate(clause):
        if _is_variable(term) or term == "_":
            constants.append(None)
            continue
        constant = _constant(db, term, entity=index in {0, 1, 3})
        if constant is None:
            return []
        constants.append(constant)

    def bound_cell(binding: Binding, position: int) -> Cell | None:
        term = clause[position]
        return binding.get(term) if _is_variable(term) else constants[position]

    def bound_reference(binding: Binding, position: int) -> int | None:
        cell = bound_cell(binding, position)
        return int(cell.value) if cell is not None and cell.tag == REF else None

    entity_variable = clause[0] if _is_variable(clause[0]) else None
    value_unbound = clause[2] == "_" or (
        _is_variable(clause[2]) and all(clause[2] not in binding for binding in bindings)
    )
    if (
        source == "current"
        and len(clause) == 3
        and entity_variable is not None
        and constants[1] is not None
        and constants[1].tag == REF
        and value_unbound
        and len(bindings) > 1
        and all(binding.get(entity_variable, Cell(-1, None)).tag == REF for binding in bindings)
    ):
        basis = db._as_of if db._as_of is not None else db._latest_tx()  # noqa: SLF001
        visibility, visibility_parameters = db._visibility(basis)  # noqa: SLF001
        rows_by_entity: dict[int, list[Any]] = {}
        entities = sorted({int(binding[entity_variable].value) for binding in bindings})
        bindings_per_entity: dict[int, int] = {}
        for binding in bindings:
            entity = int(binding[entity_variable].value)
            bindings_per_entity[entity] = bindings_per_entity.get(entity, 0) + 1
        for offset in range(0, len(entities), 400):
            chunk = entities[offset : offset + 400]
            placeholders = ",".join("?" for _ in chunk)
            rows = db._connection.execute(  # noqa: SLF001
                f"SELECT * FROM fgraph_facts WHERE a=? AND e IN ({placeholders}) AND {visibility} ORDER BY id",  # noqa: S608
                (int(constants[1].value), *chunk, *visibility_parameters),
            )
            for row in rows:
                entity = int(row["e"])
                work.spend(bindings_per_entity[entity])
                rows_by_entity.setdefault(entity, []).append(row)
        result: list[Binding] = []
        for binding in bindings:
            for row in rows_by_entity.get(int(binding[entity_variable].value), []):
                current: Binding | None = binding
                cells = (
                    Cell(REF, int(row["e"])),
                    Cell(REF, int(row["a"])),
                    db._cell(int(row["t"]), row["v"]),  # noqa: SLF001
                )
                for term, cell, constant in zip(clause, cells, constants, strict=True):
                    if current is None:
                        break
                    current = _unify(current, term, cell, constant)
                if current is not None:
                    result.append(current)
        return _dedupe(result)

    result: list[Binding] = []
    for binding in bindings:
        entity = None if clause[0] == "_" else bound_reference(binding, 0)
        attribute = None if clause[1] == "_" else bound_reference(binding, 1)
        value = None if clause[2] == "_" else bound_cell(binding, 2)
        transaction = None if len(clause) < 4 or clause[3] == "_" else bound_reference(binding, 3)
        added_cell = None if len(clause) < 5 or clause[4] == "_" else bound_cell(binding, 4)
        added = bool(added_cell.value) if added_cell is not None and added_cell.tag == BOOL else None
        if source == "current" and added is False:
            continue

        basis = db._as_of if db._as_of is not None else db._latest_tx()  # noqa: SLF001
        conditions: list[str] = []
        parameters: list[Any] = []
        if entity is not None:
            conditions.append("e=?")
            parameters.append(entity)
        if attribute is not None:
            conditions.append("a=?")
            parameters.append(attribute)
        if value is not None and attribute is not None:
            encoded = encode(wire_value(value.tag, value.value, int), int)
            conditions.extend(["t=?", "v=?"])
            parameters.extend([encoded.tag, encoded.stored])
        if source == "current":
            if transaction is not None:
                conditions.append("tx=?")
                parameters.append(transaction)
            visibility, visibility_parameters = db._visibility(basis)  # noqa: SLF001
            conditions.append(visibility)
            parameters.extend(visibility_parameters)
        else:
            conditions.append("tx<=?")
            parameters.append(basis)
            if added is True:
                if transaction is not None:
                    conditions.append("tx=?")
                    parameters.append(transaction)
            elif added is False:
                conditions.extend(["rx IS NOT NULL", "rx<=?"])
                parameters.append(basis)
                if transaction is not None:
                    conditions.append("rx=?")
                    parameters.append(transaction)
            elif transaction is not None:
                conditions.append("(tx=? OR rx=?)")
                parameters.extend([transaction, transaction])
        rows = db._connection.execute(  # noqa: SLF001
            f"SELECT * FROM fgraph_facts WHERE {' AND '.join(conditions)} ORDER BY id",  # noqa: S608
            parameters,
        )
        for row in rows:
            base = (
                Cell(REF, int(row["e"])),
                Cell(REF, int(row["a"])),
                db._cell(int(row["t"]), row["v"]),  # noqa: SLF001
            )
            raw_datoms = [(*base, Cell(REF, int(row["tx"])), Cell(BOOL, True))]
            if source == "history" and row["rx"] is not None and int(row["rx"]) <= basis:
                raw_datoms.append((*base, Cell(REF, int(row["rx"])), Cell(BOOL, False)))
            for datom in raw_datoms:
                if transaction is not None and datom[3].value != transaction:
                    continue
                if added is not None and datom[4].value is not added:
                    continue
                work.spend()
                current: Binding | None = binding
                for term, cell, constant in zip(clause, datom, constants, strict=False):
                    if current is None:
                        break
                    current = _unify(current, term, cell, constant)
                if current is not None:
                    result.append(current)
    return _dedupe(result)


def _operand(db: Any, term: Any, binding: Binding) -> Cell:
    if _is_variable(term):
        if term not in binding:
            raise QueryError(f"predicate variable {term!r} is unbound; place a binding pattern before the predicate")
        return binding[term]
    constant = _constant(db, term)
    if constant is None:
        raise QueryError(f"predicate constant {term!r} cannot be resolved; use a scalar or existing ref")
    return constant


def _compare(op: str, left: Cell, right: Cell) -> bool:
    if op in {"contains", "starts-with"}:
        if not isinstance(left.value, str) or not isinstance(right.value, str):
            raise QueryError(f"predicate {op!r} requires text operands; bind or pass strings")
        return right.value in left.value if op == "contains" else left.value.startswith(right.value)
    if op in {"=", "!="}:
        equal = left == right or (left.tag in {INT, FLOAT} and right.tag in {INT, FLOAT} and left.value == right.value)
        return equal if op == "=" else not equal
    if left.tag != right.tag and not (left.tag in {INT, FLOAT} and right.tag in {INT, FLOAT}):
        raise QueryError(f"predicate {op!r} cannot order unlike types; compare values of the same type")
    try:
        return bool(PREDICATES[op](left.value, right.value))
    except TypeError as exc:
        raise QueryError(f"predicate {op!r} cannot compare {left.value!r} and {right.value!r}") from exc


def _predicate(
    db: Any,
    clause: Sequence[Any],
    bindings: Sequence[Binding],
    work: _WorkBudget,
) -> list[Binding]:
    if len(clause) != 3 or clause[0] not in {*PREDICATES, "contains", "starts-with"}:
        raise QueryError(
            f"invalid predicate {clause!r}; use =, !=, <, <=, >, >=, contains, or starts-with with two operands"
        )
    result = []
    for binding in bindings:
        work.spend()
        if _compare(str(clause[0]), _operand(db, clause[1], binding), _operand(db, clause[2], binding)):
            result.append(binding)
    return result


def _clause_variables(clause: Any) -> set[str]:
    if isinstance(clause, Sequence) and not isinstance(clause, (str, bytes)):
        return {variable for item in clause for variable in _clause_variables(item)}
    if isinstance(clause, Mapping):
        return {variable for value in clause.values() for variable in _clause_variables(value)}
    return {clause} if _is_variable(clause) else set()


def _validate_clause_bindings(clauses: Sequence[Any], initially_bound: set[str]) -> set[str]:
    """Reject unsafe negation and unbound predicates before data-dependent evaluation."""
    bound = set(initially_bound)
    for clause in clauses:
        if isinstance(clause, Mapping) and set(clause) == {"not"}:
            inner = clause["not"]
            if not isinstance(inner, list):
                raise QueryError(f"not clause {clause!r} must contain an array of clauses")
            correlated = sorted(_clause_variables(inner) & bound)
            if not correlated:
                raise QueryError("negation is uncorrelated; bind at least one of its variables before the not clause")
            _validate_clause_bindings(inner, bound)
        elif isinstance(clause, Mapping) and set(clause) == {"or"}:
            branches = clause["or"]
            if (
                not isinstance(branches, list)
                or not branches
                or any(not isinstance(branch, list) for branch in branches)
            ):
                raise QueryError(f"or clause {clause!r} must contain one or more clause arrays")
            branch_bounds = [_validate_clause_bindings(branch, bound) for branch in branches]
            if branch_bounds:
                outward = [branch_bound - bound for branch_bound in branch_bounds]
                if any(branch != outward[0] for branch in outward[1:]):
                    raise QueryError("every or branch must bind the same outward variables")
                bound.update(outward[0])
        elif isinstance(clause, Mapping) and set(clause) == {"rule"}:
            invocation = clause["rule"]
            if not isinstance(invocation, list) or not invocation or not isinstance(invocation[0], str):
                raise QueryError(f"invalid rule invocation {invocation!r}; use ['rule-name', arguments...]")
            bound.update(_clause_variables(invocation))
        elif isinstance(clause, Mapping):
            raise QueryError(f"unknown clause object {clause!r}; use not, or, or rule")
        elif isinstance(clause, Sequence) and not isinstance(clause, (str, bytes)):
            if clause and isinstance(clause[0], str) and clause[0] in {*PREDICATES, "contains", "starts-with"}:
                if len(clause) != 3:
                    raise QueryError(
                        f"invalid predicate {clause!r}; use =, !=, <, <=, >, >=, contains, or starts-with with two operands"
                    )
                missing = sorted(_clause_variables(clause) - bound)
                if missing:
                    raise QueryError(
                        f"predicate variables {missing!r} are unbound; bind them in an earlier pattern or input"
                    )
            else:
                if len(clause) not in (3, 4, 5):
                    raise QueryError(f"invalid datom pattern {clause!r}; use [e,a,v], [e,a,v,tx], or [e,a,v,tx,added]")
                attribute = clause[1]
                if (
                    not _is_variable(attribute)
                    and attribute != "_"
                    and (not isinstance(attribute, str) or not ATTRIBUTE_PATTERN.fullmatch(attribute))
                ):
                    raise QueryError(
                        f"invalid pattern attribute {attribute!r}; use namespace/name, a variable, or underscore"
                    )
                bound.update(_clause_variables(clause))
        else:
            raise QueryError(f"invalid clause {clause!r}; use a pattern, predicate, not, or, or rule")
    return bound


def _validate_find(db: Any, find: Sequence[Any]) -> None:
    has_aggregate = False
    has_pull = False
    for item in find:
        if _is_variable(item):
            continue
        if (
            isinstance(item, list)
            and len(item) == 2
            and isinstance(item[0], str)
            and item[0] in AGGREGATES
            and _is_variable(item[1])
        ):
            has_aggregate = True
            continue
        if isinstance(item, list) and len(item) == 3 and item[0] == "pull" and _is_variable(item[1]):
            db._validate_pull_pattern(item[2], check_references=False)  # noqa: SLF001
            has_pull = True
            continue
        raise QueryError(f"invalid find item {item!r}; use a variable, aggregate, or ['pull', '?e', pattern]")
    if has_aggregate and has_pull:
        raise QueryError("pull cannot be mixed with aggregate find items in API v1; run a separate pull query")


def _rule_invocation(
    db: Any,
    invocation: Sequence[Any],
    bindings: Sequence[Binding],
    relations: Relations,
    work: _WorkBudget,
) -> list[Binding]:
    if not invocation or not isinstance(invocation[0], str):
        raise QueryError(f"invalid rule invocation {invocation!r}; use ['rule-name', arguments...]")
    name = invocation[0]
    if name not in relations:
        raise QueryError(f"rule {name!r} is not defined; add a matching head under rules")
    result: list[Binding] = []
    for binding in bindings:
        for row in relations[name]:
            work.spend()
            if len(row) != len(invocation) - 1:
                raise QueryError(f"rule {name!r} expects {len(row)} arguments, got {len(invocation) - 1}")
            current: Binding | None = binding
            for term, cell in zip(invocation[1:], row, strict=True):
                if current is None:
                    break
                constant = None if _is_variable(term) else _constant(db, term, entity=cell.tag == REF)
                current = _unify(current, term, cell, constant)
            if current is not None:
                result.append(current)
    return _dedupe(result)


def _clauses(
    db: Any,
    clauses: Sequence[Any],
    bindings: Sequence[Binding],
    relations: Relations,
    work: _WorkBudget,
    source: str,
) -> list[Binding]:
    current = list(bindings)
    position = 0
    while position < len(clauses):
        if _is_pattern_clause(clauses[position]):
            block: list[tuple[int, Sequence[Any]]] = []
            while position < len(clauses) and _is_pattern_clause(clauses[position]):
                block.append((position, clauses[position]))
                position += 1
            while block and current:
                common = set(current[0])
                for binding in current[1:]:
                    common.intersection_update(binding)
                block.sort(key=lambda item: (*_pattern_plan(item[1], common)[:1], item[0]))
                _ordinal, pattern = block.pop(0)
                current = _pattern(db, pattern, current, work, source)
            if not current:
                break
            continue
        clause = clauses[position]
        position += 1
        if isinstance(clause, Mapping):
            if set(clause) == {"not"}:
                inner = clause["not"]
                current = [
                    binding for binding in current if not _clauses(db, inner, [binding], relations, work, source)
                ]
            elif set(clause) == {"or"}:
                branches = clause["or"]
                if not isinstance(branches, list) or not branches:
                    raise QueryError(f"or clause {clause!r} has no branches; provide one or more clause lists")
                current = _dedupe(
                    [result for branch in branches for result in _clauses(db, branch, current, relations, work, source)]
                )
            elif set(clause) == {"rule"}:
                current = _rule_invocation(db, clause["rule"], current, relations, work)
            else:
                raise QueryError(f"unknown clause object {clause!r}; use not, or, or rule")
        elif isinstance(clause, Sequence) and not isinstance(clause, (str, bytes)):
            current = _predicate(db, clause, current, work)
        else:
            raise QueryError(f"invalid clause {clause!r}; use a pattern, predicate, not, or, or rule")
        if not current:
            break
    return current


def _rule_definitions(raw: Any) -> list[Mapping[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise QueryError("rules must be an array of objects with exactly head and body")
    definitions = raw
    if not all(isinstance(item, Mapping) and set(item) == {"head", "body"} for item in definitions):
        raise QueryError("rules must be an array of objects with exactly head and body")
    return definitions


def _rule_arities(definitions: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    arities: dict[str, int] = {}
    for definition in definitions:
        head = definition["head"]
        body = definition["body"]
        if (
            not isinstance(head, list)
            or not head
            or not isinstance(head[0], str)
            or any(not _is_variable(argument) for argument in head[1:])
        ):
            raise QueryError(f"invalid rule head {head!r}; use ['name', '?arg', ...]")
        if not isinstance(body, list):
            raise QueryError(f"invalid body for rule {head[0]!r}; use an array of clauses")
        name = head[0]
        arity = len(head) - 1
        previous = arities.setdefault(name, arity)
        if previous != arity:
            raise QueryError(f"all definitions of rule {name!r} must have the same arity; got {previous} and {arity}")
    return arities


def _rule_calls(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        if set(value) == {"rule"}:
            invocation = value["rule"]
            if isinstance(invocation, list) and invocation and isinstance(invocation[0], str):
                return {invocation[0]}
            return set()
        return {name for nested in value.values() for name in _rule_calls(nested)}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return {name for nested in value for name in _rule_calls(nested)}
    return set()


def _validate_rule_invocations(value: Any, arities: Mapping[str, int]) -> None:
    if isinstance(value, Mapping):
        if set(value) == {"rule"}:
            invocation = value["rule"]
            if not isinstance(invocation, list) or not invocation or not isinstance(invocation[0], str):
                raise QueryError(f"invalid rule invocation {invocation!r}; use ['rule-name', arguments...]")
            name = invocation[0]
            if name not in arities:
                raise QueryError(f"rule {name!r} is not defined; add a matching head under rules")
            if len(invocation) - 1 != arities[name]:
                raise QueryError(f"rule {name!r} expects {arities[name]} arguments, got {len(invocation) - 1}")
            return
        for nested in value.values():
            _validate_rule_invocations(nested, arities)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for nested in value:
            _validate_rule_invocations(nested, arities)


def _reject_mutual_recursion(definitions: Sequence[Mapping[str, Any]], arities: Mapping[str, int]) -> None:
    dependencies: dict[str, set[str]] = {}
    for definition in definitions:
        head = definition["head"]
        name = head[0]
        dependencies.setdefault(name, set())
        dependencies[name].update(_rule_calls(definition["body"]))
        _validate_rule_invocations(definition["body"], arities)

    def reaches(start: str, target: str, seen: set[str]) -> bool:
        for dependency in dependencies.get(start, set()):
            if dependency == target:
                return True
            if dependency not in seen and reaches(dependency, target, seen | {dependency}):
                return True
        return False

    for name, direct in dependencies.items():
        for dependency in direct - {name}:
            if reaches(dependency, name, {dependency}):
                raise QueryError(
                    f"rules {name!r} and {dependency!r} are mutually recursive; API v1 supports self-recursion only"
                )


def _relations(db: Any, definitions: Sequence[Mapping[str, Any]], work: _WorkBudget, source: str) -> Relations:
    arities = _rule_arities(definitions)
    _reject_mutual_recursion(definitions, arities)
    grouped: dict[str, list[Mapping[str, Any]]] = {name: [] for name in arities}
    dependencies: dict[str, set[str]] = {name: set() for name in arities}
    for definition in definitions:
        name = str(definition["head"][0])
        grouped[name].append(definition)
        dependencies[name].update(_rule_calls(definition["body"]) - {name})
    relations: Relations = {name: [] for name in arities}
    relation_members: dict[str, set[tuple[Cell, ...]]] = {name: set() for name in arities}
    built: set[str] = set()

    def build(name: str) -> None:
        if name in built:
            return
        for dependency in sorted(dependencies[name]):
            build(dependency)
        changed = True
        while changed:
            changed = False
            for definition in grouped[name]:
                head = definition["head"]
                rows = _clauses(db, definition["body"], [{}], relations, work, source)
                for binding in rows:
                    try:
                        relation = tuple(
                            binding[term] if _is_variable(term) else _constant(db, term) for term in head[1:]
                        )
                    except KeyError as exc:
                        raise QueryError(f"rule head variable {exc.args[0]!r} is not bound by its body") from exc
                    if any(cell is None for cell in relation):
                        continue
                    typed_relation = tuple(cell for cell in relation if cell is not None)
                    if typed_relation not in relation_members[name]:
                        relation_members[name].add(typed_relation)
                        relations[name].append(typed_relation)
                        changed = True
        built.add(name)

    for name in sorted(arities):
        build(name)
    return relations


def _column(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, list) and len(item) >= 2:
        if item[0] == "pull":
            return f"pull({item[1]})"
        return f"{item[0]}({item[1]})"
    raise QueryError(f"invalid find item {item!r}; use a variable, aggregate, or pull")


def _render_cell(db: Any, cell: Cell) -> Any:
    return wire_value(cell.tag, cell.value, db._name_or_id)  # noqa: SLF001


def _sort_key(value: Any) -> tuple[int, Any]:
    if isinstance(value, bool):
        return 0, int(value)
    if isinstance(value, (int, float)):
        return 1, value
    if isinstance(value, str):
        return 2, value
    return 3, _canonical_json_document(value)


def _find_value(db: Any, item: Any, binding: Binding, work: _WorkBudget) -> Any:
    if isinstance(item, str) and _is_variable(item):
        if item not in binding:
            raise QueryError(f"find variable {item!r} is unbound; add a matching where clause")
        return _render_cell(db, binding[item])
    if isinstance(item, list) and len(item) == 3 and item[0] == "pull":
        variable = item[1]
        if variable not in binding or binding[variable].tag != REF:
            raise QueryError(f"pull variable {variable!r} is not an entity; bind it in an entity pattern position")
        return db._query_pull(binding[variable].value, item[2], work.spend)  # noqa: SLF001
    raise QueryError(f"invalid non-aggregate find item {item!r}; use a bound variable or ['pull', '?e', pattern]")


def _aggregate(item: list[Any], rows: Sequence[Binding]) -> Any:
    if len(item) != 2 or item[0] not in AGGREGATES or not _is_variable(item[1]):
        raise QueryError(f"invalid aggregate {item!r}; use [count|count-distinct|sum|min|max|avg, '?variable']")
    cells = [row[item[1]] for row in rows if item[1] in row]
    operation = item[0]
    if operation == "count":
        return len(cells)
    if operation == "count-distinct":
        return len(set(cells))
    values = [cell.value for cell in cells]
    if not values:
        return None
    if not all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
        raise QueryError(f"aggregate {operation!r} requires numeric values; bind an int or float attribute")
    all_int = all(isinstance(value, int) for value in values)
    if operation == "sum" and all_int:
        total = 0
        for value in values:
            total += value
        if not INT64_MIN <= total <= INT64_MAX:
            raise QueryError("integer sum exceeds signed 64-bit range; aggregate smaller groups")
        return total
    if operation == "min":
        return min(values)
    if operation == "max":
        return max(values)
    total_float = 0.0
    for value in values:
        total_float += float(value)
    if not math.isfinite(total_float):
        raise QueryError("floating-point sum is non-finite; aggregate smaller finite values")
    if operation == "sum":
        return total_float
    average = total_float / len(values)
    if not math.isfinite(average):
        raise QueryError("floating-point average is non-finite; aggregate smaller finite values")
    return average


def _project(
    db: Any,
    find: Sequence[Any],
    bindings: Sequence[Binding],
    work: _WorkBudget,
) -> list[dict[str, Any]]:
    aggregate_positions = {
        index for index, item in enumerate(find) if isinstance(item, list) and item and item[0] in AGGREGATES
    }
    projected: list[dict[str, Any]] = []
    if not aggregate_positions:
        for binding in bindings:
            values = [_find_value(db, item, binding, work) for item in find]
            projected.append({"values": values, "binding": binding})
        return projected
    if any(isinstance(item, list) and item and item[0] == "pull" for item in find):
        raise QueryError("pull cannot be mixed with aggregate find items in API v1; run a separate pull query")
    non_aggregate = [index for index in range(len(find)) if index not in aggregate_positions]
    groups: dict[tuple[Cell, ...], list[Binding]] = {}
    if bindings:
        for binding in bindings:
            key = tuple(binding[find[index]] for index in non_aggregate if isinstance(find[index], str))
            groups.setdefault(key, []).append(binding)
    elif not non_aggregate:
        groups[()] = []
    for rows in groups.values():
        representative = rows[0] if rows else {}
        values = [
            _aggregate(item, rows) if index in aggregate_positions else _find_value(db, item, representative, work)
            for index, item in enumerate(find)
        ]
        projected.append({"values": values, "binding": representative})
    return projected


def explain(db: Any, query: Mapping[str, Any], args: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and explain access choices without evaluating data clauses."""
    allowed = {"find", "where", "in", "order", "limit", "offset", "rules", "source"}
    unknown = set(query) - allowed
    if unknown:
        raise QueryError(f"unknown query keys {sorted(unknown)!r}; use find/where/in/order/limit/offset/rules/source")
    source = query.get("source", "current")
    if source not in {"current", "history"}:
        raise QueryError(f"query source {source!r} is invalid; use 'current' or 'history'")
    find = query.get("find")
    where = query.get("where", [])
    inputs = query.get("in", [])
    if not isinstance(find, list) or not find:
        raise QueryError("query find must be a non-empty array of variables, aggregates, or pulls")
    if not isinstance(where, list):
        raise QueryError("query where must be an array of clauses")
    if not isinstance(inputs, list) or any(not _is_variable(item) for item in inputs):
        raise QueryError("query in must be an array of variables such as ['?min']")
    missing = [name for name in inputs if name not in args]
    if missing:
        raise QueryError(f"query inputs {missing!r} are missing from args; bind every listed variable")
    _validate_find(db, find)
    _validate_clause_bindings(where, set(inputs))
    definitions = _rule_definitions(query.get("rules"))
    arities = _rule_arities(definitions)
    _validate_rule_invocations(where, arities)

    bound = set(inputs)
    clauses: list[dict[str, Any]] = []
    warnings: list[str] = []
    position = 0
    while position < len(where):
        if _is_pattern_clause(where[position]):
            block: list[tuple[int, Sequence[Any]]] = []
            while position < len(where) and _is_pattern_clause(where[position]):
                block.append((position, where[position]))
                position += 1
            while block:
                block.sort(key=lambda item: (*_pattern_plan(item[1], bound)[:1], item[0]))
                ordinal, clause = block.pop(0)
                before = sorted(bound)
                _rank, access = _pattern_plan(clause, bound)
                if access == "value-scan":
                    warnings.append(f"clause {ordinal} requires a value scan; bind entity or attribute earlier")
                elif access == "scan":
                    warnings.append(f"clause {ordinal} requires a fact scan; bind entity, attribute, or value earlier")
                bound.update(_clause_variables(clause))
                clauses.append({"ordinal": ordinal, "kind": "pattern", "access": access, "bound": before})
            continue
        ordinal = position
        clause = where[position]
        position += 1
        before = sorted(bound)
        if isinstance(clause, Mapping):
            key = next(iter(clause), "logic")
            kind = str(key)
            access = "barrier"
            if key in {"or", "rule"}:
                bound.update(_clause_variables(clause))
        elif clause and isinstance(clause[0], str) and clause[0] in {*PREDICATES, "contains", "starts-with"}:
            kind = "predicate"
            access = "filter"
        else:
            kind = "barrier"
            access = "barrier"
        clauses.append({"ordinal": ordinal, "kind": kind, "access": access, "bound": before})
    if source == "history":
        warnings.append("history source evaluates assertion and retraction datoms within the selected basis")
    return {
        "basis_tx": db._as_of if db._as_of is not None else db._latest_tx(),  # noqa: SLF001
        "source": source,
        "work_limit": db._query_budget,  # noqa: SLF001
        "clauses": clauses,
        "warnings": warnings,
    }


def evaluate(db: Any, query: Mapping[str, Any], args: Mapping[str, Any]) -> Result:
    """Evaluate one canonical query against ``db`` and return distinct rows."""
    allowed = {"find", "where", "in", "order", "limit", "offset", "rules", "source"}
    unknown = set(query) - allowed
    if unknown:
        raise QueryError(f"unknown query keys {sorted(unknown)!r}; use find/where/in/order/limit/offset/rules/source")
    source = query.get("source", "current")
    if source not in {"current", "history"}:
        raise QueryError(f"query source {source!r} is invalid; use 'current' or 'history'")
    find = query.get("find")
    where = query.get("where", [])
    if not isinstance(find, list) or not find:
        raise QueryError("query find must be a non-empty array of variables, aggregates, or pulls")
    if not isinstance(where, list):
        raise QueryError("query where must be an array of clauses")
    _validate_find(db, find)
    inputs = query.get("in", [])
    if not isinstance(inputs, list) or any(not _is_variable(item) for item in inputs):
        raise QueryError("query in must be an array of variables such as ['?min']")
    missing = [name for name in inputs if name not in args]
    if missing:
        raise QueryError(f"query inputs {missing!r} are missing from args; bind every variable listed under in")
    potentially_bound = _validate_clause_bindings(where, set(inputs))
    find_variables = {
        item
        for find_item in find
        for item in (
            [find_item]
            if isinstance(find_item, str)
            else [find_item[1]]
            if isinstance(find_item, list) and len(find_item) >= 2
            else []
        )
        if _is_variable(item)
    }
    unbound_find = sorted(find_variables - potentially_bound)
    if unbound_find:
        raise QueryError(
            f"find variables {unbound_find!r} are not bound by where/in; add a pattern or input for each variable"
        )
    initial: Binding = {}
    for name in inputs:
        constant = _constant(db, args[name])
        if constant is None:
            raise QueryError(f"query input {name!r} cannot resolve value {args[name]!r}; bind a scalar or existing ref")
        initial[name] = constant
    definitions = _rule_definitions(query.get("rules"))
    arities = _rule_arities(definitions)
    _validate_rule_invocations(where, arities)
    for definition in definitions:
        body_bound = _validate_clause_bindings(definition["body"], set())
        missing_head = sorted(variable for variable in definition["head"][1:] if variable not in body_bound)
        if missing_head:
            raise QueryError(
                f"rule {definition['head'][0]!r} head variables {missing_head!r} are not bound by its body"
            )
    work = _WorkBudget(db._query_budget)  # noqa: SLF001
    relations = _relations(db, definitions, work, source)
    bindings = _clauses(db, where, [initial], relations, work, source)
    projected = _project(db, find, bindings, work)
    seen: set[str] = set()
    distinct: list[dict[str, Any]] = []
    for row in projected:
        key = _canonical_json_document(row["values"])
        if key not in seen:
            seen.add(key)
            distinct.append(row)
    order = query.get("order", [])
    if not isinstance(order, list):
        raise QueryError("query order must be an array such as [['?name','asc']]")
    columns = [_column(item) for item in find]
    for specification in reversed(order):
        if not isinstance(specification, list) or len(specification) != 2 or specification[1] not in {"asc", "desc"}:
            raise QueryError(f"invalid order {specification!r}; use [column-or-variable, 'asc'|'desc']")
        key_name = specification[0]
        if key_name in columns:
            index = columns.index(key_name)
            distinct.sort(key=lambda row: _sort_key(row["values"][index]), reverse=specification[1] == "desc")
        elif _is_variable(key_name):
            if key_name not in potentially_bound:
                raise QueryError(f"order variable {key_name!r} is not outward-bound by where/in")
            if any(key_name not in row["binding"] for row in distinct):
                raise QueryError(f"order variable {key_name!r} is not bound in every result row")
            distinct.sort(
                key=lambda row: _sort_key(_render_cell(db, row["binding"][key_name])),
                reverse=specification[1] == "desc",
            )
        else:
            raise QueryError(f"order key {key_name!r} is not a result column or bound variable")
    offset = query.get("offset", 0)
    limit = query.get("limit")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise QueryError(f"query offset {offset!r} is invalid; use a non-negative integer")
    if limit is not None and (not isinstance(limit, int) or isinstance(limit, bool) or limit < 0):
        raise QueryError(f"query limit {limit!r} is invalid; use a non-negative integer")
    sliced = distinct[offset:] if limit is None else distinct[offset : offset + limit]
    return Result(columns, [row["values"] for row in sliced])
