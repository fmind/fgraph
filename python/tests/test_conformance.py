"""Execute every shared JSON conformance case against the Python implementation."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

import fgraph
from fgraph.values import _canonical_json_document

CASE_ROOT = Path(__file__).parents[2] / "conformance" / "cases"
CASES = sorted(CASE_ROOT.rglob("*.json"))


class ConformanceClock:
    """Normative clock: 2026-01-01T00:00:00Z then one-second ticks."""

    def __init__(self) -> None:
        self.value = 1_767_225_600_000_000

    def __call__(self) -> int:
        result = self.value
        self.value += 1_000_000
        return result


def _matches(
    actual: Any,
    expected: Any,
    *,
    path: tuple[str | int, ...] = (),
    unordered_paths: frozenset[tuple[str | int, ...]] = frozenset(),
) -> bool:
    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            return False
        allow_extra = expected.get("...") is True
        expected_keys = set(expected) - {"..."}
        if not allow_extra and set(actual) != expected_keys:
            return False
        return all(
            key in actual
            and _matches(
                actual[key],
                value,
                path=(*path, key),
                unordered_paths=unordered_paths,
            )
            for key, value in expected.items()
            if key != "..."
        )
    if isinstance(expected, list):
        if not isinstance(actual, list):
            return False
        if len(actual) != len(expected):
            return False
        if path in unordered_paths:
            remaining = list(actual)
            for wanted in expected:
                match = next(
                    (
                        index
                        for index, candidate in enumerate(remaining)
                        if _matches(candidate, wanted, path=(*path, "*"), unordered_paths=unordered_paths)
                    ),
                    None,
                )
                if match is None:
                    return False
                remaining.pop(match)
            return True
        return all(
            _matches(candidate, wanted, path=(*path, index), unordered_paths=unordered_paths)
            for index, (candidate, wanted) in enumerate(zip(actual, expected, strict=True))
        )
    return actual == expected


def _actual(db: fgraph.Db, step: Mapping[str, Any]) -> Any:
    if "stats" in step:
        return db.stats()
    if "tx" in step:
        options = step.get("options", {})
        if not isinstance(options, Mapping):
            raise AssertionError(f"transaction options are {type(options).__name__}, expected an object")
        return db.transact(step["tx"], **dict(options)).to_dict()
    if "declare" in step:
        declaration = dict(step["declare"])
        attribute = declaration.pop("attr")
        return db.declare(attribute, **declaration).to_dict()
    if "receipt" in step:
        return db.receipt(step["receipt"])
    if "undo" in step:
        undo = dict(step["undo"])
        target = undo.pop("target")
        return db.undo(target, **undo).to_dict()
    if "shape" in step:
        shape = dict(step["shape"])
        name = shape.pop("name")
        return db.declare_shape(name, **shape).to_dict()
    if "schema" in step:
        return db.schema(**step["schema"])
    if "schema_manifest" in step:
        return db.schema_manifest()
    if "schema_check" in step:
        return db.check_schema_manifest(step["schema_check"])
    if "schema_apply" in step:
        application = dict(step["schema_apply"])
        manifest = application.pop("manifest")
        return db.apply_schema_manifest(manifest, **application).to_dict()
    if "validate" in step:
        return db.validate(step["validate"])
    if "datoms" in step:
        options = dict(step["datoms"])
        index = options.pop("index", "eavt")
        components = options.pop("components", ())
        return db.datoms(index, components, **options)
    if "explain" in step:
        return db.explain(step["explain"], step.get("args"))
    if "q" in step:
        return db.q(step["q"], step.get("args")).to_dict()
    if "entity" in step:
        return db.entity(step["entity"])
    if "history" in step:
        arguments = step["history"] if isinstance(step["history"], list) else [step["history"]]
        return db.history(*arguments)
    if "diff" in step:
        return db.diff(*step["diff"])
    if "why" in step:
        arguments = step["why"] if isinstance(step["why"], list) else [step["why"]]
        return db.why(*arguments)
    if "search" in step:
        return db.search(**step["search"]).to_dict()
    if "attributes" in step:
        return db.attributes(**step["attributes"])
    if "facts" in step:
        result = []
        for row in db._connection.execute(  # noqa: SLF001
            "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE id>39 ORDER BY id"
        ):
            values = list(row)
            if isinstance(values[3], bytes):
                values[3] = {"hex": values[3].hex()}
            result.append(values)
        return result
    raise AssertionError(f"unknown conformance step: {step}")


def _run(db: fgraph.Db, step: Mapping[str, Any]) -> None:
    expected_error = step.get("error")
    if expected_error is not None:
        with pytest.raises(fgraph.FGraphError) as raised:
            db.at(step["at"]) if "at" in step else _actual(db, step)
        assert type(raised.value).__name__ == expected_error
        return
    if "at" in step:
        view = db.at(step["at"])
        for inner in step["steps"]:
            _run(view, inner)
        return
    actual = _actual(db, step)
    if "expect" in step:
        query = step.get("q")
        unordered_paths = (
            frozenset({("rows",)}) if isinstance(query, Mapping) and not query.get("order") else frozenset()
        )
        assert _matches(actual, step["expect"], unordered_paths=unordered_paths), (
            f"actual={_canonical_json_document(actual)}\nexpected={_canonical_json_document(step['expect'])}"
        )


@pytest.mark.parametrize(
    ("actual", "expected", "unordered_paths", "matches"),
    [
        ({"value": 1}, {"value": 1}, frozenset(), True),
        ({"value": 1, "extra": 2}, {"value": 1}, frozenset(), False),
        ({"value": 1, "extra": 2}, {"value": 1, "...": True}, frozenset(), True),
        ({"rows": [[1], [2]]}, {"rows": [[2], [1]]}, frozenset(), False),
        ({"rows": [[1], [2]]}, {"rows": [[2], [1]]}, frozenset({("rows",)}), True),
        ({"rows": [[1], [2]]}, {"rows": [[1]]}, frozenset({("rows",)}), False),
    ],
)
def test_conformance_matcher_contract(
    actual: Any,
    expected: Any,
    unordered_paths: frozenset[tuple[str | int, ...]],
    matches: bool,
) -> None:
    assert _matches(actual, expected, unordered_paths=unordered_paths) is matches


@pytest.mark.parametrize("case_path", CASES, ids=lambda path: str(path.relative_to(CASE_ROOT)))
def test_conformance(case_path: Path) -> None:
    # Conformance fixtures are trusted test programs and may intentionally
    # contain an over-limit value to exercise a public decoding boundary.
    case = json.loads(case_path.read_text())
    with fgraph.connect(":memory:", clock=ConformanceClock()) as db:
        for step in case["steps"]:
            _run(db, step)
