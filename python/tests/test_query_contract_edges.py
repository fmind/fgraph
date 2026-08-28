"""Planner, history-datom, rule, and validation edge coverage."""

from __future__ import annotations

from typing import Any

import pytest

import fgraph
from fgraph.query import (
    _binding_key,
    _clauses,
    _constant,
    _operand,
    _pattern,
    _predicate,
    _project,
    _relations,
    _rule_arities,
    _rule_calls,
    _rule_invocation,
    _validate_rule_invocations,
    _WorkBudget,
)
from fgraph.values import BYTES, INT, Cell


def test_query_history_transaction_and_added_constraints(db: fgraph.Db) -> None:
    first = db.transact({"id": "query/item", "query/value": 1})
    second = db.transact({"id": "query/item", "query/value": 2})

    asserted = db.q(
        {
            "source": "history",
            "find": ["?v"],
            "where": [["query/item", "query/value", "?v", first.tx, True]],
        }
    )
    assert asserted.rows == [[1]]
    retracted = db.q(
        {
            "source": "history",
            "find": ["?v"],
            "where": [["query/item", "query/value", "?v", second.tx, False]],
        }
    )
    assert retracted.rows == [[1]]
    either = db.q(
        {
            "source": "history",
            "find": ["?v", "?added"],
            "where": [["query/item", "query/value", "?v", second.tx, "?added"]],
            "order": [["?added", "asc"]],
        }
    )
    assert either.rows == [[1, False], [2, True]]
    assert (
        db.q(
            find=["?v"],
            where=[["query/item", "query/value", "?v", "_", False]],
        ).rows
        == []
    )
    assert isinstance(db.q(find=["?v"], where=[["?same", "?same", "?v"]]).rows, list)


def test_query_low_level_validation_and_rule_failures(db: fgraph.Db) -> None:
    db.transact({"id": "query/item", "query/value": 1})
    db._refresh_cache(force=True)  # noqa: SLF001
    work = _WorkBudget(100)
    with pytest.raises(fgraph.QueryError, match="invalid datom pattern"):
        _pattern(db, ["?e", "query/value"], [{}], work, "current")
    with pytest.raises(fgraph.QueryError, match="invalid attribute term"):
        _pattern(db, ["?e", 1, "?v"], [{}], work, "current")
    with pytest.raises(fgraph.QueryError, match="invalid pattern"):
        _pattern(db, ["?e", "invalid", "?v"], [{}], work, "current")
    with pytest.raises(fgraph.QueryError, match="unbound"):
        _operand(db, "?missing", {})
    with pytest.raises(fgraph.QueryError, match="cannot be resolved"):
        _operand(db, {"ref": "missing"}, {})
    with pytest.raises(fgraph.QueryError, match="invalid predicate"):
        _predicate(db, ["=", 1], [{}], work)

    relations: Any = {"one": [(Cell(INT, 1),)]}
    with pytest.raises(fgraph.QueryError, match="invalid rule invocation"):
        _rule_invocation(db, [], [{}], relations, work)
    with pytest.raises(fgraph.QueryError, match="not defined"):
        _rule_invocation(db, ["missing"], [{}], relations, work)
    with pytest.raises(fgraph.QueryError, match="expects 1 arguments"):
        _rule_invocation(db, ["one"], [{}], relations, work)
    with pytest.raises(fgraph.QueryError, match="no branches"):
        _clauses(db, [{"or": []}], [{}], {}, work, "current")
    with pytest.raises(fgraph.QueryError, match="unknown clause object"):
        _clauses(db, [{"unknown": []}], [{}], {}, work, "current")
    with pytest.raises(fgraph.QueryError, match="invalid clause"):
        _clauses(db, [1], [{}], {}, work, "current")

    with pytest.raises(fgraph.QueryError, match="invalid rule head"):
        _rule_arities([{"head": [], "body": []}])
    with pytest.raises(fgraph.QueryError, match="invalid body"):
        _rule_arities([{"head": ["rule", "?x"], "body": {}}])
    assert _rule_calls({"rule": []}) == set()
    with pytest.raises(fgraph.QueryError, match="invalid rule invocation"):
        _validate_rule_invocations({"rule": []}, {"rule": 1})
    with pytest.raises(fgraph.QueryError, match="head variable"):
        _relations(
            db,
            [{"head": ["rule", "?missing"], "body": [["query/item", "query/value", "?value"]]}],
            _WorkBudget(100),
            "current",
        )
    with pytest.raises(fgraph.QueryError, match=r"pull.*aggregate"):
        _project(db, [["pull", "?e", ["*"]], ["count", "?e"]], [], _WorkBudget(100))

    assert _binding_key({"?bytes": Cell(BYTES, b"\x00\xff")})

    class InvalidEntityDb:
        def _resolve_read(self, _value: Any, *, missing_ok: bool) -> None:
            assert missing_ok is True
            raise fgraph.TypeError("invalid")

    with pytest.raises(fgraph.QueryError, match="invalid entity query constant"):
        _constant(InvalidEntityDb(), "value", entity=True)


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ({"find": ["?e"], "where": [], "unknown": True}, "unknown query keys"),
        ({"find": ["?e"], "where": [], "source": "future"}, "source"),
        ({"find": [], "where": []}, "find"),
        ({"find": ["?e"], "where": {}}, "where"),
        ({"find": ["?e"], "where": [], "in": ["bad"]}, "query in"),
        ({"find": ["?e"], "where": [], "in": ["?e"]}, "missing"),
    ],
)
def test_explain_rejects_malformed_query_contracts(
    db: fgraph.Db,
    query: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(fgraph.QueryError, match=message):
        db.explain(query)


def test_explain_reports_logic_barriers_and_history_warning(db: fgraph.Db) -> None:
    db.transact({"id": "query/item", "query/value": 1})
    explanation = db.explain(
        {
            "source": "history",
            "find": ["?e"],
            "where": [
                ["?e", "query/value", 1],
                {
                    "or": [
                        [["?e", "query/value", 1]],
                        [["?e", "query/value", 2]],
                    ]
                },
            ],
        }
    )
    assert explanation["clauses"][1] == {
        "ordinal": 1,
        "kind": "or",
        "access": "barrier",
        "bound": ["?e"],
    }
    assert explanation["warnings"] == [
        "history source evaluates assertion and retraction datoms within the selected basis"
    ]

    with pytest.raises(fgraph.QueryError, match="query source"):
        db.q({"find": ["?e"], "where": [], "source": "future"})
    with pytest.raises(fgraph.QueryError, match="query order"):
        db.q({"find": ["?e"], "where": [["?e", "query/value", 1]], "order": {}})
