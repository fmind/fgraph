"""Focused compare-and-swap contract tests."""

from __future__ import annotations

from typing import Any

import pytest

import fgraph


def test_cas_requires_existing_cardinality_one_target(db: fgraph.Db) -> None:
    db.transact({"id": "cas/item", "cas/value": 1})
    db.declare("cas/many", type="int", many=True)

    cases: list[tuple[Any, type[Exception], str]] = [
        (["cas", "cas/missing", "cas/value", 1, 2], fgraph.NotFound, "not found"),
        (["cas", "cas/item", "cas/missing", 1, 2], fgraph.NotFound, "was not found"),
        (["cas", "cas/item", 1, 1, 2], fgraph.TypeError, "existing attribute name"),
        (["cas", "cas/item", "cas/many", 1, 2], fgraph.SchemaError, "cardinality-many"),
    ]
    for operation, error, message in cases:
        with pytest.raises(error, match=message):
            db.transact(operation)


def test_cas_missing_sentinel_creates_and_deletes(db: fgraph.Db) -> None:
    db.transact({"id": "cas/item", "cas/value": 1})
    db.declare("cas/optional", type="text")

    with pytest.raises(fgraph.Conflict, match="expected"):
        db.transact(["cas", "cas/item", "cas/value", {"missing": True}, 2])
    with pytest.raises(fgraph.Conflict, match="expected"):
        db.transact(["cas", "cas/item", "cas/optional", "present", "other"])

    created = db.transact(["cas", "cas/item", "cas/optional", {"missing": True}, "present"])
    assert created.status == "applied"
    assert db.entity("cas/item")["cas/optional"] == "present"

    deleted = db.transact(["cas", "cas/item", "cas/optional", "present", {"missing": True}])
    assert deleted.status == "applied"
    assert db.entity("cas/item") == {"cas/value": 1}

    unchanged = db.transact(["cas", "cas/item", "cas/optional", {"missing": True}, {"missing": True}])
    assert unchanged.status == "noop"
    assert unchanged.tx is None


@pytest.mark.parametrize(
    "sentinel",
    [
        {"missing": False},
        {"missing": 1},
        {"missing": None},
        {"missing": True, "extra": True},
    ],
)
def test_cas_rejects_every_non_exact_missing_sentinel(db: fgraph.Db, sentinel: dict[str, Any]) -> None:
    db.transact({"id": "cas/item", "cas/value": 1})
    db.declare("cas/optional", type="text")

    with pytest.raises(fgraph.TypeError):
        db.transact(["cas", "cas/item", "cas/optional", sentinel, "present"])
    with pytest.raises(fgraph.TypeError):
        db.transact(["cas", "cas/item", "cas/optional", {"missing": True}, sentinel])


@pytest.mark.parametrize(
    "companion",
    [
        ["cas", "cas/item", "cas/value", 1, 3],
        ["assert", "cas/item", "cas/value", 3],
        ["retract", "cas/item", "cas/value"],
        ["retract", "cas/item"],
        {"id": "cas/item", "cas/value": 3},
    ],
)
def test_cas_target_is_isolated_from_same_transaction(
    db: fgraph.Db,
    companion: Any,
) -> None:
    db.transact({"id": "cas/item", "cas/value": 1})
    before = db.stats()["transactions"]

    cas = ["cas", "cas/item", "cas/value", 1, 2]
    for operations in ([cas, companion], [companion, cas]):
        with pytest.raises(fgraph.Conflict, match="must be isolated"):
            db.transact(operations)
        assert db.entity("cas/item") == {"cas/value": 1}
        assert db.stats()["transactions"] == before


def test_cas_allows_unrelated_changes_in_same_transaction(db: fgraph.Db) -> None:
    db.transact({"id": "cas/item", "cas/value": 1})

    report = db.transact(
        [
            ["assert", "cas/item", "cas/other", "kept"],
            ["cas", "cas/item", "cas/value", 1, 2],
        ]
    )
    assert report.status == "applied"
    assert db.entity("cas/item") == {"cas/other": "kept", "cas/value": 2}


def test_cas_detects_corrupt_cardinality_one_state(db: fgraph.Db) -> None:
    db.transact({"id": "cas/item", "cas/value": 1})
    entity = int(db._connection.execute("SELECT id FROM fgraph_ids WHERE name='cas/item'").fetchone()[0])  # noqa: SLF001
    attribute = int(
        db._connection.execute(  # noqa: SLF001
            "SELECT id FROM fgraph_ids WHERE name='cas/value'"
        ).fetchone()[0]
    )
    current = db._connection.execute(  # noqa: SLF001
        "SELECT t,tx FROM fgraph_facts WHERE e=? AND a=? AND rx IS NULL",
        (entity, attribute),
    ).fetchone()
    db._connection.execute(  # noqa: SLF001
        "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,?,?,?,?,NULL)",
        (entity, attribute, 2, current["t"], current["tx"]),
    )

    with pytest.raises(fgraph.FormatError, match="multiple current values"):
        db.transact(["cas", "cas/item", "cas/value", 1, 3])
