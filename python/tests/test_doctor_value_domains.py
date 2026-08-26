"""Corruption tests for doctor physical value validation."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

import pytest

import fgraph

CLOCK = 1_767_225_600_000_000


def _fact_id(db: fgraph.Db) -> int:
    report = db.transact({"id": "subject", "item/value": 42})
    return int(report.asserted[-1]["id"])


def test_doctor_rejects_unknown_physical_value_tag() -> None:
    with fgraph.connect(":memory:", clock=CLOCK) as db:
        fact_id = _fact_id(db)
        db._connection.execute("PRAGMA ignore_check_constraints=ON")  # noqa: SLF001
        db._connection.execute("UPDATE fgraph_facts SET t=99 WHERE id=?", (fact_id,))  # noqa: SLF001
        db._connection.execute("PRAGMA ignore_check_constraints=OFF")  # noqa: SLF001

        report = db.doctor()

        assert report["ok"] is False
        assert "invalid value tags: 1" in report["problems"]
        with pytest.raises(fgraph.FormatError):
            db.entity("subject")
        with pytest.raises(fgraph.FormatError):
            db.doctor(repair=True)


def test_doctor_rejects_renamed_system_identity_without_mutation() -> None:
    with fgraph.connect(":memory:", clock=CLOCK) as db:
        db._connection.execute("UPDATE fgraph_ids SET name='corrupt/at' WHERE id=1")  # noqa: SLF001
        before = db._connection.serialize()  # noqa: SLF001

        report = db.doctor()

        assert report["ok"] is False
        assert "invalid system identities: 1" in report["problems"]
        assert db._connection.serialize() == before  # noqa: SLF001


def test_doctor_rejects_mutated_genesis_fact_without_mutation() -> None:
    with fgraph.connect(":memory:", clock=CLOCK) as db:
        db._connection.execute("UPDATE fgraph_facts SET e=2 WHERE id=2")  # noqa: SLF001
        before = db._connection.serialize()  # noqa: SLF001

        report = db.doctor()

        assert report["ok"] is False
        assert "invalid genesis facts: 1" in report["problems"]
        assert db._connection.serialize() == before  # noqa: SLF001
        with pytest.raises(fgraph.FormatError):
            db.doctor(repair=True)
        assert db._connection.serialize() == before  # noqa: SLF001


@pytest.mark.parametrize(
    ("name", "corrupt"),
    [
        ("ref storage", lambda db, fact_id: db.execute("UPDATE fgraph_facts SET t=0,v='65' WHERE id=?", (fact_id,))),
        ("bool domain", lambda db, fact_id: db.execute("UPDATE fgraph_facts SET t=1,v=2 WHERE id=?", (fact_id,))),
        (
            "non-finite float",
            lambda db, fact_id: db.execute("UPDATE fgraph_facts SET t=3,v=? WHERE id=?", (math.inf, fact_id)),
        ),
        (
            "inline text bound",
            lambda db, fact_id: db.execute("UPDATE fgraph_facts SET t=4,v=? WHERE id=?", ("x" * 257, fact_id)),
        ),
        (
            "instant domain",
            lambda db, fact_id: db.execute("UPDATE fgraph_facts SET t=5,v=253402300800000000 WHERE id=?", (fact_id,)),
        ),
        (
            "inline bytes bound",
            lambda db, fact_id: db.execute("UPDATE fgraph_facts SET t=6,v=? WHERE id=?", (bytes(257), fact_id)),
        ),
        (
            "canonical JSON",
            lambda db, fact_id: db.execute("UPDATE fgraph_facts SET t=10,v=? WHERE id=?", ('{"b":2, "a":1}', fact_id)),
        ),
    ],
)
def test_doctor_rejects_invalid_physical_value(
    name: str,
    corrupt: Callable[[Any, int], Any],
) -> None:
    del name
    with fgraph.connect(":memory:", clock=CLOCK) as db:
        fact_id = _fact_id(db)
        corrupt(db._connection, fact_id)  # noqa: SLF001

        report = db.doctor()

        assert report["ok"] is False
        assert "invalid physical values: 1" in report["problems"]
        with pytest.raises(fgraph.FormatError):
            db.entity("subject")
        with pytest.raises(fgraph.FormatError):
            db.doctor(repair=True)
