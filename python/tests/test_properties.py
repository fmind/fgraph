"""Fixed-seed temporal model checks over generated transaction sequences."""

from __future__ import annotations

from copy import deepcopy

import pytest
from conftest import Clock
from hypothesis import given, seed, settings
from hypothesis import strategies as st

import fgraph

Operation = tuple[str, str, int]


@seed(20260824)
@settings(max_examples=30, deadline=None)
@given(
    st.lists(
        st.tuples(
            st.sampled_from(["set", "retract", "declare"]),
            st.sampled_from(["alpha", "beta", "gamma"]),
            st.integers(min_value=-3, max_value=3),
        ),
        min_size=1,
        max_size=12,
    )
)
def test_temporal_sequences_match_naive_reference(operations: list[Operation]) -> None:
    """Current, historical, timeline, and window reads agree with a small model."""
    graph = fgraph.connect(":memory:", clock=Clock())
    try:
        current: dict[str, int] = {}
        assertions: dict[str, list[int]] = {name: [] for name in ("alpha", "beta", "gamma")}
        declaration_doc: str | None = None
        snapshots: dict[int, dict[str, int]] = {64: {}}
        events: list[tuple[int, str, int | None, int | None]] = []

        for operation, entity, value in operations:
            old = current.get(entity)
            if operation == "set":
                report = graph.transact({"id": entity, "state/value": value})
                changed = old != value
                if changed:
                    current[entity] = value
                    assertions[entity].append(value)
            elif operation == "retract":
                report = graph.retract(entity, "state/value")
                changed = old is not None
                if changed:
                    del current[entity]
            else:
                next_doc = f"Reference-model declaration {value}"
                report = graph.declare("state/value", type="int", doc=next_doc)
                changed = declaration_doc != next_doc
                declaration_doc = next_doc
            assert (report.tx is not None) is changed
            if report.tx is not None:
                snapshots[report.tx] = deepcopy(current)
                if operation == "set":
                    events.append((report.tx, entity, value, old))
                elif operation == "retract":
                    events.append((report.tx, entity, None, old))
                else:
                    events.append((report.tx, entity, None, None))
            for name in ("alpha", "beta", "gamma"):
                expected = {} if name not in current else {"state/value": current[name]}
                if name in graph._names:  # noqa: SLF001
                    assert graph.entity(name) == expected

        for transaction, snapshot in snapshots.items():
            view = graph.at(transaction)
            for name in graph._names:  # noqa: SLF001
                if name.startswith("fgraph/") or "/" in name:
                    continue
                expected = {} if name not in snapshot else {"state/value": snapshot[name]}
                created_tx = graph._connection.execute(  # noqa: SLF001
                    "SELECT created_tx FROM fgraph_ids WHERE name=?", (name,)
                ).fetchone()[0]
                if created_tx > transaction:
                    with pytest.raises(fgraph.NotFound):
                        view.entity(name)
                else:
                    assert view.entity(name) == expected

        for name, values in assertions.items():
            if name in graph._names:  # noqa: SLF001
                assert [fact["v"] for fact in graph.history(name, "state/value")] == values

        previous = 64
        for transaction, _entity, asserted, retracted in events:
            change = graph.diff(previous, transaction)
            domain_asserted = [fact["v"] for fact in change["asserted"] if fact["a"] == "state/value"]
            domain_retracted = [fact["v"] for fact in change["retracted"] if fact["a"] == "state/value"]
            assert domain_asserted == ([] if asserted is None else [asserted])
            assert domain_retracted == ([] if retracted is None else [retracted])
            previous = transaction
    finally:
        graph.close()
