"""Pre-v1 regressions for coherent reads, initialization, and bounded values."""

from __future__ import annotations

import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import Any

import pytest

import fgraph
from fgraph import search as search_module
from fgraph.jsonio import loads
from fgraph.values import MAX_JSON_DEPTH, MAX_JSON_DOCUMENT_DEPTH, canonical_json


def _nested_json(containers: int) -> Any:
    value: Any = 0
    for _ in range(containers):
        value = [value]
    return value


def test_json_depth_is_checked_before_recursive_canonicalization() -> None:
    assert canonical_json(_nested_json(MAX_JSON_DEPTH))

    with pytest.raises(fgraph.TooLarge, match="depth"):
        canonical_json(_nested_json(MAX_JSON_DEPTH + 1))
    assert loads("[" * MAX_JSON_DOCUMENT_DEPTH + "0" + "]" * MAX_JSON_DOCUMENT_DEPTH)
    with pytest.raises(fgraph.TooLarge, match="depth"):
        loads("[" * (MAX_JSON_DOCUMENT_DEPTH + 1) + "0" + "]" * (MAX_JSON_DOCUMENT_DEPTH + 1))

    cyclic: list[Any] = []
    cyclic.append(cyclic)
    with pytest.raises(fgraph.TooLarge, match="depth"):
        canonical_json(cyclic)


def test_maximum_depth_json_survives_event_and_snapshot_envelopes() -> None:
    value = _nested_json(MAX_JSON_DEPTH)
    with fgraph.connect(":memory:", clock=1_767_225_600_000_000) as source:
        source.transact({"id": "depth/item", "depth/value": {"json": value}})
        assert source.entity("depth/item")["depth/value"] == {"json": value}
        snapshot = source.snapshot()
        assert isinstance(snapshot, str)

    with fgraph.connect(":memory:") as restored:
        restored.restore(snapshot)
        assert restored.entity("depth/item")["depth/value"] == {"json": value}


def test_search_reads_candidates_pulls_and_basis_from_one_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "search-snapshot.db"
    with fgraph.connect(path, clock=1_767_225_600_000_000) as seed:
        initial = seed.transact({"id": "search/item", "doc/text": "needle old"})

    with (
        fgraph.connect(path, clock=1_767_225_600_000_000) as reader,
        fgraph.connect(path, clock=1_767_225_600_000_000) as writer,
    ):
        original_pull = search_module._compact_pull  # noqa: SLF001
        committed = False

        def commit_then_pull(database: fgraph.Db, entity: int) -> dict[str, Any]:
            nonlocal committed
            if not committed:
                committed = True
                writer.transact({"id": "search/item", "doc/text": "replacement"})
            return original_pull(database, entity)

        monkeypatch.setattr(search_module, "_compact_pull", commit_then_pull)
        result = reader.search("needle")

        assert committed is True
        assert result.basis_tx == initial.tx
        assert result.hits[0]["pull"]["doc/text"] == "needle old"
        assert writer._latest_tx() > result.basis_tx  # noqa: SLF001


def test_search_compact_pull_bounds_attribute_and_value_reads(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db.declare("a/many", many=True)
    document: dict[str, Any] = {
        "id": "compact/target",
        "a/many": list(range(40)),
        "search/text": "compact window needle",
        "z/hidden": "outside the compact pull",
    }
    document.update({f"a/{index:02d}": index for index in range(31)})
    db.transact(document)

    decoded = 0
    original_wire = type(db)._wire  # noqa: SLF001

    def counted_wire(database: fgraph.Db, tag: int, stored: Any) -> Any:
        nonlocal decoded
        decoded += 1
        return original_wire(database, tag, stored)

    statements: list[str] = []
    monkeypatch.setattr(type(db), "_wire", counted_wire)
    db._connection.set_trace_callback(statements.append)  # noqa: SLF001
    try:
        result = db.search("compact window needle", k=1)
    finally:
        db._connection.set_trace_callback(None)  # noqa: SLF001

    pull = result.hits[0]["pull"]
    expected_attributes = [f"a/{index:02d}" for index in range(31)] + ["a/many"]
    assert list(pull) == expected_attributes
    assert pull["a/many"] == list(range(32))
    assert decoded == 63

    normalized = [" ".join(statement.lower().split()) for statement in statements]
    assert any("group by f.a,i.name order by i.name collate binary limit 32" in statement for statement in normalized)
    value_reads = [
        statement
        for statement in normalized
        if "from fgraph_facts f" in statement
        and "f.e=" in statement
        and "f.a=" in statement
        and "order by f.id limit 32" in statement
    ]
    assert len(value_reads) == 32


def test_coherent_reads_reuse_an_existing_transaction(db: fgraph.Db) -> None:
    with db.speculate():
        db.transact({"id": "search/speculative", "doc/text": "inside transaction"})
        assert db._connection.in_transaction is True  # noqa: SLF001

        result = db.search("inside transaction")
        assert result.hits[0]["entity"] == "search/speculative"
        assert db._connection.in_transaction is True  # noqa: SLF001

        lines = db.iter_snapshot()
        next(lines)
        lines.close()
        assert db._connection.in_transaction is True  # noqa: SLF001

    with pytest.raises(fgraph.NotFound):
        db.entity("search/speculative")


@pytest.mark.parametrize("mutation", ["nohistory", "excise"])
def test_snapshot_generator_keeps_one_basis_after_header(
    tmp_path: Path,
    mutation: str,
) -> None:
    path = tmp_path / f"snapshot-{mutation}.db"
    with fgraph.connect(path, clock=1_767_225_600_000_000) as seed:
        if mutation == "nohistory":
            seed.declare("secret/value", type="text", nohistory=True)
        seed.transact({"id": "secret/item", "secret/value": "before"})
        expected = seed.snapshot()

    with (
        fgraph.connect(path, clock=1_767_225_600_000_000) as reader,
        fgraph.connect(path, clock=1_767_225_600_000_000) as writer,
    ):
        lines = reader.iter_snapshot()
        first = next(lines)
        if mutation == "nohistory":
            writer.transact({"id": "secret/item", "secret/value": "after"})
        else:
            writer.excise("secret/item")
        observed = first + "".join(lines)

    assert observed == expected


def test_concurrent_pristine_initializers_accept_the_winner(tmp_path: Path) -> None:
    path = tmp_path / "initialize.db"
    # Establish WAL before the synchronized openers so the test isolates the
    # format initialization lock instead of journal-mode negotiation.
    import sqlite3

    raw = sqlite3.connect(path)
    raw.execute("PRAGMA journal_mode=WAL")
    raw.close()
    barrier = Barrier(2)

    def initialize() -> int:
        def synchronized_clock() -> int:
            barrier.wait(timeout=5)
            return 1_767_225_600_000_000

        with fgraph.connect(path, clock=synchronized_clock) as database:
            return int(database.stats()["transactions"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        transactions = list(executor.map(lambda _index: initialize(), range(2)))

    assert transactions == [1, 1]
    with fgraph.connect(path, read_only=True) as database:
        assert database.doctor()["ok"] is True


def test_vector_search_loads_candidate_blobs_in_the_scan_query(db: fgraph.Db) -> None:
    db.declare("note/embedding", type="vector", dims=2)
    db.transact([{"id": f"note/{index}", "note/embedding": {"vector": [1, index / 10]}} for index in range(1, 5)])
    statements: list[str] = []
    db._connection.set_trace_callback(statements.append)  # noqa: SLF001
    try:
        result = db.search(vector=[1, 0], vector_attribute="note/embedding", k=1)
    finally:
        db._connection.set_trace_callback(None)  # noqa: SLF001

    blob_point_reads = [
        statement
        for statement in statements
        if "select data from fgraph_blobs where hash=" in " ".join(statement.lower().split())
    ]
    assert result.hits[0]["entity"] == "note/1"
    # The compact pull may load the winning vector once, but candidate count
    # must not produce point reads from fgraph_blobs.
    assert len(blob_point_reads) == 1

    db._connection.execute(  # noqa: SLF001
        "UPDATE fgraph_blobs SET data=? WHERE hash=(SELECT v FROM fgraph_facts WHERE t=7 ORDER BY id LIMIT 1)",
        (b"\x00" * 8,),
    )
    with pytest.raises(fgraph.FormatError, match="content-addressed hash"):
        db.search(vector=[1, 0], vector_attribute="note/embedding", k=1)


def test_vector_candidate_scan_does_not_retain_joined_payloads() -> None:
    class Rows:
        def __iter__(self) -> Any:
            for index in range(32):
                yield {
                    "id": index + 1,
                    "e": index + 65,
                    "a": 65,
                    "v": bytes(32),
                    "t": 7,
                    "tx": 64,
                    "rx": None,
                    "fgraph_blob_hash": bytes(32),
                    "fgraph_blob_data": bytes([index]) * (1024 * 1024),
                }

        def fetchall(self) -> list[dict[str, Any]]:
            return list(self)

    class Connection:
        def execute(self, *_args: Any) -> Rows:
            return Rows()

    class FakeDb:
        def __init__(self) -> None:
            self._connection = Connection()
            self._names = {"note/embedding": 65}
            self._query_budget = 100

        def _schema(self, _attribute: int) -> SimpleNamespace:
            return SimpleNamespace(type="vector", dims=2)

        def _logical_indirect(self, *_args: Any, **_kwargs: Any) -> tuple[float, float]:
            return (1.0, 0.0)

        def _render_row(self, row: dict[str, Any], *, logical_override: Any) -> dict[str, Any]:
            return {
                "id": row["id"],
                "e": row["e"],
                "a": "note/embedding",
                "v": {"vector": list(logical_override)},
                "tx": row["tx"],
                "rx": row["rx"],
            }

        def _tx_metadata(self, _transaction: int) -> dict[str, Any]:
            return {}

    tracemalloc.start()
    try:
        ranks, matched, truncated = search_module._semantic(  # noqa: SLF001
            FakeDb(),
            [1, 0],
            "note/embedding",
            None,
            1,
            search_module._WorkBudget(100),  # noqa: SLF001
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert len(ranks) == 1
    assert len(matched) == 1
    assert truncated is True
    assert peak < 8 * 1024 * 1024


def test_utf8_search_excerpt_exact_boundary_is_not_truncated() -> None:
    exact = "é" * (search_module.MAX_MATCH_TEXT_BYTES // 2)
    assert search_module._bounded_text(exact) == (exact, False)  # noqa: SLF001

    bounded, truncated = search_module._bounded_text(exact + "é")  # noqa: SLF001
    assert truncated is True
    assert bounded.endswith("…")
    assert len(bounded.encode()) <= search_module.MAX_MATCH_TEXT_BYTES
    assert bounded == "é" * 1022 + "…"
