"""Temporal, search, maintenance, and portable event behavior."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from conftest import Clock

import fgraph
from fgraph.values import canonical_json


def test_time_travel_history_diff_changes_and_why(db: fgraph.Db) -> None:
    first = db.transact({"id": "ada", "person/city": "London"}, source="book", by="reader")
    second = db.transact({"id": "ada", "person/city": "Lyon"}, source="chat")
    assert db.at(first.tx).entity("ada")["person/city"] == "London"
    assert db.at({"instant": first.at}).entity("ada")["person/city"] == "London"
    past_history = db.at(first.tx).history("ada", "person/city")
    assert past_history[0]["rx"] is None
    history = db.history("ada", "person/city")
    assert history[0] == {
        **{key: history[0][key] for key in ("id", "e", "a", "v", "tx", "rx")},
        "at": first.at,
        "by": "reader",
        "source": "book",
        "rx_at": second.at,
        "rx_source": "chat",
    }
    assert history[1]["at"] == second.at
    window = db.diff(first.tx, second.tx)
    assert any(fact["a"] == "person/city" and fact["v"] == "Lyon" for fact in window["asserted"])
    assert window["retracted"][0]["v"] == "London"
    assert db.changes(first.tx) == window
    provenance = db.why("ada", "person/city")[0]["provenance"]
    assert provenance["fgraph/source"] == "chat"
    with pytest.raises(fgraph.QueryError):
        db.diff(second.tx, first.tx)


def test_speculate_undo_and_undo_undo(db: fgraph.Db) -> None:
    original = db.transact({"id": "ada", "person/editor": "vim"})
    changed = db.transact({"id": "ada", "person/editor": "helix"})
    assert changed.tx is not None
    with db.speculate():
        speculative = db.transact({"id": "ada", "person/editor": "emacs"})
        assert speculative.tx is not None
        assert db.entity("ada")["person/editor"] == "emacs"
    assert db.entity("ada")["person/editor"] == "helix"
    undone = db.undo(changed.tx, by="mcp:test-client")
    assert undone.tx is not None
    assert db.entity("ada")["person/editor"] == "vim"
    assert db.entity(undone.tx)["fgraph/undoes"] == {"ref": changed.tx}
    assert db.receipt(undone.tx)["by"] == "mcp:test-client"
    redone = db.undo(undone.tx)
    assert redone.tx is not None
    assert db.entity("ada")["person/editor"] == "helix"
    with pytest.raises(fgraph.NotFound):
        db.undo(999_999)
    assert original.tx is not None


def test_undo_plans_after_a_concurrent_reassertion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "undo-race.db"
    with (
        fgraph.connect(path, clock=Clock()) as target,
        fgraph.connect(path, clock=Clock()) as concurrent,
    ):
        original = target.transact({"id": "item", "undo/value": "kept"})
        assert original.tx is not None
        transact = target.transact
        interleaved = False

        def intercept_transact(data: Any, **options: Any) -> fgraph.TxReport:
            nonlocal interleaved
            if not interleaved:
                interleaved = True
                concurrent.retract("item", "undo/value", "kept")
                concurrent.transact({"id": "item", "undo/value": "kept"})
            return transact(data, **options)

        monkeypatch.setattr(target, "transact", intercept_transact)
        undone = target.undo(original.tx)

        assert interleaved is True
        assert undone.status == "applied"
        assert target.entity("item") == {"undo/value": "kept"}


def test_follow_yields_event_records(db: fgraph.Db) -> None:
    report = db.transact({"id": "ada", "person/name": "Ada"})
    stream = db.follow(64, interval=0.001)
    record = next(stream)
    stream.close()
    assert record["fgraph"] == "event/1"
    assert record["event"] == report.event
    assert "tx" not in record
    assert record["asserted"] == [["ada", "person/name", "Ada", "text"]]


def test_keyword_vector_hybrid_filter_and_expansion(db: fgraph.Db) -> None:
    db.declare("person/knows", ref=True, many=True)
    db.declare("note/embedding", type="vector")
    db.transact(
        [
            {
                "id": "one",
                "note/text": "small composable tools",
                "note/kind": "guide",
                "note/embedding": {"vector": [1, 0]},
            },
            {"id": "two", "note/text": "large framework", "note/kind": "guide", "note/embedding": {"vector": [0, 1]}},
            {"id": "three", "note/text": "neighbor"},
        ]
    )
    db.transact({"id": "one", "person/knows": {"ref": "three"}})
    keyword = db.search("composable tools")
    assert keyword.hits[0]["entity"] == "one"
    assert "[composable]" in keyword.hits[0]["matched"][0]["snippet"]
    semantic = db.search(vector=[1, 0], vector_attribute="note/embedding")
    assert semantic.hits[0]["entity"] == "one"
    hybrid = db.search(
        "small",
        vector=[1, 0],
        vector_attribute="note/embedding",
        k=1,
        expand=1,
        filters=[["note/kind", "guide"]],
    )
    assert hybrid.hits[0]["score"] == pytest.approx(2 / 61)
    assert hybrid.expanded[0]["entity"] == "three"
    assert hybrid.expanded[0]["via"][0]["a"] == "person/knows"


def test_semantic_rank_uses_best_fact_position_and_validates_dims(db: fgraph.Db) -> None:
    db.declare("note/embedding", type="vector", many=True)
    db.transact(
        [
            {
                "id": "multi",
                "note/embedding": [
                    {"vector": [1, 0]},
                    {"vector": [0.9, 0.1]},
                ],
            },
            {"id": "single", "note/embedding": [{"vector": [0.8, 0.2]}]},
        ]
    )
    result = db.search(vector=[1, 0], vector_attribute="note/embedding")
    assert [hit["entity"] for hit in result.hits] == ["multi", "single"]
    assert result.hits[0]["score"] == pytest.approx(1 / 61)
    assert result.hits[1]["score"] == pytest.approx(1 / 62)
    with pytest.raises(fgraph.TypeError, match="dimensions"):
        db.search(vector=[1, 0, 0], vector_attribute="note/embedding")

    db.declare("note/not-vector", type="text")
    with pytest.raises(fgraph.TypeError, match="type='vector'"):
        db.search(vector=[1, 0], vector_attribute="note/not-vector")


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"text": "   \t"},
        {"vector": [0, 0]},
        {"text": "x", "k": 0},
        {"text": "x", "expand": -1},
        {"vector": [1, 0], "vector_attribute": "missing/vector"},
        {"text": "x", "filters": [["bad"]]},
    ],
)
def test_search_errors(db: fgraph.Db, kwargs) -> None:
    with pytest.raises(fgraph.FGraphError):
        db.search(**kwargs)
    db.transact({"id": "e", "note/text": "x"})
    with pytest.raises(fgraph.Unsupported):
        db.at(db._latest_tx()).search("x")  # noqa: SLF001


def test_event_apply_preserves_named_anonymous_metadata_and_undo(db: fgraph.Db) -> None:
    named = db.transact({"id": "ada", "person/name": "Ada"}, source="source", meta={"x": 1})
    anonymous = db.transact({"note/text": "anonymous"})
    anonymous_id = next(fact["e"] for fact in anonymous.asserted if fact["a"] == "note/text")
    changed = db.transact(["assert", anonymous_id, "note/status", "active"])
    assert changed.tx is not None
    reverted = db.undo(changed.tx)
    events = db.event_records()
    stream = "".join(f"{canonical_json(event)}\n" for event in events)
    assert all(event["fgraph"] == "event/1" and "tx" not in event for event in events)
    assert events[0]["source"] == "source"
    assert events[0]["meta"] == {"x": 1}
    assert events[-1]["tx_facts"][0][0] == "fgraph/undoes"

    target = fgraph.connect(":memory:", clock=Clock(1_800_000_000_000_000))
    try:
        target.transact({"id": "occupied", "target/value": 1})
        reports = target.apply(stream)
        assert len(reports) == len(events)
        assert target.entity("ada")["person/name"] == "Ada"
        result = target.q(find=["?e"], where=[["?e", "note/text", "anonymous"]])
        applied_id = result.rows[0][0]["ref"]
        assert isinstance(applied_id, int)
        assert applied_id != anonymous_id
        applied_undo_tx = reports[-1].tx
        assert target.entity(applied_undo_tx)["fgraph/undoes"] == {"ref": reports[-2].tx}
    finally:
        target.close()
    assert named.tx is not None
    assert reverted.tx is not None


def test_portable_event_preserves_null_metadata_and_allows_large_records(db: fgraph.Db) -> None:
    payload = "x" * 600_000
    source = db.transact(
        {
            "id": "large",
            "large/json": {"json": {"edge": float(2**63), "large": 1e20}},
            "large/left": payload,
            "large/right": payload,
        },
        meta=None,
    )
    stream = "".join(f"{canonical_json(event)}\n" for event in db.event_records())
    assert len(stream.encode()) > 1_048_576

    with fgraph.connect(":memory:", clock=Clock(1_800_000_000_000_000)) as target:
        reports = target.apply(stream)
        imported = target.entity("large")
        assert imported == {
            "large/json": {"json": {"edge": float(2**63), "large": 1e20}},
            "large/left": payload,
            "large/right": payload,
        }
        assert target.entity(reports[0].tx)["fgraph/meta"] == {"json": None}
    assert source.tx is not None


def test_excise_and_doctor_detect_damage(tmp_path: Path) -> None:
    path = tmp_path / "excise.db"
    with fgraph.connect(path, clock=Clock()) as graph:
        graph.transact({"id": "user", "user/private": "secret" * 100})
        graph.declare("audit/subject", ref=True)
        audit = graph.transact({}, tx={"audit/subject": {"ref": "user"}})
        report = graph.excise("user")
        assert any(fact["a"] == "user/private" for fact in report.retracted)
        assert graph.entity("user") == {}
        assert "audit/subject" not in graph.entity(audit.tx)
        assert graph.entity(report.tx)["fgraph/excised"] == {"ref": "user"}
        assert graph.stats()["blobs"] == 0
        with pytest.raises(fgraph.Unsupported):
            graph.excise("fgraph/at")
        graph.declare("note/vector", type="vector")
        graph.transact({"id": "note", "note/vector": {"vector": [1, 2]}})
        graph._connection.execute("DELETE FROM fgraph_blobs")  # noqa: SLF001
        report = graph.doctor()
        assert report["ok"] is False
        assert any("missing blobs" in problem for problem in report["problems"])
        with pytest.raises(fgraph.FormatError, match="missing blobs"):
            graph.doctor(repair=True)
