from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Callable, Iterator
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

import fgraph
from fgraph import Conflict, FormatError, NotFound, SchemaError
from fgraph.store import MAX_EVENT_BYTES
from fgraph.values import MAX_VALUE_BYTES, canonical_json


def event_factory(*values: str) -> Callable[[], str]:
    events: Iterator[str] = iter(values)
    return lambda: next(events)


@pytest.fixture(autouse=True)
def close_databases(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    original = fgraph.connect
    opened: list[fgraph.Db] = []

    def tracked_connect(*args: Any, **kwargs: Any) -> fgraph.Db:
        database = original(*args, **kwargs)
        opened.append(database)
        return database

    monkeypatch.setattr(fgraph, "connect", tracked_connect)
    yield
    for database in reversed(opened):
        database.close()


def test_format_v2_registry_events_and_genesis() -> None:
    db = fgraph.connect(
        ":memory:",
        clock=1_767_225_600_000_000,
        event_factory=event_factory("00000000-0000-4000-8000-000000000065"),
    )

    assert db.stats()["format_version"] == 2
    assert db._connection.execute("SELECT count(*) FROM fgraph_facts").fetchone()[0] == 39  # noqa: SLF001
    assert db._connection.execute("SELECT count(*) FROM fgraph_ids").fetchone()[0] == 19  # noqa: SLF001
    genesis = db._connection.execute(  # noqa: SLF001
        "SELECT hex(gid),created_tx FROM fgraph_ids WHERE id=64"
    ).fetchone()
    assert tuple(genesis) == ("00000000000040008000000000000040", 64)
    assert (
        db._connection.execute(  # noqa: SLF001
            "SELECT count(*) FROM fgraph_events WHERE tx=64"
        ).fetchone()[0]
        == 1
    )

    report = db.transact({"id": "empty"})
    assert report.status == "applied"
    assert report.event == "00000000-0000-4000-8000-000000000065"
    assert report.tx is not None
    identity = db._connection.execute(  # noqa: SLF001
        "SELECT name,gid,created_tx FROM fgraph_ids WHERE name='empty'"
    ).fetchone()
    assert tuple(identity) == ("empty", None, report.tx)
    event = db._connection.execute(  # noqa: SLF001
        "SELECT length(event_hash),operation_id,request_hash FROM fgraph_events WHERE tx=?",
        (report.tx,),
    ).fetchone()
    assert tuple(event) == (32, None, None)


def test_historical_views_do_not_leak_future_identities_or_stats() -> None:
    db = fgraph.connect(
        ":memory:",
        clock=1_767_225_600_000_000,
        event_factory=event_factory(
            "00000000-0000-4000-8000-000000000065",
            "00000000-0000-4000-8000-000000000066",
        ),
    )
    first = db.transact({"id": "before", "doc/value": "before"})
    current_before = db.at(first.tx).stats()
    db.transact({"id": "future", "future/value": "future"})

    historical = db.at(first.tx)
    with pytest.raises(NotFound):
        historical.entity("future")
    assert all(item["name"] != "future/value" for item in historical.attributes())
    assert historical.stats() == current_before


def test_operation_id_basis_and_cas_are_retry_safe() -> None:
    db = fgraph.connect(
        ":memory:",
        clock=1_767_225_600_000_000,
        event_factory=event_factory(
            "00000000-0000-4000-8000-000000000065",
            "00000000-0000-4000-8000-000000000066",
            "00000000-0000-4000-8000-000000000067",
        ),
    )
    initial = db.transact({"id": "counter", "counter/value": 1}, operation_id="request:one")
    duplicate = db.transact({"id": "counter", "counter/value": 1}, operation_id="request:one")
    assert duplicate.status == "already_applied"
    assert (duplicate.event, duplicate.tx, duplicate.at) == (initial.event, initial.tx, initial.at)
    assert db.stats()["transactions"] == 2  # Genesis plus the applied write.

    with pytest.raises(Conflict):
        db.transact({"id": "counter", "counter/value": 2}, operation_id="request:one")
    with pytest.raises(Conflict):
        db.transact({"id": "counter", "counter/value": 2}, if_basis_tx=64)

    changed = db.transact(
        [["cas", "counter", "counter/value", 1, 2]],
        if_basis_tx=initial.tx,
    )
    assert changed.status == "applied"
    assert db.entity("counter")["counter/value"] == 2
    with pytest.raises(Conflict):
        db.transact([["cas", "counter", "counter/value", 1, 3]])


def test_excise_is_cas_guarded_idempotent_and_receipted() -> None:
    db = fgraph.connect(":memory:", clock=1_767_225_600_000_000)
    created = db.transact({"id": "subject", "secret/value": "erase me"})

    erased = db.excise(
        "subject",
        operation_id="erase:subject",
        if_basis_tx=created.tx,
    )
    assert erased.status == "applied"
    assert erased.tx is not None
    assert db.entity("subject") == {}
    assert db._connection.execute(  # noqa: SLF001
        "SELECT event_data IS NULL FROM fgraph_events WHERE tx=?", (created.tx,)
    ).fetchone()[0]
    redaction = json.loads(
        db._connection.execute(  # noqa: SLF001
            "SELECT event_data FROM fgraph_events WHERE tx=?", (erased.tx,)
        ).fetchone()[0]
    )
    assert redaction["redacted"] is True
    assert created.event in redaction["redacts"]
    assert db.doctor()["ok"] is True
    db.transact({"id": "later", "later/value": True})

    retry = db.excise(
        "subject",
        operation_id="erase:subject",
        if_basis_tx=created.tx,
    )
    assert (retry.status, retry.event, retry.tx, retry.basis_tx) == (
        "already_applied",
        erased.event,
        erased.tx,
        created.tx,
    )
    assert db.receipt(erased.tx)["operation_id"] == "erase:subject"

    with pytest.raises(Conflict, match="different request"):
        db.excise("later", operation_id="erase:subject", if_basis_tx=created.tx)
    with pytest.raises(Conflict, match="basis transaction changed"):
        db.excise("later", operation_id="erase:later", if_basis_tx=created.tx)
    with pytest.raises(Conflict, match="already excised under another operation"):
        db.excise(
            "subject",
            operation_id="erase:subject:again",
            if_basis_tx=db._latest_tx(),  # noqa: SLF001
        )

    snapshot = db.snapshot()
    assert isinstance(snapshot, str)
    restored = fgraph.connect(":memory:")
    restored.restore(snapshot)
    assert restored.doctor()["ok"] is True
    assert any(record.get("event_hash") for record in restored.event_records(64))
    restored.close()


def test_excise_purges_payload_only_nohistory_values() -> None:
    db = fgraph.connect(":memory:", clock=1_767_225_600_000_000)
    db.declare("secret/value", type="text", nohistory=True)
    first = db.transact({"id": "secret/subject", "secret/value": "old"})
    second = db.transact({"id": "secret/subject", "secret/value": "new"})
    before = "\n".join(
        str(row[0])
        for row in db._connection.execute(  # noqa: SLF001
            "SELECT event_data FROM fgraph_events WHERE event_data IS NOT NULL ORDER BY tx"
        )
    )
    assert '"old"' in before
    assert '"new"' in before

    excision = db.excise("secret/subject")
    after = "\n".join(
        str(row[0])
        for row in db._connection.execute(  # noqa: SLF001
            "SELECT event_data FROM fgraph_events WHERE event_data IS NOT NULL ORDER BY tx"
        )
    )
    assert '"old"' not in after
    assert '"new"' not in after
    assert first.event is not None
    assert second.event is not None
    redaction = json.loads(
        db._connection.execute(  # noqa: SLF001
            "SELECT event_data FROM fgraph_events WHERE tx=?", (excision.tx,)
        ).fetchone()[0]
    )
    assert redaction["redacts"] == sorted([first.event, second.event])
    assert db.doctor()["ok"] is True


def test_excise_erases_user_attributes_and_their_event_payloads() -> None:
    db = fgraph.connect(":memory:", clock=1_767_225_600_000_000)
    declaration = db.declare("private/value", type="text")
    value = db.transact({"id": "private/holder", "private/value": "secret"})
    attribute = db._names["private/value"]  # noqa: SLF001
    hashes_before = {
        int(row["tx"]): bytes(row["event_hash"])
        for row in db._connection.execute(  # noqa: SLF001
            "SELECT tx,event_hash FROM fgraph_events WHERE tx IN (?,?)", (declaration.tx, value.tx)
        )
    }

    excision = db.excise("private/value")
    assert db.entity("private/holder") == {}
    assert (
        db._connection.execute(  # noqa: SLF001
            "SELECT count(*) FROM fgraph_facts WHERE a=?", (attribute,)
        ).fetchone()[0]
        == 0
    )
    payloads = db._connection.execute(  # noqa: SLF001
        "SELECT tx,event_hash,event_data FROM fgraph_events WHERE tx IN (?,?) ORDER BY tx",
        (declaration.tx, value.tx),
    ).fetchall()
    assert all(row["event_data"] is None for row in payloads)
    assert {int(row["tx"]): bytes(row["event_hash"]) for row in payloads} == hashes_before
    assert declaration.event is not None
    assert value.event is not None
    redaction = json.loads(
        db._connection.execute(  # noqa: SLF001
            "SELECT event_data FROM fgraph_events WHERE tx=?", (excision.tx,)
        ).fetchone()[0]
    )
    assert redaction["redacts"] == sorted([declaration.event, value.event])
    assert db.doctor()["ok"] is True


def test_excise_redacts_tx_fact_refs_without_matching_arbitrary_values() -> None:
    db = fgraph.connect(":memory:", clock=1_767_225_600_000_000)
    db.declare("audit/subject", ref=True)
    db.transact({"id": "audit/target", "audit/value": True})
    tx_fact = db.transact([], tx={"audit/subject": {"ref": "audit/target"}})
    unrelated = db.transact(
        {
            "id": "audit/unrelated",
            "audit/text": "audit/target",
            "audit/json": {"json": {"ref": "audit/target"}},
        }
    )

    db.excise("audit/target")
    assert (
        db._connection.execute(  # noqa: SLF001
            "SELECT event_data FROM fgraph_events WHERE tx=?", (tx_fact.tx,)
        ).fetchone()[0]
        is None
    )
    unrelated_payload = db._connection.execute(  # noqa: SLF001
        "SELECT event_data FROM fgraph_events WHERE tx=?", (unrelated.tx,)
    ).fetchone()[0]
    assert unrelated_payload is not None
    assert "audit/target" in unrelated_payload
    assert db.doctor()["ok"] is True


def test_excise_matches_payload_only_anonymous_eid_selectors() -> None:
    db = fgraph.connect(":memory:", clock=1_767_225_600_000_000)
    db.declare("anonymous/value", type="text", nohistory=True)
    created = db.transact({"anonymous/value": "private"})
    anonymous = next(fact["e"] for fact in created.asserted if fact["a"] == "anonymous/value")
    removed = db.transact(["retract", anonymous, "anonymous/value"])
    assert (
        db._connection.execute(  # noqa: SLF001
            "SELECT count(*) FROM fgraph_facts WHERE e=?", (anonymous,)
        ).fetchone()[0]
        == 0
    )

    excision = db.excise(anonymous)
    redaction = json.loads(
        db._connection.execute(  # noqa: SLF001
            "SELECT event_data FROM fgraph_events WHERE tx=?", (excision.tx,)
        ).fetchone()[0]
    )
    assert created.event is not None
    assert removed.event is not None
    assert redaction["redacts"] == sorted([created.event, removed.event])
    assert db.entity(anonymous) == {}
    assert db.doctor()["ok"] is True


def test_event_payload_bound_is_atomic_and_doctor_verifies_stored_hash() -> None:
    db = fgraph.connect(":memory:")
    db.declare("large/values", type="text", many=True)
    before = db.stats()
    values = [(str(index) + "x" * MAX_VALUE_BYTES)[:MAX_VALUE_BYTES] for index in range(9)]
    with pytest.raises(fgraph.TooLarge, match="canonical event"):
        db.transact({"id": "oversized", "large/values": values})
    assert db.stats() == before

    report = db.transact({"id": "small", "small/value": 1})
    db._connection.execute(  # noqa: SLF001
        "UPDATE fgraph_events SET event_hash=zeroblob(32) WHERE tx=?", (report.tx,)
    )
    checked = db.doctor()
    assert checked["ok"] is False
    assert any("stored canonical payload" in problem for problem in checked["problems"])
    db.close()


def test_doctor_reports_malformed_event_identity_without_crashing() -> None:
    db = fgraph.connect(":memory:")
    report = db.transact({"id": "note", "note/text": "hello"})
    db._connection.execute("PRAGMA ignore_check_constraints=ON")  # noqa: SLF001
    db._connection.execute(  # noqa: SLF001
        "UPDATE fgraph_ids SET gid=? WHERE id=?",
        (b"short", report.tx),
    )
    checked = db.doctor()
    assert checked["ok"] is False
    assert any("invalid event identity" in problem for problem in checked["problems"])
    db.close()


def test_connection_is_durable_defensive_and_dedicated(tmp_path: Path) -> None:
    path = tmp_path / "graph.db"
    db = fgraph.connect(path)
    assert db._connection.execute("PRAGMA synchronous").fetchone()[0] == 2  # noqa: SLF001
    assert db._connection.execute("PRAGMA trusted_schema").fetchone()[0] == 0  # noqa: SLF001
    db.close()

    readonly = fgraph.connect(path, read_only=True)
    assert readonly._connection.execute("PRAGMA query_only").fetchone()[0] == 1  # noqa: SLF001
    readonly.close()

    foreign = tmp_path / "foreign.db"
    connection = sqlite3.connect(foreign)
    connection.execute("CREATE TABLE application_data(value TEXT)")
    connection.close()
    with pytest.raises(FormatError):
        fgraph.connect(foreign)


def test_vector_inference_and_raw_schema_share_final_validation() -> None:
    db = fgraph.connect(
        ":memory:",
        event_factory=event_factory(
            "00000000-0000-4000-8000-000000000065",
            "00000000-0000-4000-8000-000000000066",
        ),
    )
    db.transact({"id": "note", "memory/embedding": {"vector": [0.1, 0.2]}})
    schema = db.entity("memory/embedding")
    assert schema["fgraph/type"] == "vector"
    assert schema["fgraph/dims"] == 2

    with pytest.raises(SchemaError):
        db.transact({"id": "memory/embedding", "fgraph/type": "text"})
    assert db.doctor()["ok"] is True


def test_point_update_does_not_materialize_the_complete_live_store() -> None:
    db = fgraph.connect(
        ":memory:",
        event_factory=event_factory(
            "00000000-0000-4000-8000-000000000065",
            "00000000-0000-4000-8000-000000000066",
        ),
    )
    db.transact([{"id": f"entity-{index}", "item/value": index} for index in range(20)])
    statements: list[str] = []
    db._connection.set_trace_callback(statements.append)  # noqa: SLF001
    db.transact({"id": "entity-0", "item/value": 100})
    db._connection.set_trace_callback(None)  # noqa: SLF001

    normalized = [" ".join(statement.lower().split()) for statement in statements]
    assert "select * from fgraph_facts where rx is null" not in normalized
    assert not any("delete from fgraph_blobs where hash not in" in statement for statement in normalized)


def test_event_factory_rejects_noncanonical_or_reused_ids() -> None:
    db = fgraph.connect(
        ":memory:",
        event_factory=event_factory(
            "not-a-uuid",
        ),
    )
    with pytest.raises(fgraph.TypeError):
        db.transact({"id": "invalid", "item/value": 1})

    repeated = str(uuid.UUID("00000000-0000-4000-8000-000000000065"))
    db = fgraph.connect(":memory:", event_factory=event_factory(repeated, repeated))
    db.transact({"id": "first", "item/value": 1})
    with pytest.raises(Conflict):
        db.transact({"id": "second", "item/value": 2})


def test_seeded_event_and_anonymous_ids_match_cross_runtime_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    seed = "portable-seed"
    monkeypatch.setenv("FGRAPH_EVENT_SEED", seed)
    db = fgraph.connect(":memory:", clock=1_767_225_600_000_000)

    report = db.transact([{"id": "named", "demo/value": 1}, {"demo/value": 2}])
    assert report.tx == 68
    raw = bytearray(hashlib.sha256(f"fgraph-event/1\0{seed}\0{report.tx}".encode()).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    event = uuid.UUID(bytes=bytes(raw))
    assert report.event == str(event)

    created = db._connection.execute(  # noqa: SLF001
        "SELECT id,name,gid FROM fgraph_ids WHERE created_tx=? AND id<>? ORDER BY id", (report.tx, report.tx)
    ).fetchall()
    assert [row["name"] for row in created] == ["named", "demo/value", None]
    expected = bytearray(hashlib.sha1(event.bytes + (2).to_bytes(8, "big"), usedforsecurity=False).digest()[:16])
    expected[6] = (expected[6] & 0x0F) | 0x50
    expected[8] = (expected[8] & 0x3F) | 0x80
    assert bytes(created[2]["gid"]) == bytes(expected)


def test_exact_layout_rejects_extra_objects_and_modified_ddl(tmp_path: Path) -> None:
    extra_path = tmp_path / "extra.db"
    db = fgraph.connect(extra_path)
    db._connection.execute("CREATE TABLE application_data(value TEXT)")  # noqa: SLF001
    report = db.doctor()
    assert report["ok"] is False
    assert any("non-format table 'application_data'" in problem for problem in report["problems"])
    db.close()
    with pytest.raises(FormatError, match="dedicated fgraph file"):
        fgraph.connect(extra_path)

    drift_path = tmp_path / "drift.db"
    fgraph.connect(drift_path).close()
    connection = sqlite3.connect(drift_path)
    connection.execute("DROP INDEX fgraph_avet")
    connection.execute("CREATE INDEX fgraph_avet ON fgraph_facts (a, e, v) WHERE rx IS NULL")
    connection.close()
    with pytest.raises(FormatError, match="modified index 'fgraph_avet'"):
        fgraph.connect(drift_path)


def test_receipt_retry_basis_and_follow_portable_events() -> None:
    db = fgraph.connect(
        ":memory:",
        clock=1_767_225_600_000_000,
        event_factory=event_factory(
            "00000000-0000-4000-8000-000000000065",
            "00000000-0000-4000-8000-000000000066",
        ),
    )
    first = db.transact(
        {"id": "counter", "counter/value": 1},
        tx={"audit/tag": "seed"},
        operation_id="request:one",
        if_basis_tx=64,
    )
    second = db.transact({"id": "other", "counter/value": 2}, operation_id="request:two")
    retry = db.transact(
        {"id": "counter", "counter/value": 1},
        tx={"audit/tag": "seed"},
        operation_id="request:one",
        if_basis_tx=64,
    )
    assert retry.status == "already_applied"
    assert retry.basis_tx == 64

    assert first.tx is not None
    assert second.tx is not None
    receipt = db.receipt(first.tx)
    assert receipt["read_basis_tx"] == second.tx
    assert receipt["basis_tx"] == 64
    assert receipt["event"] == first.event
    assert receipt["event_hash"].startswith("sha256:")
    assert receipt["operation_id"] == "request:one"
    assert receipt["request_hash"].startswith("sha256:")
    assert receipt["facts"][0]["a"] == "audit/tag"
    with pytest.raises(NotFound, match="after this view's basis"):
        db.at(first.tx).receipt(second.tx)

    stream = db.follow(64, interval=0.001)
    first_event = next(stream)
    second_event = next(stream)
    stream.close()
    assert first_event["fgraph"] == "event/1"
    assert first_event["event"] == first.event
    assert "tx" not in first_event
    assert second_event["event"] == second.event
    assert db.event_records(64) == [first_event, second_event]


def test_tail_apply_is_portable_atomic_and_idempotent() -> None:
    source = fgraph.connect(
        ":memory:",
        clock=1_767_225_600_000_000,
        event_factory=event_factory(
            "00000000-0000-4000-8000-000000000065",
            "00000000-0000-4000-8000-000000000066",
        ),
    )
    first = source.transact(
        [
            {"id": "app/link", "fgraph/type": "ref"},
            {"id": "named", "app/link": {"app/value": 1}},
        ],
        source="source-graph",
    )
    source.transact({"id": "named", "app/status": "ready"})
    records = source.event_records()
    payload = "".join(f"{canonical_json(record)}\n" for record in records)

    target = fgraph.connect(":memory:", clock=1_767_225_700_000_000)
    reports = target.apply(StringIO(payload))
    assert [report.status for report in reports] == ["applied", "applied"]
    assert [report.event for report in reports] == [record["event"] for record in records]
    assert target.event_records() == records
    assert target.entity("named")["app/status"] == "ready"
    linked = target.entity("named")["app/link"]
    assert isinstance(linked, dict)
    assert target.entity(linked["ref"])["app/value"] == 1
    assert reports[0].tx is not None
    receipt = target.receipt(reports[0].tx)
    assert receipt["at"] > records[0]["at"]
    assert receipt["imported_at"] == records[0]["at"]
    assert first.event == records[0]["event"]

    before = target.stats()
    duplicate = target.apply(payload)
    assert [report.to_dict() for report in duplicate] == [
        {
            "status": "already_applied",
            "event": report.event,
            "basis_tx": report.basis_tx,
            "tx": report.tx,
            "at": report.at,
            "ids": {},
            "asserted": [],
            "retracted": [],
        }
        for report in reports
    ]
    assert target.stats() == before


def test_apply_rejects_an_oversized_later_line_before_json_parsing_and_rolls_back() -> None:
    source = fgraph.connect(":memory:")
    source.transact({"id": "portable/first", "portable/value": 1})
    valid = canonical_json(source.event_records()[0])

    target = fgraph.connect(":memory:")
    before = target.stats()
    with pytest.raises(fgraph.TooLarge, match="event line 2"):
        target.apply(f"{valid}\n{'x' * (MAX_EVENT_BYTES + 1)}")
    assert target.stats() == before
    with pytest.raises(fgraph.NotFound):
        target.entity("portable/first")


def test_doctor_rejects_logical_schema_shape_and_event_drift(tmp_path: Path) -> None:
    path = tmp_path / "logical-drift.db"
    db = fgraph.connect(path)
    db.declare_shape(
        "shape/person",
        required=["person/name"],
        allowed=["person/name", "person/count"],
        closed=True,
    )
    report = db.transact(
        {
            "id": "ada",
            "fgraph/shape": {"ref": "shape/person"},
            "person/name": "Ada",
            "person/count": 1,
        }
    )
    assert report.tx is not None
    db._refresh_cache()  # noqa: SLF001
    entity = db._names["ada"]  # noqa: SLF001
    count_attribute = db._names["person/count"]  # noqa: SLF001
    name_attribute = db._names["person/name"]  # noqa: SLF001
    db._connection.execute(  # noqa: SLF001
        "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,?,?,?,?,NULL)",
        (entity, count_attribute, 2, 2, report.tx),
    )
    name_fact = db._connection.execute(  # noqa: SLF001
        "SELECT id FROM fgraph_facts WHERE e=? AND a=? AND rx IS NULL",
        (entity, name_attribute),
    ).fetchone()
    db._connection.execute("DELETE FROM fgraph_fts WHERE rowid=?", (name_fact["id"],))  # noqa: SLF001
    db._connection.execute("DELETE FROM fgraph_facts WHERE id=?", (name_fact["id"],))  # noqa: SLF001

    checked = db.doctor()
    assert checked["ok"] is False
    assert checked["schema_problems"] >= 1
    assert checked["shape_violations"] >= 1
    assert checked["unverifiable_event_hashes"] == 0
    with pytest.raises(FormatError, match="non-rebuildable"):
        db.doctor(repair=True)


def test_restore_checks_logical_invariants_before_publication(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    snapshot = tmp_path / "snapshot.db"
    destination = tmp_path / "destination.db"
    with fgraph.connect(source) as db:
        report = db.transact({"id": "counter", "counter/value": 1})
        assert report.tx is not None
        db.backup(snapshot)

    connection = sqlite3.connect(snapshot)
    entity = connection.execute("SELECT id FROM fgraph_ids WHERE name='counter'").fetchone()[0]
    attribute = connection.execute("SELECT id FROM fgraph_ids WHERE name='counter/value'").fetchone()[0]
    transaction = connection.execute("SELECT max(tx) FROM fgraph_events").fetchone()[0]
    connection.execute(
        "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,?,?,?,?,NULL)",
        (entity, attribute, 2, 2, transaction),
    )
    connection.commit()
    connection.close()

    with pytest.raises(FormatError, match="violates format invariants"):
        fgraph.restore_backup(snapshot, destination)
    assert not destination.exists()


def test_portable_snapshot_restore_is_exact_checked_and_atomic() -> None:
    source = fgraph.connect(
        ":memory:",
        clock=1_767_225_600_000_000,
        event_factory=event_factory(
            "00000000-0000-4000-8000-000000000065",
            "00000000-0000-4000-8000-000000000066",
            "00000000-0000-4000-8000-000000000067",
        ),
    )
    source.declare("item/link", type="ref", many=True)
    first = source.transact(
        [
            {"id": "named", "item/value": "before", "item/link": {"item/value": "anonymous"}},
        ],
        operation_id="snapshot:create",
    )
    source.transact({"id": "named", "item/value": "after"})
    stream = source.snapshot()
    assert isinstance(stream, str)
    records = [json.loads(line) for line in stream.splitlines()]
    assert records[0]["fgraph"] == "snapshot/1"
    assert records[-1]["fgraph"] == "end"

    target = fgraph.connect(":memory:", clock=1_800_000_000_000_000)
    target.restore(StringIO(stream))
    assert target.snapshot() == stream
    assert target.entity("named")["item/value"] == "after"
    assert [fact["v"] for fact in target.history("named", "item/value")] == ["before", "after"]
    assert first.tx is not None
    restored_event = next(
        int(row["tx"])
        for row in target._connection.execute(  # noqa: SLF001
            "SELECT tx,operation_id FROM fgraph_events WHERE operation_id IS NOT NULL"
        )
        if row["operation_id"] == "snapshot:create"
    )
    assert target.receipt(restored_event)["operation_id"] == "snapshot:create"
    assert target.doctor()["ok"] is True

    tampered = stream.replace('"before"', '"tampered"', 1)
    pristine = fgraph.connect(":memory:")
    before = pristine.stats()
    with pytest.raises(Conflict, match="digest"):
        pristine.restore(tampered)
    assert pristine.stats() == before
    target.close()
    pristine.close()
    source.close()


def test_nohistory_vector_replacement_remains_replayable_and_snapshot_exact() -> None:
    source = fgraph.connect(":memory:", clock=1_767_225_600_000_000)
    source.declare("note/embedding", type="vector", dims=2, nohistory=True)
    first = source.transact({"id": "note", "note/embedding": {"vector": [1, 0]}})
    second = source.transact({"id": "note", "note/embedding": {"vector": [0, 1]}})
    assert first.event is not None
    assert second.event is not None

    events = source.event_records(64)
    first_event = next(record for record in events if record["event"] == first.event)
    assert first_event["asserted"] == [["note", "note/embedding", {"vector": [1.0, 0.0]}, "vector"]]
    assert source.doctor()["unverifiable_event_hashes"] == 0

    replica = fgraph.connect(":memory:", clock=1_800_000_000_000_000)
    reports = replica.apply("\n".join(canonical_json(event) for event in events))
    assert all(report.status == "applied" for report in reports)
    assert replica.entity("note")["note/embedding"] == {"vector": [0.0, 1.0]}

    snapshot = source.snapshot()
    assert isinstance(snapshot, str)
    receipts = [json.loads(line)["receipt"] for line in snapshot.splitlines() if '"receipt"' in line]
    assert all(receipt["event_data"] is not None for receipt in receipts)
    restored = fgraph.connect(":memory:", clock=1_900_000_000_000_000)
    restored.restore(snapshot)
    restored_first = next(record for record in restored.event_records(64) if record["event"] == first.event)
    assert restored_first == first_event
    assert restored.entity("note")["note/embedding"] == {"vector": [0.0, 1.0]}
    assert restored.doctor()["ok"] is True

    restored.close()
    replica.close()
    source.close()
