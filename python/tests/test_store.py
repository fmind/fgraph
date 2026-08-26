"""Format, transaction, schema, and lifecycle unit tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest
from conftest import Clock

import fgraph
from fgraph import store


def test_genesis_format_and_pragmas(db: fgraph.Db) -> None:
    assert db.stats() == {
        "application_id": 0x66677261,
        "format_version": 2,
        "entities": 18,
        "attributes": 18,
        "facts": 39,
        "live_facts": 39,
        "transactions": 1,
        "blobs": 0,
        "size": 0,
    }
    facts = db._connection.execute("SELECT e,a,v,t,tx,rx FROM fgraph_facts ORDER BY id").fetchall()  # noqa: SLF001
    assert tuple(facts[0]) == (64, 1, 1_767_225_600_000_000, 5, 64, None)
    assert [row[2] for row in facts[1:19]] == list(store.SYSTEM_TYPES)
    assert len(facts) == 39
    assert db._connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0  # noqa: SLF001
    assert db._connection.execute("PRAGMA busy_timeout").fetchone()[0] == 5000  # noqa: SLF001


def test_init_on_existing_database_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "existing.db"
    raw = sqlite3.connect(path)
    raw.execute("CREATE TABLE app_data(value TEXT)")
    raw.commit()
    raw.close()
    with pytest.raises(fgraph.FormatError, match="dedicated"):
        fgraph.connect(path, clock=Clock())

    path = tmp_path / "dedicated.db"
    with fgraph.connect(path, clock=Clock()) as graph:
        graph.transact({"id": "ada", "person/name": "Ada"})
    with fgraph.connect(path, read_only=True) as reopened:
        assert reopened.entity("ada") == {"person/name": "Ada"}
        assert reopened._connection.execute("SELECT name FROM sqlite_master WHERE name='app_data'").fetchone() is None  # noqa: SLF001


def test_live_connections_refresh_names_before_allocation(tmp_path: Path) -> None:
    path = tmp_path / "shared.db"
    first = fgraph.connect(path, clock=Clock())
    second = fgraph.connect(path, clock=Clock())
    try:
        one = first.transact({"id": "one", "one/value": 1})
        two = second.transact({"id": "two", "two/value": 2})
        three = first.transact({"id": "three", "three/value": 3})
        assert len({one.ids["one"], two.ids["two"], three.ids["three"]}) == 3
        assert second.entity("three") == {"three/value": 3}
        assert first.entity("two") == {"two/value": 2}
    finally:
        first.close()
        second.close()


def test_live_read_only_connection_refreshes_names_for_portable_events(tmp_path: Path) -> None:
    path = tmp_path / "follower.db"
    writer = fgraph.connect(path, clock=Clock())
    follower = fgraph.connect(path, read_only=True)
    try:
        writer.transact({"id": "new-name", "new/value": 1})
        assert follower.entity("new-name") == {"new/value": 1}
        events = follower.event_records()
        assert events[-1]["asserted"] == [["new-name", "new/value", 1, "int"]]
    finally:
        writer.close()
        follower.close()


def test_writer_lock_contention_is_typed_conflict(tmp_path: Path) -> None:
    path = tmp_path / "locked.db"
    first = fgraph.connect(path, clock=Clock())
    second = fgraph.connect(path, clock=Clock())
    try:
        second._connection.execute("PRAGMA busy_timeout=1")  # noqa: SLF001
        first._connection.execute("BEGIN IMMEDIATE")  # noqa: SLF001
        with pytest.raises(fgraph.Conflict, match="retry"):
            second.transact({"id": "blocked", "blocked/value": 1})
        first._connection.execute("ROLLBACK")  # noqa: SLF001
        assert second.transact({"id": "available", "available/value": 1}).tx is not None
    finally:
        if first._connection.in_transaction:  # noqa: SLF001
            first._connection.execute("ROLLBACK")  # noqa: SLF001
        first.close()
        second.close()


def test_reject_foreign_partial_and_corrupt_files(tmp_path: Path) -> None:
    foreign = tmp_path / "foreign.db"
    connection = sqlite3.connect(foreign)
    connection.execute("PRAGMA application_id=123")
    connection.commit()
    connection.close()
    with pytest.raises(fgraph.FormatError):
        fgraph.connect(foreign)

    partial = tmp_path / "partial.db"
    connection = sqlite3.connect(partial)
    connection.execute("CREATE TABLE fgraph_ids(id INTEGER)")
    connection.commit()
    connection.close()
    with pytest.raises(fgraph.FormatError):
        fgraph.connect(partial)

    orphaned_fts = tmp_path / "orphaned-fts.db"
    connection = sqlite3.connect(orphaned_fts)
    connection.execute("CREATE TABLE fgraph_fts_data(id INTEGER)")
    connection.commit()
    connection.close()
    with pytest.raises(fgraph.FormatError):
        fgraph.connect(orphaned_fts)

    valid = tmp_path / "valid.db"
    with fgraph.connect(valid, clock=Clock()):
        pass
    connection = sqlite3.connect(valid)
    connection.execute("DROP VIEW fgraph_now")
    connection.commit()
    connection.close()
    with pytest.raises(fgraph.FormatError):
        fgraph.connect(valid)


@pytest.mark.parametrize(("application_id", "user_version"), [(0x66677261, 1), (0x66677261, 0), (0, 1)])
def test_marked_file_without_complete_layout_is_corrupt(tmp_path: Path, application_id: int, user_version: int) -> None:
    path = tmp_path / f"claimed-{application_id}-{user_version}.db"
    connection = sqlite3.connect(path)
    connection.execute(f"PRAGMA application_id={application_id}")
    connection.execute(f"PRAGMA user_version={user_version}")
    connection.commit()
    connection.close()

    with pytest.raises(fgraph.FormatError, match="no complete fgraph layout"):
        fgraph.connect(path, clock=Clock())

    connection = sqlite3.connect(path)
    try:
        assert connection.execute("SELECT count(*) FROM sqlite_master WHERE name LIKE 'fgraph_%'").fetchone()[0] == 0
    finally:
        connection.close()


def test_read_only_requires_initialized_file(tmp_path: Path) -> None:
    with pytest.raises(fgraph.NotFound):
        fgraph.connect(tmp_path / "missing.db", read_only=True)
    with pytest.raises(fgraph.ReadOnly):
        fgraph.connect(":memory:", read_only=True)


def test_identity_only_and_empty_anonymous(db: fgraph.Db) -> None:
    assert db.transact({}).tx is None
    assert db.stats()["entities"] == 18
    report = db.transact({"id": "empty"})
    assert report.status == "applied"
    assert report.tx == 66
    assert report.ids == {"empty": 65}
    assert db.entity("empty") == {}
    assert db.transact({"id": "empty"}).ids == {}
    assert db._next_available_id() == 67  # noqa: SLF001


def test_canceled_tempid_is_not_reported_or_allocated(db: fgraph.Db) -> None:
    db.declare("item/value", type="int")
    report = db.transact(
        [
            {"id": {"tmp": "discard"}, "item/value": 1},
            ["retract", {"tmp": "discard"}, "item/value", 1],
        ]
    )
    assert report.tx is None
    assert report.ids == {}
    created = db.transact({"id": "after", "item/value": 2})
    assert created.ids == {"after": 67}
    assert created.tx == 68


def test_canceled_tempid_compacts_later_entities_and_refs(db: fgraph.Db) -> None:
    db.declare("item/value", type="int")
    db.declare("item/ref", ref=True)
    db.declare("audit/subject", ref=True)
    report = db.transact(
        [
            {"id": {"tmp": "discard"}, "item/value": 1},
            ["retract", {"tmp": "discard"}, "item/value", 1],
            {"id": {"tmp": "keep"}, "item/value": 2},
            {"id": "holder", "item/ref": {"ref": {"tmp": "keep"}}},
        ],
        tx={"audit/subject": {"ref": {"tmp": "keep"}}},
    )
    assert report.ids == {"keep": 71, "holder": 72}
    assert "discard" not in report.ids
    assert db.entity("holder")["item/ref"] == {"ref": 71}
    assert db.entity(report.tx)["audit/subject"] == {"ref": 71}


def test_raw_schema_retractions_drive_the_pending_view(db: fgraph.Db) -> None:
    db.transact(
        {
            "id": "clear/scalar",
            "fgraph/doc": "temporary",
            "fgraph/many": True,
            "fgraph/nohistory": True,
            "fgraph/type": "text",
            "fgraph/unique": True,
        }
    )
    db.transact(
        [
            ["retract", "clear/scalar", "fgraph/many", True],
            ["retract", "clear/scalar", "fgraph/unique", True],
            ["retract", "clear/scalar", "fgraph/nohistory", True],
            ["retract", "clear/scalar", "fgraph/type", "text"],
            ["retract", "clear/scalar", "fgraph/doc", "temporary"],
            ["assert", "clear-owner", "clear/scalar", 1],
        ]
    )
    assert db.entity("clear-owner") == {"clear/scalar": 1}

    db.declare("clear/vector", type="vector", dims=2)
    db.transact(
        [
            ["retract", "clear/vector", "fgraph/dims", 2],
            ["retract", "clear/vector", "fgraph/type", "vector"],
            ["assert", "vector-owner", "clear/vector", "untyped"],
        ]
    )
    assert db.entity("vector-owner") == {"clear/vector": "untyped"}

    db.declare("clear/broad", type="int")
    db.transact(
        [
            ["retract", "clear/broad"],
            ["assert", "broad-owner", "clear/broad", "untyped"],
        ]
    )
    assert db.entity("broad-owner") == {"clear/broad": "untyped"}

    db.declare("keep/type", type="text")
    db.transact(
        [
            ["retract", "keep/type", "fgraph/type", "int"],
            ["assert", "typed-owner", "keep/type", "still text"],
        ]
    )
    assert db.entity("typed-owner") == {"keep/type": "still text"}

    db.transact({"id": "odd/value", "other/value": 1})
    db.transact(
        [
            ["retract", "odd/value", "other/value"],
            ["assert", "odd-owner", "odd/value", "works"],
        ]
    )
    assert db.entity("odd-owner") == {"odd/value": "works"}


def test_custom_transaction_facts_share_normal_constraints(db: fgraph.Db) -> None:
    db.declare("unique/key", type="text", unique=True)
    db.transact({"id": "owner", "unique/key": "taken"})
    with pytest.raises(fgraph.Conflict):
        db.transact({}, tx={"unique/key": "taken"})
    with pytest.raises(fgraph.TypeError, match="nested transaction map"):
        db.transact({}, tx={"audit/scalar": {"nested/value": 1}})


def test_failed_transaction_does_not_publish_names(db: fgraph.Db) -> None:
    with pytest.raises(fgraph.Conflict):
        db.transact([["assert", "ada", "person/age", 40], ["assert", "ada", "person/age", 41]])
    assert "ada" not in db._names  # noqa: SLF001
    report = db.transact({"id": "ada", "person/age": 40})
    assert report.ids == {"ada": 65}
    assert report.tx == 67


def test_late_metadata_failure_does_not_consume_clock() -> None:
    clock = Clock()
    with fgraph.connect(":memory:", clock=clock) as graph:
        first = graph.transact({"id": "first", "item/value": 1})
        with pytest.raises(fgraph.TooLarge):
            graph.transact({"id": "failed", "item/value": 2}, by="x" * 1_048_577)
        second = graph.transact({"id": "second", "item/value": 3})
    assert first.at is not None
    assert second.at is not None
    assert second.at - first.at == 1_000_000


def test_system_entities_and_genesis_are_immutable(db: fgraph.Db) -> None:
    receipt = db.transact({"id": "application", "app/value": 1})
    before = db.stats()
    attempts = [
        lambda: db.transact({"id": "fgraph/at", "fgraph/doc": "changed"}),
        lambda: db.transact(["assert", 1, "fgraph/doc", "changed"]),
        lambda: db.retract("fgraph/at"),
        lambda: db.declare("fgraph/at", doc="changed"),
        lambda: db.undo(64),
        lambda: db.transact({"id": receipt.tx, "audit/value": "late"}),
        lambda: db.retract(receipt.tx),
    ]
    for attempt in attempts:
        with pytest.raises(fgraph.Unsupported):
            attempt()
    assert db.stats() == before


def test_mapping_keys_are_lexically_allocated(db: fgraph.Db) -> None:
    report = db.transact({"z/value": 1, "id": "e", "a/value": 2})
    assert report.tx == 68
    assert db.entity("e") == {"a/value": 2, "z/value": 1}
    assert db._names == {  # noqa: SLF001
        **{name: index for index, name in enumerate(store.SYSTEM_NAMES, start=1)},
        "e": 65,
        "a/value": 66,
        "z/value": 67,
    }


def test_supersession_noop_and_retractions(db: fgraph.Db) -> None:
    first = db.transact({"id": "ada", "person/city": "London"})
    assert db.transact({"id": "ada", "person/city": "London"}).tx is None
    second = db.transact({"id": "ada", "person/city": "Lyon"})
    assert second.retracted[0]["rx"] == second.tx
    assert db.at(first.tx).entity("ada") == {"person/city": "London"}
    assert db.retract("ada", "person/city", "Paris").tx is None
    assert db.retract("ada", "person/city").tx is not None
    assert db.entity("ada") == {}
    assert db.retract("unknown").tx is None


def test_many_refs_nested_maps_and_inbound_retract(db: fgraph.Db) -> None:
    db.declare("person/knows", ref=True, many=True)
    db.transact(
        {
            "id": "ada",
            "person/knows": [
                {"ref": "grace"},
                {"ref": "linus"},
            ],
        }
    )
    assert db.entity("ada")["person/knows"] == [{"ref": "grace"}, {"ref": "linus"}]
    db.transact({"id": "grace", "person/name": "Grace"})
    db.transact({"id": "ada", "person/knows": {"id": "margaret", "person/name": "Margaret"}})
    db.retract("grace")
    assert {item["ref"] for item in db.entity("ada")["person/knows"]} == {"linus", "margaret"}


def test_schema_unique_upsert_and_validation(db: fgraph.Db) -> None:
    with pytest.raises(fgraph.SchemaError):
        db.declare("person/email", unique=True)
    db.declare("person/email", type="text", unique=True)
    db.transact({"id": "ada", "person/email": "ada@x"})
    report = db.transact({"person/email": "ada@x", "person/name": "Ada"})
    assert report.tx is not None
    assert db.entity("ada")["person/name"] == "Ada"
    with pytest.raises(fgraph.Conflict):
        db.transact({"id": "grace", "person/email": "ada@x"})
    with pytest.raises(fgraph.TypeError):
        db.transact({"id": "ada", "person/email": 1})
    with pytest.raises(fgraph.SchemaError):
        db.declare("person/email", type="int")
    with pytest.raises(fgraph.SchemaError):
        db.declare("person/email", ref=True, type="text")
    with pytest.raises(fgraph.SchemaError):
        db.declare("person/email")


def test_many_disable_and_unique_duplicates(db: fgraph.Db) -> None:
    db.declare("tag/name", type="text", many=True)
    db.transact({"id": "e", "tag/name": ["a", "b"]})
    with pytest.raises(fgraph.SchemaError):
        db.declare("tag/name", many=False)
    db.transact({"id": "other", "tag/name": ["a"]})
    with pytest.raises(fgraph.SchemaError):
        db.declare("tag/name", unique=True)


def test_blobs_vectors_dims_and_nohistory(db: fgraph.Db) -> None:
    long = "x" * 257
    db.transact({"id": "note", "note/text": long, "note/bytes": b"y" * 257})
    assert db.entity("note")["note/text"] == long
    assert db.stats()["blobs"] == 2
    db.declare("note/vector", type="vector")
    first = db.transact({"id": "note", "note/vector": {"vector": [1, 2]}})
    dims = [fact for fact in first.asserted if fact["a"] == "fgraph/dims"]
    assert dims == [{**dims[0], "v": 2}]
    second = db.transact({"id": "note", "note/vector": {"vector": [2, 3]}})
    assert second.retracted
    assert len(db.history("note", "note/vector")) == 1
    with pytest.raises(fgraph.TypeError):
        db.transact({"id": "note", "note/vector": {"vector": [1, 2, 3]}})
    db.retract("note")
    # Historical text/bytes intentionally retain their blobs; the vector uses
    # implied nohistory and its superseded/retracted blobs are collected.
    assert db.stats()["blobs"] == 2


def test_metadata_tx_facts_context_and_close(db: fgraph.Db) -> None:
    report = db.transact(
        {"id": "ada", "person/name": "Ada"},
        source="book",
        by="agent",
        meta={"page": 1},
        tx={"audit/quality": "high"},
    )
    why = db.why("ada")[0]["provenance"]
    assert why["fgraph/by"] == "agent"
    assert why["fgraph/source"] == "book"
    assert why["fgraph/meta"] == {"json": {"page": 1}}
    assert why["audit/quality"] == "high"
    assert report.at == 1_767_225_601_000_000
    null_meta = db.transact({}, meta=None)
    assert db.entity(null_meta.tx)["fgraph/meta"] == {"json": None}
    for attribute in ("fgraph/at", "fgraph/by", "fgraph/source", "fgraph/meta"):
        with pytest.raises(fgraph.SchemaError):
            db.transact({}, tx={attribute: "duplicate"})
    assert db.__enter__() is db
    view = db.at(report.tx)
    view.close()
    assert db.entity("ada")


def test_backup_and_doctor(tmp_path: Path, db: fgraph.Db) -> None:
    db.transact({"id": "ada", "person/name": "Ada"})
    with pytest.raises(fgraph.FormatError, match="backup"):
        db.backup(tmp_path / "missing" / "backup.db")
    backup = tmp_path / "backup.db"
    db.backup(backup)
    with fgraph.connect(backup, read_only=True) as restored:
        assert restored.entity("ada")["person/name"] == "Ada"
    with pytest.raises(fgraph.Conflict):
        db.backup(backup)
    empty = tmp_path / "empty.db"
    empty.touch()
    with pytest.raises(fgraph.Conflict):
        db.backup(empty)
    report = db.doctor()
    assert report["ok"] is True
    assert report["repaired"] is False
    assert report["fts_rows_rebuilt"] == 0
    repaired = db.doctor(repair=True)
    assert repaired["ok"] is True
    assert repaired["fts_rows_rebuilt"] >= 1

    correct_next = db._next_available_id()  # noqa: SLF001
    db._connection.execute("UPDATE fgraph_meta SET value=65 WHERE key='next_id'")  # noqa: SLF001
    invalid_allocator = db.doctor()
    assert invalid_allocator["ok"] is False
    assert any("next_id" in problem for problem in invalid_allocator["problems"])
    db._connection.execute(  # noqa: SLF001
        "UPDATE fgraph_meta SET value=? WHERE key='next_id'", (correct_next,)
    )
    db._connection.execute("UPDATE fgraph_meta SET value=value+1 WHERE key='created_at'")  # noqa: SLF001
    invalid_genesis = db.doctor()
    assert invalid_genesis["ok"] is False
    assert any("created_at" in problem for problem in invalid_genesis["problems"])

    source = tmp_path / "source.db"
    with fgraph.connect(source, clock=Clock()) as file_graph, pytest.raises(fgraph.Conflict):
        file_graph.backup(source)


def test_schema_manifest_round_trip_and_atomic_validation() -> None:
    with fgraph.connect(":memory:", clock=Clock()) as source:
        source.declare("person/name", type="text", doc="Display name")
        source.declare_shape("person/shape", required=["person/name"], closed=True)
        manifest = source.schema_manifest()
        assert source.check_schema_manifest(manifest)["valid"] is True

    with fgraph.connect(":memory:", clock=Clock()) as target:
        report = target.apply_schema_manifest(manifest, operation_id="schema:v1")
        assert report.status == "applied"
        assert target.schema_manifest() == manifest
        before = target.snapshot()
        invalid = {**manifest, "attributes": [{"name": "person/name", "declared": {"dims": 0}}]}
        with pytest.raises(fgraph.SchemaError, match="dims"):
            target.apply_schema_manifest(invalid)
        assert target.snapshot() == before


def test_schema_manifest_replacement_holds_writer_lock_during_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "schema-race.db"
    with (
        fgraph.connect(path, clock=Clock()) as target,
        fgraph.connect(path, clock=Clock()) as concurrent,
    ):
        target.declare("stale/attribute", type="text")
        concurrent._connection.execute("PRAGMA busy_timeout=1")  # noqa: SLF001
        schema_manifest = target.schema_manifest
        concurrent_writer_blocked = False

        def intercept_schema_discovery() -> dict[str, Any]:
            nonlocal concurrent_writer_blocked
            snapshot = schema_manifest()
            try:
                concurrent.declare("race/new", type="text")
            except fgraph.Conflict:
                concurrent_writer_blocked = True
            return snapshot

        monkeypatch.setattr(target, "schema_manifest", intercept_schema_discovery)
        report = target.apply_schema_manifest({"fgraph": "schema/1", "attributes": [], "shapes": []})

        assert report.status == "applied"
        assert concurrent_writer_blocked is True
        assert schema_manifest()["attributes"] == []


@pytest.mark.parametrize(
    ("manifest", "message"),
    [
        (None, "fgraph='schema/1'"),
        ({"fgraph": "schema/2"}, "fgraph='schema/1'"),
        ({"fgraph": "schema/1", "extra": True}, "unknown fields"),
        ({"fgraph": "schema/1", "attributes": {}, "shapes": []}, "must be arrays"),
        ({"fgraph": "schema/1", "attributes": [], "shapes": {}}, "must be arrays"),
        ({"fgraph": "schema/1", "attributes": ["bad"]}, "attributes need exactly"),
        (
            {"fgraph": "schema/1", "attributes": [{"name": 1, "declared": {}}]},
            "name must be text",
        ),
        (
            {"fgraph": "schema/1", "attributes": [{"name": "item/id", "declared": []}]},
            "declared must be an object",
        ),
        (
            {
                "fgraph": "schema/1",
                "attributes": [
                    {"name": "item/id", "declared": {"type": "text"}},
                    {"name": "item/id", "declared": {"type": "text"}},
                ],
            },
            "repeats attribute",
        ),
        (
            {"fgraph": "schema/1", "attributes": [{"name": "item/id", "declared": {"other": 1}}]},
            "unknown declaration field",
        ),
        (
            {"fgraph": "schema/1", "attributes": [{"name": "item/id", "declared": {"many": 1}}]},
            "must be boolean",
        ),
        (
            {"fgraph": "schema/1", "attributes": [{"name": "item/id", "declared": {"dims": True}}]},
            "positive integer",
        ),
        (
            {"fgraph": "schema/1", "attributes": [{"name": "item/id", "declared": {"doc": 1}}]},
            "must be text",
        ),
        (
            {
                "fgraph": "schema/1",
                "attributes": [{"name": "item/id", "declared": {"vector_model": " "}}],
            },
            "non-blank",
        ),
        ({"fgraph": "schema/1", "shapes": ["bad"]}, "shapes need exactly"),
        (
            {
                "fgraph": "schema/1",
                "shapes": [{"name": 1, "required": [], "allowed": [], "closed": False}],
            },
            "name must be text",
        ),
        (
            {
                "fgraph": "schema/1",
                "shapes": [
                    {"name": "shape/item", "required": [], "allowed": [], "closed": False},
                    {"name": "shape/item", "required": [], "allowed": [], "closed": False},
                ],
            },
            "repeats shape",
        ),
        (
            {
                "fgraph": "schema/1",
                "shapes": [{"name": "shape/item", "required": "item/id", "allowed": [], "closed": False}],
            },
            "invalid types",
        ),
        (
            {
                "fgraph": "schema/1",
                "shapes": [{"name": "shape/item", "required": [], "allowed": [1], "closed": False}],
            },
            "invalid types",
        ),
    ],
)
def test_schema_manifest_rejects_malformed_control_plane(manifest: Any, message: str) -> None:
    with fgraph.connect(":memory:") as db, pytest.raises(fgraph.SchemaError, match=message):
        db.check_schema_manifest(manifest)


def test_schema_manifest_normalizes_order_and_reports_drift() -> None:
    manifest = {
        "fgraph": "schema/1",
        "attributes": [
            {"name": "item/tags", "declared": {"many": True, "type": "text"}},
            {"name": "item/id", "declared": {}},
        ],
        "shapes": [
            {
                "name": "shape/item",
                "required": ["item/id", "item/id"],
                "allowed": ["item/tags"],
                "closed": True,
            }
        ],
    }
    with fgraph.connect(":memory:") as db:
        check = db.check_schema_manifest(manifest)
        assert check["valid"] is False
        assert check["changes"] == [
            {"kind": "attribute", "name": "item/tags", "before": None, "after": {"many": True, "type": "text"}},
            {
                "kind": "shape",
                "name": "shape/item",
                "before": None,
                "after": {
                    "name": "shape/item",
                    "required": ["item/id"],
                    "allowed": ["item/id", "item/tags"],
                    "closed": True,
                },
            },
        ]


def test_large_transaction_loads_touched_entities_in_bounded_pages() -> None:
    with fgraph.connect(":memory:", clock=Clock()) as db:
        statements: list[str] = []
        db._connection.set_trace_callback(statements.append)  # noqa: SLF001
        try:
            db.transact([{"id": f"bulk/{index}", "bulk/value": index} for index in range(500)])
        finally:
            db._connection.set_trace_callback(None)  # noqa: SLF001

        pair_loads = [
            statement
            for statement in statements
            if statement.startswith("SELECT * FROM fgraph_facts WHERE e=") and " AND a=" in statement
        ]
        batched_loads = [
            statement
            for statement in statements
            if statement.startswith("SELECT * FROM fgraph_facts WHERE rx IS NULL AND e IN (")
        ]
        assert pair_loads == []
        assert len(batched_loads) == 2
