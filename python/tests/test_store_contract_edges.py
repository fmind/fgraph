"""High-risk persistence, identity, pagination, and portable-event regressions."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import math
import uuid
from pathlib import Path
from typing import Any

import pytest

import fgraph
from fgraph.store import MAX_EVENT_BYTES, Db
from fgraph.values import (
    BOOL,
    BYTES,
    BYTES_REF,
    FLOAT,
    INSTANT,
    INT,
    INT64_MAX,
    INT64_MIN,
    JSON,
    REF,
    TEXT,
    TEXT_REF,
    VECTOR,
)


def test_restore_backup_rejects_unsafe_paths(tmp_path: Path) -> None:
    assert not hasattr(fgraph, "restore")
    assert hasattr(fgraph, "restore_backup")
    snapshot = tmp_path / "snapshot.db"
    with fgraph.connect(snapshot):
        pass

    with pytest.raises(fgraph.Conflict, match="same file"):
        fgraph.restore_backup(snapshot, snapshot)
    with pytest.raises(fgraph.NotFound, match="was not found"):
        fgraph.restore_backup(tmp_path / "missing.db", tmp_path / "restored.db")

    occupied = tmp_path / "occupied.db"
    occupied.write_bytes(b"occupied")
    with pytest.raises(fgraph.Conflict, match="not empty"):
        fgraph.restore_backup(snapshot, occupied)
    with pytest.raises(fgraph.FormatError, match="parent"):
        fgraph.restore_backup(snapshot, tmp_path / "missing" / "restored.db")


def test_connection_lifecycle_and_identity_boundaries(tmp_path: Path, db: fgraph.Db) -> None:
    with pytest.raises(fgraph.ReadOnly, match="read-only :memory:"):
        fgraph.connect(":memory:", read_only=True)
    with pytest.raises(fgraph.NotFound, match="does not exist"):
        fgraph.connect(tmp_path / "absent.db", read_only=True)

    report = db.transact({"edge/value": 1})
    identity = next(fact["e"] for fact in report.asserted if fact["a"] == "edge/value")
    assert isinstance(identity, int)
    db.stats()
    stable_identity = db._identity_selector(identity)  # noqa: SLF001
    assert isinstance(stable_identity, dict)
    assert db.entity(stable_identity)["edge/value"] == 1
    assert db._resolve_read({"eid": str(uuid.uuid4())}, missing_ok=True) is None  # noqa: SLF001
    with pytest.raises(fgraph.TypeError, match="stable entity id"):
        db.entity({"eid": "not-a-uuid"})
    with pytest.raises(fgraph.FormatError, match="stable registry selector"):
        db._identity_selector(INT64_MAX)  # noqa: SLF001

    with pytest.raises(fgraph.TypeError, match="valid UTF-8"):
        db.transact({"id": "\ud800", "edge/value": 1})
    with pytest.raises(fgraph.SchemaError, match="reserved"):
        db.transact({"id": "fgraph/private", "edge/value": 1})
    with pytest.raises(fgraph.TypeError, match="invalid entity reference"):
        db.entity(True)
    for selector in [INT64_MIN - 1, INT64_MAX + 1]:
        with pytest.raises(fgraph.TypeError, match="signed 64-bit"):
            db.entity(selector)

    db.close()
    with pytest.raises(fgraph.FormatError, match="closed"):
        db.stats()
    db.close()


def test_legacy_export_api_is_not_public(db: fgraph.Db) -> None:
    assert not hasattr(db, "export")
    assert not hasattr(db, "import_")


@pytest.mark.parametrize(
    ("tag", "stored", "message"),
    [
        (99, 1, "unknown physical tag"),
        (BOOL, 2, "bool fact"),
        (BOOL, "1", "bool fact"),
        (REF, 0, "ref fact"),
        (REF, "1", "ref fact"),
        (INT, 1.0, "int fact"),
        (FLOAT, 1, "float fact"),
        (FLOAT, math.inf, "float fact"),
        (TEXT, b"text", "text fact"),
        (TEXT, "x" * 257, "inline text"),
        (TEXT, "\ud800", "valid UTF-8"),
        (INSTANT, True, "instant fact"),
        (INSTANT, 10**30, "instant fact"),
        (BYTES, "bytes", "inline bytes"),
        (BYTES, b"x" * 257, "inline bytes"),
        (TEXT_REF, b"x" * 31, "32-byte hash"),
        (TEXT_REF, b"x" * 32, "missing blob"),
        (JSON, b"{}", "JSON fact"),
        (JSON, '{"value": 1}', "canonical JSON"),
    ],
)
def test_physical_value_decoder_fails_closed(
    db: fgraph.Db,
    tag: int,
    stored: Any,
    message: str,
) -> None:
    with pytest.raises(fgraph.FormatError, match=message):
        db._logical(tag, stored)  # noqa: SLF001


def test_indirect_value_decoder_rejects_wrong_blob_domains(db: fgraph.Db) -> None:
    cases = [
        (TEXT_REF, "short text"),
        (BYTES_REF, b"short bytes"),
        (VECTOR, b"bad"),
        (TEXT_REF, b"wrong storage"),
        (BYTES_REF, "wrong storage"),
    ]
    for index, (tag, data) in enumerate(cases):
        digest = bytes([index + 1]) * 32
        db._connection.execute(  # noqa: SLF001
            "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)",
            (digest, data),
        )
        with pytest.raises(fgraph.FormatError):
            db._logical(tag, digest)  # noqa: SLF001


@pytest.mark.parametrize("operation_id", [1, "", "bad\nkey", "x" * 513])
def test_transaction_rejects_invalid_operation_ids(db: fgraph.Db, operation_id: Any) -> None:
    with pytest.raises(fgraph.TypeError, match="operation_id"):
        db.transact([], operation_id=operation_id)


@pytest.mark.parametrize("basis", [True, "64", 63])
def test_transaction_rejects_invalid_basis_values(db: fgraph.Db, basis: Any) -> None:
    with pytest.raises(fgraph.TypeError, match="if_basis_tx"):
        db.transact([], if_basis_tx=basis)


def test_transaction_rejects_metadata_and_internal_event_contract_edges(db: fgraph.Db) -> None:
    invalid_text: Any = 1
    with pytest.raises(fgraph.TypeError, match="author"):
        db.transact([], by=invalid_text)
    with pytest.raises(fgraph.TypeError, match="provenance"):
        db.transact([], source=invalid_text)
    with pytest.raises(fgraph.TypeError, match="32-byte"):
        db.transact([], _force=True, _event_hash=b"bad")
    with pytest.raises(fgraph.TypeError, match="event_data"):
        db.transact([], _force=True, _event_data={})

    oversized = "x" * (MAX_EVENT_BYTES + 1)
    with pytest.raises(fgraph.TooLarge, match="canonical event"):
        db.transact(
            [],
            _force=True,
            _event_data=oversized,
            _event_hash=hashlib.sha256(oversized.encode()).digest(),
        )
    for payload, message in [("{", "canonical JSON"), ('{"value": 1}', "canonical JSON encoding")]:
        with pytest.raises(fgraph.TypeError, match=message):
            db.transact(
                [],
                _force=True,
                _event_data=payload,
                _event_hash=hashlib.sha256(payload.encode()).digest(),
            )
    with pytest.raises(fgraph.Conflict, match="event hash differs"):
        db.transact([], _force=True, _event_data="{}", _event_hash=b"0" * 32)


def test_transaction_parser_rejects_malformed_maps_and_operations(db: fgraph.Db) -> None:
    invalid: list[tuple[Any, type[Exception], str]] = [
        ({1: "value"}, fgraph.TypeError, "map keys"),
        ({"id": {"bad": "selector"}, "edge/value": 1}, fgraph.TypeError, "invalid map id"),
        ([object()], fgraph.TypeError, "transaction item"),
        (object(), fgraph.TypeError, "transaction"),
        (["unknown"], fgraph.TypeError, "transaction item"),
        (["assert", "entity", "edge/value"], fgraph.TypeError, "assert operation"),
        (["retract"], fgraph.TypeError, "retract operation"),
        (["cas", "entity", "edge/value", 1], fgraph.TypeError, "cas operation"),
    ]
    for data, error, message in invalid:
        with pytest.raises(error, match=message):
            db.transact(data)

    db.transact({"id": "edge/entity", "edge/value": 1})
    with pytest.raises(fgraph.SchemaError, match="retract attribute"):
        db.transact(["retract", "edge/entity", 1])
    with pytest.raises(fgraph.Conflict, match="holds one value"):
        db.transact({"id": "edge/array", "edge/value": [1, 2]})
    with pytest.raises(fgraph.TypeError, match="nested map"):
        db.transact({"id": "edge/parent", "edge/value": {"id": "edge/child", "edge/label": "child"}})

    db.declare("edge/child", ref=True)
    nested = db.transact({"id": "edge/parent", "edge/child": {"id": "edge/nested", "edge/label": "child"}})
    assert nested.tx is not None


def test_transaction_fact_and_cas_contract_edges(db: fgraph.Db) -> None:
    db.transact({"id": "counter", "counter/value": 1})
    db.declare("counter/tags", many=True)
    db.declare("counter/ref", ref=True)

    with pytest.raises(fgraph.SchemaError, match="cannot set"):
        db.transact([], tx={"fgraph/at": 1})
    with pytest.raises(fgraph.SchemaError, match="cannot set id"):
        db.transact([], tx={"id": "bad"})
    with pytest.raises(fgraph.Conflict, match="declare it many"):
        db.transact([], tx={"counter/value": [1, 2]})
    with pytest.raises(fgraph.TypeError, match="nested transaction map"):
        db.transact([], tx={"counter/value": {"id": "nested"}})

    report = db.transact(
        [],
        tx={
            "counter/tags": ["one", "two"],
            "counter/ref": {"id": "counter/audit", "counter/value": 2},
        },
    )
    assert report.tx is not None

    db.declare("counter/many", many=True)
    with pytest.raises(fgraph.SchemaError, match="cardinality-many"):
        db.transact(["cas", "counter", "counter/many", 1, 2])
    with pytest.raises(fgraph.Conflict, match="CAS"):
        db.transact(["cas", "counter", "counter/value", 2, 3])


def test_schema_and_shape_declaration_validation(db: fgraph.Db) -> None:
    invalid_declarations: list[dict[str, Any]] = [
        {"type": "text", "ref": True},
        {"type": "unknown"},
        {"type": "text", "dims": 2},
        {"type": "text", "vector_model": "model"},
        {"type": "vector", "vector_model": ""},
    ]
    for options in invalid_declarations:
        with pytest.raises(fgraph.SchemaError):
            db.declare("schema/value", **options)
    with pytest.raises(fgraph.SchemaError, match="sets no behavior"):
        db.declare("schema/value")

    invalid_shapes: list[dict[str, Any]] = [
        {"required": "schema/value"},
        {"allowed": [1]},
        {"required": ["invalid"]},
        {"closed": 1},
    ]
    for options in invalid_shapes:
        with pytest.raises(fgraph.SchemaError):
            db.declare_shape("schema/shape", **options)


def test_pull_patterns_fail_at_the_boundary(db: fgraph.Db) -> None:
    db.transact({"id": "pull/entity", "pull/value": 1})
    invalid: list[tuple[Any, str]] = [
        ("pull/value", "attribute array"),
        ([1], "pull item"),
        (["invalid"], "pull attribute"),
        ([{1: ["*"]}], "nested pull attribute"),
        ([{"pull/_value": ["*"]}], "reverse"),
        ([{"invalid": ["*"]}], "nested pull attribute"),
        ([{"pull/missing": ["*"]}], "unknown"),
        ([{"pull/value": ["*"]}], "not a ref"),
    ]
    for pattern, message in invalid:
        with pytest.raises(fgraph.QueryError, match=message):
            db.pull("pull/entity", pattern)
    with pytest.raises(fgraph.QueryError, match="negative"):
        db.entity("pull/entity", depth=-1)


def _decode_cursor(cursor: str) -> dict[str, Any]:
    return json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))


def test_datoms_rejects_invalid_options_and_tampered_cursor(db: fgraph.Db) -> None:
    first = db.transact({"id": "datom/one", "datom/value": 1})
    db.transact({"id": "datom/two", "datom/value": 2})
    page = db.datoms(limit=1)
    assert isinstance(page["next_cursor"], str)
    valid_payload = _decode_cursor(page["next_cursor"])

    invalid_calls: list[dict[str, Any]] = [
        {"index": "bad"},
        {"source": "bad"},
        {"components": "bad"},
        {"components": [1, 2, 3, 4, 5, 6]},
        {"limit": True},
        {"limit": 0},
        {"limit": 1001},
    ]
    for options in invalid_calls:
        with pytest.raises(fgraph.QueryError):
            db.datoms(**options)

    issued_cursor = page["next_cursor"]
    assert isinstance(issued_cursor, str)
    for cursor in [
        "",
        "x" * 4097,
        base64.urlsafe_b64encode(b"{").decode(),
        issued_cursor + "=",
        issued_cursor + "!!",
    ]:
        with pytest.raises(fgraph.QueryError):
            db.datoms(cursor=cursor)
    list_cursor = Db._encode_cursor({"not": "a mapping"})  # noqa: SLF001
    decoded = _decode_cursor(list_cursor)
    assert decoded == {"not": "a mapping"}

    variants: list[dict[str, Any]] = []
    mismatch = copy.deepcopy(valid_payload)
    mismatch["source"] = "history"
    variants.append(mismatch)
    invalid_bounds = copy.deepcopy(valid_payload)
    invalid_bounds["basis"] = "64"
    variants.append(invalid_bounds)
    invalid_seek = copy.deepcopy(valid_payload)
    invalid_seek["seek"] = [1]
    variants.append(invalid_seek)
    bad_wrapper = copy.deepcopy(valid_payload)
    bad_wrapper["seek"][2] = {"bad": "value"}
    variants.append(bad_wrapper)
    bad_base64 = copy.deepcopy(valid_payload)
    bad_base64["seek"][2] = {"bytes": "%%%"}
    variants.append(bad_base64)
    bad_value = copy.deepcopy(valid_payload)
    bad_value["seek"][2] = []
    variants.append(bad_value)
    bad_coordinate = copy.deepcopy(valid_payload)
    bad_coordinate["seek"][0] = True
    variants.append(bad_coordinate)
    outside = copy.deepcopy(valid_payload)
    assert first.tx is not None
    outside["basis"] = first.tx + 10_000
    variants.append(outside)
    for payload in variants:
        with pytest.raises(fgraph.QueryError):
            db.datoms(cursor=Db._encode_cursor(payload))  # noqa: SLF001


def test_datoms_prefix_semantics_cover_all_indexes(db: fgraph.Db) -> None:
    report = db.transact(
        [
            {"id": "datom/target", "datom/value": 1},
            {"id": "datom/source", "datom/ref": {"ref": "datom/target"}},
        ]
    )
    db.declare("datom/ref", ref=True)
    target = db.entity("datom/target")
    assert target["datom/value"] == 1

    assert db.datoms("eavt", ["missing"])["items"] == []
    assert db.datoms("avet", ["missing/attribute"])["items"] == []
    assert db.datoms("eavt", ["datom/target", "datom/value", 1, report.tx, True])["items"]
    assert db.datoms("avet", ["datom/value", 1, "datom/target"])["items"]
    assert db.datoms("vaet", [{"ref": "datom/target"}])["items"]
    assert db.datoms("vaet", ["datom/target"])["items"]
    with pytest.raises(fgraph.QueryError, match="boolean"):
        db.datoms("eavt", ["datom/target", "datom/value", 1, report.tx, "yes"])


def test_temporal_receipt_event_and_follow_boundaries(db: fgraph.Db) -> None:
    created = db.transact({"id": "temporal/entity", "temporal/value": 1})
    changed = db.transact({"id": "temporal/entity", "temporal/value": 2})

    for value in [True, object(), 10**30]:
        with pytest.raises(fgraph.TypeError):
            db.at(value)
    with pytest.raises(fgraph.NotFound, match="precedes"):
        db.at({"instant": -62_135_596_800_000_000})
    with pytest.raises(fgraph.QueryError, match="reversed"):
        db.diff(changed.tx, created.tx)
    assert db.at(created.tx).diff(created.tx, changed.tx) == {"asserted": [], "retracted": []}

    for transaction in [True, "65", INT64_MAX + 1]:
        invalid_transaction: Any = transaction
        with pytest.raises(fgraph.TypeError, match="transaction"):
            db.receipt(invalid_transaction)
    with pytest.raises(fgraph.NotFound, match="was not found"):
        db.receipt(65)
    with pytest.raises(fgraph.NotFound, match="attribute"):
        db.history("temporal/entity", "missing/attribute")
    with pytest.raises(fgraph.NotFound, match="attribute"):
        db.why("temporal/entity", "missing/attribute")

    for since in [True, 63, INT64_MAX + 1]:
        with pytest.raises(fgraph.TypeError, match="event cursor"):
            db.event_records(since)
    for through in [True, 63, INT64_MAX + 1]:
        with pytest.raises(fgraph.TypeError, match="event through"):
            db.event_records(through=through)
    with pytest.raises(fgraph.FormatError, match="stable event receipt"):
        db._event_record_for_tx(999_999)  # noqa: SLF001
    with pytest.raises(fgraph.Unsupported, match="historical"):
        next(db.at(created.tx).follow())
    with pytest.raises(fgraph.TypeError, match="positive"):
        next(db.follow(interval=0))


def test_speculation_undo_and_excision_boundaries(db: fgraph.Db) -> None:
    with db.speculate():
        db.transact({"id": "speculative", "spec/value": 1})
        with pytest.raises(fgraph.Unsupported, match="nested speculation"), db.speculate():
            pass
    with pytest.raises(fgraph.NotFound):
        db.entity("speculative")

    with pytest.raises(fgraph.Unsupported, match="system transaction"):
        db.undo(64)
    for transaction in ["65", INT64_MAX + 1]:
        invalid_transaction: Any = transaction
        with pytest.raises(fgraph.TypeError, match="transaction"):
            db.undo(invalid_transaction)
    with pytest.raises(fgraph.NotFound, match="transaction"):
        db.undo(999_999)
    for operation_id in ["", "bad\nkey", "\ud800"]:
        with pytest.raises(fgraph.TypeError, match="operation_id"):
            db.excise("missing", operation_id=operation_id)
        with pytest.raises(fgraph.TypeError, match="operation_id"):
            db.transact({"id": "invalid-operation", "item/value": 1}, operation_id=operation_id)
    for basis in [True, INT64_MAX + 1]:
        with pytest.raises(fgraph.TypeError, match="if_basis_tx"):
            db.excise("missing", if_basis_tx=basis)
        with pytest.raises(fgraph.TypeError, match="if_basis_tx"):
            db.transact({"id": "invalid-basis", "item/value": 1}, if_basis_tx=basis)
    with pytest.raises(fgraph.TypeError):
        db.excise(object())
    with pytest.raises(fgraph.Unsupported, match="cannot be excised"):
        db.excise("fgraph/at")


def test_snapshot_backup_schema_and_doctor_boundary_options(tmp_path: Path, db: fgraph.Db) -> None:
    with pytest.raises(fgraph.TypeError, match="snapshot identity selector"):
        Db._snapshot_selector_key(1)  # noqa: SLF001
    with pytest.raises(fgraph.TypeError, match="not a UUID"):
        Db._snapshot_selector_key({"eid": "bad"})  # noqa: SLF001
    with pytest.raises(fgraph.TypeError, match="canonical"):
        Db._snapshot_selector_key({"eid": str(uuid.uuid4()).upper()})  # noqa: SLF001

    path = tmp_path / "graph.db"
    with fgraph.connect(path) as file_db:
        with pytest.raises(fgraph.Conflict, match="open database"):
            file_db.backup(path)
        occupied = tmp_path / "occupied.db"
        occupied.touch()
        with pytest.raises(fgraph.Conflict, match="already exists"):
            file_db.backup(occupied)
        with pytest.raises(fgraph.FormatError, match="cannot be created"):
            file_db.backup(tmp_path / "missing" / "backup.db")

    invalid_option: Any = 1
    for method in [db.attributes, db.schema]:
        with pytest.raises(fgraph.TypeError, match="prefix"):
            method(invalid_option)
        with pytest.raises(fgraph.TypeError, match="include_system"):
            method(include_system=invalid_option)
    with pytest.raises(fgraph.TypeError, match="repair"):
        db.doctor(repair=invalid_option)


def test_canonical_wire_value_decoder_rejects_tag_mismatches(db: fgraph.Db) -> None:
    with pytest.raises(fgraph.TypeError, match="tag"):
        db._decode_tagged_wire_value(1, "bad")  # noqa: SLF001
    with pytest.raises(fgraph.TypeError, match="finite JSON number"):
        db._decode_tagged_wire_value("one", "float")  # noqa: SLF001
    assert db._decode_tagged_wire_value(1, "float") == 1.0  # noqa: SLF001
    with pytest.raises(fgraph.TypeError, match="does not match"):
        db._decode_tagged_wire_value(1, "text")  # noqa: SLF001
