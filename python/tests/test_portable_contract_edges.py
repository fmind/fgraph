"""Malformed portable event and snapshot contract regressions."""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from collections.abc import Callable
from typing import Any

import pytest

import fgraph
from fgraph.store import (
    MAX_EVENT_BYTES,
    _canonical_event_data,
    _derived_entity_id,
    _event_mentions_selector,
    _valid_physical_value,
)
from fgraph.values import JSON, TEXT, canonical_json


def _event(event: str | None = None) -> dict[str, Any]:
    return {
        "fgraph": "event/1",
        "event": event or str(uuid.uuid4()),
        "at": 1_767_225_600_000_000,
        "created": [],
        "asserted": [],
        "retracted": [],
    }


def _encoded(record: dict[str, Any]) -> tuple[str, bytes]:
    data = canonical_json(record)
    return data, hashlib.sha256(data.encode()).digest()


def test_portable_helper_rejections_and_semantic_selector_matching() -> None:
    with pytest.raises(fgraph.TooLarge, match="uint64"):
        _derived_entity_id(uuid.uuid4(), -1)
    with pytest.raises(fgraph.TypeError, match="invalid JSON"):
        _canonical_event_data({"unsupported": object()})

    assert _event_mentions_selector({}, 1) is False
    assert _event_mentions_selector({"asserted": None}, "target") is False
    assert _event_mentions_selector({"asserted": [None]}, "target") is False
    assert (
        _event_mentions_selector(
            {"asserted": [["entity", "edge/ref", {"ref": "target"}, "ref"]]},
            "target",
        )
        is True
    )
    assert _valid_physical_value(TEXT, "text", None, b"\xff") is False
    assert _valid_physical_value(JSON, "text", None, b"x" * (1024 * 1024 + 1)) is False
    assert _valid_physical_value(JSON, "text", None, b"{") is False
    assert _valid_physical_value(999, "null", None, b"") is False


def test_event_payload_decoder_rejects_every_malformed_envelope(db: fgraph.Db) -> None:
    event = str(uuid.uuid4())
    valid = _event(event)

    malformed: list[tuple[Any, bytes, str]] = [
        (None, b"0" * 32, "physical TEXT"),
        ("x" * (MAX_EVENT_BYTES + 1), b"0" * 32, "exceeds"),
        ("{", hashlib.sha256(b"{").digest(), "valid JSON"),
        ("[]", hashlib.sha256(b"[]").digest(), "canonical JSON object"),
    ]

    variants: list[tuple[dict[str, Any], str]] = []
    wrong_identity = copy.deepcopy(valid)
    wrong_identity["event"] = str(uuid.uuid4())
    variants.append((wrong_identity, "invalid identity"))
    invalid_at = copy.deepcopy(valid)
    invalid_at["at"] = True
    variants.append((invalid_at, "invalid at"))
    invalid_arrays = copy.deepcopy(valid)
    invalid_arrays["created"] = {}
    variants.append((invalid_arrays, "must be arrays"))
    malformed_redaction = copy.deepcopy(valid)
    malformed_redaction.update({"redacted": True, "redacts": []})
    malformed_redaction["created"] = ["unexpected"]
    variants.append((malformed_redaction, "malformed excision"))
    invalid_redaction_uuid = copy.deepcopy(valid)
    invalid_redaction_uuid.update({"redacted": True, "redacts": ["not-a-uuid"]})
    variants.append((invalid_redaction_uuid, "invalid UUID"))
    noncanonical_redaction_uuid = copy.deepcopy(valid)
    noncanonical_redaction_uuid.update({"redacted": True, "redacts": [str(uuid.uuid4()).upper()]})
    variants.append((noncanonical_redaction_uuid, "non-canonical UUID"))
    unknown = copy.deepcopy(valid)
    unknown["unknown"] = True
    variants.append((unknown, "unknown fields"))
    invalid_by = copy.deepcopy(valid)
    invalid_by["by"] = 1
    variants.append((invalid_by, "by field"))
    invalid_source = copy.deepcopy(valid)
    invalid_source["source"] = 1
    variants.append((invalid_source, "source field"))
    invalid_tx_facts = copy.deepcopy(valid)
    invalid_tx_facts["tx_facts"] = {}
    variants.append((invalid_tx_facts, "tx_facts field"))
    for variant, message in variants:
        data, digest = _encoded(variant)
        malformed.append((data, digest, message))

    for data, digest, message in malformed:
        with pytest.raises(fgraph.FormatError, match=message):
            db._decode_event_data(event, digest, data)  # noqa: SLF001


def test_apply_rejects_malformed_event_records_atomically(db: fgraph.Db) -> None:
    event = str(uuid.uuid4())
    base = _event(event)

    def changed(**fields: Any) -> dict[str, Any]:
        record = copy.deepcopy(base)
        record.update(fields)
        return record

    malformed: list[tuple[Any, str]] = [
        ([], "event/1 object"),
        (changed(unknown=True), "unknown fields"),
        (changed(event=None), "no UUID"),
        (changed(event="not-a-uuid"), "invalid event UUID"),
        (changed(event=event.upper()), "canonical RFC 4122"),
        (changed(at=True), "needs integer at"),
        (changed(created=[1]), "entity selector"),
        (changed(created=[{"eid": "not-a-uuid"}]), "entity id"),
        (changed(created=[{"eid": str(uuid.uuid4()).upper()}]), "not canonical"),
        (changed(asserted=[["entity", "edge/value"]]), "assert tuple"),
        (changed(asserted=[["entity", "edge/ref", "bad", "ref"]]), "ref value"),
        (changed(tx_facts={}), "tx_facts must be"),
        (changed(tx_facts=[["edge/value"]]), "tx fact must be"),
        (changed(by=1), "by must be text"),
        (changed(source=1), "source must be text"),
    ]
    before = db.stats()
    for record, message in malformed:
        with pytest.raises(fgraph.TypeError, match=message):
            db.apply(canonical_json(record))
        assert db.stats() == before


def test_apply_skips_blanks_reuses_stable_entities_and_detects_event_collisions(db: fgraph.Db) -> None:
    stable = str(uuid.uuid4())
    first = _event()
    first["created"] = [{"eid": stable}, "edge/value"]
    first["asserted"] = [[{"eid": stable}, "edge/value", 1, "int"]]
    first_report = db.apply("\n" + canonical_json(first))[0]
    assert first_report.status == "applied"

    second = _event()
    second["created"] = [{"eid": stable}]
    second["asserted"] = [[{"eid": stable}, "edge/value", 2, "int"]]
    assert db.apply(canonical_json(second))[0].status == "applied"

    collision = copy.deepcopy(first)
    collision["meta"] = {"changed": True}
    with pytest.raises(fgraph.Conflict, match="collides"):
        db.apply(canonical_json(collision))


def _snapshot_records() -> list[dict[str, Any]]:
    with fgraph.connect(":memory:", clock=1_767_225_600_000_000) as db:
        db.declare("edge/ref", ref=True)
        db.transact({"id": "edge/one", "edge/value": 1, "edge/ref": {"ref": "edge/two"}})
        db.transact({"id": "edge/one", "edge/value": 2})
        snapshot = db.snapshot()
    assert isinstance(snapshot, str)
    return [json.loads(line) for line in snapshot.splitlines()]


def _render_snapshot(records: list[dict[str, Any]]) -> str:
    footer = records[-1]
    body = [canonical_json(record) for record in records[:-1]]
    footer["sha256"] = hashlib.sha256(("\n".join(body) + "\n").encode()).hexdigest()
    return "\n".join([*body, canonical_json(footer)]) + "\n"


def test_snapshot_envelope_and_pristine_restore_rejections() -> None:
    records = _snapshot_records()
    with fgraph.connect(":memory:") as target:
        with pytest.raises(fgraph.TypeError, match="truncated"):
            target.restore("")

        invalid = copy.deepcopy(records)
        invalid[0]["format"] = 999
        with pytest.raises(fgraph.TypeError, match="header/footer"):
            target.restore(_render_snapshot(invalid))

        invalid = copy.deepcopy(records)
        invalid[-1]["receipts"] = -1
        with pytest.raises(fgraph.TypeError, match="non-negative"):
            target.restore(_render_snapshot(invalid))

        invalid = copy.deepcopy(records)
        invalid[1] = {"unknown": True}
        with pytest.raises(fgraph.TypeError, match="record kinds"):
            target.restore(_render_snapshot(invalid))

        invalid = copy.deepcopy(records)
        invalid[0]["basis"] = str(uuid.uuid4())
        with pytest.raises(fgraph.Conflict, match="basis"):
            target.restore(_render_snapshot(invalid))

        invalid = copy.deepcopy(records)
        invalid[0]["created_at"] = True
        with pytest.raises(fgraph.TypeError, match="created_at"):
            target.restore(_render_snapshot(invalid))

        target.transact({"id": "occupied"})
        with pytest.raises(fgraph.Conflict, match="pristine"):
            target.restore(_render_snapshot(copy.deepcopy(records)))


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda receipt: receipt.pop("created"), "receipt is malformed"),
        (lambda receipt: receipt.__setitem__("created", None), "created must be"),
        (lambda receipt: receipt.__setitem__("created", [receipt["created"][0]] * 2), "repeats identity"),
        (lambda receipt: receipt.__setitem__("event_hash", "bad"), "event_hash"),
        (lambda receipt: receipt.__setitem__("event_data", "bad"), "event_data must be"),
        (lambda receipt: receipt.__setitem__("operation_id", ""), "operation_id"),
        (lambda receipt: receipt.__setitem__("request_hash", "bad"), "request_hash"),
        (lambda receipt: receipt.__setitem__("operation_id", "operation"), "must both be"),
        (lambda receipt: receipt.__setitem__("at", True), "at/origin_at"),
    ],
)
def test_snapshot_receipt_rejections(
    mutate: Callable[[dict[str, Any]], Any],
    message: str,
) -> None:
    records = _snapshot_records()
    receipt = records[1]["receipt"]
    mutate(receipt)
    with fgraph.connect(":memory:") as target, pytest.raises((fgraph.TypeError, fgraph.Conflict), match=message):
        target.restore(_render_snapshot(records))
