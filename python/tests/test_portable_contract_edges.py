"""Malformed portable event and snapshot contract regressions."""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from collections.abc import Callable, Iterator
from io import StringIO
from typing import Any

import pytest

import fgraph
import fgraph.store as store_module
from fgraph.store import (
    MAX_EVENT_BYTES,
    MAX_SNAPSHOT_LINE_BYTES,
    _bounded_portable_lines,
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


@pytest.mark.parametrize("separator", ["\u0085", "\u2028", "\u2029"])
def test_apply_string_splits_ndjson_only_on_lf(separator: str) -> None:
    value = f"before{separator}after"
    with fgraph.connect(":memory:", clock=1_767_225_600_000_000) as source:
        source.transact({"id": "unicode/item", "unicode/text": value})
        stream = "".join(f"{canonical_json(record)}\n" for record in source.event_records())
    assert separator in stream

    with fgraph.connect(":memory:") as target:
        target.apply(stream)
        assert target.entity("unicode/item")["unicode/text"] == value


def test_portable_text_reader_enforces_the_payload_cap_before_reading_a_later_line() -> None:
    class ReadProbe(StringIO):
        def __init__(self, value: str) -> None:
            super().__init__(value)
            self.limits: list[int] = []

        def readline(self, size: int = -1) -> str:
            self.limits.append(size)
            return super().readline(size)

    exact = ReadProbe("xxxx\nlater")
    lines = _bounded_portable_lines(exact, maximum_bytes=4, description="test")
    assert next(lines) == "xxxx\n"
    assert exact.limits == [6]

    oversized = ReadProbe("xxxxx\nlater")
    with pytest.raises(fgraph.TooLarge, match="test line"):
        next(_bounded_portable_lines(oversized, maximum_bytes=4, description="test"))
    assert oversized.limits == [6]

    assert list(_bounded_portable_lines("one\ntwo", maximum_bytes=3, description="test")) == ["one", "two"]
    invalid_lines: Any = iter([b"bytes"])
    with pytest.raises(fgraph.TypeError, match="must be text"):
        next(_bounded_portable_lines(invalid_lines, maximum_bytes=5, description="test"))
    with pytest.raises(fgraph.TypeError, match="valid UTF-8"):
        next(_bounded_portable_lines(iter(["\ud800"]), maximum_bytes=5, description="test"))


def test_snapshot_writer_rejects_a_record_beyond_the_derived_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        store_module,
        "_canonical_json_document",
        lambda _record: "x" * (MAX_SNAPSHOT_LINE_BYTES + 1),
    )
    with fgraph.connect(":memory:") as db, pytest.raises(fgraph.TooLarge, match="snapshot record"):
        next(db.iter_snapshot())


@pytest.mark.parametrize("separator", ["\u0085", "\u2028", "\u2029"])
def test_restore_string_splits_ndjson_only_on_lf(separator: str) -> None:
    value = f"before{separator}after"
    with fgraph.connect(":memory:", clock=1_767_225_600_000_000) as source:
        source.transact({"id": "unicode/item", "unicode/text": value})
        snapshot = source.snapshot()
    assert isinstance(snapshot, str)
    assert separator in snapshot

    with fgraph.connect(":memory:") as target:
        target.restore(snapshot)
        assert target.entity("unicode/item")["unicode/text"] == value


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


def test_snapshot_receipt_created_must_match_authenticated_event() -> None:
    records = _snapshot_records()
    records[1]["receipt"]["created"].append("edge/ghost")

    with fgraph.connect(":memory:") as target, pytest.raises(fgraph.Conflict, match="created"):
        target.restore(_render_snapshot(records))


def test_snapshot_fact_attribute_must_be_a_valid_named_attribute() -> None:
    with fgraph.connect(":memory:", clock=1_767_225_600_000_000) as source:
        source.transact({"anonymous/value": 1})
        snapshot = source.snapshot()
    assert isinstance(snapshot, str)
    records = [json.loads(line) for line in snapshot.splitlines()]
    anonymous = next(
        selector
        for record in records
        if "receipt" in record
        for selector in record["receipt"]["created"]
        if isinstance(selector, dict)
    )
    fact = next(record["fact"] for record in records if record.get("fact", [None, None])[1] == "anonymous/value")
    fact[1] = anonymous

    with fgraph.connect(":memory:") as target, pytest.raises(fgraph.TypeError, match="named attribute"):
        target.restore(_render_snapshot(records))


def test_snapshot_receipt_rejects_unicode_control_operation_id() -> None:
    records = _snapshot_records()
    receipt = records[1]["receipt"]
    receipt["operation_id"] = "\u0080"
    receipt["request_hash"] = "00" * 32

    with fgraph.connect(":memory:") as target, pytest.raises(fgraph.TypeError, match="operation_id"):
        target.restore(_render_snapshot(records))


def test_restore_consumes_a_one_shot_iterator_without_materializing_it() -> None:
    snapshot = _render_snapshot(_snapshot_records())

    class OneShotSnapshot(Iterator[str]):
        def __init__(self) -> None:
            self._lines = iter(snapshot.splitlines(keepends=True))
            self._iterated = False

        def __iter__(self) -> OneShotSnapshot:
            if self._iterated:
                raise AssertionError("snapshot iterator was restarted")
            self._iterated = True
            return self

        def __next__(self) -> str:
            return next(self._lines)

        def __length_hint__(self) -> int:
            raise AssertionError("snapshot iterator was materialized")

    with fgraph.connect(":memory:") as target:
        target.restore(OneShotSnapshot())
        assert target.entity("edge/one")["edge/value"] == 2


def test_doctor_rejects_anonymous_fact_attributes() -> None:
    with fgraph.connect(":memory:") as db:
        db.transact({"anonymous/value": 1})
        anonymous = int(
            db._connection.execute(  # noqa: SLF001
                "SELECT i.id FROM fgraph_ids i LEFT JOIN fgraph_events ev ON ev.tx=i.id "
                "WHERE i.name IS NULL AND ev.tx IS NULL ORDER BY i.id LIMIT 1"
            ).fetchone()[0]
        )
        db._connection.execute(  # noqa: SLF001
            "UPDATE fgraph_facts SET a=? WHERE a=(SELECT id FROM fgraph_ids WHERE name='anonymous/value')",
            (anonymous,),
        )

        report = db.doctor()

    assert report["ok"] is False
    assert any("invalid fact attributes" in problem for problem in report["problems"])


def test_doctor_rejects_unicode_control_operation_id() -> None:
    with fgraph.connect(":memory:") as db:
        report = db.transact({"id": "operation/item"}, operation_id="operation:valid")
        db._connection.execute(  # noqa: SLF001
            "UPDATE fgraph_events SET operation_id=? WHERE tx=?",
            ("\u0080", report.tx),
        )

        checked = db.doctor()

    assert checked["ok"] is False
    assert any("malformed physical receipt fields" in problem for problem in checked["problems"])
