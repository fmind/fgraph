"""Protocol-level parity tests for the v1 MCP resource contract."""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from typing import Any

import pytest
from mcp.server.mcpserver.exceptions import ResourceError
from mcp.types import CallToolResult, InputRequiredResult

import fgraph
from fgraph import mcp_server
from fgraph.values import canonical_json


async def _read_resource(server: Any, uri: str) -> dict[str, Any]:
    result = await server.read_resource(uri)
    assert not isinstance(result, InputRequiredResult)
    [content] = list(result)
    assert content.mime_type == "application/json"
    assert isinstance(content.content, str)
    value = json.loads(content.content)
    assert isinstance(value, dict)
    return value


def _tool_data(result: CallToolResult | InputRequiredResult) -> dict[str, Any]:
    assert isinstance(result, CallToolResult)
    assert isinstance(result.structured_content, dict)
    assert result.structured_content["ok"] is True
    data = result.structured_content["data"]
    assert isinstance(data, dict)
    return data


@pytest.mark.anyio
async def test_entity_resource_pages_basis_pinned_eavt_datoms(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mcp_server, "_MAX_TOOL_ITEMS", 2)
    db.transact({"id": "page/entity", "page/a": 1, "page/b": 2, "page/c": 3})
    server = mcp_server.create_server(db)

    first = await _read_resource(server, "fgraph://entity/page%2Fentity")
    assert set(first) == {"basis_tx", "items", "next_uri"}
    assert len(first["items"]) == 2
    assert all(set(item) == {"e", "a", "v", "tx", "added", "fact_id"} for item in first["items"])
    pinned_basis = first["basis_tx"]

    db.transact({"id": "page/entity", "page/new": 4})
    second = await _read_resource(server, first["next_uri"])
    assert second["basis_tx"] == pinned_basis
    assert "next_uri" not in second
    items = [*first["items"], *second["items"]]
    assert {item["a"] for item in items} == {"page/a", "page/b", "page/c"}


@pytest.mark.anyio
async def test_changes_resource_pages_pinned_portable_events(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mcp_server, "_MAX_TOOL_ITEMS", 2)
    for index in range(3):
        db.transact({"id": f"change/{index}", "change/value": index})
    server = mcp_server.create_server(db)

    first = await _read_resource(server, "fgraph://changes?since=64")
    assert set(first) == {"basis_tx", "events", "next_uri"}
    assert len(first["events"]) == 2
    assert all(event["fgraph"] == "event/1" and "tx" not in event for event in first["events"])
    pinned_basis = first["basis_tx"]

    db.transact({"id": "change/later", "change/value": 4})
    second = await _read_resource(server, first["next_uri"])
    assert second["basis_tx"] == pinned_basis
    assert "next_uri" not in second
    events = [*first["events"], *second["events"]]
    assert events == db.event_records(64, through=pinned_basis)


@pytest.mark.anyio
async def test_changes_resource_chunks_one_oversized_event_and_reaches_later_events(db: fgraph.Db) -> None:
    oversized = db.transact({"id": "change/oversized", "change/value": "x" * 300_000})
    later = db.transact({"id": "change/later", "change/value": "reachable"})
    expected_events = db.event_records(64)
    expected_bytes = canonical_json(expected_events[0]).encode()
    expected_digest = hashlib.sha256(expected_bytes).hexdigest()
    server = mcp_server.create_server(db)

    first = await _read_resource(server, "fgraph://changes?since=64")
    assert first["events"] == []
    assert first["oversized_event"] == {
        "event": oversized.event,
        "event_hash": expected_digest,
        "bytes": len(expected_bytes),
        "uri": first["oversized_event"]["uri"],
    }
    assert first["oversized_event"]["uri"].startswith(f"fgraph://event/{oversized.event}?")
    assert "next_uri" in first
    assert len(canonical_json(first).encode()) <= 192 * 1024
    pinned_basis = first["basis_tx"]

    db.transact({"id": "change/unpinned", "change/value": "excluded"})
    assembled = bytearray()
    uri: str | None = first["oversized_event"]["uri"]
    expected_offset = 0
    while uri is not None:
        chunk = await _read_resource(server, uri)
        assert chunk["basis_tx"] == pinned_basis
        assert chunk["event"] == oversized.event
        assert chunk["event_hash"] == expected_digest
        assert chunk["offset"] == expected_offset
        assert chunk["encoding"] == "base64"
        decoded = base64.b64decode(chunk["data"], validate=True)
        assert 0 < len(decoded) <= 128 * 1024
        assembled.extend(decoded)
        expected_offset += len(decoded)
        assert len(canonical_json(chunk).encode()) <= 256 * 1024
        uri = chunk.get("next_uri")
    assert bytes(assembled) == expected_bytes

    continued = await _read_resource(server, first["next_uri"])
    assert continued["basis_tx"] == pinned_basis
    assert continued["events"] == [expected_events[1]]
    assert continued["events"][0]["event"] == later.event
    assert "next_uri" not in continued


@pytest.mark.anyio
async def test_changes_resource_uses_the_complete_192_kib_event_budget(db: fgraph.Db) -> None:
    db.transact({"id": "change/near-limit", "change/value": "x" * 194_000})
    [expected] = db.event_records(64)
    encoded_size = len(canonical_json(expected).encode())
    assert 188 * 1024 < encoded_size <= 192 * 1024
    server = mcp_server.create_server(db)

    page = await _read_resource(server, "fgraph://changes?since=64")

    assert page["events"] == [expected]
    assert "oversized_event" not in page


@pytest.mark.anyio
async def test_changes_resource_stops_decoding_at_its_byte_budget(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for index in range(3):
        db.transact({"id": f"budget/{index}", "budget/value": str(index) * 110_000})
    decoded: list[int] = []
    original = db._event_record_for_tx  # noqa: SLF001

    def tracked(transaction: int) -> dict[str, Any]:
        decoded.append(transaction)
        return original(transaction)

    monkeypatch.setattr(db, "_event_record_for_tx", tracked)
    server = mcp_server.create_server(db)

    first = await _read_resource(server, "fgraph://changes?since=64")

    assert len(first["events"]) == 1
    assert "next_uri" in first
    assert len(decoded) == 2
    assert len(canonical_json(first).encode()) <= 192 * 1024


@pytest.mark.anyio
async def test_event_chunk_resource_validates_pinned_coordinates(db: fgraph.Db) -> None:
    report = db.transact({"id": "chunk/item", "chunk/value": "x" * 300_000})
    [record] = db.event_records(64)
    digest = hashlib.sha256(canonical_json(record).encode()).hexdigest()
    server = mcp_server.create_server(db)
    event_resource = server._resource_manager._templates[  # noqa: SLF001
        "fgraph://event/{event}{?basis,offset,digest}"
    ].fn
    assert report.event is not None
    payload_bytes = canonical_json(record).encode()
    identity = int(
        db._connection.execute("SELECT id FROM fgraph_ids WHERE name='chunk/item'").fetchone()[0]  # noqa: SLF001
    )
    with pytest.raises(fgraph.TypeError, match="event id"):
        mcp_server._event_coordinates(db, 1, str(report.tx), "0", digest)  # noqa: SLF001

    invalid = [
        {"event": "not-a-uuid", "basis": str(report.tx), "offset": "0", "digest": digest},
        {"event": report.event.upper(), "basis": str(report.tx), "offset": "0", "digest": digest},
        {"event": report.event, "basis": str(identity), "offset": "0", "digest": digest},
        {"event": report.event, "basis": f"0{report.tx}", "offset": "0", "digest": digest},
        {"event": report.event, "basis": "63", "offset": "0", "digest": digest},
        {"event": report.event, "basis": str(report.tx), "offset": "-1", "digest": digest},
        {"event": report.event, "basis": str(report.tx), "offset": "\uff11\uff12", "digest": digest},
        {"event": report.event, "basis": str(report.tx), "offset": str(len(payload_bytes)), "digest": digest},
        {"event": report.event, "basis": str(report.tx), "offset": "0", "digest": digest.upper()},
        {"event": report.event, "basis": str(report.tx), "offset": "0", "digest": "0" * 64},
        {"event": str(uuid.uuid4()), "basis": str(report.tx), "offset": "0", "digest": digest},
    ]
    for arguments in invalid:
        with pytest.raises(ResourceError):
            await event_resource(**arguments)

    excised = db.excise("chunk/item", operation_id="chunk:excise", if_basis_tx=report.tx)
    with pytest.raises(ResourceError, match="unavailable"):
        await event_resource(event=report.event, basis=str(excised.tx), offset="0", digest=digest)


@pytest.mark.anyio
async def test_changes_resource_omits_continuation_after_final_oversized_event(db: fgraph.Db) -> None:
    db.transact({"id": "oversized/final", "oversized/value": "x" * 300_000})
    server = mcp_server.create_server(db)

    page = await _read_resource(server, "fgraph://changes?since=64")

    assert page["events"] == []
    assert "oversized_event" in page
    assert "next_uri" not in page


@pytest.mark.anyio
async def test_schema_tool_pages_attributes_and_shapes_as_one_sequence(
    db: fgraph.Db,
) -> None:
    db.declare("page/a", type="int")
    db.declare("page/b", type="int")
    db.declare_shape("shape/first", required=["page/a"])
    db.declare_shape("shape/second", required=["page/b"])
    server = mcp_server.create_server(db)

    first = _tool_data(await server.call_tool("schema", {"prefix": "page/", "limit": 2}))
    assert [item["name"] for item in first["attributes"]] == ["page/a", "page/b"]
    assert first["shapes"] == []
    assert first["truncated"] is True

    second = _tool_data(
        await server.call_tool(
            "schema",
            {"prefix": "page/", "limit": 2, "cursor": first["next_cursor"]},
        )
    )
    assert second["attributes"] == []
    assert [item["name"] for item in second["shapes"]] == ["shape/first", "shape/second"]
    assert second["next_cursor"] is None
    assert second["truncated"] is False


@pytest.mark.anyio
async def test_schema_resource_combines_pages_and_pins_basis(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mcp_server, "_MAX_TOOL_ITEMS", 2)
    db.declare("resource/a", type="int")
    db.declare("resource/b", type="int")
    db.declare_shape("shape/first", required=["resource/a"])
    db.declare_shape("shape/second", required=["resource/b"])
    server = mcp_server.create_server(db)

    first = await _read_resource(server, "fgraph://schema?prefix=resource%2F")
    assert [item["name"] for item in first["attributes"]] == ["resource/a", "resource/b"]
    assert first["shapes"] == []
    assert "next_uri" in first
    pinned_basis = first["basis_tx"]

    db.declare("resource/later", type="int")
    second = await _read_resource(server, first["next_uri"])
    assert second["basis_tx"] == pinned_basis
    assert second["attributes"] == []
    assert [item["name"] for item in second["shapes"]] == ["shape/first", "shape/second"]
    assert "next_uri" not in second


def test_resource_cursor_boundaries_keep_typed_errors(db: fgraph.Db) -> None:
    cursor = mcp_server._changes_cursor(db, 64, 64, "64")  # noqa: SLF001
    with pytest.raises(fgraph.TypeError, match="does not match"):
        mcp_server._read_changes_cursor(db, cursor, "65")  # noqa: SLF001

    invalid = db._encode_cursor(  # noqa: SLF001
        {
            "resource": "changes",
            "basis": 65,
            "position": 64,
            "arguments": {"since": "64"},
        }
    )
    with pytest.raises(fgraph.TypeError, match="cursor is invalid"):
        mcp_server._read_changes_cursor(db, invalid, "64")  # noqa: SLF001

    with pytest.raises(fgraph.TypeError, match="outside this snapshot"):
        mcp_server._schema_page({"attributes": [], "shapes": []}, 1, 100)  # noqa: SLF001


@pytest.mark.anyio
async def test_changes_resource_rejects_non_transaction_boundaries(db: fgraph.Db) -> None:
    server = mcp_server.create_server(db)
    invalid = ["2026-01-01T00%3A00%3A00Z", "63", "9" * 5000]
    for since in invalid:
        with pytest.raises(ResourceError, match="transaction at or after genesis"):
            await server.read_resource(f"fgraph://changes?since={since}")
