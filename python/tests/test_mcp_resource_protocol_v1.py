"""Protocol-level parity tests for the v1 MCP resource contract."""

from __future__ import annotations

import json
from typing import Any

import pytest
from mcp.server.mcpserver.exceptions import ResourceError
from mcp.types import CallToolResult, InputRequiredResult

import fgraph
from fgraph import mcp_server


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
