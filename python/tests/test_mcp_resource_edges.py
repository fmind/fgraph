"""Bounded MCP resource and validation edge coverage."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from mcp.server.mcpserver.exceptions import ToolError

import fgraph
from fgraph import mcp_server


def _tool(server: Any, name: str) -> Any:
    return server._tool_manager._tools[name].fn  # noqa: SLF001


def _resource(server: Any, template: str) -> Any:
    return server._resource_manager._templates[template].fn  # noqa: SLF001


def _cursor(uri: str) -> str:
    return parse_qs(urlsplit(uri).query)["cursor"][0]


def test_mcp_selector_compaction_and_cursor_contract(db: fgraph.Db) -> None:
    assert mcp_server._selector("-12") == -12  # noqa: SLF001
    event = uuid.uuid4()
    assert mcp_server._selector(str(event)) == {"eid": str(event)}  # noqa: SLF001
    assert mcp_server._selector("person/ada") == "person/ada"  # noqa: SLF001

    compact = mcp_server._compact_entity(  # noqa: SLF001
        {f"z/{index:02d}": list(range(40)) for index in range(40)}
    )
    assert len(compact) == 32
    assert all(len(value) == 32 for value in compact.values())

    cursor = mcp_server._page_cursor(db, "schema", 64, 2, prefix="app/")  # noqa: SLF001
    assert mcp_server._read_page_cursor(db, cursor, "schema", prefix="app/") == (64, 2)  # noqa: SLF001
    for malformed in (cursor + "=", cursor + "!!", "x" * 4097):
        with pytest.raises(fgraph.QueryError, match="cursor"):
            mcp_server._read_page_cursor(db, malformed, "schema", prefix="app/")  # noqa: SLF001
    with pytest.raises(fgraph.TypeError, match="does not match"):
        mcp_server._read_page_cursor(db, cursor, "schema", prefix="other/")  # noqa: SLF001
    invalid = db._encode_cursor(  # noqa: SLF001
        {"resource": "schema", "basis": "64", "offset": -1, "arguments": {"prefix": "app/"}}
    )
    with pytest.raises(fgraph.TypeError, match="cursor is invalid"):
        mcp_server._read_page_cursor(db, invalid, "schema", prefix="app/")  # noqa: SLF001


def test_mcp_tool_validation_and_optional_mutation_paths(db: fgraph.Db) -> None:
    async def scenario() -> None:
        server = mcp_server.create_server(db, read_only=False)
        remember = _tool(server, "remember")
        forget = _tool(server, "forget")
        about = _tool(server, "about")
        why = _tool(server, "why")
        history = _tool(server, "history")
        query = _tool(server, "query")
        datoms = _tool(server, "datoms")
        schema = _tool(server, "schema")

        with pytest.raises(ToolError, match="key is empty"):
            await remember(operation_id="empty-key", text="value", key="")
        with pytest.raises(ToolError, match="needs facts or text"):
            await remember(operation_id="empty-memory")

        created = db.transact({"id": "mcp/item", "mcp/value": "old", "mcp/other": True})
        forgotten = await forget(
            entity="mcp/item",
            attribute="mcp/value",
            value="old",
            operation_id="forget-exact",
            if_basis_tx=created.tx,
        )
        assert forgotten["data"]["retracted"][0]["v"] == "old"

        for depth in (True, -1, 4):
            with pytest.raises(ToolError, match="depth"):
                await about(entity="mcp/item", depth=depth)
        for handler in (why, history):
            with pytest.raises(ToolError, match="limit"):
                await handler(entity="mcp/item", limit=0)
        for limit in (True, -1, 1001):
            with pytest.raises(ToolError, match="query limit"):
                await query(q={"find": ["?e"], "where": [], "limit": limit})
        recall = _tool(server, "recall")
        for arguments in ({"query": "value", "k": 0}, {"query": "value", "k": 21}, {"query": "value", "expand": 3}):
            with pytest.raises(ToolError):
                await recall(**arguments)
        with pytest.raises(ToolError, match="datoms limit"):
            await datoms(limit=101)
        with pytest.raises(ToolError, match="schema limit"):
            await schema(limit=0)

    asyncio.run(scenario())


def test_mcp_resources_are_pinned_bounded_and_pageable(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mcp_server, "_MAX_TOOL_ITEMS", 2)

    first = db.transact(
        {
            "id": "resource/item",
            "resource/a": 1,
            "resource/b": 2,
            "resource/c": 3,
        },
        tx={"resource/tx-a": 1, "resource/tx-b": 2, "resource/tx-c": 3},
    )
    db.transact({"id": "resource/second", "resource/a": 2})
    db.transact({"id": "resource/third", "resource/a": 3})
    server = mcp_server.create_server(db, read_only=True)

    async def scenario() -> None:
        schema_resource = _resource(server, "fgraph://schema{?prefix,cursor}")
        entity_resource = _resource(server, "fgraph://entity/{selector}{?at,cursor}")
        transaction_resource = _resource(server, "fgraph://tx/{selector}")
        changes_resource = _resource(server, "fgraph://changes{?since,cursor}")

        schema_page = await schema_resource(prefix="resource/", cursor=None)
        assert len(schema_page["attributes"]) == 2
        assert schema_page["shapes"] == []
        schema_basis = schema_page["basis_tx"]
        schema_attributes = list(schema_page["attributes"])
        schema_shapes = list(schema_page["shapes"])
        while "next_uri" in schema_page:
            schema_page = await schema_resource(
                prefix="resource/",
                cursor=_cursor(schema_page["next_uri"]),
            )
            assert schema_page["basis_tx"] == schema_basis
            schema_attributes.extend(schema_page["attributes"])
            schema_shapes.extend(schema_page["shapes"])
        expected_attributes = {item["name"] for item in db.at(schema_basis).schema("resource/")["attributes"]}
        assert {item["name"] for item in schema_attributes} == expected_attributes
        assert schema_shapes == []

        entity_page = await entity_resource(selector="resource/item", at=str(first.tx), cursor=None)
        assert len(entity_page["items"]) == 2
        second_entity_page = await entity_resource(
            selector="resource/item",
            at=str(first.tx),
            cursor=_cursor(entity_page["next_uri"]),
        )
        assert second_entity_page["basis_tx"] == first.tx
        assert {item["a"] for item in [*entity_page["items"], *second_entity_page["items"]]} == {
            "resource/a",
            "resource/b",
            "resource/c",
        }

        receipt = await transaction_resource(selector=str(first.tx))
        assert len(receipt["facts"]) == 2
        assert receipt["truncated"] is True

        changes = await changes_resource(since="64", cursor=None)
        assert len(changes["events"]) == 2
        assert all(event["fgraph"] == "event/1" for event in changes["events"])
        next_changes = await changes_resource(since="64", cursor=_cursor(changes["next_uri"]))
        assert next_changes["basis_tx"] == changes["basis_tx"]
        assert next_changes["events"]

        datoms = await _tool(server, "datoms")(limit=2)
        assert datoms["data"]["items"]
        explained = await _tool(server, "explain")(q={"find": ["?e"], "where": [["?e", "resource/a", 1]]})
        assert explained["data"]["clauses"]
        snapshot = await _tool(server, "schema")(limit=2)
        assert len(snapshot["data"]["attributes"]) == 2
        assert snapshot["data"]["truncated"] is True

    asyncio.run(scenario())


def test_mcp_writable_run_delegates_to_stdio(db: fgraph.Db, monkeypatch: pytest.MonkeyPatch) -> None:
    transports: list[str] = []

    class FakeServer:
        def run(self, transport: str) -> None:
            transports.append(transport)

    monkeypatch.setattr(mcp_server, "create_server", lambda *_args, **_kwargs: FakeServer())
    mcp_server.run(db, read_only=False)
    assert transports == ["stdio"]


def test_mcp_tool_and_resource_payload_caps() -> None:
    oversized = {"value": "x" * (256 * 1024)}
    with pytest.raises(fgraph.TooLarge, match="MCP response"):
        mcp_server._tool_result(oversized, 64)  # noqa: SLF001
    with pytest.raises(fgraph.TooLarge, match="MCP resource"):
        mcp_server._resource_result(oversized)  # noqa: SLF001
