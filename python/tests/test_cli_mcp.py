"""Thin CLI and official MCP adapter acceptance tests."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, InputRequiredResult
from typer.testing import CliRunner

import fgraph
from fgraph.cli import app
from fgraph.errors import ReadOnly
from fgraph.errors import TypeError as FGraphTypeError
from fgraph.mcp_server import create_server, embed, run

runner = CliRunner()


def _completed(result: CallToolResult | InputRequiredResult) -> CallToolResult:
    assert isinstance(result, CallToolResult)
    return result


def _data(result: CallToolResult) -> Any:
    assert result.structured_content["ok"] is True
    assert isinstance(result.structured_content["basis_tx"], int)
    return result.structured_content["data"]


def _invoke(arguments: list[str], *, stdin: str | None = None):
    result = runner.invoke(
        app,
        arguments,
        input=stdin,
        env={"FGRAPH_CLOCK": "1767225600000000"},
        catch_exceptions=True,
    )
    assert result.exit_code == 0, result.output
    return result


def test_cli_full_workflow(tmp_path: Path) -> None:
    database = tmp_path / "cli.db"
    initialized = _invoke(["init", "--db", str(database), "--json"])
    assert json.loads(initialized.stdout)["format_version"] == 2
    declared = _invoke(["declare", "person/knows", "--ref", "--many", "--db", str(database)])
    assert json.loads(declared.stdout)["tx"] == 66
    first = _invoke(["add", "--db", str(database), '{"id":"ada","person/name":"Ada","note/text":"hello tools"}'])
    first_tx = json.loads(first.stdout)["tx"]
    multiple = _invoke(
        ["add", "--db", str(database), "-"],
        stdin=('{"id":"grace","person/name":"Grace"}\n{"id":"ada","person/knows":{"ref":"grace"}}\n'),
    )
    reports = json.loads(multiple.stdout)
    assert len(reports) == 2
    assert json.loads(_invoke(["get", "ada", "--db", str(database)]).stdout)["person/name"] == "Ada"
    query = '{"find":["?n"],"where":[["ada","person/knows","?f"],["?f","person/name","?n"]]}'
    assert json.loads(_invoke(["q", query, "--db", str(database)]).stdout)["rows"] == [["Grace"]]
    bound = '{"find":["?n"],"where":[["?e","person/name","?n"],["=","?n","?wanted"]],"in":["?wanted"]}'
    bound_args = '{"?wanted":"Ada"}'
    assert json.loads(_invoke(["q", bound, "--args", bound_args, "--db", str(database)]).stdout)["rows"] == [["Ada"]]
    assert json.loads(_invoke(["search", "--text", "hello tools", "--db", str(database)]).stdout)["hits"]
    assert json.loads(_invoke(["history", "ada", "person/name", "--db", str(database)]).stdout)
    assert json.loads(_invoke(["why", "ada", "person/name", "--db", str(database)]).stdout)
    latest_tx = reports[-1]["tx"]
    assert json.loads(_invoke(["diff", str(first_tx), str(latest_tx), "--db", str(database)]).stdout)["asserted"]
    assert json.loads(_invoke(["info", "--db", str(database)]).stdout)["transactions"] == 5

    tail = _invoke(["tail", "--since", "64", "--db", str(database)]).stdout
    tail_records = [json.loads(line) for line in tail.splitlines()]
    assert len(tail_records) == 4
    assert all(record["fgraph"] == "event/1" and "tx" not in record for record in tail_records)
    retracted = _invoke(["retract", "ada", "person/name", '"Ada"', "--db", str(database)])
    retract_tx = json.loads(retracted.stdout)["tx"]
    assert json.loads(_invoke(["undo", str(retract_tx), "--db", str(database)]).stdout)["tx"] > retract_tx
    backup = tmp_path / "copy.db"
    assert json.loads(_invoke(["backup", str(backup), "--db", str(database)]).stdout) == {"path": str(backup)}
    assert json.loads(_invoke(["doctor", "--db", str(database)]).stdout)["ok"] is True
    applied = tmp_path / "applied.db"
    replayed = _invoke(["apply", "--db", str(applied), "-"], stdin=tail)
    replay_summary = json.loads(replayed.stdout)
    assert replay_summary["events"] == 4
    assert replay_summary["applied"] == 4
    assert replay_summary["already_applied"] == 0
    assert json.loads(_invoke(["get", "ada", "--db", str(applied)]).stdout)["person/name"] == "Ada"
    assert _invoke(["version"]).stdout.strip() == "1.0.0"


def test_cli_strict_json_duplicate_and_usage(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["add", "--db", str(tmp_path / "bad.db"), '{"id":"a","id":"b"}'],
        catch_exceptions=True,
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, FGraphTypeError)
    assert "duplicate JSON key" in str(result.exception)
    usage = runner.invoke(app, ["add"], catch_exceptions=True)
    assert usage.exit_code == 2
    invalid_args = runner.invoke(
        app,
        ["q", '{"find":["?x"],"where":[]}', "--args", "[]", "--db", str(tmp_path / "bad.db")],
        catch_exceptions=True,
    )
    assert invalid_args.exit_code == 1
    assert isinstance(invalid_args.exception, FGraphTypeError)
    missing = runner.invoke(app, ["q", f"@{tmp_path / 'missing.json'}"], catch_exceptions=True)
    assert missing.exit_code == 1
    assert isinstance(missing.exception, fgraph.FormatError)
    process = subprocess.run(
        [sys.executable, "-m", "fgraph", "--wat"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert process.returncode == 2
    assert "Traceback" not in process.stderr


def test_cli_global_options_and_human_output(tmp_path: Path) -> None:
    database = tmp_path / "global.db"
    initialized = _invoke(["--db", str(database), "--json", "init"])
    assert initialized.stdout.count("\n") == 1
    assert json.loads(initialized.stdout)["format_version"] == 2

    human = _invoke(["--db", str(database), "info"])
    assert human.stdout.startswith('{\n  "application_id"')
    machine = _invoke(["info", "--db", str(database), "--json"])
    assert machine.stdout.count("\n") == 1
    help_result = runner.invoke(app, ["--help"])
    assert help_result.exit_code == 0
    assert "--db" in help_result.stdout
    assert "--json" in help_result.stdout


def test_cli_shape_validation_vector_model_and_receipt_options(tmp_path: Path) -> None:
    database = tmp_path / "shape.db"
    _invoke(["init", "--db", str(database), "--json"])
    declared = json.loads(
        _invoke(
            [
                "declare",
                "note/embedding",
                "--type",
                "vector",
                "--dims",
                "2",
                "--vector-model",
                "example/model-v1",
                "--operation-id",
                "declare:embedding",
                "--if-basis-tx",
                "64",
                "--db",
                str(database),
            ]
        ).stdout
    )
    retried = json.loads(
        _invoke(
            [
                "declare",
                "note/embedding",
                "--type",
                "vector",
                "--dims",
                "2",
                "--vector-model",
                "example/model-v1",
                "--operation-id",
                "declare:embedding",
                "--if-basis-tx",
                "64",
                "--db",
                str(database),
            ]
        ).stdout
    )
    assert retried["status"] == "already_applied"
    assert retried["tx"] == declared["tx"]

    shaped = json.loads(
        _invoke(
            [
                "shape",
                "shape/person",
                "--required",
                "person/name",
                "--closed",
                "--operation-id",
                "shape:person",
                "--if-basis-tx",
                str(declared["tx"]),
                "--db",
                str(database),
            ]
        ).stdout
    )
    _invoke(
        [
            "add",
            '{"id":"ada","fgraph/shape":{"ref":"shape/person"},"person/name":"Ada"}',
            "--operation-id",
            "add:ada",
            "--if-basis-tx",
            str(shaped["tx"]),
            "--db",
            str(database),
        ]
    )
    validation = json.loads(_invoke(["validate", "ada", "--db", str(database)]).stdout)
    assert validation["valid"] is True
    snapshot = json.loads(_invoke(["schema", "note/", "--db", str(database)]).stdout)
    assert snapshot["attributes"][0]["effective"]["vector_model"] == "example/model-v1"


def test_embed_command() -> None:
    code = "print('[0.25, -0.5]')"
    command = json.dumps([sys.executable, "-c", code])
    assert embed(command, "text") == [0.25, -0.5]
    with pytest.raises(FGraphTypeError):
        embed("", "text")
    failing = json.dumps([sys.executable, "-c", "raise SystemExit(3)"])
    with pytest.raises(FGraphTypeError):
        embed(failing, "text")
    invalid_code = "print('{}')"
    invalid = json.dumps([sys.executable, "-c", invalid_code])
    with pytest.raises(FGraphTypeError):
        embed(invalid, "text")
    boolean_code = "print('[true]')"
    boolean = json.dumps([sys.executable, "-c", boolean_code])
    with pytest.raises(FGraphTypeError):
        embed(boolean, "text")
    with pytest.raises(FGraphTypeError):
        embed("[1]", "text")
    non_finite = json.dumps([sys.executable, "-c", "print('[1e309]')"])
    with pytest.raises(FGraphTypeError, match="non-finite"):
        embed(non_finite, "text")
    oversized_code = "import sys\nwhile True:\n sys.stdout.write('0' * 65536)\n sys.stdout.flush()"
    oversized = json.dumps([sys.executable, "-c", oversized_code])
    with pytest.raises(FGraphTypeError, match="1 MiB"):
        embed(oversized, "text")
    invalid_utf8 = json.dumps([sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'\\xff')"])
    with pytest.raises(FGraphTypeError, match="UTF-8 JSON"):
        embed(invalid_utf8, "text")


def test_mcp_tools_and_read_only_surface(db: fgraph.Db) -> None:
    async def scenario() -> None:
        server = create_server(db, read_only=False)
        tools = await server.list_tools()
        assert [tool.name for tool in tools] == [
            "remember",
            "forget",
            "undo",
            "recall",
            "about",
            "why",
            "history",
            "query",
            "datoms",
            "receipt",
            "schema",
            "explain",
        ]
        assert all(tool.description is not None and "Example:" in tool.description for tool in tools)
        assert all(tool.output_schema is not None for tool in tools)
        assert all(tool.annotations is not None for tool in tools)
        assert all(tool.annotations.open_world_hint is False for tool in tools if tool.annotations is not None)
        assert all(
            tool.annotations.read_only_hint is (tool.name not in {"remember", "forget", "undo"})
            for tool in tools
            if tool.annotations is not None
        )
        remembered = _completed(
            await server.call_tool(
                "remember",
                {
                    "text": "User prefers small tools",
                    "source": "chat",
                    "operation_id": "mcp:remember:1",
                    "if_basis_tx": 64,
                },
            )
        )
        assert remembered.is_error is False
        remembered_data = _data(remembered)
        assert remembered_data["tx"] == 67
        assert remembered.structured_content["basis_tx"] == remembered_data["tx"]
        receipt = _completed(await server.call_tool("receipt", {"tx": remembered_data["tx"]}))
        assert _data(receipt)["event"] == remembered_data["event"]
        entity = next(fact["e"] for fact in remembered_data["asserted"] if fact["a"] == "memory/text")
        about = _completed(await server.call_tool("about", {"entity": entity}))
        assert _data(about)["memory/text"] == "User prefers small tools"
        recalled = _completed(await server.call_tool("recall", {"query": "small tools"}))
        assert _data(recalled)["hits"][0]["entity"] == entity
        explained = _completed(await server.call_tool("why", {"entity": entity}))
        assert _data(explained)["items"][0]["provenance"]["fgraph/by"] == "mcp:unknown"
        timeline = _completed(await server.call_tool("history", {"entity": entity}))
        assert _data(timeline)["items"]
        queried = _completed(
            await server.call_tool(
                "query",
                {"q": {"find": ["?text"], "where": [["?e", "memory/text", "?text"]]}},
            )
        )
        assert _data(queried)["rows"] == [["User prefers small tools"]]
        forgotten = _completed(
            await server.call_tool(
                "forget",
                {
                    "entity": entity,
                    "attribute": "memory/text",
                    "operation_id": "mcp:forget:1",
                    "if_basis_tx": remembered_data["tx"],
                },
            )
        )
        forgotten_data = _data(forgotten)
        undo_tx = forgotten_data["tx"]
        assert forgotten.structured_content["basis_tx"] == undo_tx
        assert db.receipt(undo_tx)["by"] == "mcp:unknown"
        restored = _completed(
            await server.call_tool(
                "undo",
                {
                    "tx": undo_tx,
                    "operation_id": "mcp:undo:1",
                    "if_basis_tx": undo_tx,
                },
            )
        )
        restored_data = _data(restored)
        assert restored_data["tx"] > undo_tx
        assert restored.structured_content["basis_tx"] == restored_data["tx"]
        assert db.receipt(restored_data["tx"])["by"] == "mcp:unknown"
        undo_retry = _completed(
            await server.call_tool(
                "undo",
                {
                    "tx": undo_tx,
                    "operation_id": "mcp:undo:1",
                    "if_basis_tx": undo_tx,
                },
            )
        )
        assert _data(undo_retry)["status"] == "already_applied"
        # Direct server calls expose validation failures as ToolError. The MCP
        # transport converts the same exception into an error result for clients.
        with pytest.raises(ToolError):
            await server.call_tool("remember", {})
        with pytest.raises(ToolError):
            await server.call_tool("remember", {"facts": []})
        with pytest.raises(ToolError):
            await server.call_tool("remember", {"text": ""})
        with pytest.raises(ToolError):
            await server.call_tool("remember", {"text": "value", "key": ""})

        mapped = _completed(
            await server.call_tool(
                "remember",
                {"facts": {"id": "mapped", "note/kind": "map"}, "operation_id": "mcp:mapped"},
            )
        )
        assert _data(mapped)["tx"] is not None
        mapped_list = _completed(
            await server.call_tool(
                "remember",
                {
                    "facts": [{"id": "mapped-list", "note/kind": "list"}],
                    "operation_id": "mcp:mapped-list",
                },
            )
        )
        assert _data(mapped_list)["tx"] is not None
        operation = _completed(
            await server.call_tool(
                "remember",
                {
                    "facts": ["assert", "operation", "note/kind", "operation"],
                    "operation_id": "mcp:operation",
                },
            )
        )
        assert _data(operation)["tx"] is not None
        combined = _completed(
            await server.call_tool(
                "remember",
                {
                    "facts": {"id": "combined", "note/kind": "structured"},
                    "text": "searchable companion",
                    "operation_id": "mcp:combined",
                },
            )
        )
        assert len(_data(combined)["asserted"]) >= 3

        read_only_server = create_server(db, read_only=True)
        assert [tool.name for tool in await read_only_server.list_tools()] == [
            "recall",
            "about",
            "why",
            "history",
            "query",
            "datoms",
            "receipt",
            "schema",
            "explain",
        ]

    asyncio.run(scenario())
    with pytest.raises(ReadOnly):
        run(db, read_only=True)


def test_mcp_read_envelope_is_pinned_before_evaluation(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seeded = db.transact({"id": "pinned/entity", "pinned/value": "seen"})
    assert seeded.tx is not None
    server = create_server(db, read_only=True)
    original_at = db.at
    advanced = False

    def advance_after_pin(basis: int) -> fgraph.Db:
        nonlocal advanced
        view = original_at(basis)
        if not advanced:
            advanced = True
            db.transact({"id": "pinned/entity", "pinned/new": "unseen"})
        return view

    monkeypatch.setattr(db, "at", advance_after_pin)

    async def scenario() -> None:
        about = _completed(await server.call_tool("about", {"entity": "pinned/entity"}))
        assert about.structured_content["basis_tx"] == seeded.tx
        assert _data(about) == {"pinned/value": "seen"}

    asyncio.run(scenario())
