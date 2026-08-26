"""CLI coverage for API v1 pagination, recovery, and typed error paths."""

from __future__ import annotations

import json
import subprocess
import sys
from contextlib import nullcontext
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from typer._click._compat import strip_ansi
from typer._click.exceptions import Exit as TyperExit
from typer.testing import CliRunner

import fgraph
from fgraph import cli

runner = CliRunner()


def test_cli_import_keeps_optional_mcp_sdk_lazy() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import fgraph.cli; raise SystemExit('mcp' in sys.modules)",
        ],
        check=False,
    )

    assert completed.returncode == 0


def _invoke(arguments: list[str], *, stdin: str | None = None) -> Any:
    result = runner.invoke(
        cli.app,
        arguments,
        input=stdin,
        env={"FGRAPH_CLOCK": "1767225600000000"},
        catch_exceptions=True,
    )
    assert result.exit_code == 0, result.output
    return result


@pytest.mark.parametrize("command", ["export", "import"])
def test_cli_omits_legacy_portable_commands(command: str) -> None:
    result = runner.invoke(cli.app, [command, "--help"])

    assert result.exit_code == 2
    assert f"No such command '{command}'" in result.output


@pytest.mark.parametrize(
    ("arguments", "missing"),
    [
        (["excise", "entity", "--if-basis-tx", "64"], "--operation-id"),
        (["excise", "entity", "--operation-id", "excise:entity"], "--if-basis-tx"),
    ],
)
def test_cli_excise_requires_idempotency_and_basis_options(arguments: list[str], missing: str) -> None:
    result = runner.invoke(cli.app, arguments)

    assert result.exit_code == 2
    output = strip_ansi(result.output)
    assert missing in output
    assert "Missing option" in output


def test_cli_explain_datoms_receipt_snapshot_restore_and_excise(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    _invoke(["init", "--db", str(database)])
    added = json.loads(_invoke(["add", '{"id":"cli/item","cli/value":1}', "--db", str(database)]).stdout)

    explanation = json.loads(
        _invoke(
            [
                "explain",
                '{"find":["?v"],"where":[["cli/item","cli/value","?v"]]}',
                "--args",
                "{}",
                "--db",
                str(database),
            ]
        ).stdout
    )
    assert explanation["clauses"][0]["access"] == "eavt/ea"
    datoms = json.loads(
        _invoke(
            [
                "datoms",
                "eavt",
                "--components",
                '["cli/item"]',
                "--source",
                "history",
                "--db",
                str(database),
            ]
        ).stdout
    )
    assert datoms["items"]
    help_result = runner.invoke(cli.app, ["datoms", "--help"])
    assert help_result.exit_code == 0
    assert "[index]" in help_result.stdout
    receipt = json.loads(_invoke(["tx", str(added["tx"]), "--db", str(database)]).stdout)
    assert receipt["event"] == added["event"]

    snapshot = _invoke(["snapshot", "--db", str(database)]).stdout
    restored = tmp_path / "restored.db"
    assert json.loads(_invoke(["restore", "-", "--db", str(restored)], stdin=snapshot).stdout)["ok"] is True
    assert json.loads(_invoke(["get", "cli/item", "--db", str(restored)]).stdout)["cli/value"] == 1

    excised = json.loads(
        _invoke(
            [
                "excise",
                "cli/item",
                "--operation-id",
                "excise:cli-item",
                "--if-basis-tx",
                str(added["tx"]),
                "--db",
                str(database),
            ]
        ).stdout
    )
    assert excised["status"] == "applied"


def test_cli_new_commands_reject_invalid_structured_inputs(tmp_path: Path) -> None:
    database = tmp_path / "errors.db"
    _invoke(["init", "--db", str(database)])

    ndjson = '{"id":"one"}\n{"id":"two"}\n'
    duplicate_receipt = runner.invoke(
        cli.app,
        ["add", "-", "--operation-id", "one-operation", "--db", str(database)],
        input=ndjson,
    )
    assert isinstance(duplicate_receipt.exception, fgraph.TypeError)
    assert "one JSON transaction" in str(duplicate_receipt.exception)

    for arguments, message in (
        (["explain", "[]", "--db", str(database)], "JSON objects"),
        (["explain", "{}", "--args", "[]", "--db", str(database)], "JSON objects"),
        (["datoms", "--components", "{}", "--db", str(database)], "JSON array"),
        (["search", "--text", "   ", "--embed-cmd", "embedder", "--db", str(database)], "blank"),
    ):
        result = runner.invoke(cli.app, arguments)
        assert isinstance(result.exception, fgraph.TypeError)
        assert message in str(result.exception)

    for command in ("apply", "restore"):
        missing = runner.invoke(cli.app, [command, str(tmp_path / "missing"), "--db", str(database)])
        assert isinstance(missing.exception, fgraph.FormatError)


def test_cli_batched_add_is_bounded_and_resumable(tmp_path: Path) -> None:
    database = tmp_path / "bulk.db"
    _invoke(["init", "--db", str(database)])
    stream = "\n".join(f'{{"id":"bulk/{index}","bulk/value":{index}}}' for index in range(3)) + "\n"
    arguments = [
        "add",
        "-",
        "--batch-size",
        "2",
        "--operation-id-prefix",
        "import:bulk",
        "--db",
        str(database),
    ]

    first = json.loads(_invoke(arguments, stdin=stream).stdout)
    assert first == {
        "batches": 2,
        "items": 3,
        "applied": 2,
        "already_applied": 0,
        "noop": 0,
        "basis_tx": first["tx"],
        "tx": first["tx"],
    }
    retried = json.loads(_invoke(arguments, stdin=stream).stdout)
    assert retried["batches"] == 2
    assert retried["applied"] == 0
    assert retried["already_applied"] == 2
    assert retried["basis_tx"] == first["tx"]


def test_cli_batched_add_commits_valid_prefix_before_malformed_line(tmp_path: Path) -> None:
    database = tmp_path / "partial.db"
    arguments = [
        "add",
        "-",
        "--batch-size",
        "1",
        "--operation-id-prefix",
        "import:partial",
        "--db",
        str(database),
    ]
    failed = runner.invoke(
        cli.app,
        arguments,
        input='{"id":"first","bulk/value":1}\n{\n',
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert isinstance(failed.exception, fgraph.TypeError)

    retried = json.loads(
        _invoke(
            arguments,
            stdin='{"id":"first","bulk/value":1}\n{"id":"second","bulk/value":2}\n',
        ).stdout
    )
    assert retried["items"] == 2
    assert retried["already_applied"] == 1
    assert retried["applied"] == 1


def test_cli_batched_add_validates_idempotency_options_before_mutation(tmp_path: Path) -> None:
    database = tmp_path / "options.db"
    stream = '{"id":"one"}\n{"id":"two"}\n'

    conflicting = runner.invoke(
        cli.app,
        [
            "add",
            "-",
            "--batch-size",
            "1",
            "--operation-id",
            "import:one",
            "--operation-id-prefix",
            "import",
            "--db",
            str(database),
        ],
        input=stream,
    )
    assert isinstance(conflicting.exception, fgraph.TypeError)

    missing_batch_size = runner.invoke(
        cli.app,
        ["add", "-", "--operation-id-prefix", "import", "--db", str(database)],
        input=stream,
    )
    assert isinstance(missing_batch_size.exception, fgraph.TypeError)

    spans_batches = runner.invoke(
        cli.app,
        [
            "add",
            "-",
            "--batch-size",
            "1",
            "--operation-id",
            "import:one",
            "--db",
            str(database),
        ],
        input=stream,
    )
    assert isinstance(spans_batches.exception, fgraph.TypeError)
    assert json.loads(_invoke(["info", "--db", str(database)]).stdout)["transactions"] == 1


def test_cli_schema_manifest_round_trip(tmp_path: Path) -> None:
    database = tmp_path / "schema.db"
    _invoke(["declare", "item/code", "--type", "text", "--unique", "--db", str(database)])

    manifest = json.loads(_invoke(["schema-export", "--db", str(database)]).stdout)
    checked = json.loads(_invoke(["schema-check", json.dumps(manifest), "--db", str(database)]).stdout)
    applied = json.loads(
        _invoke(
            [
                "schema-apply",
                json.dumps(manifest),
                "--operation-id",
                "schema:round-trip",
                "--db",
                str(database),
            ]
        ).stdout
    )

    assert manifest["fgraph"] == "schema/1"
    assert checked == {
        "valid": True,
        "changes": [],
        "basis_tx": checked["basis_tx"],
        "current_digest": manifest["digest"],
        "desired_digest": manifest["digest"],
    }
    assert applied["status"] == "applied"


def test_cli_stream_and_process_error_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "stdin", StringIO("streamed"))
    assert cli._input_text("-", context="test") == "streamed"  # noqa: SLF001
    with pytest.raises(fgraph.FormatError, match="cannot be read"):
        cli._input_text(str(tmp_path / "missing"), context="test")  # noqa: SLF001

    lines_path = tmp_path / "lines.ndjson"
    lines_path.write_text("one\ntwo\n", encoding="utf-8")
    with cli._input_lines(str(lines_path), context="test") as lines:  # noqa: SLF001
        assert list(lines) == ["one\n", "two\n"]

    payload_path = tmp_path / "payload.ndjson"
    payload_path.write_text('\n{"id":"one"}\n', encoding="utf-8")
    with cli._batch_payloads(f"@{payload_path}", context="test") as payloads:  # noqa: SLF001
        assert list(payloads) == [{"id": "one"}]
    with cli._batch_payloads('{"id":"inline"}', context="test") as payloads:  # noqa: SLF001
        assert list(payloads) == [{"id": "inline"}]
    with (
        pytest.raises(fgraph.FormatError, match="cannot be read"),
        cli._batch_payloads(f"@{tmp_path / 'missing.ndjson'}", context="test"),  # noqa: SLF001
    ):
        pass
    with (
        pytest.raises(fgraph.TypeError, match="is empty"),
        cli._batch_payloads("   \n", context="test") as payloads,  # noqa: SLF001
    ):
        list(payloads)

    class SnapshotlessDb:
        def snapshot(self) -> None:
            return None

    monkeypatch.setattr(cli, "_open", lambda *_args, **_kwargs: nullcontext(SnapshotlessDb()))
    cli.snapshot_command(db=None)

    def exit_app(*_args: Any, **_kwargs: Any) -> None:
        raise TyperExit(7)

    monkeypatch.setattr(cli, "app", exit_app)
    with pytest.raises(SystemExit) as exited:
        cli.main()
    assert exited.value.code == 7
