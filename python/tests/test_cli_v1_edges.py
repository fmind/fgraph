"""CLI coverage for API v1 pagination, recovery, and typed error paths."""

from __future__ import annotations

import json
import subprocess
import sys
from contextlib import nullcontext
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


def test_cli_default_database_path_is_facts_graph(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FGRAPH_DB", raising=False)

    result = runner.invoke(
        cli.app,
        ["init"],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )

    assert result.exit_code == 0, result.output
    assert Path("facts.fgraph").is_file()
    assert not Path("fgraph.db").exists()

    environment_path = tmp_path / "environment.fgraph"
    monkeypatch.setenv("FGRAPH_DB", str(environment_path))
    result = runner.invoke(cli.app, ["init"], env={"FGRAPH_CLOCK": "1767225600000000"})
    assert result.exit_code == 0, result.output
    assert environment_path.is_file()

    explicit_path = tmp_path / "explicit.fgraph"
    result = runner.invoke(
        cli.app,
        ["init", "--db", str(explicit_path)],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert result.exit_code == 0, result.output
    assert explicit_path.is_file()


def test_cli_explicit_database_path_overrides_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    environment_path = tmp_path / "environment.fgraph"
    monkeypatch.setenv("FGRAPH_DB", str(environment_path))

    for placement in ("global", "command"):
        explicit_path = tmp_path / f"{placement}.fgraph"
        arguments = ["--db", str(explicit_path), "init"]
        if placement == "command":
            arguments = ["init", "--db", str(explicit_path)]
        result = runner.invoke(
            cli.app,
            arguments,
            env={"FGRAPH_CLOCK": "1767225600000000"},
        )
        assert result.exit_code == 0, result.output
        assert explicit_path.is_file()

    assert not environment_path.exists()

    rejected = runner.invoke(
        cli.app,
        ["--db", "", "init"],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert rejected.exit_code == 1
    assert isinstance(rejected.exception, fgraph.FormatError)
    assert "database path is empty" in str(rejected.exception)
    assert not environment_path.exists()


def test_cli_legacy_implicit_default_remains_compatible(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FGRAPH_DB", raising=False)

    legacy = runner.invoke(
        cli.app,
        ["init", "--db", "fgraph.db"],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert legacy.exit_code == 0, legacy.output

    implicit = runner.invoke(
        cli.app,
        ["init"],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert implicit.exit_code == 0, implicit.output
    assert not Path("facts.fgraph").exists()

    empty_environment = runner.invoke(
        cli.app,
        ["init"],
        env={"FGRAPH_CLOCK": "1767225600000000", "FGRAPH_DB": ""},
    )
    assert empty_environment.exit_code == 1
    assert isinstance(empty_environment.exception, fgraph.FormatError)
    assert "database path is empty" in str(empty_environment.exception)
    assert not Path("facts.fgraph").exists()

    invalid_syntax = runner.invoke(
        cli.app,
        ["init", "--definitely-invalid"],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert invalid_syntax.exit_code == 2

    Path("facts.fgraph").touch()
    unclaimed = runner.invoke(
        cli.app,
        ["init"],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert unclaimed.exit_code == 1
    assert isinstance(unclaimed.exception, fgraph.FormatError)
    assert "not an initialized fgraph database" in str(unclaimed.exception)
    assert Path("facts.fgraph").stat().st_size == 0
    Path("facts.fgraph").unlink()

    environment_path = tmp_path / "environment.fgraph"
    environment = runner.invoke(
        cli.app,
        ["init"],
        env={
            "FGRAPH_CLOCK": "1767225600000000",
            "FGRAPH_DB": str(environment_path),
        },
    )
    assert environment.exit_code == 0, environment.output

    explicit = runner.invoke(
        cli.app,
        ["init", "--db", "facts.fgraph"],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert explicit.exit_code == 0, explicit.output

    both = runner.invoke(
        cli.app,
        ["init"],
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )
    assert both.exit_code == 0, both.output


@pytest.mark.parametrize(
    "arguments",
    [
        ["tx", "not-an-int"],
        ["diff", "not-an-int", "64"],
        ["diff", "64", "not-an-int"],
        ["undo", "not-an-int"],
        ["declare", "person/name", "--many", "--one"],
        ["declare", "person/name", "--unique", "--not-unique"],
        ["declare", "person/name", "--nohistory", "--history"],
        ["shape", "person", "--closed", "--open"],
        ["excise", "person"],
        ["add", "{}", "--batch-size", "0"],
        ["add", "{}", "--batch-size", "10001"],
        ["add", "{}", "--operation-id", "one", "--operation-id-prefix", "many"],
        ["add", "{}", "--operation-id-prefix", "many"],
        ["mcp", "--write", "--read-only"],
    ],
)
def test_cli_rejects_semantic_usage_before_opening_database(
    arguments: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FGRAPH_DB", raising=False)
    legacy = Path("fgraph.db")
    legacy.touch()

    result = runner.invoke(
        cli.app,
        arguments,
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )

    assert result.exit_code == 2, (arguments, result.output, result.exception)
    assert legacy.read_bytes() == b""
    assert not Path("facts.fgraph").exists()


@pytest.mark.parametrize(
    "arguments",
    [
        ["tx", str(2**63)],
        ["tx", "--", str(-(2**63) - 1)],
        ["diff", str(2**63), "64"],
        ["diff", "64", "--", str(-(2**63) - 1)],
        ["undo", str(2**63)],
    ],
)
def test_cli_rejects_out_of_range_transactions_before_opening_database(
    arguments: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FGRAPH_DB", raising=False)
    legacy = Path("fgraph.db")
    legacy.touch()

    result = runner.invoke(
        cli.app,
        arguments,
        env={"FGRAPH_CLOCK": "1767225600000000"},
    )

    assert result.exit_code == 1, (arguments, result.output, result.exception)
    assert isinstance(result.exception, fgraph.TypeError)
    assert "signed 64-bit" in str(result.exception)
    assert legacy.read_bytes() == b""
    assert not Path("facts.fgraph").exists()


def test_cli_maps_inverse_declaration_flags_explicitly(tmp_path: Path) -> None:
    database = tmp_path / "inverse.fgraph"

    _invoke(
        [
            "declare",
            "edge/value",
            "--ref",
            "--one",
            "--not-unique",
            "--history",
            "--db",
            str(database),
        ]
    )
    manifest = json.loads(_invoke(["schema", "edge/value", "--db", str(database)]).stdout)

    assert manifest["attributes"][0]["declared"] == {
        "many": False,
        "nohistory": False,
        "type": "ref",
        "unique": False,
    }


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


def test_cli_snapshot_streams_directly_to_stdout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class StreamingSnapshotDb:
        def snapshot(self, writer: Any) -> None:
            writer.write("snapshot-line\n")

    monkeypatch.setattr(cli, "_open", lambda *_args, **_kwargs: nullcontext(StreamingSnapshotDb()))

    cli.snapshot_command(db=None)

    assert capsys.readouterr().out == "snapshot-line\n"


def test_cli_restore_streams_input_lines(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = tmp_path / "snapshot.ndjson"
    snapshot.write_text("header\nfooter\n", encoding="utf-8")
    consumed: list[str] = []

    class StreamingRestoreDb:
        def restore(self, lines: Any) -> None:
            assert not isinstance(lines, str)
            consumed.extend(lines)

        def schema(self) -> dict[str, int]:
            return {"basis_tx": 64}

    monkeypatch.setattr(cli, "_open", lambda *_args, **_kwargs: nullcontext(StreamingRestoreDb()))

    cli.restore_command(source=str(snapshot), db=None, json_output=True)

    assert consumed == ["header\n", "footer\n"]


def test_cli_tail_writes_each_private_iterator_event_before_reading_the_next(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    written: list[str] = []
    events = [
        {"fgraph": "event/1", "event": "first"},
        {"fgraph": "event/1", "event": "second"},
    ]

    class StreamingTailDb:
        def _iter_event_records(self, _since: int) -> Any:
            yield 65, events[0]
            assert len(written) == 1
            yield 66, events[1]

    monkeypatch.setattr(cli, "_open", lambda *_args, **_kwargs: nullcontext(StreamingTailDb()))
    monkeypatch.setattr(cli.typer, "echo", written.append)

    cli.tail(since=64, follow=False, db=None)

    assert written == [json.dumps(event, separators=(",", ":"), sort_keys=True) for event in events]


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
    assert conflicting.exit_code == 2, conflicting.output
    assert not database.exists()

    missing_batch_size = runner.invoke(
        cli.app,
        ["add", "-", "--operation-id-prefix", "import", "--db", str(database)],
        input=stream,
    )
    assert missing_batch_size.exit_code == 2, missing_batch_size.output
    assert not database.exists()

    for option in (["--operation-id", "import:one"], ["--if-basis-tx", "64"]):
        spans_transactions = runner.invoke(
            cli.app,
            ["add", "-", *option, "--db", str(database)],
            input=stream,
        )
        assert isinstance(spans_transactions.exception, fgraph.TypeError)
        assert not database.exists()

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
        def snapshot(self, _writer: Any) -> None:
            return None

    monkeypatch.setattr(cli, "_open", lambda *_args, **_kwargs: nullcontext(SnapshotlessDb()))
    cli.snapshot_command(db=None)

    def exit_app(*_args: Any, **_kwargs: Any) -> None:
        raise TyperExit(7)

    monkeypatch.setattr(cli, "app", exit_app)
    with pytest.raises(SystemExit) as exited:
        cli.main()
    assert exited.value.code == 7
