#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "rich>=15.0.0",
#     "typer>=0.27.1",
# ]
# ///
"""Replay seeded operation traces through every fgraph runtime and compare them."""

from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import subprocess
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEEDS = [7, 42, 202609]
ERROR_NAMES = re.compile(
    r"\b(NotFound|Conflict|SchemaError|TypeError|ErrType|QueryError|FormatError|ReadOnly|TooLarge|Unsupported)\b"
)
RUNTIME_COMMANDS = {
    "python": ["uv", "run", "--project", str(ROOT / "python"), "fgraph"],
    "go": [str(ROOT / "go" / "bin" / "fgraph")],
    "typescript": ["node", str(ROOT / "typescript" / "dist" / "cli.js")],
}

app = typer.Typer(add_completion=False)
out = Console()
err = Console(stderr=True)


def _invoke(
    command: Sequence[str],
    arguments: Sequence[str],
    *,
    stdin: bytes | None = None,
    succeeds: bool = True,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        [*command, *arguments],
        input=stdin,
        capture_output=True,
        env={
            **os.environ,
            "FGRAPH_CLOCK": "1767225600000000",
            "FGRAPH_EVENT_SEED": "fgraph-differential-v2",
            **(environment or {}),
        },
        check=False,
    )
    if succeeds and result.returncode != 0:
        raise RuntimeError(
            f"command {[*command, *arguments]!r} failed ({result.returncode}): {result.stderr.decode(errors='replace')}"
        )
    if not succeeds and result.returncode == 0:
        raise AssertionError(f"command {[*command, *arguments]!r} unexpectedly succeeded")
    return result


def _json_output(result: subprocess.CompletedProcess[bytes]) -> Any:
    return json.loads(result.stdout)


def _typed_error(result: subprocess.CompletedProcess[bytes], expected: str, *, context: str) -> str:
    stderr = result.stderr.decode(errors="replace")
    match = ERROR_NAMES.search(stderr)
    if match is None:
        raise AssertionError(f"{context} did not emit a typed fgraph error name: {stderr.strip()!r}")
    actual = match.group(1)
    if actual != expected:
        raise AssertionError(f"{context} emitted {actual}, expected {expected}: {stderr.strip()!r}")
    return actual


def _difference_preview(value: Any, limit: int = 4_096) -> str:
    """Keep parity failures actionable without flooding CI logs."""
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if len(encoded) <= limit:
        return encoded
    return encoded[:limit] + "…"


def _first_difference(expected: Any, actual: Any, path: str = "$") -> str:
    """Locate the first structural mismatch in two JSON-compatible values."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        expected_keys = set(expected)
        actual_keys = set(actual)
        if expected_keys != actual_keys:
            return (
                f"{path}: missing={sorted(expected_keys - actual_keys)!r} "
                f"unexpected={sorted(actual_keys - expected_keys)!r}"
            )
        for key in sorted(expected_keys):
            if expected[key] != actual[key]:
                return _first_difference(expected[key], actual[key], f"{path}[{key!r}]")
    elif isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            return f"{path}: expected length {len(expected)}, actual length {len(actual)}"
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual, strict=True)):
            if expected_item != actual_item:
                return _first_difference(expected_item, actual_item, f"{path}[{index}]")
    return f"{path}: expected={_difference_preview(expected, 512)} actual={_difference_preview(actual, 512)}"


def _trace(seed: int, steps: int) -> bytes:
    generator = random.Random(seed)
    records: list[Any] = []
    current_status: dict[int, str] = {}
    for index in range(12):
        status = "active" if index % 2 == 0 else "paused"
        current_status[index] = status
        entity_record: dict[str, Any] = {
            "id": f"entity-{index:02d}",
            "person/email": f"user-{index:02d}@example.test",
            "person/name": f"Person {index}",
            "person/score": index,
            "person/status": status,
            "person/tags": ["project", f"group-{index % 3}"],
            "project/text": f"project decision number {index}",
            "person/vector": {"vector": [1 if axis == index % 3 else 0 for axis in range(3)]},
        }
        if index == 0:
            entity_record["person/boundary"] = 9_223_372_036_854_775_807
        elif index == 1:
            entity_record["person/boundary"] = -9_223_372_036_854_775_808
        records.append(entity_record)
    for _ in range(steps):
        index = generator.randrange(12)
        entity = f"entity-{index:02d}"
        operation = generator.randrange(5)
        if operation == 0:
            records.append({"id": entity, "person/score": generator.randrange(10_000)})
        elif operation == 1:
            status = generator.choice(["active", "paused", "archived"])
            current_status[index] = status
            records.append({"id": entity, "person/status": status})
        elif operation == 2:
            records.append(["assert", entity, "person/tags", f"seed-{generator.randrange(5)}"])
        elif operation == 3:
            target = f"entity-{generator.randrange(12):02d}"
            records.append(["assert", entity, "person/knows", {"ref": target}])
        else:
            status = current_status.get(index)
            if status is not None:
                records.append(["retract", entity, "person/status", status])
                current_status.pop(index, None)
    records.append(
        {
            "person/email": "user-03@example.test",
            "person/name": "Person Three",
            "project/text": "project decision corrected by unique upsert",
        }
    )
    return "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records).encode()


def _core_rows(database: Path) -> list[list[Any]]:
    connection = sqlite3.connect(database)
    try:
        rows: list[list[Any]] = []
        for table, columns, order in (
            ("fgraph_meta", "key,typeof(value),quote(value)", "key"),
            ("fgraph_ids", "id,quote(name),hex(gid),created_tx", "id"),
            (
                "fgraph_events",
                "tx,hex(event_hash),quote(event_data),quote(operation_id),hex(request_hash)",
                "tx",
            ),
            ("fgraph_facts", "id,e,a,typeof(v),quote(v),t,tx,ifnull(rx,'NULL')", "id"),
            ("fgraph_blobs", "hex(hash),typeof(data),quote(data)", "hash"),
        ):
            rows.extend(
                [[table, *row] for row in connection.execute(f"SELECT {columns} FROM {table} ORDER BY {order}")]
            )
        return rows
    finally:
        connection.close()


def _ndjson_records(text: str) -> list[Any]:
    # NDJSON is delimited only by LF; Unicode line separators are valid JSON string data.
    return [json.loads(line) for line in text.split("\n") if line]


def _run_runtime(runtime: str, command: Sequence[str], seed: int, trace: bytes, directory: Path) -> dict[str, Any]:
    database = directory / f"{seed}-{runtime}.db"
    common = ["--db", str(database), "--json"]
    declarations = [
        ["declare", "person/email", "--type", "text", "--unique"],
        ["declare", "person/tags", "--type", "text", "--many"],
        ["declare", "person/knows", "--ref", "--many"],
        ["declare", "person/vector", "--type", "vector", "--dims", "3"],
    ]
    for declaration in declarations:
        _invoke(command, [*declaration, *common])
    add_reports = _json_output(_invoke(command, ["add", *common, "-"], stdin=trace))
    if not isinstance(add_reports, list) or len(add_reports) < 12:
        raise AssertionError(f"seed={seed} runtime={runtime} did not report every NDJSON transaction")

    query = json.dumps(
        {
            "find": ["?e", "?score"],
            "where": [["?e", "person/score", "?score"]],
            "order": [["?score", "desc"], ["?e", "asc"]],
            "limit": 8,
        },
        separators=(",", ":"),
    )
    probes = {
        "info": ["info", *common],
        "doctor": ["doctor", *common],
        "schema": ["schema", "person/", *common],
        "entity": ["get", "entity-03", *common],
        "query": ["q", query, *common],
        "history": ["history", "entity-03", *common],
        "why": ["why", "entity-03", "person/name", *common],
        "keyword_search": ["search", "--text", "project decision", *common],
        "semantic_search": [
            "search",
            "--vector",
            "[1,0,0]",
            "--vector-attribute",
            "person/vector",
            *common,
        ],
        "hybrid_search": [
            "search",
            "--text",
            "project decision",
            "--vector",
            "[1,0,0]",
            "--vector-attribute",
            "person/vector",
            *common,
        ],
    }
    actual = {name: _json_output(_invoke(command, arguments)) for name, arguments in probes.items()}
    size = actual["info"].get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise AssertionError(f"seed={seed} runtime={runtime} reported invalid file size {size!r}")
    # SQLite pager layouts are deliberately non-normative across engine builds;
    # retain the field-shape check without comparing unrelated byte counts.
    actual["info"]["size"] = "<runtime-file-size>"
    event_bytes = _invoke(command, ["tail", "--since", "64", "--db", str(database)]).stdout
    event_text = event_bytes.decode()
    actual["events"] = _ndjson_records(event_text)
    snapshot_bytes = _invoke(command, ["snapshot", "--db", str(database)]).stdout
    actual["snapshot"] = _ndjson_records(snapshot_bytes.decode())
    actual["core"] = _core_rows(database)

    first_entity_report = add_reports[3]
    latest_report = next(
        (report for report in reversed(add_reports) if isinstance(report, dict) and report.get("tx") is not None),
        None,
    )
    if not isinstance(first_entity_report, dict) or first_entity_report.get("tx") is None or latest_report is None:
        raise AssertionError(f"seed={seed} runtime={runtime} omitted transaction receipts")
    as_of = first_entity_report["tx"]
    latest = latest_report["tx"]
    actual["as_of_entity"] = _json_output(_invoke(command, ["get", "entity-03", "--at", str(as_of), *common]))
    actual["as_of_query"] = _json_output(_invoke(command, ["q", query, "--at", str(as_of), *common]))
    actual["diff"] = _json_output(_invoke(command, ["diff", str(as_of), str(latest), *common]))

    event_path = directory / f"{seed}-{runtime}.events.ndjson"
    applied = directory / f"{seed}-{runtime}-applied.db"
    event_path.write_bytes(event_bytes)
    _invoke(command, ["apply", str(event_path), "--db", str(applied), "--json"])
    applied_events = _invoke(command, ["tail", "--since", "64", "--db", str(applied)]).stdout.decode()
    actual["applied_events"] = _ndjson_records(applied_events)
    actual["applied_core"] = _core_rows(applied)
    if actual["applied_events"] != actual["events"]:
        raise AssertionError(f"seed={seed} runtime={runtime} event apply changed the portable stream")

    snapshot_path = directory / f"{seed}-{runtime}.snapshot.ndjson"
    restored = directory / f"{seed}-{runtime}-restored.db"
    snapshot_path.write_bytes(snapshot_bytes)
    _invoke(command, ["restore", str(snapshot_path), "--db", str(restored), "--json"])
    restored_snapshot = _invoke(command, ["snapshot", "--db", str(restored)]).stdout.decode()
    actual["restored_snapshot"] = _ndjson_records(restored_snapshot)
    actual["restored_core"] = _core_rows(restored)
    if actual["restored_snapshot"] != actual["snapshot"] or actual["restored_core"] != actual["core"]:
        raise AssertionError(f"seed={seed} runtime={runtime} snapshot restore differs")

    invalid = _invoke(
        command,
        ["q", '{"find":["?e"],"where":[],"bogus":true}', *common],
        succeeds=False,
    )
    actual["invalid_query_error"] = _typed_error(invalid, "QueryError", context="invalid query")

    over_budget = _invoke(
        command,
        ["q", query, *common],
        succeeds=False,
        environment={"FGRAPH_QUERY_BUDGET": "1"},
    )
    actual["query_budget_error"] = _typed_error(over_budget, "TooLarge", context="query budget")
    return actual


def _check_artifacts(runtimes: Sequence[str]) -> None:
    for runtime in runtimes:
        executable = RUNTIME_COMMANDS[runtime][0]
        if os.sep in executable and not Path(executable).is_file():
            raise FileNotFoundError(f"{runtime} artifact {executable!r} is missing; run mise run build first")


@app.command()
def main(
    seeds: Annotated[list[int] | None, typer.Option("--seed", help="Repeatable deterministic seed.")] = None,
    steps: Annotated[int, typer.Option(min=1, max=500)] = 30,
) -> None:
    """Fail on the first cross-runtime semantic or physical mismatch."""
    selected_seeds = seeds or DEFAULT_SEEDS
    runtimes = list(RUNTIME_COMMANDS)
    _check_artifacts(runtimes)
    with tempfile.TemporaryDirectory(prefix="fgraph-differential-") as raw_directory:
        directory = Path(raw_directory)
        for seed in selected_seeds:
            trace = _trace(seed, steps)
            results = {
                runtime: _run_runtime(runtime, RUNTIME_COMMANDS[runtime], seed, trace, directory)
                for runtime in runtimes
            }
            baseline = results["python"]
            for runtime in runtimes[1:]:
                for surface, expected in baseline.items():
                    if results[runtime][surface] != expected:
                        raise AssertionError(
                            f"seed={seed} runtime={runtime} surface={surface} differs; rerun with --seed {seed}\n"
                            f"difference={_first_difference(expected, results[runtime][surface])}\n"
                            f"expected={_difference_preview(expected)}\n"
                            f"actual={_difference_preview(results[runtime][surface])}"
                        )
            out.print(
                json.dumps({"seed": seed, "steps": steps, "status": "ok"}, separators=(",", ":")),
                markup=False,
                soft_wrap=True,
            )


if __name__ == "__main__":
    try:
        app()
    except Exception:
        err.print_exception(show_locals=False)
        raise typer.Exit(code=1) from None
