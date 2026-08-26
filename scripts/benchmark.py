#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "rich>=15.0.0",
#     "typer>=0.27.1",
# ]
# ///
"""Run the reproducible cross-runtime fgraph reference workload."""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import tempfile
import time
import tomllib
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console

ROOT = Path(__file__).resolve().parents[1]
VECTOR_DIMS = 384
DEFAULT_SIZES = [1_000, 10_000, 100_000]
RUNTIME_COMMANDS = {
    "python": [str(ROOT / "python" / ".venv" / "bin" / "fgraph")],
    "go": [str(ROOT / "go" / "bin" / "fgraph")],
    "typescript": ["node", str(ROOT / "typescript" / "dist" / "cli.js")],
}
COLORS = {"python": "#646CFF", "go": "#22D3EE", "typescript": "#FB923C"}
DASHES = {"python": "", "go": "8 5", "typescript": "2 4"}
BACKGROUND = "#0F172A"
PANEL = "#1E293B"
FOREGROUND = "#F8FAFC"
MUTED = "#CBD5E1"
BORDER = "#334155"
HEADING_FONT = "'Outfit Variable', Outfit, sans-serif"
BODY_FONT = "'Inter Variable', Inter, sans-serif"
BENCHMARK_FORMAT = "fgraph-benchmark/1"
BENCHMARK_OPERATIONS = {
    "apply",
    "datoms_page",
    "file_size",
    "history",
    "ingest_batched",
    "keyword_search",
    "point_get",
    "query_join",
    "query_scalar_filter",
    "restore",
    "snapshot",
    "tail",
    "vector_search_384",
}
READ_OPERATIONS = {
    "datoms_page",
    "history",
    "keyword_search",
    "point_get",
    "query_join",
    "query_scalar_filter",
    "vector_search_384",
}

app = typer.Typer(add_completion=False, no_args_is_help=False)
out = Console()
err = Console(stderr=True)


def _vector(index: int) -> list[float]:
    values = [0.0] * VECTOR_DIMS
    values[index % VECTOR_DIMS] = 1.0
    return values


def _write_corpus(path: Path, count: int) -> None:
    with path.open("w", encoding="utf-8") as stream:
        for index in range(count):
            entity: dict[str, Any] = {
                "id": f"doc-{index:09d}",
                "doc/group": index % 100,
                "doc/text": f"common benchmark token document {index}",
                "doc/title": f"Benchmark document {index}",
            }
            if index % 20 == 0:
                entity["doc/embedding"] = {"vector": _vector(index)}
            stream.write(json.dumps(entity, separators=(",", ":")))
            stream.write("\n")


def _run(
    command: Sequence[str],
    *arguments: str,
    stdin: Any = None,
    stdout: Any = subprocess.DEVNULL,
) -> float:
    environment = {**os.environ, "FGRAPH_CLOCK": "1767225600000000", "FGRAPH_EVENT_SEED": "benchmark"}
    started = time.perf_counter()
    completed = subprocess.run(
        [*command, *arguments],
        stdin=stdin,
        stdout=stdout,
        stderr=subprocess.PIPE,
        check=False,
        env=environment,
    )
    elapsed = time.perf_counter() - started
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace")[-4_000:]
        raise RuntimeError(f"{' '.join([*command, *arguments])} failed ({completed.returncode}): {detail}")
    return elapsed


def _output(command: Sequence[str], *, cwd: Path = ROOT) -> str:
    completed = subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)
    return (completed.stdout or completed.stderr).strip()


def _sqlite_versions() -> dict[str, str]:
    go_declaration = _output(["go", "doc", "modernc.org/sqlite/lib.SQLITE_VERSION"], cwd=ROOT / "go")
    try:
        go_version = go_declaration.rsplit('"', maxsplit=2)[1]
    except IndexError as exc:
        raise RuntimeError("cannot read the Go runtime's embedded SQLite version") from exc
    typescript_version = _output(
        [
            "node",
            "--input-type=module",
            "--eval",
            "import Database from './typescript/node_modules/better-sqlite3/lib/index.js'; "
            "const db = new Database(':memory:'); "
            "console.log(db.prepare('select sqlite_version() AS version').get().version); "
            "db.close();",
        ]
    )
    return {
        "python": _output(
            [
                str(ROOT / "python" / ".venv" / "bin" / "python"),
                "-c",
                "import sqlite3; print(sqlite3.sqlite_version)",
            ]
        ),
        "go": go_version,
        "typescript": typescript_version,
    }


def _source_digest() -> str:
    paths = [
        ROOT / "docs" / "content" / "docs" / "spec.md",
        ROOT / "mise.toml",
        ROOT / "mise.lock",
        ROOT / "python" / ".python-version",
        ROOT / "python" / "pyproject.toml",
        ROOT / "python" / "uv.lock",
        ROOT / "go" / "go.mod",
        ROOT / "go" / "go.sum",
        ROOT / "typescript" / "package.json",
        ROOT / "typescript" / "package-lock.json",
        ROOT / "typescript" / "tsconfig.json",
        Path(__file__),
        Path(__file__).with_suffix(".py.lock"),
    ]
    paths.extend((ROOT / "typescript").glob("tsconfig*.json"))
    for directory, pattern in (
        (ROOT / "python" / "src", "*.py"),
        (ROOT / "go", "*.go"),
        (ROOT / "typescript" / "src", "*.ts"),
    ):
        paths.extend(path for path in directory.rglob(pattern) if not path.name.endswith("_test.go"))
    digest = hashlib.sha256()
    for path in sorted(set(paths)):
        relative = path.relative_to(ROOT).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        data = path.read_bytes()
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return f"sha256:{digest.hexdigest()}"


def _project_version() -> str:
    project = tomllib.loads((ROOT / "python" / "pyproject.toml").read_text(encoding="utf-8"))
    return str(project["project"]["version"])


def _benchmark_metadata(*, sizes: Sequence[int], batch_size: int, trials: int) -> dict[str, Any]:
    return {
        "record": "metadata",
        "fgraph": BENCHMARK_FORMAT,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "fgraph_version": _project_version(),
        "source_revision": _output(["git", "rev-parse", "HEAD"]),
        "source_tree": "dirty" if _output(["git", "status", "--porcelain", "--untracked-files=all"]) else "clean",
        "source_digest": _source_digest(),
        "runtime_versions": {
            "python": _output([str(ROOT / "python" / ".venv" / "bin" / "python"), "--version"]),
            "go": _output(["go", "version"]),
            "node": _output(["node", "--version"]),
        },
        "sqlite_versions": _sqlite_versions(),
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "workload": {
            "sizes": list(sizes),
            "batch_size": batch_size,
            "trials": trials,
            "vector_dimensions": VECTOR_DIMS,
            "measurement": "fresh-process CLI end-to-end",
        },
    }


def _measure_read(command: Sequence[str], arguments: Sequence[str], trials: int) -> dict[str, Any]:
    samples = [_run(command, *arguments) for _ in range(trials)]
    return {
        "seconds": round(statistics.median(samples), 6),
        "min_seconds": round(min(samples), 6),
        "max_seconds": round(max(samples), 6),
        "trials": trials,
        "process": "fresh",
    }


def _record(runtime: str, size: int, operation: str, seconds: float, **extra: Any) -> dict[str, Any]:
    return {
        "runtime": runtime,
        "entities": size,
        "measurement": "cli_end_to_end",
        "operation": operation,
        "seconds": round(seconds, 6),
        **extra,
    }


def _benchmark_runtime(
    runtime: str,
    command: Sequence[str],
    size: int,
    directory: Path,
    *,
    batch_size: int,
    trials: int,
) -> list[dict[str, Any]]:
    corpus = directory / "corpus.ndjson"
    _write_corpus(corpus, size)
    database = directory / "graph.db"
    snapshot_path = directory / "snapshot.ndjson"
    restored = directory / "restored.db"
    event_path = directory / "events.ndjson"
    replica = directory / "replica.db"

    _run(
        command,
        "declare",
        "doc/embedding",
        "--type",
        "vector",
        "--dims",
        str(VECTOR_DIMS),
        "--db",
        str(database),
        "--json",
    )
    with corpus.open("rb") as stream:
        ingest = _run(
            command,
            "add",
            "--batch-size",
            str(batch_size),
            "--operation-id-prefix",
            f"benchmark-{size}",
            "--db",
            str(database),
            "--json",
            "-",
            stdin=stream,
        )

    query = json.dumps(
        {
            "find": ["?e"],
            "where": [["?e", "doc/group", 77]],
            "order": [["?e", "asc"]],
            "limit": 10,
        },
        separators=(",", ":"),
    )
    join = json.dumps(
        {
            "find": ["?e", "?title"],
            "where": [["?e", "doc/group", 77], ["?e", "doc/title", "?title"]],
            "order": [["?e", "asc"]],
            "limit": 10,
        },
        separators=(",", ":"),
    )
    vector = json.dumps(_vector(0), separators=(",", ":"))
    reads = {
        "point_get": ["get", f"doc-{size - 1:09d}", "--db", str(database), "--json"],
        "query_scalar_filter": ["q", query, "--db", str(database), "--json"],
        "query_join": ["q", join, "--db", str(database), "--json"],
        "keyword_search": ["search", "--text", "common benchmark", "--db", str(database), "--json"],
        "vector_search_384": [
            "search",
            "--vector",
            vector,
            "--vector-attribute",
            "doc/embedding",
            "--db",
            str(database),
            "--json",
        ],
        "datoms_page": ["datoms", "avet", "--components", '["doc/group",77]', "--db", str(database), "--json"],
        "history": ["history", "doc-000000000", "--db", str(database), "--json"],
    }
    records = [
        _record(
            runtime,
            size,
            "ingest_batched",
            ingest,
            batch_size=batch_size,
            entities_per_second=round(size / ingest, 2),
        ),
        _record(runtime, size, "file_size", 0.0, bytes=database.stat().st_size),
    ]
    for operation, arguments in reads.items():
        measurement = _measure_read(command, arguments, trials)
        records.append(_record(runtime, size, operation, measurement.pop("seconds"), **measurement))

    with snapshot_path.open("wb") as stream:
        snapshot_time = _run(command, "snapshot", "--db", str(database), stdout=stream)
    restore_time = _run(command, "restore", str(snapshot_path), "--db", str(restored), "--json")
    records.append(_record(runtime, size, "snapshot", snapshot_time, bytes=snapshot_path.stat().st_size))
    records.append(_record(runtime, size, "restore", restore_time))

    with event_path.open("wb") as stream:
        tail_time = _run(command, "tail", "--since", "64", "--db", str(database), stdout=stream)
    apply_time = _run(command, "apply", str(event_path), "--db", str(replica), "--json")
    records.append(_record(runtime, size, "tail", tail_time, bytes=event_path.stat().st_size))
    records.append(_record(runtime, size, "apply", apply_time))
    return records


def _svg_chart(
    title: str,
    description: str,
    series: dict[str, list[tuple[float, float]]],
    *,
    x_label: str,
    y_label: str,
    log_x: bool = False,
) -> str:
    width, height = 920, 560
    left, top, right, bottom = 90, 55, 30, 125
    plot_width, plot_height = width - left - right, height - top - bottom
    points = [point for values in series.values() for point in values]
    x_values = [math.log10(point[0]) if log_x else point[0] for point in points]
    y_values = [point[1] for point in points]
    x_min, x_max = min(x_values), max(x_values)
    y_max = max(y_values) * 1.1 or 1.0

    def x_position(value: float) -> float:
        normalized = math.log10(value) if log_x else value
        return left + (normalized - x_min) / max(x_max - x_min, 1) * plot_width

    def y_position(value: float) -> float:
        return top + plot_height - value / y_max * plot_height

    def marker(runtime: str, x: float, y: float, color: str) -> str:
        if runtime == "go":
            return f'<rect x="{x - 5:.1f}" y="{y - 5:.1f}" width="10" height="10" fill="{color}"/>'
        if runtime == "typescript":
            return (
                f'<path d="M {x:.1f} {y - 6:.1f} L {x + 6:.1f} {y:.1f} '
                f'L {x:.1f} {y + 6:.1f} L {x - 6:.1f} {y:.1f} Z" fill="{color}"/>'
            )
        return f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}"/>'

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-labelledby="chart-title chart-description" viewBox="0 0 {width} {height}">',
        f'<title id="chart-title">{title}</title><desc id="chart-description">{description}</desc>',
        f'<rect width="{width}" height="{height}" fill="{BACKGROUND}"/>',
        f'<rect x="{left}" y="{top}" width="{plot_width}" height="{plot_height}" fill="{PANEL}" stroke="{BORDER}"/>',
        f'<text x="{width / 2}" y="28" text-anchor="middle" '
        f'font-family="{HEADING_FONT}" font-size="20" font-weight="600" fill="{FOREGROUND}">{title}</text>',
    ]
    for step in range(6):
        value = y_max * step / 5
        y = y_position(value)
        lines.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_width}" y2="{y:.1f}" stroke="{BORDER}"/>')
        lines.append(
            f'<text x="{left - 10}" y="{y + 4:.1f}" text-anchor="end" '
            f'font-family="{BODY_FONT}" font-size="12" fill="{MUTED}">'
            f"{value:,.0f}</text>"
            if y_max >= 100
            else f'<text x="{left - 10}" y="{y + 4:.1f}" text-anchor="end" '
            f'font-family="{BODY_FONT}" font-size="12" fill="{MUTED}">{value:.1f}</text>'
        )
    for runtime, values in series.items():
        coordinates = " ".join(f"{x_position(x):.1f},{y_position(y):.1f}" for x, y in values)
        color = COLORS[runtime]
        dash = DASHES[runtime]
        dash_attribute = "" if dash == "" else f' stroke-dasharray="{dash}"'
        lines.append(
            f'<polyline points="{coordinates}" fill="none" stroke="{color}" stroke-width="3"{dash_attribute}/>'
        )
        for x, y in values:
            lines.append(marker(runtime, x_position(x), y_position(y), color))
    lines.extend(
        f'<text x="{x_position(x):.1f}" y="{top + plot_height + 24}" text-anchor="middle" '
        f'font-family="{BODY_FONT}" font-size="12" fill="{MUTED}">{int(x):,}</text>'
        for x in sorted({point[0] for point in points})
    )
    legend_x = left
    for runtime in series:
        color = COLORS[runtime]
        dash = DASHES[runtime]
        dash_attribute = "" if dash == "" else f' stroke-dasharray="{dash}"'
        lines.append(
            f'<line x1="{legend_x}" y1="{height - 23}" x2="{legend_x + 24}" y2="{height - 23}" '
            f'stroke="{color}" stroke-width="3"{dash_attribute}/>'
        )
        lines.append(marker(runtime, legend_x + 12, height - 23, color))
        lines.append(
            f'<text x="{legend_x + 34}" y="{height - 18}" font-family="{BODY_FONT}" '
            f'font-size="13" fill="{FOREGROUND}">{runtime}</text>'
        )
        legend_x += 150
    lines.extend(
        [
            f'<text x="{left + plot_width / 2}" y="{height - 68}" text-anchor="middle" '
            f'font-family="{BODY_FONT}" font-size="13" fill="{MUTED}">{x_label}</text>',
            f'<text x="20" y="{top + plot_height / 2}" '
            f'transform="rotate(-90 20 {top + plot_height / 2})" text-anchor="middle" '
            f'font-family="{BODY_FONT}" font-size="13" fill="{MUTED}">{y_label}</text>',
            "</svg>",
        ]
    )
    return "\n".join(lines) + "\n"


def _write_charts(records: Sequence[dict[str, Any]], directory: Path) -> None:
    observations = [record for record in records if record.get("record") != "metadata"]
    directory.mkdir(parents=True, exist_ok=True)
    ingest: dict[str, list[tuple[float, float]]] = {}
    for runtime in RUNTIME_COMMANDS:
        ingest[runtime] = sorted(
            (
                (float(record["entities"]), float(record["entities_per_second"]))
                for record in observations
                if record["runtime"] == runtime and record["operation"] == "ingest_batched"
            ),
            key=lambda point: point[0],
        )
    (directory / "ingest-throughput.svg").write_text(
        _svg_chart(
            "Batched ingest throughput",
            "Entities ingested per second using bounded transactions.",
            ingest,
            x_label="entities (log scale)",
            y_label="entities / second",
            log_x=True,
        ),
        encoding="utf-8",
    )

    largest = max(int(record["entities"]) for record in observations)
    reads: dict[str, list[tuple[float, float]]] = {}
    operations = ["point_get", "query_scalar_filter", "query_join", "keyword_search", "vector_search_384"]
    for runtime in RUNTIME_COMMANDS:
        reads[runtime] = [
            (
                float(index + 1),
                float(
                    next(
                        record["seconds"]
                        for record in observations
                        if record["runtime"] == runtime
                        and record["entities"] == largest
                        and record["operation"] == operation
                    )
                ),
            )
            for index, operation in enumerate(operations)
        ]
    (directory / "read-latency.svg").write_text(
        _svg_chart(
            f"Fresh-process read latency at {largest:,} entities",
            "Median end-to-end CLI latency: point get, scalar filter, connected join, "
            "keyword search, and vector search.",
            reads,
            x_label="1 get · 2 scalar filter · 3 join · 4 keyword · 5 vector",
            y_label="seconds (median)",
        ),
        encoding="utf-8",
    )


def _write_records(path: Path, records: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )


def _read_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid benchmark record at {path}:{line_number}: {exc.msg}") from exc
        if not isinstance(record, dict):
            raise ValueError(f"invalid benchmark record at {path}:{line_number}: expected an object")
        records.append(record)
    return records


def _source_revision_is_ancestor(revision: object) -> bool:
    if not isinstance(revision, str):
        return False
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", revision, "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    return result.returncode == 0


def _nonnegative_number(value: object) -> bool:
    return (
        isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)) and value >= 0
    )


def _verify_records(
    records: Sequence[dict[str, Any]],
    metadata: dict[str, Any],
    *,
    runtimes: Sequence[str],
    sizes: Sequence[int],
) -> None:
    metadata_records = [record for record in records if record.get("record") == "metadata"]
    if len(metadata_records) != 1 or records[0] is not metadata_records[0]:
        raise ValueError("benchmark output must start with exactly one provenance metadata record")
    prior = metadata_records[0]
    for field in (
        "fgraph",
        "fgraph_version",
        "source_tree",
        "source_digest",
        "runtime_versions",
        "sqlite_versions",
        "platform",
        "workload",
    ):
        if prior.get(field) != metadata[field]:
            raise ValueError(f"benchmark metadata {field} does not match the current candidate")
    source_revision = prior.get("source_revision")
    if not isinstance(source_revision, str):
        raise ValueError("benchmark metadata source_revision is missing")
    if not _source_revision_is_ancestor(source_revision):
        raise ValueError("benchmark source revision is not an ancestor of the current candidate")

    observations = [record for record in records if record.get("record") != "metadata"]
    expected_groups = {(runtime, size) for runtime in runtimes for size in sizes}
    actual_groups: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for record in observations:
        runtime = record.get("runtime")
        entities = record.get("entities")
        operation = record.get("operation")
        if not isinstance(runtime, str) or not isinstance(entities, int) or not isinstance(operation, str):
            raise ValueError("benchmark observation is missing runtime, integer entities, or operation")
        if record.get("measurement") != "cli_end_to_end" or not _nonnegative_number(record.get("seconds")):
            raise ValueError("benchmark observation has an invalid measurement or elapsed time")
        if operation in READ_OPERATIONS:
            minimum = record.get("min_seconds")
            maximum = record.get("max_seconds")
            if (
                record.get("process") != "fresh"
                or record.get("trials") != metadata["workload"]["trials"]
                or not _nonnegative_number(minimum)
                or not _nonnegative_number(maximum)
                or not float(minimum) <= float(record["seconds"]) <= float(maximum)
            ):
                raise ValueError("benchmark read observation has invalid trial statistics")
        if operation == "ingest_batched" and (
            record.get("batch_size") != metadata["workload"]["batch_size"]
            or not _nonnegative_number(record.get("entities_per_second"))
            or float(record["entities_per_second"]) <= 0
        ):
            raise ValueError("benchmark ingest observation has invalid throughput metadata")
        if operation in {"file_size", "snapshot", "tail"} and (
            not isinstance(record.get("bytes"), int)
            or isinstance(record.get("bytes"), bool)
            or int(record["bytes"]) <= 0
        ):
            raise ValueError("benchmark size observation has invalid byte count")
        actual_groups.setdefault((runtime, entities), []).append(record)
    if set(actual_groups) != expected_groups:
        raise ValueError("benchmark output does not contain exactly the selected runtime/size groups")
    for group, group_records in actual_groups.items():
        counts = Counter(str(record["operation"]) for record in group_records)
        if set(counts) != BENCHMARK_OPERATIONS or any(count != 1 for count in counts.values()):
            raise ValueError(f"benchmark group {group} does not contain one observation per operation")


def _verify_charts(records: Sequence[dict[str, Any]], directory: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="fgraph-benchmark-charts-") as temporary:
        expected_directory = Path(temporary)
        _write_charts(records, expected_directory)
        for name in ("ingest-throughput.svg", "read-latency.svg"):
            chart = directory / name
            expected = expected_directory / name
            if not chart.is_file() or chart.read_bytes() != expected.read_bytes():
                raise ValueError(f"benchmark chart {chart} is missing or does not match the raw observations")


@app.command()
def main(
    sizes: Annotated[list[int] | None, typer.Option("--size", min=1, help="Entity count; repeat.")] = None,
    runtimes: Annotated[list[str] | None, typer.Option("--runtime", help="Runtime; repeat.")] = None,
    batch_size: Annotated[int, typer.Option("--batch-size", min=1, max=10_000)] = 500,
    trials: Annotated[int, typer.Option("--trials", min=1, max=20)] = 3,
    output: Annotated[Path | None, typer.Option("--output", help="Persist NDJSON observations.")] = None,
    charts_dir: Annotated[Path | None, typer.Option("--charts-dir", help="Generate accessible SVG charts.")] = None,
    resume: Annotated[bool, typer.Option("--resume", help="Reuse completed groups from the output file.")] = False,
    rerun: Annotated[
        bool,
        typer.Option("--rerun", help="Replace the selected groups in an existing output file."),
    ] = False,
    verify: Annotated[
        bool,
        typer.Option("--verify", help="Verify stored provenance and completeness without timing workloads."),
    ] = False,
) -> None:
    """Print one observation per line; this harness intentionally sets no pass threshold."""
    selected_sizes = sizes or DEFAULT_SIZES
    selected_runtimes = runtimes or list(RUNTIME_COMMANDS)
    unknown = sorted(set(selected_runtimes) - set(RUNTIME_COMMANDS))
    if unknown:
        raise typer.BadParameter(f"unknown runtimes {unknown}; choose from {sorted(RUNTIME_COMMANDS)}")
    if (resume or rerun or verify) and output is None:
        raise typer.BadParameter("--resume, --rerun, and --verify require --output")
    if sum((resume, rerun, verify)) > 1:
        raise typer.BadParameter("choose only one of --resume, --rerun, or --verify")
    reuse_output = (resume or rerun) and output is not None and output.is_file()
    metadata = _benchmark_metadata(sizes=selected_sizes, batch_size=batch_size, trials=trials)
    if verify:
        if output is None or not output.is_file():
            raise typer.BadParameter("--verify requires an existing --output file")
        records = _read_records(output)
        _verify_records(records, metadata, runtimes=selected_runtimes, sizes=selected_sizes)
        if charts_dir is not None:
            _verify_charts(records, charts_dir)
        out.print(f"benchmark: verified {len(selected_runtimes) * len(selected_sizes)} runtime/size groups")
        return
    records = _read_records(output) if reuse_output else [metadata]
    if reuse_output:
        prior = next((record for record in records if record.get("record") == "metadata"), None)
        if prior is None:
            raise typer.BadParameter("existing output has no provenance metadata; start a fresh run")
        for field in (
            "fgraph",
            "fgraph_version",
            "source_digest",
            "runtime_versions",
            "sqlite_versions",
            "platform",
            "workload",
        ):
            if prior.get(field) != metadata[field]:
                raise typer.BadParameter(
                    f"existing output {field} differs from this candidate; start a fresh run instead of mixing results"
                )
        if not _source_revision_is_ancestor(prior.get("source_revision")):
            raise typer.BadParameter(
                "existing output source revision is not an ancestor of this candidate; start a fresh run"
            )
    else:
        out.print(json.dumps(metadata, separators=(",", ":")), markup=False, soft_wrap=True)
    if rerun:
        selected_groups = {(runtime, size) for runtime in selected_runtimes for size in selected_sizes}
        records = [
            record
            for record in records
            if (str(record.get("runtime")), int(record.get("entities", -1))) not in selected_groups
        ]
    completed = {
        (str(record.get("runtime")), int(record["entities"]))
        for record in records
        if record.get("operation") == "ingest_batched" and isinstance(record.get("entities"), int)
    }
    for runtime in selected_runtimes:
        command = RUNTIME_COMMANDS[runtime]
        executable = command[0]
        if os.sep in executable and not Path(executable).is_file():
            raise FileNotFoundError(f"{runtime} artifact {executable!r} is missing; run mise run build first")
        for size in selected_sizes:
            if (runtime, size) in completed:
                err.print(f"benchmark: reuse {runtime} with {size:,} entities")
                continue
            err.print(f"benchmark: {runtime} with {size:,} entities")
            with tempfile.TemporaryDirectory(prefix=f"fgraph-benchmark-{runtime}-{size}-") as raw_directory:
                observed = _benchmark_runtime(
                    runtime,
                    command,
                    size,
                    Path(raw_directory),
                    batch_size=batch_size,
                    trials=trials,
                )
            for record in observed:
                records.append(record)
                out.print(json.dumps(record, separators=(",", ":")), markup=False, soft_wrap=True)
            # Persist every completed runtime/size group so an interrupted long
            # run still leaves honest, reusable observations.
            if output is not None:
                _write_records(output, records)
    if charts_dir is not None:
        if set(selected_runtimes) != set(RUNTIME_COMMANDS) or len(selected_sizes) < 2:
            raise typer.BadParameter("chart generation requires all runtimes and at least two sizes")
        _write_charts(records, charts_dir)


if __name__ == "__main__":
    try:
        app()
    except Exception:
        err.print_exception(show_locals=False)
        raise typer.Exit(code=1) from None
