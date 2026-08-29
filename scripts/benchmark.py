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
from html import escape
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
CANONICAL_OUTPUT = ROOT / "benchmarks" / "latest.ndjson"
CANONICAL_CHARTS = ROOT / "benchmarks"
README_START = "<!-- benchmark-results:start -->"
README_END = "<!-- benchmark-results:end -->"
VECTOR_DIMS = 384
DEFAULT_SIZES = [1_000, 10_000, 100_000]
RUNTIME_COMMANDS = {
    "python": [str(ROOT / "python" / ".venv" / "bin" / "fgraph")],
    "go": [str(ROOT / "go" / "bin" / "fgraph")],
    "typescript": ["node", str(ROOT / "typescript" / "dist" / "cli.js")],
}
RUNTIME_LABELS = {"python": "Python", "go": "Go", "typescript": "TypeScript"}
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
READ_CHART_OPERATIONS = (
    ("point_get", "Point get"),
    ("query_scalar_filter", "Scalar-filter query"),
    ("query_join", "Connected-join query"),
    ("keyword_search", "Keyword search"),
    ("vector_search_384", "Exact 384-d vector search"),
)

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
        ROOT / "docs" / "content" / "spec.md",
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


def _nice_scale(maximum: float, *, target_intervals: int) -> tuple[float, float]:
    if maximum <= 0:
        return 1.0, 1.0
    rough_step = maximum / target_intervals
    magnitude = 10 ** math.floor(math.log10(rough_step))
    normalized = rough_step / magnitude
    factor = next(candidate for candidate in (1.0, 2.0, 2.5, 5.0, 10.0) if normalized <= candidate)
    step = factor * magnitude
    return math.ceil(maximum / step) * step, step


def _runtime_marker(
    runtime: str,
    x: float,
    y: float,
    *,
    fill: str,
    stroke: str | None = None,
    css_class: str | None = None,
    size: float = 5,
) -> str:
    attributes = f' class="{css_class}" data-runtime="{runtime}"' if css_class is not None else ""
    stroke_attribute = f' stroke="{stroke}" stroke-width="1.5"' if stroke is not None else ""
    if runtime == "go":
        return (
            f'<rect{attributes} x="{x - size:.1f}" y="{y - size:.1f}" width="{size * 2:.1f}" '
            f'height="{size * 2:.1f}" fill="{fill}"{stroke_attribute}/>'
        )
    if runtime == "typescript":
        return (
            f'<path{attributes} d="M {x:.1f} {y - size - 1:.1f} L {x + size + 1:.1f} {y:.1f} '
            f'L {x:.1f} {y + size + 1:.1f} L {x - size - 1:.1f} {y:.1f} Z" '
            f'fill="{fill}"{stroke_attribute}/>'
        )
    return f'<circle{attributes} cx="{x:.1f}" cy="{y:.1f}" r="{size:.1f}" fill="{fill}"{stroke_attribute}/>'


def _line_chart(
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
    y_max, y_step = _nice_scale(max(y_values), target_intervals=6)

    def x_position(value: float) -> float:
        normalized = math.log10(value) if log_x else value
        return left + (normalized - x_min) / max(x_max - x_min, 1) * plot_width

    def y_position(value: float) -> float:
        return top + plot_height - value / y_max * plot_height

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-labelledby="chart-title chart-description" viewBox="0 0 {width} {height}">',
        f'<title id="chart-title">{escape(title)}</title><desc id="chart-description">{escape(description)}</desc>',
        f'<rect width="{width}" height="{height}" fill="{BACKGROUND}"/>',
        f'<rect x="{left}" y="{top}" width="{plot_width}" height="{plot_height}" fill="{PANEL}" stroke="{BORDER}"/>',
        f'<text x="{width / 2}" y="28" text-anchor="middle" '
        f'font-family="{HEADING_FONT}" font-size="20" font-weight="600" fill="{FOREGROUND}">{escape(title)}</text>',
    ]
    for step in range(round(y_max / y_step) + 1):
        value = y_step * step
        y = y_position(value)
        lines.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_width}" y2="{y:.1f}" stroke="{BORDER}"/>')
        lines.append(
            f'<text x="{left - 10}" y="{y + 4:.1f}" text-anchor="end" '
            f'font-family="{BODY_FONT}" font-size="12" fill="{MUTED}">'
            f"{value:,.0f}</text>"
            if y_step >= 1
            else f'<text x="{left - 10}" y="{y + 4:.1f}" text-anchor="end" '
            f'font-family="{BODY_FONT}" font-size="12" fill="{MUTED}">{value:.2f}</text>'
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
            lines.append(_runtime_marker(runtime, x_position(x), y_position(y), fill=color))
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
        lines.append(_runtime_marker(runtime, legend_x + 12, height - 23, fill=color))
        lines.append(
            f'<text x="{legend_x + 34}" y="{height - 18}" font-family="{BODY_FONT}" '
            f'font-size="13" fill="{FOREGROUND}">{RUNTIME_LABELS[runtime]}</text>'
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


def _grouped_bar_chart(
    title: str,
    description: str,
    series: dict[str, dict[str, tuple[float, float, float]]],
    *,
    categories: Sequence[tuple[str, str]],
    x_label: str,
) -> str:
    width, height = 920, 600
    left, top, right, bottom = 220, 60, 55, 110
    plot_width, plot_height = width - left - right, height - top - bottom
    maximum = max(bounds[2] for values in series.values() for bounds in values.values())
    x_max, x_step = _nice_scale(maximum, target_intervals=7)
    group_height = plot_height / len(categories)
    bar_height, bar_gap = 16.0, 6.0
    bars_height = len(series) * bar_height + (len(series) - 1) * bar_gap

    def x_position(value: float) -> float:
        return left + value / x_max * plot_width

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-labelledby="chart-title chart-description" viewBox="0 0 {width} {height}">',
        f'<title id="chart-title">{escape(title)}</title><desc id="chart-description">{escape(description)}</desc>',
        f'<rect width="{width}" height="{height}" fill="{BACKGROUND}"/>',
        f'<rect x="{left}" y="{top}" width="{plot_width}" height="{plot_height}" fill="{PANEL}" stroke="{BORDER}"/>',
        f'<text x="{width / 2}" y="30" text-anchor="middle" '
        f'font-family="{HEADING_FONT}" font-size="20" font-weight="600" fill="{FOREGROUND}">{escape(title)}</text>',
    ]

    for step in range(round(x_max / x_step) + 1):
        value = x_step * step
        x = x_position(value)
        lines.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top + plot_height}" stroke="{BORDER}"/>')
        lines.append(
            f'<text x="{x:.1f}" y="{top + plot_height + 23}" text-anchor="middle" '
            f'font-family="{BODY_FONT}" font-size="12" fill="{MUTED}">{value:,.0f}</text>'
        )

    for category_index, (operation, label) in enumerate(categories):
        group_top = top + category_index * group_height
        center = group_top + group_height / 2
        if category_index:
            lines.append(
                f'<line x1="{left}" y1="{group_top:.1f}" x2="{left + plot_width}" y2="{group_top:.1f}" '
                f'stroke="{BORDER}" stroke-opacity="0.7"/>'
            )
        lines.append(
            f'<text x="{left - 15}" y="{center + 4:.1f}" text-anchor="end" '
            f'font-family="{BODY_FONT}" font-size="13" fill="{FOREGROUND}">{escape(label)}</text>'
        )
        first_bar_y = center - bars_height / 2
        for runtime_index, runtime in enumerate(series):
            median, minimum, maximum = series[runtime][operation]
            bar_y = first_bar_y + runtime_index * (bar_height + bar_gap)
            bar_center = bar_y + bar_height / 2
            median_x = x_position(median)
            minimum_x = x_position(minimum)
            maximum_x = x_position(maximum)
            median_label = f"{median:.0f}"
            label_padding = 6.0
            estimated_label_width = len(median_label) * 7.0
            if median_x + label_padding + estimated_label_width <= left + plot_width:
                label_x = median_x + label_padding
                label_anchor = "start"
            else:
                label_x = median_x - label_padding
                label_anchor = "end"
            accessible = (
                f"{RUNTIME_LABELS[runtime]}, {label}: median {median_label} milliseconds; "
                f"observed range {minimum:.0f} to {maximum:.0f} milliseconds"
            )
            lines.append(
                f'<rect class="data-bar" data-runtime="{runtime}" data-operation="{operation}" '
                f'x="{left}" y="{bar_y:.1f}" width="{median_x - left:.1f}" height="{bar_height:.1f}" '
                f'fill="{COLORS[runtime]}" fill-opacity="0.72" stroke="{COLORS[runtime]}" role="img" '
                f'aria-label="{escape(accessible)}">'
                f"<title>{escape(accessible)}</title></rect>"
            )
            lines.extend(
                (
                    f'<line x1="{minimum_x:.1f}" y1="{bar_center:.1f}" x2="{maximum_x:.1f}" y2="{bar_center:.1f}" '
                    f'stroke="{FOREGROUND}" stroke-width="1.5"/>',
                    f'<line x1="{minimum_x:.1f}" y1="{bar_center - 4:.1f}" x2="{minimum_x:.1f}" '
                    f'y2="{bar_center + 4:.1f}" stroke="{FOREGROUND}" stroke-width="1.5"/>',
                    f'<line x1="{maximum_x:.1f}" y1="{bar_center - 4:.1f}" x2="{maximum_x:.1f}" '
                    f'y2="{bar_center + 4:.1f}" stroke="{FOREGROUND}" stroke-width="1.5"/>',
                    _runtime_marker(
                        runtime,
                        median_x,
                        bar_center,
                        fill=BACKGROUND,
                        stroke=FOREGROUND,
                        css_class="runtime-marker",
                        size=3.5,
                    ),
                    f'<text class="data-label" x="{label_x:.1f}" y="{bar_center + 4:.1f}" '
                    f'text-anchor="{label_anchor}" font-family="{BODY_FONT}" font-size="11" '
                    f'fill="{FOREGROUND}">{median_label}</text>',
                )
            )

    lines.append(
        f'<text x="{left + plot_width / 2}" y="{height - 54}" text-anchor="middle" '
        f'font-family="{BODY_FONT}" font-size="13" fill="{MUTED}">{escape(x_label)}</text>'
    )
    legend_x = left
    for runtime in series:
        lines.append(
            f'<rect x="{legend_x}" y="{height - 30}" width="22" height="12" '
            f'fill="{COLORS[runtime]}" fill-opacity="0.72" stroke="{COLORS[runtime]}"/>'
        )
        lines.append(_runtime_marker(runtime, legend_x + 11, height - 24, fill=BACKGROUND, stroke=FOREGROUND, size=3.5))
        lines.append(
            f'<text x="{legend_x + 30}" y="{height - 19}" font-family="{BODY_FONT}" '
            f'font-size="13" fill="{FOREGROUND}">{RUNTIME_LABELS[runtime]}</text>'
        )
        legend_x += 155
    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def write_charts(records: Sequence[dict[str, Any]], directory: Path) -> None:
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
        _line_chart(
            "Batched NDJSON import throughput",
            "End-to-end CLI throughput for NDJSON imports using bounded transactions; higher is better.",
            ingest,
            x_label="Entities imported (log scale)",
            y_label="Throughput (entities/s; higher is better)",
            log_x=True,
        ),
        encoding="utf-8",
    )

    largest = max(int(record["entities"]) for record in observations)
    reads: dict[str, dict[str, tuple[float, float, float]]] = {}
    for runtime in RUNTIME_COMMANDS:
        reads[runtime] = {}
        for operation, _label in READ_CHART_OPERATIONS:
            record = next(
                record
                for record in observations
                if record["runtime"] == runtime and record["entities"] == largest and record["operation"] == operation
            )
            reads[runtime][operation] = (
                float(record["seconds"]) * 1_000,
                float(record["min_seconds"]) * 1_000,
                float(record["max_seconds"]) * 1_000,
            )
    trials = {int(record["trials"]) for record in observations if record.get("operation") in READ_OPERATIONS}
    if len(trials) != 1:
        raise ValueError("benchmark read observations disagree on trial count")
    (directory / "read-latency.svg").write_text(
        _grouped_bar_chart(
            f"Fresh-process CLI read latency at {largest:,} entities",
            f"Grouped medians of {next(iter(trials))} fresh-process end-to-end CLI trials; lower is better. "
            "Whiskers show the observed minimum and maximum.",
            reads,
            categories=READ_CHART_OPERATIONS,
            x_label="Median fresh-process CLI latency (ms; lower is better)",
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
        write_charts(records, expected_directory)
        for name in ("ingest-throughput.svg", "read-latency.svg"):
            chart = directory / name
            expected = expected_directory / name
            if not chart.is_file() or chart.read_bytes() != expected.read_bytes():
                raise ValueError(f"benchmark chart {chart} is missing or does not match the raw observations")


def _markdown_table(headers: Sequence[str], rows: Sequence[Sequence[str]], *, numeric_from: int = 1) -> str:
    widths = [max(len(headers[index]), *(len(row[index]) for row in rows)) for index in range(len(headers))]

    def cells(values: Sequence[str], *, header: bool = False) -> str:
        rendered = []
        for index, value in enumerate(values):
            if header or index < numeric_from:
                rendered.append(value.ljust(widths[index]))
            else:
                rendered.append(value.rjust(widths[index]))
        return f"| {' | '.join(rendered)} |"

    separator = ["-" * width if index < numeric_from else f"{'-' * (width - 1)}:" for index, width in enumerate(widths)]
    return "\n".join([cells(headers, header=True), cells(separator, header=True), *(cells(row) for row in rows)])


def _readme_results(records: Sequence[dict[str, Any]]) -> str:
    metadata = records[0]
    observations = [record for record in records if record.get("record") != "metadata"]
    largest = max(int(record["entities"]) for record in observations)
    by_key = {
        (str(record["runtime"]), int(record["entities"]), str(record["operation"])): record for record in observations
    }

    def observation(runtime: str, operation: str) -> dict[str, Any]:
        try:
            return by_key[(runtime, largest, operation)]
        except KeyError as error:
            raise ValueError(f"benchmark README is missing {runtime}/{largest}/{operation}") from error

    labels = {"python": "Python", "go": "Go", "typescript": "TypeScript"}
    runtimes = list(RUNTIME_COMMANDS)
    read_operations = [
        "point_get",
        "query_scalar_filter",
        "query_join",
        "keyword_search",
        "vector_search_384",
    ]
    throughput_rows = [
        [
            labels[runtime],
            f"{float(observation(runtime, 'ingest_batched')['entities_per_second']):,.0f}",
            *(f"{float(observation(runtime, operation)['seconds']) * 1_000:.0f} ms" for operation in read_operations),
        ]
        for runtime in runtimes
    ]
    portable_rows = [
        [
            labels[runtime],
            *(
                f"{float(observation(runtime, operation)['seconds']):.2f} s"
                for operation in ("snapshot", "restore", "tail", "apply")
            ),
        ]
        for runtime in runtimes
    ]
    common_bytes = {int(observation(runtime, "file_size")["bytes"]) for runtime in runtimes}
    snapshot_bytes = {int(observation(runtime, "snapshot")["bytes"]) for runtime in runtimes}
    tail_bytes = {int(observation(runtime, "tail")["bytes"]) for runtime in runtimes}
    if len(common_bytes) != 1 or len(snapshot_bytes) != 1 or len(tail_bytes) != 1:
        raise ValueError("benchmark runtimes disagree on portable file, snapshot, or event-stream size")
    source_revision = str(metadata["source_revision"])
    generated_date = str(metadata["generated_at"]).split("T", maxsplit=1)[0]
    tree = str(metadata["source_tree"])
    return "\n\n".join(
        [
            f"At {largest:,} entities:",
            _markdown_table(
                [
                    "Runtime",
                    "Ingest entities/s",
                    "Point get",
                    "Scalar filter",
                    "Connected join",
                    "Keyword search",
                    "Exact vector search",
                ],
                throughput_rows,
            ),
            _markdown_table(
                ["Runtime", "Snapshot", "Restore", "Event tail", "Event apply"],
                portable_rows,
            ),
            "The common logical state occupied "
            f"{next(iter(common_bytes)) / (1024 * 1024):.2f} MiB; its snapshot was "
            f"{next(iter(snapshot_bytes)) / (1024 * 1024):.2f} MiB and its event stream "
            f"{next(iter(tail_bytes)) / (1024 * 1024):.2f} MiB. Vector search is intentionally exact over the "
            f"{largest // 20:,} vectors, not ANN. These measurements validate the tested {largest // 1_000}k envelope, "
            "not millions of entities or a service-level objective; SQLite build, filesystem, runtime startup, and "
            "hardware affect the numbers.",
            f"This release run was generated on {generated_date} from {tree} source commit "
            f"[`{source_revision[:7]}`](https://github.com/fmind/fgraph/commit/{source_revision}) and source digest "
            f"`{metadata['source_digest']}`. The raw metadata records the exact runtime, SQLite, platform, workload, "
            "and clean-tree provenance.",
        ]
    )


def _replace_readme_results(records: Sequence[dict[str, Any]], *, verify: bool) -> None:
    document = README.read_text(encoding="utf-8")
    if document.count(README_START) != 1 or document.count(README_END) != 1:
        raise ValueError("README benchmark result markers are missing or duplicated")
    prefix, remainder = document.split(README_START, maxsplit=1)
    _old, suffix = remainder.split(README_END, maxsplit=1)
    expected = f"{prefix}{README_START}\n\n{_readme_results(records)}\n\n{README_END}{suffix}"
    if verify:
        if document != expected:
            raise ValueError("README benchmark results do not match the raw observations")
        return
    README.write_text(expected, encoding="utf-8")


def _canonical_artifacts(output: Path | None, charts_dir: Path | None) -> bool:
    return (
        output is not None
        and charts_dir is not None
        and output.resolve() == CANONICAL_OUTPUT
        and charts_dir.resolve() == CANONICAL_CHARTS
    )


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
        if _canonical_artifacts(output, charts_dir):
            _replace_readme_results(records, verify=True)
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
        write_charts(records, charts_dir)
    if _canonical_artifacts(output, charts_dir):
        _replace_readme_results(records, verify=False)


if __name__ == "__main__":
    try:
        app()
    except Exception:
        err.print_exception(show_locals=False)
        raise typer.Exit(code=1) from None
