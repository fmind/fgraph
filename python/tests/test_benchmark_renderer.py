from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import pytest

ROOT = Path(__file__).resolve().parents[2]
SVG = {"svg": "http://www.w3.org/2000/svg"}


@pytest.fixture(scope="module")
def benchmark() -> Any:
    script = ROOT / "scripts" / "benchmark.py"
    spec = importlib.util.spec_from_file_location("fgraph_benchmark", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load benchmark harness from {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def records() -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in (ROOT / "benchmarks" / "latest.ndjson").read_text(encoding="utf-8").splitlines()
    ]


def _texts(root: ElementTree.Element) -> set[str]:
    return {text.strip() for text in root.itertext() if text.strip()}


def _parse_generated_svg(path: Path) -> ElementTree.Element:
    # The input is deterministic SVG emitted locally by the benchmark harness.
    return ElementTree.parse(path).getroot()  # noqa: S314


def test_benchmark_charts_encode_quantitative_and_categorical_axes(
    benchmark: Any,
    records: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    benchmark.write_charts(records, first)
    benchmark.write_charts(records, second)

    for name in ("ingest-throughput.svg", "read-latency.svg"):
        assert (first / name).read_bytes() == (second / name).read_bytes()

    ingest = _parse_generated_svg(first / "ingest-throughput.svg")
    assert ingest.attrib["role"] == "img"
    assert ingest.attrib["aria-labelledby"] == "chart-title chart-description"
    ingest_title = ingest.find("svg:title", SVG)
    assert ingest_title is not None
    assert ingest_title.text == "Batched NDJSON import throughput"
    ingest_text = _texts(ingest)
    assert "Entities imported (log scale)" in ingest_text
    assert "Throughput (entities/s; higher is better)" in ingest_text
    assert len(ingest.findall(".//svg:polyline", SVG)) == 3

    latency = _parse_generated_svg(first / "read-latency.svg")
    assert latency.attrib["role"] == "img"
    assert latency.attrib["aria-labelledby"] == "chart-title chart-description"
    latency_title = latency.find("svg:title", SVG)
    assert latency_title is not None
    assert latency_title.text == "Fresh-process CLI read latency at 100,000 entities"
    description = latency.find("svg:desc", SVG)
    assert description is not None
    assert "lower is better" in (description.text or "")
    assert not latency.findall(".//svg:polyline", SVG)

    bars = latency.findall(".//svg:rect[@class='data-bar']", SVG)
    assert len(bars) == 15
    assert {(bar.attrib["data-runtime"], bar.attrib["data-operation"]) for bar in bars} == {
        (runtime, operation)
        for runtime in ("python", "go", "typescript")
        for operation in (
            "point_get",
            "query_scalar_filter",
            "query_join",
            "keyword_search",
            "vector_search_384",
        )
    }
    assert all(float(bar.attrib["x"]) == 220 for bar in bars)
    assert all(bar.find("svg:title", SVG) is not None for bar in bars)
    runtime_markers = [element for element in latency.iter() if element.attrib.get("class") == "runtime-marker"]
    assert len(runtime_markers) == 15
    assert {marker.attrib["data-runtime"] for marker in runtime_markers} == {"python", "go", "typescript"}

    latency_text = _texts(latency)
    assert {
        "Point get",
        "Scalar-filter query",
        "Connected-join query",
        "Keyword search",
        "Exact 384-d vector search",
        "Median fresh-process CLI latency (ms; lower is better)",
    } <= latency_text
    assert len(latency.findall(".//svg:text[@class='data-label']", SVG)) == 15


def test_read_chart_places_value_label_next_to_median_marker(benchmark: Any) -> None:
    chart = benchmark._grouped_bar_chart(  # noqa: SLF001
        "Read latency",
        "Median with observed range.",
        {"python": {"point_get": (100.0, 50.0, 1_000.0)}},
        categories=(("point_get", "Point get"),),
        x_label="Latency (ms)",
    )
    # The input is deterministic SVG emitted locally by the benchmark harness.
    root = ElementTree.fromstring(chart)  # noqa: S314
    label = root.find(".//svg:text[@class='data-label']", SVG)
    marker = root.find(".//svg:circle[@class='runtime-marker']", SVG)
    whisker = next(
        line
        for line in root.findall(".//svg:line", SVG)
        if line.attrib.get("stroke") == benchmark.FOREGROUND and line.attrib["y1"] == line.attrib["y2"]
    )

    assert label is not None
    assert label.text == "100"
    assert marker is not None
    label_x = float(label.attrib["x"])
    median_x = float(marker.attrib["cx"])
    maximum_x = float(whisker.attrib["x2"])
    assert abs(label_x - median_x) <= 10
    assert abs(label_x - median_x) < abs(label_x - maximum_x)
