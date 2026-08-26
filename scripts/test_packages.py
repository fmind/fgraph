#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Install built package artifacts in disposable consumers and exercise them."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from collections.abc import Sequence
from pathlib import Path
from queue import Empty, Queue
from threading import Thread

ROOT = Path(__file__).resolve().parents[1]


def _run(
    command: Sequence[str | Path],
    *,
    cwd: Path | None = None,
    capture_output: bool = False,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        check=True,
        capture_output=capture_output,
        env=environment,
        text=True,
    )


def _python_smoke(work: Path, version: str) -> None:
    wheel = ROOT / "python" / "dist" / f"fgraph-{version}-py3-none-any.whl"
    if not wheel.is_file():
        raise FileNotFoundError(f"built Python wheel is missing: {wheel}; run mise run build:python")
    with zipfile.ZipFile(wheel) as archive:
        packaged = set(archive.namelist())
        if not any(name.endswith(".dist-info/licenses/LICENSE") for name in packaged):
            raise RuntimeError(f"{wheel.name} does not contain its MIT license text")
        if "fgraph/py.typed" not in packaged:
            raise RuntimeError(f"{wheel.name} does not contain its PEP 561 py.typed marker")

    project_python = ROOT / "python" / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not project_python.is_file():
        raise FileNotFoundError("the installed Python environment is missing; run mise run install:python")
    dependency_path = _run(
        [project_python, "-c", "import site; print(site.getsitepackages()[0])"], capture_output=True
    ).stdout.strip()
    environment = work / "python"
    _run(["uv", "venv", "--offline", "--python", project_python, environment])
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    fgraph = environment / ("Scripts/fgraph.exe" if os.name == "nt" else "bin/fgraph")
    _run(["uv", "pip", "install", "--offline", "--no-deps", "--python", python, wheel])
    # Dependency resolution is already locked and installed by mise. Reusing
    # that site-packages keeps this artifact boundary fully offline while the
    # wheel's own code and console script still come from the disposable venv.
    smoke_environment = {**os.environ, "PYTHONPATH": dependency_path}
    smoke = """
import sys
from pathlib import Path
import fgraph

if fgraph.__version__ != sys.argv[1]:
    raise RuntimeError(f"version {fgraph.__version__} != {sys.argv[1]}")
if not Path(fgraph.__file__).is_relative_to(Path(sys.prefix)):
    raise RuntimeError(f"fgraph was not imported from the installed wheel: {fgraph.__file__}")
with fgraph.connect(":memory:") as db:
    db.transact({"id": "artifact", "package/status": "installed"})
    if db.entity("artifact") != {"package/status": "installed"}:
        raise RuntimeError("installed wheel failed its transaction round trip")
"""
    _run([python, "-c", smoke, version], environment=smoke_environment)
    result = _run([fgraph, "version"], capture_output=True, environment=smoke_environment)
    if result.stdout.strip() != version:
        raise RuntimeError(f"installed Python CLI reports {result.stdout.strip()!r}, expected {version!r}")


def _typescript_smoke(work: Path, version: str) -> None:
    package_dir = work / "npm-package"
    package_dir.mkdir()
    packed = _run(
        ["npm", "pack", "--pack-destination", package_dir, "--json"],
        cwd=ROOT / "typescript",
        capture_output=True,
    )
    records = json.loads(packed.stdout)
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0].get("filename"), str):
        raise RuntimeError(f"npm pack returned an unexpected manifest: {records!r}")
    archive = package_dir / records[0]["filename"]
    with tarfile.open(archive, "r:gz") as package:
        names = set(package.getnames())
    required = {
        "package/LICENSE",
        "package/README.md",
        "package/dist/cli.js",
        "package/dist/index.d.ts",
        "package/dist/index.js",
    }
    if missing := sorted(required - names):
        raise RuntimeError(f"{archive.name} is missing packaged files: {missing}")

    consumer = work / "npm-consumer"
    consumer.mkdir()
    _run(["npm", "install", "--offline", "--no-audit", "--no-fund", archive], cwd=consumer)
    smoke = consumer / "smoke.mjs"
    smoke.write_text(
        """import { connect, version } from "@fmind/fgraph";

if (version !== process.env.FGRAPH_EXPECTED_VERSION) {
  throw new Error(`version ${version} != ${process.env.FGRAPH_EXPECTED_VERSION}`);
}
const db = connect(":memory:");
try {
  db.transact({ id: "artifact", "package/status": "installed" });
  const entity = db.entity("artifact");
  if (entity["package/status"] !== "installed") throw new Error("installed package failed its transaction round trip");
} finally {
  db.close();
}
""",
        encoding="utf-8",
    )
    environment = {**os.environ, "FGRAPH_EXPECTED_VERSION": version}
    subprocess.run(["node", smoke], cwd=consumer, env=environment, check=True)
    executable = consumer / "node_modules" / ".bin" / ("fgraph.cmd" if os.name == "nt" else "fgraph")
    result = subprocess.run([executable, "version"], cwd=consumer, check=True, capture_output=True, text=True)
    if result.stdout.strip() != version:
        raise RuntimeError(f"installed npm CLI reports {result.stdout.strip()!r}, expected {version!r}")

    database = work / "npm-mcp.db"
    subprocess.run([executable, "init", "--db", database], cwd=consumer, check=True, capture_output=True, text=True)
    mcp = subprocess.Popen(
        [executable, "--db", database, "mcp"],
        cwd=consumer,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    def response(request: dict[str, object]) -> dict[str, object]:
        assert mcp.stdin is not None
        assert mcp.stdout is not None
        mcp.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
        mcp.stdin.flush()
        lines: Queue[str] = Queue(maxsize=1)
        Thread(target=lambda: lines.put(mcp.stdout.readline()), daemon=True).start()
        try:
            line = lines.get(timeout=10)
        except Empty as exc:
            raise RuntimeError(f"installed TypeScript MCP timed out waiting for {request['method']}") from exc
        if not line:
            raise RuntimeError(f"installed TypeScript MCP exited before answering {request['method']}")
        value = json.loads(line)
        if not isinstance(value, dict):
            raise RuntimeError(f"installed TypeScript MCP returned a non-object response: {value!r}")
        return value

    try:
        initialized = response(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "fgraph-package-smoke", "version": version},
                },
            }
        )
        if "error" in initialized:
            raise RuntimeError(f"installed TypeScript MCP initialize failed: {initialized!r}")
        assert mcp.stdin is not None
        mcp.stdin.write('{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        mcp.stdin.flush()
        listed = response({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
        result_value = listed.get("result")
        tools = result_value.get("tools", []) if isinstance(result_value, dict) else []
        names = sorted(tool.get("name") for tool in tools if isinstance(tool, dict))
        expected = ["about", "datoms", "explain", "history", "query", "recall", "receipt", "schema", "why"]
        if names != expected:
            raise RuntimeError(f"installed TypeScript MCP tools differ: {names!r} != {expected!r}")
    finally:
        if mcp.stdin is not None and not mcp.stdin.closed:
            mcp.stdin.close()
        try:
            mcp.wait(timeout=5)
        except subprocess.TimeoutExpired:
            mcp.terminate()
            try:
                mcp.wait(timeout=5)
            except subprocess.TimeoutExpired:
                mcp.kill()
                mcp.wait(timeout=5)
        assert mcp.stderr is not None
        errors = mcp.stderr.read()
        if mcp.returncode != 0 or errors:
            raise RuntimeError(f"installed TypeScript MCP exited with {mcp.returncode}: {errors}")


def main() -> None:
    project = tomllib.loads((ROOT / "python" / "pyproject.toml").read_text(encoding="utf-8"))
    version = str(project["project"]["version"])
    with tempfile.TemporaryDirectory(prefix="fgraph-package-smoke-") as raw_work:
        work = Path(raw_work)
        _python_smoke(work, version)
        _typescript_smoke(work, version)
    sys.stdout.write(f"package artifacts: {version} OK\n")


if __name__ == "__main__":
    main()
