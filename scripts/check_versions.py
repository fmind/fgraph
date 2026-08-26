#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Fail when any public package or runtime reports a different release version."""

from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _single_match(path: Path, pattern: str, label: str) -> str:
    matches = re.findall(pattern, path.read_text(encoding="utf-8"), flags=re.MULTILINE)
    if len(matches) != 1:
        raise RuntimeError(f"{label}: expected one version declaration in {path}, found {len(matches)}")
    return matches[0]


def _python_lock_version() -> str:
    lock = tomllib.loads((ROOT / "python" / "uv.lock").read_text(encoding="utf-8"))
    packages = [
        package
        for package in lock["package"]
        if package.get("name") == "fgraph" and package.get("source") == {"editable": "."}
    ]
    if len(packages) != 1:
        raise RuntimeError(f"python/uv.lock: expected one editable fgraph package, found {len(packages)}")
    return str(packages[0]["version"])


def main() -> None:
    python_project = tomllib.loads((ROOT / "python" / "pyproject.toml").read_text(encoding="utf-8"))
    canonical = str(python_project["project"]["version"])
    npm_package = json.loads((ROOT / "typescript" / "package.json").read_text(encoding="utf-8"))
    npm_lock = json.loads((ROOT / "typescript" / "package-lock.json").read_text(encoding="utf-8"))
    versions = {
        "go API": _single_match(ROOT / "go" / "types.go", r'^\s*Version\s*=\s*"([^"]+)"', "go API"),
        "go README install": _single_match(
            ROOT / "go" / "README.md",
            r"go get github\.com/fmind/fgraph/go@v([^`\s]+)",
            "go README install",
        ),
        "go README tag": _single_match(
            ROOT / "go" / "README.md",
            r"repository tag `go/v([^`]+)`",
            "go README tag",
        ),
        "npm lock root": str(npm_lock["version"]),
        "npm lock workspace": str(npm_lock["packages"][""]["version"]),
        "npm package": str(npm_package["version"]),
        "Python API": _single_match(
            ROOT / "python" / "src" / "fgraph" / "__init__.py",
            r'^__version__\s*=\s*"([^"]+)"',
            "Python API",
        ),
        "Python lock": _python_lock_version(),
        "Python MCP": _single_match(
            ROOT / "python" / "src" / "fgraph" / "mcp_server.py",
            r'^\s*version\s*=\s*"([^"]+)",',
            "Python MCP",
        ),
        "Python project": canonical,
        "TypeScript API": _single_match(
            ROOT / "typescript" / "src" / "index.ts",
            r'^export const version\s*=\s*"([^"]+)";',
            "TypeScript API",
        ),
        "TypeScript CLI": _single_match(
            ROOT / "typescript" / "src" / "cli.ts",
            r'^const VERSION\s*=\s*"([^"]+)";',
            "TypeScript CLI",
        ),
        "TypeScript MCP": _single_match(
            ROOT / "typescript" / "src" / "mcp.ts",
            r'\{\s*name:\s*"fgraph",\s*version:\s*"([^"]+)"\s*\}',
            "TypeScript MCP",
        ),
    }
    mismatches = {label: version for label, version in versions.items() if version != canonical}
    if mismatches:
        details = ", ".join(f"{label}={version}" for label, version in sorted(mismatches.items()))
        raise RuntimeError(f"release version is {canonical}, but {details}")
    sys.stdout.write(f"versions: {canonical} ({len(versions)} sources)\n")


if __name__ == "__main__":
    main()
