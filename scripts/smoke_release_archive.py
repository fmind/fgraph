#!/usr/bin/env python3
"""Validate and execute one Go release archive on its native hosted runner."""

from __future__ import annotations

import argparse
import hashlib
import platform
import stat
import subprocess
import tarfile
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from zipfile import ZipFile


@dataclass(frozen=True)
class Target:
    system: str
    machines: frozenset[str]
    archive_suffix: str
    executable: str


TARGETS = {
    "linux-amd64": Target("Linux", frozenset({"amd64", "x86_64"}), ".tar.gz", "fgraph"),
    "linux-arm64": Target("Linux", frozenset({"aarch64", "arm64"}), ".tar.gz", "fgraph"),
    "darwin-amd64": Target("Darwin", frozenset({"amd64", "x86_64"}), ".tar.gz", "fgraph"),
    "darwin-arm64": Target("Darwin", frozenset({"aarch64", "arm64"}), ".tar.gz", "fgraph"),
    "windows-amd64": Target("Windows", frozenset({"amd64", "x86_64"}), ".zip", "fgraph.exe"),
}


def load_version(path: Path) -> str:
    """Read the single package version used by the release workflow."""
    with path.open("rb") as handle:
        version = tomllib.load(handle).get("project", {}).get("version")
    if not isinstance(version, str) or not version:
        raise ValueError(f"missing project.version in {path}")
    return version


def verify_manifest(release_directory: Path) -> dict[str, str]:
    """Verify every regular release file against the checksum manifest."""
    manifest = release_directory / "SHA256SUMS"
    entries: dict[str, str] = {}
    for line_number, line in enumerate(manifest.read_text(encoding="utf-8").splitlines(), start=1):
        if len(line) < 67 or line[64:66] not in {"  ", " *"}:
            raise ValueError(f"invalid SHA256SUMS line {line_number}")
        digest = line[:64]
        name = line[66:]
        if any(character not in "0123456789abcdef" for character in digest) or not name:
            raise ValueError(f"invalid SHA256SUMS line {line_number}")
        if name in entries or PurePosixPath(name).name != name:
            raise ValueError(f"unsafe or duplicate SHA256SUMS entry: {name!r}")
        entries[name] = digest

    actual = {
        path.name
        for path in release_directory.iterdir()
        if path.name != manifest.name and path.is_file() and not path.is_symlink()
    }
    unexpected = [
        path.name
        for path in release_directory.iterdir()
        if path.name != manifest.name and (path.is_symlink() or not path.is_file())
    ]
    if unexpected or set(entries) != actual:
        raise ValueError(
            f"release files differ from SHA256SUMS: listed={sorted(entries)!r}, "
            f"actual={sorted(actual)!r}, invalid={sorted(unexpected)!r}"
        )
    for name, expected in entries.items():
        observed = hashlib.sha256((release_directory / name).read_bytes()).hexdigest()
        if observed != expected:
            raise ValueError(f"checksum mismatch for {name}: expected {expected}, observed {observed}")
    return entries


def _member_name(raw_name: str) -> str:
    if "\\" in raw_name:
        raise ValueError(f"archive member uses a non-portable separator: {raw_name!r}")
    name = raw_name.removeprefix("./")
    path = PurePosixPath(name)
    if not name or path.is_absolute() or len(path.parts) != 1 or path.parts[0] in {".", ".."}:
        raise ValueError(f"archive member is not flat: {raw_name!r}")
    return name


def _expected_modes(target: Target) -> dict[str, int]:
    executable_mode = 0o644 if target.system == "Windows" else 0o755
    return {"LICENSE": 0o644, "README.md": 0o644, target.executable: executable_mode}


def _read_tar_archive(asset: Path, expected_modes: dict[str, int]) -> dict[str, bytes]:
    contents: dict[str, bytes] = {}
    with tarfile.open(asset, mode="r:gz") as archive:
        for member in archive.getmembers():
            if member.isdir() and member.name in {".", "./"}:
                continue
            name = _member_name(member.name)
            if not member.isreg() or member.uid != 0 or member.gid != 0:
                raise ValueError(f"unsafe or non-root-owned tar member: {member.name!r}")
            expected_mode = expected_modes.get(name)
            observed_mode = stat.S_IMODE(member.mode)
            if expected_mode is None or observed_mode != expected_mode or name in contents:
                raise ValueError(f"unexpected tar member {name!r}: mode={observed_mode:o}, expected={expected_mode!r}")
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"cannot read tar member: {name!r}")
            contents[name] = extracted.read()
    return contents


def _read_zip_archive(asset: Path, expected_modes: dict[str, int]) -> dict[str, bytes]:
    contents: dict[str, bytes] = {}
    with ZipFile(asset) as archive:
        for member in archive.infolist():
            name = _member_name(member.filename)
            observed_mode = stat.S_IMODE(member.external_attr >> 16)
            expected_mode = expected_modes.get(name)
            if member.is_dir() or member.create_system != 3 or expected_mode is None:
                raise ValueError(f"unexpected ZIP member: {member.filename!r}")
            if observed_mode != expected_mode or name in contents:
                raise ValueError(f"unexpected ZIP member {name!r}: mode={observed_mode:o}, expected={expected_mode:o}")
            contents[name] = archive.read(member)
    return contents


def extract_and_validate_archive(asset: Path, target_name: str, destination: Path) -> Path:
    """Validate the flat archive contract and materialize its exact files."""
    target = TARGETS[target_name]
    expected_modes = _expected_modes(target)
    if target.archive_suffix == ".zip":
        contents = _read_zip_archive(asset, expected_modes)
    else:
        contents = _read_tar_archive(asset, expected_modes)
    if set(contents) != set(expected_modes):
        raise ValueError(f"archive members differ: expected={sorted(expected_modes)!r}, actual={sorted(contents)!r}")

    destination.mkdir()
    for name, data in contents.items():
        output = destination / name
        output.write_bytes(data)
        output.chmod(expected_modes[name])
    return destination / target.executable


def assert_native_host(target_name: str) -> None:
    """Reject emulated or incorrectly labelled runner combinations."""
    target = TARGETS[target_name]
    observed_system = platform.system()
    observed_machine = platform.machine().lower()
    if observed_system != target.system or observed_machine not in target.machines:
        raise RuntimeError(
            f"target {target_name} requires {target.system}/{sorted(target.machines)!r}, "
            f"runner is {observed_system}/{observed_machine}"
        )


def run_version(executable: Path, expected_version: str) -> None:
    """Execute the packaged binary and check its public version contract."""
    completed = subprocess.run(
        [executable, "version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if completed.returncode != 0 or completed.stdout.strip() != expected_version or completed.stderr:
        raise RuntimeError(
            f"packaged CLI version smoke failed: code={completed.returncode}, "
            f"stdout={completed.stdout!r}, stderr={completed.stderr!r}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release_directory", type=Path)
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--version-file", type=Path, required=True)
    arguments = parser.parse_args()

    release_directory = arguments.release_directory.resolve(strict=True)
    version = load_version(arguments.version_file.resolve(strict=True))
    verify_manifest(release_directory)
    target = TARGETS[arguments.target]
    asset = release_directory / f"fgraph-{arguments.target}{target.archive_suffix}"
    if not asset.is_file():
        raise FileNotFoundError(asset)
    assert_native_host(arguments.target)
    with tempfile.TemporaryDirectory(prefix="fgraph-release-smoke-") as temporary:
        executable = extract_and_validate_archive(asset, arguments.target, Path(temporary) / "archive")
        run_version(executable, version)
    print(f"{asset.name}: OK ({arguments.target}, {version})")


if __name__ == "__main__":
    main()
