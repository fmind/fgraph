from __future__ import annotations

import hashlib
import importlib.util
import stat
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from types import ModuleType
from zipfile import ZipFile

import pytest


def _script_module(name: str) -> ModuleType:
    path = Path(__file__).resolve().parents[2] / "scripts" / name
    module_name = f"_fgraph_test_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[module_name]
    return module


def test_release_workflow_publishes_local_npm_tarball() -> None:
    workflow = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "release.yml"

    assert "npm publish ./release/fmind-fgraph-*.tgz --access public --provenance" in workflow.read_text(
        encoding="utf-8"
    )


def test_release_zip_is_flat_and_byte_reproducible(tmp_path: Path) -> None:
    module = _script_module("deterministic_zip.py")
    source = tmp_path / "stage"
    source.mkdir()
    (source / "fgraph.exe").write_bytes(b"binary")
    (source / "LICENSE").write_text("MIT\n", encoding="utf-8")
    (source / "README.md").write_text("# fgraph\n", encoding="utf-8")

    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    epoch = 1_767_225_600
    module.create_archive(source, first, epoch)
    module.create_archive(source, second, epoch)

    assert first.read_bytes() == second.read_bytes()
    with ZipFile(first) as archive:
        assert archive.namelist() == ["LICENSE", "README.md", "fgraph.exe"]
        for info in archive.infolist():
            assert info.date_time == (2026, 1, 1, 0, 0, 0)
            assert stat.S_IMODE(info.external_attr >> 16) == 0o644

    smoke = _script_module("smoke_release_archive.py")
    executable = smoke.extract_and_validate_archive(first, "windows-amd64", tmp_path / "zip-output")
    assert executable.read_bytes() == b"binary"


def test_release_tar_manifest_and_flat_contract(tmp_path: Path) -> None:
    smoke = _script_module("smoke_release_archive.py")
    release = tmp_path / "release"
    release.mkdir()
    asset = release / "fgraph-linux-amd64.tar.gz"
    expected = {
        "LICENSE": (b"MIT\n", 0o644),
        "README.md": (b"# fgraph\n", 0o644),
        "fgraph": (b"binary", 0o755),
    }
    with tarfile.open(asset, mode="w:gz") as archive:
        for name, (data, mode) in expected.items():
            member = tarfile.TarInfo(f"./{name}")
            member.mode = mode
            member.uid = 0
            member.gid = 0
            member.size = len(data)
            archive.addfile(member, BytesIO(data))

    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    (release / "SHA256SUMS").write_text(f"{digest}  {asset.name}\n", encoding="utf-8")
    assert smoke.verify_manifest(release) == {asset.name: digest}
    executable = smoke.extract_and_validate_archive(asset, "linux-amd64", tmp_path / "tar-output")
    assert executable.read_bytes() == b"binary"

    asset.write_bytes(asset.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="checksum mismatch"):
        smoke.verify_manifest(release)
