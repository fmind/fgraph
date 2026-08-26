"""Read-only storage regressions for the public database handle."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import fgraph


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory modes are required")
def test_read_only_database_opens_without_writable_sidecar_directory(tmp_path: Path) -> None:
    directory = tmp_path / "media"
    directory.mkdir()
    path = directory / "graph.db"
    with fgraph.connect(path, clock=1_767_225_600_000_000) as writer:
        writer.transact({"id": "readonly/item", "readonly/value": "available"})

    path.chmod(0o444)
    directory.chmod(0o555)
    try:
        # Prove the fixture actually rejects SQLite sidecar creation for the
        # user running the test; chmod alone is not meaningful under root.
        with pytest.raises(PermissionError):
            (directory / "write-probe").touch()

        with fgraph.connect(path, read_only=True) as reader:
            assert reader.entity("readonly/item") == {"readonly/value": "available"}
            assert reader.doctor()["ok"] is True

        assert [entry.name for entry in directory.iterdir()] == ["graph.db"]
    finally:
        directory.chmod(0o700)
        path.chmod(0o600)


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory modes are required")
def test_read_only_open_never_ignores_existing_wal(tmp_path: Path) -> None:
    directory = tmp_path / "media"
    directory.mkdir()
    path = directory / "graph.db"
    writer = fgraph.connect(path, clock=1_767_225_600_000_000)
    wal = Path(f"{path}-wal")
    shm = Path(f"{path}-shm")
    try:
        writer._connection.execute("PRAGMA wal_autocheckpoint = 0")  # noqa: SLF001
        writer._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        writer.transact({"id": "wal/item", "wal/value": "uncheckpointed"})
        assert wal.is_file()
        assert shm.is_file()

        for entry in (path, wal, shm):
            entry.chmod(0o444)
        directory.chmod(0o555)
        try:
            try:
                reader = fgraph.connect(path, read_only=True)
            except fgraph.FormatError:
                # A platform may require writable WAL locks. Refusing the open
                # is correct; immutable fallback would silently hide this row.
                pass
            else:
                try:
                    assert reader.entity("wal/item") == {"wal/value": "uncheckpointed"}
                finally:
                    reader.close()
        finally:
            directory.chmod(0o700)
            for entry in (path, wal, shm):
                entry.chmod(0o600)
    finally:
        writer.close()
