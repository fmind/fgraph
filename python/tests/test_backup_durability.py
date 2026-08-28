"""Cross-platform backup durability and online-copy regressions."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

import fgraph
from fgraph import store as store_module


class _SteppedBackupConnection:
    def __init__(self, connection: Any, progress: Any) -> None:
        self._connection = connection
        self._progress = progress

    def backup(self, target: Any) -> None:
        self._connection.backup(target, pages=1, progress=self._progress, sleep=0)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def test_backup_syncs_regular_file_through_write_capable_handle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    real_open = Path.open
    temporary_modes: list[str] = []

    def tracking_open(path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if path.name.startswith(f".{target.name}."):
            temporary_modes.append(mode)
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", tracking_open)
    with fgraph.connect(source) as database:
        database.transact({"id": "backup/item", "item/value": "preserved"})
        database.backup(target)

    assert temporary_modes == ["r+b"]


def test_windows_backup_skips_unsupported_directory_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(store_module, "_WINDOWS", True)

    def unexpected_open(*_args: Any, **_kwargs: Any) -> int:
        raise AssertionError("Windows directory synchronization must not call os.open")

    monkeypatch.setattr(store_module.os, "open", unexpected_open)
    store_module._sync_backup_directory(tmp_path)  # noqa: SLF001


def test_online_backup_allows_a_second_handle_to_commit_between_steps(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    with fgraph.connect(source, clock=1_767_225_600_000_000) as seed:
        seed.transact(
            [{"id": f"backup/item-{index}", "item/value": f"{index:04d}-" + "x" * 4096} for index in range(64)]
        )

    with (
        fgraph.connect(source, clock=1_767_225_600_000_001) as reader,
        fgraph.connect(source, clock=1_767_225_600_000_002) as writer,
    ):
        committed = False

        def commit_between_steps(_status: int, remaining: int, _total: int) -> None:
            nonlocal committed
            if remaining > 0 and not committed:
                writer.transact({"id": "backup/during", "item/value": "during"})
                committed = True

        connection = reader._connection  # noqa: SLF001
        reader._connection = cast(Any, _SteppedBackupConnection(connection, commit_between_steps))  # noqa: SLF001
        try:
            reader.backup(target)
        finally:
            reader._connection = connection  # noqa: SLF001

    assert committed is True
    with fgraph.connect(target, read_only=True) as backup:
        assert backup.entity("backup/during") == {"item/value": "during"}
        assert backup.doctor()["ok"] is True


def test_backup_refuses_reserved_application_attribute_corruption(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    with fgraph.connect(source) as database:
        database.transact({"id": "reserved/item", "reserved/value": 1})
        database._connection.execute(  # noqa: SLF001
            "UPDATE fgraph_ids SET name='fgraph/forged' WHERE name='reserved/value'"
        )

        with pytest.raises(fgraph.FormatError, match="backup failed verification"):
            database.backup(target)

    assert target.exists() is False
