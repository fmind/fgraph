#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "rich>=15.0.0",
#     "typer>=0.27.1",
# ]
# ///
"""Create a deterministic flat ZIP archive for release assets."""

from __future__ import annotations

import os
import stat
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import typer
from rich.console import Console

app = typer.Typer(add_completion=False, pretty_exceptions_show_locals=False)
out = Console()
err = Console(stderr=True)


def create_archive(source_directory: Path, output: Path, source_date_epoch: int) -> None:
    """Write the source directory's regular files with stable names and metadata."""
    source = source_directory.resolve(strict=True)
    destination = output.resolve()
    if destination.is_relative_to(source):
        raise ValueError("output must be outside the source directory")
    if not destination.parent.is_dir():
        raise FileNotFoundError(f"output directory does not exist: {destination.parent}")

    entries = sorted(source.iterdir(), key=lambda path: path.name)
    if not entries:
        raise ValueError("source directory is empty")
    invalid = [path.name for path in entries if path.is_symlink() or not path.is_file()]
    if invalid:
        raise ValueError(f"source directory contains non-regular entries: {invalid!r}")

    timestamp = datetime.fromtimestamp(source_date_epoch, UTC)
    if not 1980 <= timestamp.year <= 2107:
        raise ValueError("source date must be within the ZIP timestamp range 1980-2107")

    handle, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        with ZipFile(temporary, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
            for path in entries:
                info = ZipInfo(path.name, date_time=timestamp.timetuple()[:6])
                info.compress_type = ZIP_DEFLATED
                info.create_system = 3
                # Release ZIPs target Windows; stable regular-file modes avoid
                # leaking the builder's umask while remaining portable.
                info.external_attr = (stat.S_IFREG | 0o644) << 16
                archive.writestr(info, path.read_bytes(), compress_type=ZIP_DEFLATED, compresslevel=9)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


@app.command()
def main(
    source_directory: Annotated[
        Path,
        typer.Argument(help="Directory containing the flat files to archive", exists=True, file_okay=False),
    ],
    output: Annotated[Path, typer.Option("--output", "-o", help="ZIP archive to create")],
    source_date_epoch: Annotated[
        int,
        typer.Option("--source-date-epoch", min=315_532_800, help="UTC Unix timestamp stored in every entry"),
    ],
) -> None:
    """Create a byte-reproducible ZIP without relying on an ambient zip CLI."""
    try:
        create_archive(source_directory, output, source_date_epoch)
        out.print(str(output))
    except Exception:
        err.print_exception(show_locals=False)
        raise typer.Exit(code=1) from None


if __name__ == "__main__":
    app()
