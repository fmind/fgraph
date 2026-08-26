"""Fail-fast read checks for content-addressed values."""

from __future__ import annotations

import pytest

import fgraph


def test_read_rejects_tampered_indirect_blob(db: fgraph.Db) -> None:
    db.transact({"id": "value", "value/data": "x" * 257})
    db._connection.execute("UPDATE fgraph_blobs SET data=?", ("y" * 257,))  # noqa: SLF001

    with pytest.raises(fgraph.FormatError, match="content-addressed hash"):
        db.entity("value")
