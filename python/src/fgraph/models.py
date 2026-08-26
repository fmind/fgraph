"""Small immutable result values used by the public API."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class TxReport(Mapping[str, Any]):
    """Receipt for one committed transaction, or an elided no-op."""

    status: Literal["applied", "already_applied", "noop"] = "noop"
    event: str | None = None
    basis_tx: int = 64
    tx: int | None = None
    at: int | None = None
    ids: dict[str, int] = field(default_factory=dict)
    asserted: list[dict[str, Any]] = field(default_factory=list)
    retracted: list[dict[str, Any]] = field(default_factory=list)

    def __getitem__(self, key: str) -> Any:
        return asdict(self)[key]

    def __iter__(self) -> Iterator[str]:
        return iter(("status", "event", "basis_tx", "tx", "at", "ids", "asserted", "retracted"))

    def __len__(self) -> int:
        return 8

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class Result(Mapping[str, Any]):
    """Canonical query result."""

    columns: list[str]
    rows: list[list[Any]]

    def __getitem__(self, key: str) -> Any:
        if key == "columns":
            return self.columns
        if key == "rows":
            return self.rows
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(("columns", "rows"))

    def __len__(self) -> int:
        return 2

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping."""
        return {"columns": self.columns, "rows": self.rows}


@dataclass(frozen=True, slots=True)
class SearchResult(Mapping[str, Any]):
    """Keyword/vector hits plus graph-expanded neighbors."""

    basis_tx: int
    hits: list[dict[str, Any]]
    expanded: list[dict[str, Any]]
    truncated: bool = False
    work_used: int = 0

    def __getitem__(self, key: str) -> Any:
        return asdict(self)[key]

    def __iter__(self) -> Iterator[str]:
        return iter(("basis_tx", "hits", "expanded", "truncated", "work_used"))

    def __len__(self) -> int:
        return 5

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping."""
        return asdict(self)
