"""Shared deterministic fixtures."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

import fgraph


class Clock:
    """Simple deterministic microsecond clock."""

    def __init__(self, start: int = 1_767_225_600_000_000) -> None:
        self.value = start

    def __call__(self) -> int:
        result = self.value
        self.value += 1_000_000
        return result


@pytest.fixture
def db() -> Iterator[fgraph.Db]:
    graph = fgraph.connect(":memory:", clock=Clock())
    yield graph
    graph.close()
