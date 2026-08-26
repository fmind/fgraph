"""Search validation, deduplication, expansion, and output-bound edges."""

from __future__ import annotations

from typing import Any

import pytest

import fgraph
from fgraph import search as search_module


def test_search_rejects_agent_facing_option_edges(db: fgraph.Db) -> None:
    created = db.transact({"id": "search/item", "search/text": "needle"})
    invalid_text: Any = 1

    with pytest.raises(fgraph.Unsupported):
        db.at(created.tx).search("needle")
    with pytest.raises(fgraph.TypeError, match="search text"):
        db.search(invalid_text)
    with pytest.raises(fgraph.TypeError, match="vector_attribute requires"):
        db.search("needle", vector_attribute="search/vector")
    with pytest.raises(fgraph.TypeError, match="all zeroes"):
        db.search(vector=[0, 0], vector_attribute="search/vector")
    with pytest.raises(fgraph.NotFound, match="text search attribute"):
        db.search("needle", text_attributes=["missing/text"])
    with pytest.raises(fgraph.TypeError, match="not vector-typed"):
        db.search(vector=[1, 0], vector_attribute="search/text")

    db.transact({"id": "schema-only", "empty/vector": "not populated as a vector"})
    with pytest.raises(fgraph.TypeError, match="not vector-typed"):
        db.search(vector=[1, 0], vector_attribute="empty/vector")

    invalid_cases: list[dict[str, Any]] = [
        {"text": "needle", "filters": [["search/text", "needle"]] * 17},
        {"text": "needle", "text_attributes": "search/text"},
        {"text": "needle", "text_attributes": ["search/text"] * 17},
        {"text": "needle", "text_attributes": [""]},
        {"text": "needle", "filters": [[1, "needle"]]},
        {"text": "needle", "filters": [["search/text"]]},
    ]
    for options in invalid_cases:
        with pytest.raises(fgraph.TypeError):
            db.search(**options)


def test_search_deduplicates_keyword_facts_and_stops_empty_filter_intersections(db: fgraph.Db) -> None:
    db.transact(
        [
            {
                "id": "search/one",
                "search/title": "shared needle",
                "search/body": "shared needle",
                "search/kind": "guide",
                "search/color": "red",
            },
            {
                "id": "search/two",
                "search/title": "shared needle",
                "search/kind": "note",
                "search/color": "blue",
            },
        ]
    )
    result = db.search("shared needle", text_attributes=["search/title", "search/body"])
    assert [hit["entity"] for hit in result.hits].count("search/one") == 1

    empty = db.search(
        "shared needle",
        filters=[["search/kind", "guide"], ["search/color", "blue"]],
    )
    assert empty.hits == []


def test_search_expansion_and_complete_result_size_are_bounded(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db.declare("search/link", ref=True, many=True)
    db.transact(
        [
            {
                "id": "search/root",
                "search/text": "root needle",
                "search/link": [{"ref": "search/a"}, {"ref": "search/b"}],
            },
            {"id": "search/a", "search/text": "neighbor a"},
            {"id": "search/b", "search/text": "neighbor b"},
        ]
    )
    monkeypatch.setattr(search_module, "MAX_EXPANDED_NODES", 1)
    expanded = db.search("root needle", expand=1)
    assert len(expanded.expanded) == 1
    assert expanded.truncated is True

    monkeypatch.setattr(search_module, "MAX_RESULT_BYTES", 100)
    bounded = search_module._bounded_result(  # noqa: SLF001
        64,
        [
            {"entity": "one", "score": 1.0, "matched": [{"v": "x" * 300}]},
            {"entity": "two", "score": 0.5, "matched": [{"v": "y" * 300}]},
        ],
        [{"entity": "neighbor", "pull": {"value": "z" * 300}}],
        False,
        1,
    )
    assert bounded.truncated is True
    assert bounded.expanded == []
    assert all(hit["matched"] == [] for hit in bounded.hits)
    assert len(bounded.hits) < 2
