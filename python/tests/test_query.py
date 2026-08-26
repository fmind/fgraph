"""Canonical in-process Datalog and pull behavior."""

from __future__ import annotations

import pytest

import fgraph


@pytest.fixture
def people(db: fgraph.Db) -> fgraph.Db:
    db.declare("person/knows", ref=True, many=True)
    db.transact(
        [
            {"id": "ada", "person/name": "Ada", "person/age": 36, "person/city": "Lyon"},
            {"id": "grace", "person/name": "Grace", "person/age": 40, "person/city": "Metz"},
            {"id": "linus", "person/name": "Linus", "person/age": 20, "person/status": "archived"},
        ]
    )
    db.transact({"id": "ada", "person/knows": [{"ref": "grace"}, {"ref": "linus"}]})
    db.transact({"id": "grace", "person/knows": {"ref": "linus"}})
    return db


def test_patterns_joins_and_unknowns(people: fgraph.Db) -> None:
    result = people.q(
        find=["?friend"],
        where=[["ada", "person/knows", "?f"], ["?f", "person/name", "?friend"]],
        order=[["?friend", "asc"]],
    )
    assert result.rows == [["Grace"], ["Linus"]]
    assert people.q(find=["?x"], where=[["missing", "person/name", "?x"]]).rows == []
    assert people.q(find=["?x"], where=[["?e", "unknown/attr", "?x"]]).rows == []
    assert people.q(find=["?e"], where=[["?e", "person/name", "_"]]).rows == [
        [{"ref": "ada"}],
        [{"ref": "grace"}],
        [{"ref": "linus"}],
    ]


def test_predicates_inputs_not_and_or(people: fgraph.Db) -> None:
    query = {
        "find": ["?name"],
        "where": [
            ["?e", "person/name", "?name"],
            ["?e", "person/age", "?age"],
            {"not": [["?e", "person/status", "archived"]]},
            {"or": [[["?e", "person/city", "Lyon"]], [["?e", "person/city", "Metz"]]]},
            [">=", "?age", "?min"],
            ["starts-with", "?name", "A"],
            ["contains", "?name", "d"],
        ],
        "in": ["?min"],
    }
    assert people.q(query, {"?min": 30}).rows == [["Ada"]]
    assert people.q(find=["?n"], where=[["?e", "person/name", "?n"], ["!=", "?n", "Ada"]]).rows == [
        ["Grace"],
        ["Linus"],
    ]


def test_aggregates_grouping_distinct_order_offset_limit(people: fgraph.Db) -> None:
    result = people.q(
        find=["?city", ["count", "?e"], ["sum", "?age"], ["avg", "?age"]],
        where=[["?e", "person/city", "?city"], ["?e", "person/age", "?age"]],
        order=[["?city", "desc"]],
    )
    assert result.columns == ["?city", "count(?e)", "sum(?age)", "avg(?age)"]
    assert result.rows == [["Metz", 1, 40, 40.0], ["Lyon", 1, 36, 36.0]]
    assert people.q(
        find=[["count-distinct", "?city"], ["min", "?age"], ["max", "?age"]],
        where=[["?e", "person/age", "?age"], ["?e", "person/city", "?city"]],
    ).rows == [[2, 36, 40]]
    ordered = people.q(
        find=["?age"],
        where=[["?e", "person/age", "?age"]],
        order=[["?age", "asc"]],
        offset=1,
        limit=1,
    )
    assert ordered.rows == [[36]]
    assert people.q(find=[["count", "?e"]], where=[["?e", "missing/value", "x"]]).rows == [[0]]


def test_order_by_unprojected_variable_uses_value_semantics(db: fgraph.Db) -> None:
    db.transact(
        [
            {"id": "ten", "item/name": "ten", "item/rank": 10},
            {"id": "two", "item/name": "two", "item/rank": 2},
        ]
    )
    result = db.q(
        find=["?name"],
        where=[["?e", "item/name", "?name"], ["?e", "item/rank", "?rank"]],
        order=[["?rank", "asc"]],
    )
    assert result.rows == [["two"], ["ten"]]


def test_pull_reverse_refs_and_depth(people: fgraph.Db) -> None:
    pulled = people.q(
        find=[["pull", "?e", ["person/name"]]],
        where=[["?e", "person/name", "Ada"]],
    )
    assert pulled.rows == [[{"person/name": "Ada"}]]
    assert people.pull("grace", ["person/_knows"])["person/_knows"] == [{"ref": "ada"}]
    assert people.entity("ada", depth=2)["person/knows"][0]["person/name"] == "Grace"


def test_self_recursive_rules(people: fgraph.Db) -> None:
    query = {
        "find": ["?name"],
        "where": [{"rule": ["ancestor", {"ref": "ada"}, "?desc"]}, ["?desc", "person/name", "?name"]],
        "rules": [
            {
                "head": ["ancestor", "?x", "?y"],
                "body": [["?x", "person/knows", "?y"]],
            },
            {
                "head": ["ancestor", "?x", "?y"],
                "body": [["?x", "person/knows", "?z"], {"rule": ["ancestor", "?z", "?y"]}],
            },
        ],
        "order": [["?name", "asc"]],
    }
    assert people.q(query).rows == [["Grace"], ["Linus"]]


@pytest.mark.parametrize(
    "query",
    [
        {},
        {"find": ["?x"], "where": "bad"},
        {"find": ["?x"], "where": [["bad"]]},
        {"find": ["?x"], "where": [["?e", "person/name", "?x"], [">", "?missing", 1]]},
        {"find": ["?x"], "where": [{"unknown": []}]},
        {"find": ["?x"], "where": [{"or": []}]},
        {
            "find": ["?x"],
            "where": [{"or": [[["?e", "person/name", "?x"]], [["?e", "person/age", "?age"]]]}],
        },
        {"find": ["?x"], "where": [], "unknown": True},
        {"find": ["?x"], "where": [], "in": ["bad"]},
        {"find": ["?x"], "where": [], "limit": -1},
    ],
)
def test_query_errors_are_typed(people: fgraph.Db, query) -> None:
    with pytest.raises(fgraph.QueryError):
        people.q(query)


def test_rule_and_order_errors(people: fgraph.Db) -> None:
    with pytest.raises(fgraph.QueryError):
        people.q(
            {
                "find": ["?x"],
                "where": [{"rule": ["a", "?x"]}],
                "rules": [
                    {"head": ["a", "?x"], "body": [{"rule": ["b", "?x"]}]},
                    {"head": ["b", "?x"], "body": [{"rule": ["a", "?x"]}]},
                ],
            }
        )
    with pytest.raises(fgraph.QueryError):
        people.q(find=["?x"], where=[["?e", "person/name", "?x"]], order=[["bad", "sideways"]])
    with pytest.raises(fgraph.QueryError):
        people.q({"find": ["?x"], "where": [], "in": ["?x"]}, {})


def test_safe_negation_attribute_rule_and_pull_aggregate_errors(people: fgraph.Db) -> None:
    with pytest.raises(fgraph.QueryError, match="negation is uncorrelated"):
        people.q(find=["?name"], where=[{"not": [["?e", "person/name", "?name"]]}])

    with pytest.raises(fgraph.QueryError, match="invalid pattern"):
        people.q(find=["?value"], where=[["?e", "person//name", "?value"]])

    with pytest.raises(fgraph.QueryError, match="same arity"):
        people.q(
            {
                "find": ["?x"],
                "where": [{"rule": ["r", "?x"]}],
                "rules": [
                    {"head": ["r", "?x"], "body": [["?e", "person/name", "?x"]]},
                    {"head": ["r", "?x", "?y"], "body": [["?e", "person/name", "?x"]]},
                ],
            }
        )
    with pytest.raises(fgraph.QueryError, match="expects 1 arguments"):
        people.q(
            {
                "find": ["?x"],
                "where": [{"rule": ["empty", "?x", "extra"]}],
                "rules": [{"head": ["empty", "?x"], "body": [["?e", "missing/value", "?x"]]}],
            }
        )

    with pytest.raises(fgraph.QueryError, match="mutually recursive"):
        people.q(
            {
                "find": ["?x"],
                "where": [{"rule": ["a", "?x"]}],
                "rules": [
                    {"head": ["a", "?x"], "body": [{"or": [[{"rule": ["b", "?x"]}]]}]},
                    {
                        "head": ["b", "?x"],
                        "body": [
                            ["?e", "person/name", "?x"],
                            {"not": [{"rule": ["a", "?x"]}]},
                        ],
                    },
                ],
            }
        )

    with pytest.raises(fgraph.QueryError, match=r"pull.*aggregate"):
        people.q(
            find=[["pull", "?e", ["person/name"]], ["count", "?age"]],
            where=[["?e", "person/age", "?age"]],
        )


def test_undeclared_attribute_constants_use_value_index() -> None:
    with fgraph.connect(":memory:", clock=1_767_225_600_000_000, query_budget=3) as graph:
        graph.transact([{"id": f"item/{index}", "item/group": index} for index in range(100)])

        result = graph.q(find=["?entity"], where=[["?entity", "item/group", 77]])

    assert result.rows == [[{"ref": "item/77"}]]
