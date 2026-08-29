"""Regression tests for API v1 hardening contracts."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from mcp.server.mcpserver.exceptions import ToolError, UnexpectedToolError
from mcp.types import CallToolResult
from typer.testing import CliRunner

import fgraph
from fgraph.cli import app
from fgraph.mcp_server import create_server
from fgraph.values import INT64_MAX, canonical_json

runner = CliRunner()


class _BudgetGuardedCursor:
    def __init__(self, cursor: Any, owner: _BudgetGuardedConnection) -> None:
        self._cursor = cursor
        self._owner = owner

    def __iter__(self) -> Iterator[Any]:
        for row in self._cursor:
            self._owner.rows_seen += 1
            if self._owner.rows_seen > self._owner.maximum_rows:
                raise AssertionError("query consumed candidates beyond its remaining work budget")
            yield row

    def fetchall(self) -> list[Any]:
        raise AssertionError("query eagerly materialized its candidate cursor")


class _BudgetGuardedConnection:
    def __init__(
        self,
        connection: sqlite3.Connection,
        guarded: Callable[[str], bool],
        maximum_rows: int,
    ) -> None:
        self._connection = connection
        self._guarded = guarded
        self.maximum_rows = maximum_rows
        self.rows_seen = 0

    def execute(self, sql: str, parameters: Any = ()) -> Any:
        cursor = self._connection.execute(sql, parameters)
        return _BudgetGuardedCursor(cursor, self) if self._guarded(sql) else cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def _physical_state(db: fgraph.Db) -> dict[str, list[tuple[Any, ...]]]:
    connection = db._connection  # noqa: SLF001
    return {
        "meta": [tuple(row) for row in connection.execute("SELECT key,value FROM fgraph_meta ORDER BY key")],
        "ids": [
            tuple(row) for row in connection.execute("SELECT id,name,hex(gid),created_tx FROM fgraph_ids ORDER BY id")
        ],
        "events": [
            tuple(row)
            for row in connection.execute(
                "SELECT tx,hex(event_hash),event_data,operation_id,hex(request_hash) FROM fgraph_events ORDER BY tx"
            )
        ],
        "facts": [tuple(row) for row in connection.execute("SELECT id,e,a,v,t,tx,rx FROM fgraph_facts ORDER BY id")],
        "blobs": [
            tuple(row) for row in connection.execute("SELECT hex(hash),hex(data) FROM fgraph_blobs ORDER BY hash")
        ],
        "fts": [tuple(row) for row in connection.execute("SELECT rowid,text FROM fgraph_fts ORDER BY rowid")],
        "sequence": [tuple(row) for row in connection.execute("SELECT name,seq FROM sqlite_sequence ORDER BY name")],
    }


def _event_line(entity: str, value: Any) -> str:
    with fgraph.connect(":memory:", clock=1_767_225_600_000_000) as source:
        source.transact({"id": entity, "item/value": value})
        return f"{canonical_json(source.event_records()[0])}\n"


def test_apply_rolls_back_the_complete_stream(db: fgraph.Db) -> None:
    before = _physical_state(db)
    first = _event_line("first", "x" * 300)

    with pytest.raises(fgraph.TypeError, match="event line 2"):
        db.apply(first + "not-json\n")

    assert _physical_state(db) == before


def test_apply_rolls_back_process_level_interruptions(db: fgraph.Db) -> None:
    before = _physical_state(db)
    first = _event_line("first", "value")

    def interrupted_lines() -> Iterator[str]:
        yield first
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        db.apply(interrupted_lines())

    assert not db._connection.in_transaction  # noqa: SLF001
    assert _physical_state(db) == before
    assert db.transact({"id": "after", "note/text": "committed"}).tx == 67
    assert not db._connection.in_transaction  # noqa: SLF001


def test_committed_write_never_fails_during_cache_publication(db: fgraph.Db, monkeypatch: pytest.MonkeyPatch) -> None:
    original_refresh = db._refresh_cache  # noqa: SLF001

    def reject_post_commit_refresh(*, force: bool = False) -> None:
        if force and not db._connection.in_transaction:  # noqa: SLF001
            raise sqlite3.DatabaseError("injected post-commit cache failure")
        original_refresh(force=force)

    monkeypatch.setattr(db, "_refresh_cache", reject_post_commit_refresh)
    report = db.transact({"id": "direct", "item/value": 1})

    assert report.tx is not None
    assert db._cache_version >= 0  # noqa: SLF001
    assert db.entity("direct")["item/value"] == 1

    applied = db.apply(_event_line("merged", 2))
    assert len(applied) == 1
    assert db._cache_version >= 0  # noqa: SLF001
    assert db.entity("merged")["item/value"] == 2


def test_sequential_writes_do_not_rescan_the_identity_registry(db: fgraph.Db) -> None:
    statements: list[str] = []
    db._connection.set_trace_callback(statements.append)  # noqa: SLF001

    for index in range(10):
        db.transact({"id": f"cached/{index}", "item/value": index})

    registry_scans = [statement for statement in statements if statement == "SELECT id, name, gid FROM fgraph_ids"]
    assert registry_scans == []


def test_allocator_exhaustion_is_typed_atomic_and_diagnosable(db: fgraph.Db) -> None:
    db._connection.execute("UPDATE fgraph_meta SET value=? WHERE key='next_id'", (INT64_MAX,))  # noqa: SLF001
    before = _physical_state(db)

    with pytest.raises(fgraph.TooLarge, match="allocator is exhausted"):
        db.transact({"id": "overflow", "item/value": 1})

    assert _physical_state(db) == before
    db._connection.execute(  # noqa: SLF001
        "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,NULL,?)",
        (INT64_MAX, "corrupt/max", 64),
    )
    report = db.doctor()
    assert report["ok"] is False
    assert any("allocator exhausted" in problem for problem in report["problems"])


@pytest.mark.parametrize(
    ("kind", "problem"),
    [
        ("identity", "invalid identity ids"),
        ("fact", "invalid fact identifiers"),
        ("named_tx", "named identities overlap transaction receipts"),
        ("assert_tx", "facts reference missing asserting transactions"),
        ("retract_tx", "facts reference missing retracting transactions"),
    ],
)
def test_doctor_rejects_broken_identity_and_transaction_links(db: fgraph.Db, kind: str, problem: str) -> None:
    receipt = db.transact({"id": "doctor-subject", "item/value": 1})
    connection = db._connection  # noqa: SLF001
    if kind == "identity":
        connection.execute("INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (-1,'corrupt/identity',NULL,64)")
    elif kind == "fact":
        connection.execute("INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (-1,2,'corrupt',4,64,NULL)")
    elif kind == "named_tx":
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute("UPDATE fgraph_ids SET name='corrupt/transaction' WHERE id=64")
    elif kind == "assert_tx":
        connection.execute("DELETE FROM fgraph_facts WHERE e=? AND a=1 AND tx=e", (receipt.tx,))
    else:
        retraction = db.retract("doctor-subject", "item/value")
        connection.execute("DELETE FROM fgraph_facts WHERE e=? AND a=1 AND tx=e", (retraction.tx,))

    checked = db.doctor()
    assert checked["ok"] is False
    assert any(problem in message for message in checked["problems"])
    with pytest.raises(fgraph.FormatError, match="non-rebuildable"):
        db.doctor(repair=True)


def test_doctor_rejects_blob_content_corruption(db: fgraph.Db) -> None:
    db.transact({"id": "vector", "item/vector": {"vector": [1.0, 2.0]}})
    db._connection.execute(  # noqa: SLF001
        "UPDATE fgraph_blobs SET data=zeroblob(length(data))"
    )

    checked = db.doctor()

    assert checked["ok"] is False
    assert "invalid indirect blobs: 1" in checked["problems"]
    with pytest.raises(fgraph.FormatError, match="non-rebuildable"):
        db.doctor(repair=True)


def test_keyword_search_filters_system_facts_before_ranking_and_adds_provenance(db: fgraph.Db) -> None:
    for index in range(55):
        db.transact({}, source="needle", by=f"noise-{index}")
    report = db.transact(
        {"id": "domain", "note/text": "the needle belongs to the application"},
        source="project.md",
        by="agent",
    )

    result = db.search("needle")

    assert [hit["entity"] for hit in result.hits] == ["domain"]
    matched = result.hits[0]["matched"]
    assert len(matched) == 1
    assert matched[0]["at"] == report.at
    assert matched[0]["by"] == "agent"
    assert matched[0]["source"] == "project.md"


def test_query_budget_is_connection_scoped_and_counts_candidate_pairs() -> None:
    with fgraph.connect(":memory:", query_budget=1) as graph:
        graph.transact({"id": "one", "item/value": 1})
        query = {"find": ["?e"], "where": [["?e", "item/value", "_"]]}
        assert graph.q(query).rows == [[{"ref": "one"}]]
        graph.transact({"id": "two", "item/value": 2})
        with pytest.raises(fgraph.TooLarge, match="work budget"):
            graph.q(query)


def test_query_pull_projection_consumes_active_work_budget() -> None:
    query = {
        "find": [["pull", "?entity", ["*"]]],
        "where": [["?entity", "pull/name", "target"]],
    }
    with fgraph.connect(":memory:", query_budget=2) as graph:
        graph.transact({"id": "pull/target", "pull/name": "target", "pull/enabled": True})

        with pytest.raises(fgraph.TooLarge, match="work budget"):
            graph.q(query)

    with fgraph.connect(":memory:", query_budget=3) as graph:
        graph.transact({"id": "pull/target", "pull/name": "target", "pull/enabled": True})

        assert graph.q(query).rows == [[{"pull/enabled": True, "pull/name": "target"}]]


def test_query_nested_pull_projection_consumes_active_work_budget() -> None:
    with fgraph.connect(":memory:", query_budget=3) as graph:
        graph.declare("pull/child", ref=True)
        graph.transact(
            [
                {"id": "pull/root", "pull/match": "root", "pull/child": {"ref": "pull/leaf"}},
                {"id": "pull/leaf", "pull/name": "leaf"},
            ]
        )
        where = [["?entity", "pull/match", "root"]]

        assert graph.q(find=[["pull", "?entity", ["pull/match"]]], where=where).rows == [[{"pull/match": "root"}]]
        with pytest.raises(fgraph.TooLarge, match="work budget"):
            graph.q(find=[["pull", "?entity", [{"pull/child": ["*"]}]]], where=where)


def test_query_budget_bounds_general_candidate_cursor_consumption(monkeypatch: pytest.MonkeyPatch) -> None:
    with fgraph.connect(":memory:", query_budget=1) as graph:
        graph.transact([{"id": f"item/{index}", "item/value": index} for index in range(3)])
        guarded = _BudgetGuardedConnection(
            graph._connection,  # noqa: SLF001
            lambda sql: sql.startswith("SELECT * FROM fgraph_facts WHERE"),
            maximum_rows=2,
        )
        monkeypatch.setattr(graph, "_connection", guarded)

        with pytest.raises(fgraph.TooLarge, match="work budget"):
            graph.q({"find": ["?e"], "where": [["?e", "item/value", "_"]]})

        assert guarded.rows_seen == 2


def test_query_budget_bounds_batched_candidate_cursor_consumption(monkeypatch: pytest.MonkeyPatch) -> None:
    with fgraph.connect(":memory:", query_budget=4) as graph:
        graph.transact([{"id": f"item/{index}", "item/key": index, "item/name": f"Item {index}"} for index in range(3)])
        guarded = _BudgetGuardedConnection(
            graph._connection,  # noqa: SLF001
            lambda sql: " e IN (" in sql,
            maximum_rows=2,
        )
        monkeypatch.setattr(graph, "_connection", guarded)

        with pytest.raises(fgraph.TooLarge, match="work budget"):
            graph.q(
                {
                    "find": ["?e", "?name"],
                    "where": [["?e", "item/key", "_"], ["?e", "item/name", "?name"]],
                }
            )

        assert guarded.rows_seen == 2


def test_query_budget_uses_set_semantics_between_patterns() -> None:
    with fgraph.connect(":memory:", query_budget=3) as graph:
        graph.declare("item/tags", many=True)
        graph.transact({"id": "one", "item/name": "One", "item/tags": ["a", "b"]})
        result = graph.q(
            {
                "find": ["?e"],
                "where": [["?e", "item/tags", "_"], ["?e", "item/name", "_"]],
            }
        )

    assert result.rows == [[{"ref": "one"}]]


def test_query_budget_uses_set_semantics_after_rules() -> None:
    with fgraph.connect(":memory:", query_budget=7) as graph:
        graph.declare("item/tags", many=True)
        graph.transact({"id": "one", "item/name": "One", "item/tags": ["a", "b"]})
        result = graph.q(
            {
                "find": ["?e"],
                "rules": [
                    {
                        "head": ["has-tag", "?entity", "?tag"],
                        "body": [["?entity", "item/tags", "?tag"]],
                    }
                ],
                "where": [
                    {"rule": ["has-tag", "?e", "_"]},
                    ["?e", "item/name", "_"],
                ],
            }
        )

    assert result.rows == [[{"ref": "one"}]]


def test_query_budget_short_circuits_unresolved_pattern_constants() -> None:
    with fgraph.connect(":memory:", query_budget=1) as graph:
        graph.declare("item/link", ref=True, many=True)
        graph.transact(
            [
                {"id": "one", "item/value": 1, "item/link": {"ref": "target-one"}},
                {"id": "two", "item/value": 2, "item/link": {"ref": "target-two"}},
            ]
        )
        assert graph.q({"find": ["?value"], "where": [["missing", "item/value", "?value"]]}).rows == []
        assert graph.q({"find": ["?e"], "where": [["?e", "item/link", {"ref": "missing"}]]}).rows == []
        with pytest.raises(fgraph.TooLarge):
            graph.q({"find": ["?e"], "where": [["?e", "item/value", "_"]]})


def test_query_budget_normalizes_signed_zero_between_or_branches() -> None:
    with fgraph.connect(":memory:", query_budget=3) as graph:
        graph.transact({"id": "one", "number/negative": -0.0, "number/positive": 0.0, "item/name": "One"})
        result = graph.q(
            {
                "find": ["?e"],
                "where": [
                    {
                        "or": [
                            [["?e", "number/negative", "?number"]],
                            [["?e", "number/positive", "?number"]],
                        ]
                    },
                    ["?e", "item/name", "_"],
                ],
            }
        )

    assert result.rows == [[{"ref": "one"}]]


def test_query_budget_builds_rule_dependencies_topologically() -> None:
    with fgraph.connect(":memory:", query_budget=5) as graph:
        graph.transact({"id": "one", "item/tag": "a"})
        result = graph.q(
            {
                "find": ["?e"],
                "where": [{"rule": ["derived", "?e"]}],
                "rules": [
                    {
                        "head": ["derived", "?entity"],
                        "body": [{"rule": ["base", "?entity"]}],
                    },
                    {
                        "head": ["base", "?entity"],
                        "body": [["?entity", "item/tag", "a"]],
                    },
                ],
            }
        )

    assert result.rows == [[{"ref": "one"}]]


@pytest.mark.parametrize("budget", [0, -1, True, 1.5])
def test_query_budget_must_be_a_positive_integer(budget: Any) -> None:
    with pytest.raises(fgraph.TypeError, match="query_budget"):
        fgraph.connect(":memory:", query_budget=budget)


def test_attributes_discovers_effective_schema_and_observed_types(db: fgraph.Db) -> None:
    db.declare("person/name", type="text", unique=True, doc="Display name")
    db.declare("person/tags", many=True)
    db.transact(
        {
            "id": "ada",
            "person/name": "Ada",
            "person/tags": ["compiler", 1],
            "note/vector": {"vector": [1, 0]},
        }
    )

    attributes = db.attributes()

    assert [item["name"] for item in attributes] == ["note/vector", "person/name", "person/tags"]
    assert attributes[0] == {
        "name": "note/vector",
        "types": ["vector"],
        "facts": 1,
        "many": False,
        "unique": False,
        "nohistory": True,
        "dims": 2,
    }
    assert attributes[1] == {
        "name": "person/name",
        "types": ["text"],
        "facts": 1,
        "many": False,
        "unique": True,
        "nohistory": False,
        "doc": "Display name",
    }
    assert attributes[2]["types"] == ["int", "text"]
    assert attributes[2]["many"] is True
    assert [item["name"] for item in db.attributes("person/")] == ["person/name", "person/tags"]
    assert any(item["name"] == "fgraph/at" for item in db.attributes(include_system=True))


def test_attributes_validates_options(db: fgraph.Db) -> None:
    invalid: Any = 1
    with pytest.raises(fgraph.TypeError, match="prefix"):
        db.attributes(invalid)
    with pytest.raises(fgraph.TypeError, match="include_system"):
        db.attributes(include_system=invalid)


def test_doctor_is_check_only_by_default_and_repairs_explicitly(tmp_path: Path) -> None:
    path = tmp_path / "doctor.db"
    with fgraph.connect(path) as graph:
        graph.transact({"id": "note", "note/text": "searchable"})
        graph._connection.execute(  # noqa: SLF001
            "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", (b"orphan", b"unused")
        )
        graph._connection.execute("DELETE FROM fgraph_fts")  # noqa: SLF001
        before = _physical_state(graph)

        checked = graph.doctor()

        assert set(checked) == {
            "ok",
            "integrity",
            "problems",
            "repair_needed",
            "repaired",
            "fts_rows",
            "expected_fts_rows",
            "orphaned_blobs",
            "unverifiable_event_hashes",
            "schema_problems",
            "shape_violations",
            "fts_rows_rebuilt",
            "orphaned_blobs_removed",
        }
        assert checked["ok"] is False
        assert checked["repair_needed"] is True
        assert checked["repaired"] is False
        assert checked["fts_rows"] == 0
        assert checked["expected_fts_rows"] >= 1
        assert checked["orphaned_blobs"] == 1
        assert _physical_state(graph) == before

        repaired = graph.doctor(repair=True)
        assert repaired["ok"] is True
        assert repaired["repair_needed"] is False
        assert repaired["repaired"] is True
        assert repaired["fts_rows"] == repaired["expected_fts_rows"]
        assert repaired["fts_rows_rebuilt"] == repaired["expected_fts_rows"]
        assert repaired["orphaned_blobs"] == 0
        assert repaired["orphaned_blobs_removed"] == 1

    with fgraph.connect(path, read_only=True) as read_only:
        assert read_only.doctor()["ok"] is True
        with pytest.raises(fgraph.ReadOnly):
            read_only.doctor(repair=True)


def test_cli_schema_and_explicit_doctor_repair(tmp_path: Path) -> None:
    path = tmp_path / "cli.db"
    env = {"FGRAPH_CLOCK": "1767225600000000"}
    added = runner.invoke(
        app,
        ["add", '{"id":"ada","person/name":"Ada"}', "--db", str(path), "--json"],
        env=env,
    )
    assert added.exit_code == 0, added.output
    transaction = json.loads(added.stdout)["tx"]
    updated = runner.invoke(app, ["add", '{"id":"ada","person/name":"Augusta"}', "--db", str(path)], env=env)
    assert updated.exit_code == 0, updated.output
    historical = runner.invoke(
        app,
        ["get", "ada", "--at", str(transaction), "--db", str(path), "--json"],
        env=env,
    )
    assert json.loads(historical.stdout)["person/name"] == "Ada"
    query = '{"find":["?name"],"where":[["ada","person/name","?name"]]}'
    queried = runner.invoke(
        app,
        ["q", query, "--at", str(transaction), "--db", str(path), "--json"],
        env=env,
    )
    assert json.loads(queried.stdout)["rows"] == [["Ada"]]

    schema = runner.invoke(app, ["schema", "person/", "--db", str(path), "--json"], env=env)
    assert schema.exit_code == 0, schema.output
    assert [item["name"] for item in json.loads(schema.stdout)["attributes"]] == ["person/name"]
    system = runner.invoke(app, ["schema", "fgraph/", "--system", "--db", str(path), "--json"], env=env)
    assert system.exit_code == 0, system.output
    assert any(item["name"] == "fgraph/at" for item in json.loads(system.stdout)["attributes"])

    checked = runner.invoke(app, ["doctor", "--db", str(path), "--json"], env=env)
    assert checked.exit_code == 0, checked.output
    assert json.loads(checked.stdout)["repaired"] is False
    repaired = runner.invoke(app, ["doctor", "--repair", "--db", str(path), "--json"], env=env)
    assert repaired.exit_code == 0, repaired.output
    assert json.loads(repaired.stdout)["repaired"] is True


def test_cli_query_budget_flag_and_environment_are_forwarded(tmp_path: Path) -> None:
    path = tmp_path / "cli-budget.db"
    with fgraph.connect(path) as graph:
        graph.transact({"id": "one", "item/value": 1})
        graph.transact({"id": "two", "item/value": 2})
    query = '{"find":["?e"],"where":[["?e","item/value","_"]]}'

    by_flag = runner.invoke(app, ["--query-budget", "1", "q", query, "--db", str(path)])
    assert by_flag.exit_code == 1
    assert isinstance(by_flag.exception, fgraph.TooLarge)

    by_environment = runner.invoke(
        app,
        ["q", query, "--db", str(path)],
        env={"FGRAPH_QUERY_BUDGET": "1"},
    )
    assert by_environment.exit_code == 1
    assert isinstance(by_environment.exception, fgraph.TooLarge)


@pytest.mark.anyio
async def test_mcp_keyed_memory_and_schema_tool(db: fgraph.Db) -> None:
    server = create_server(db, read_only=False)
    tools = await server.list_tools()
    assert "schema" in [tool.name for tool in tools]

    first = await server.call_tool(
        "remember",
        {"key": "decision/editor", "text": "Use Vim", "operation_id": "remember:vim"},
    )
    second = await server.call_tool(
        "remember",
        {"key": "decision/editor", "text": "Use Helix", "operation_id": "remember:helix"},
    )
    assert isinstance(first, CallToolResult)
    assert isinstance(second, CallToolResult)
    assert first.structured_content["data"]["tx"] < second.structured_content["data"]["tx"]
    about = await server.call_tool("about", {"entity": "decision/editor"})
    assert isinstance(about, CallToolResult)
    assert about.structured_content["data"]["memory/text"] == "Use Helix"
    timeline = db.history("decision/editor", "memory/text")
    assert [item["v"] for item in timeline] == ["Use Vim", "Use Helix"]

    schema = await server.call_tool("schema", {"prefix": "memory/"})
    assert isinstance(schema, CallToolResult)
    assert schema.structured_content["data"]["attributes"][0]["name"] == "memory/text"
    with pytest.raises(ToolError, match="key requires text"):
        await server.call_tool(
            "remember",
            {"key": "decision/invalid", "facts": {"id": "other"}, "operation_id": "remember:invalid"},
        )


@pytest.mark.anyio
async def test_failed_embedded_remember_is_atomic(db: fgraph.Db, monkeypatch: pytest.MonkeyPatch) -> None:
    before_stats = db.stats()
    before_state = _physical_state(db)
    monkeypatch.setattr("fgraph.mcp_server.embed", lambda _command, _text: [1.0, 0.0])
    server = create_server(db, read_only=False, embed_cmd="test-embedder")

    with pytest.raises(ToolError, match="reserved fgraph"):
        await server.call_tool(
            "remember",
            {"key": "fgraph/bad", "text": "must fail atomically", "operation_id": "remember:bad"},
        )

    assert db.stats() == before_stats
    assert _physical_state(db) == before_state


@pytest.mark.anyio
async def test_mcp_unexpected_tool_failures_do_not_leak_internal_details(
    db: fgraph.Db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = create_server(db)

    def crash(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("private implementation detail")

    monkeypatch.setattr(fgraph.Db, "entity", crash)
    with pytest.raises(UnexpectedToolError) as raised:
        await server.call_tool("about", {"entity": "anything"})

    assert str(raised.value) == "Error executing tool about"
    assert "private implementation detail" not in str(raised.value)


def test_version_is_v1() -> None:
    assert fgraph.__version__ == "1.2.0"
