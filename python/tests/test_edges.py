"""Boundary and failure-path tests that keep the public contract fail-fast."""

from __future__ import annotations

import asyncio
import base64
import io
import runpy
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from conftest import Clock
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult
from typer.testing import CliRunner

import fgraph
import fgraph._embed_runner as embed_runner
import fgraph.cli as cli
import fgraph.mcp_server as mcp_server
import fgraph.store as store
from fgraph.jsonio import loads
from fgraph.mcp_server import create_server, embed
from fgraph.models import Result, SearchResult, TxReport
from fgraph.query import _aggregate, _column, _compare, _find_value, _sort_key
from fgraph.search import _cosine
from fgraph.values import (
    BLOB_THRESHOLD,
    BYTES_REF,
    INT,
    JSON,
    MAX_VALUE_BYTES,
    TEXT_REF,
    VECTOR,
    Cell,
    canonical_json,
    encode,
    indirect_digest,
)


def test_result_values_implement_mappings() -> None:
    report = TxReport(
        status="applied",
        event="00000000-0000-4000-8000-000000000001",
        basis_tx=64,
        tx=1,
        at=2,
        ids={"x": 3},
        asserted=[{"a": 1}],
        retracted=[{"a": 2}],
    )
    assert len(report) == 8
    assert list(report) == ["status", "event", "basis_tx", "tx", "at", "ids", "asserted", "retracted"]
    assert report["ids"] == {"x": 3}
    result = Result(["?x"], [[1]])
    assert len(result) == 2
    assert list(result) == ["columns", "rows"]
    assert result["columns"] == ["?x"]
    with pytest.raises(KeyError):
        _ = result["missing"]
    search = SearchResult(64, [{"entity": 1}], [{"entity": 2}])
    assert len(search) == 5
    assert list(search) == ["basis_tx", "hits", "expanded", "truncated", "work_used"]
    assert search["hits"] == [{"entity": 1}]
    assert search["expanded"] == [{"entity": 2}]
    with pytest.raises(KeyError):
        _ = search["missing"]


def test_module_entrypoint_calls_main(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[bool] = []
    monkeypatch.setattr(cli, "main", lambda: called.append(True))
    runpy.run_module("fgraph.__main__", run_name="__main__")
    assert called == [True]


def test_windows_embed_runner_forwards_streams(monkeypatch: pytest.MonkeyPatch) -> None:
    stdin = SimpleNamespace(buffer=io.BytesIO(b"text"))
    stdout = SimpleNamespace(buffer=io.BytesIO())
    command = ["embedder", "--json"]

    def run(arguments: list[str], **options: Any) -> SimpleNamespace:
        assert arguments == command
        assert options == {
            "input": b"text",
            "stdout": stdout.buffer,
            "stderr": subprocess.DEVNULL,
            "check": False,
        }
        return SimpleNamespace(returncode=7)

    monkeypatch.setattr(embed_runner.sys, "argv", ["fgraph._embed_runner", canonical_json(command)])
    monkeypatch.setattr(embed_runner.sys, "stdin", stdin)
    monkeypatch.setattr(embed_runner.sys, "stdout", stdout)
    monkeypatch.setattr(embed_runner.subprocess, "run", run)

    assert embed_runner.main() == 7

    stderr = SimpleNamespace(buffer=io.BytesIO())
    monkeypatch.setattr(embed_runner.sys, "stderr", stderr)

    def missing(*_args: Any, **_options: Any) -> None:
        raise FileNotFoundError

    monkeypatch.setattr(embed_runner.subprocess, "run", missing)
    assert embed_runner.main() == 1
    assert stderr.buffer.getvalue() == embed_runner.START_ERROR


def test_windows_job_owns_and_terminates_process_tree(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[Any, ...]] = []
    handle = SimpleNamespace(Close=lambda: events.append(("close",)))
    api = SimpleNamespace(
        CreateJobObject=lambda _security, _name: handle,
        AssignProcessToJobObject=lambda job, process: events.append(("assign", job, process)),
        TerminateJobObject=lambda job, code: events.append(("terminate", job, code)),
    )
    monkeypatch.setattr(mcp_server, "import_module", lambda _name: api)

    job = mcp_server._WindowsJob(cast(Any, SimpleNamespace(_handle=42)))  # noqa: SLF001
    job.terminate()
    job.close()
    assert events == [("assign", handle, 42), ("terminate", handle, 1), ("close",)]

    def fail_assignment(_job: Any, _process: Any) -> None:
        raise RuntimeError("assignment failed")

    api.AssignProcessToJobObject = fail_assignment
    with pytest.raises(OSError, match="assign"):
        mcp_server._WindowsJob(cast(Any, SimpleNamespace(_handle=43)))  # noqa: SLF001
    assert events[-1] == ("close",)

    def fail_termination(_job: Any, _code: int) -> None:
        raise RuntimeError("termination failed")

    api.AssignProcessToJobObject = lambda job, process: events.append(("assign", job, process))
    api.TerminateJobObject = fail_termination
    job = mcp_server._WindowsJob(cast(Any, SimpleNamespace(_handle=44)))  # noqa: SLF001
    with pytest.raises(OSError, match="terminate"):
        job.terminate()
    job.close()


def test_windows_embed_runner_preserves_startup_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class Job:
        def __init__(self, _process: subprocess.Popen[bytes]) -> None:
            events.append("assigned")

        def terminate(self) -> None:
            events.append("terminated")

        def close(self) -> None:
            events.append("closed")

    monkeypatch.setattr(mcp_server.os, "name", "nt")
    monkeypatch.setattr(mcp_server, "_WindowsJob", Job)
    command = canonical_json(["/definitely/missing/fgraph-embedder"])

    with pytest.raises(fgraph.TypeError, match="could not be started"):
        embed(command, "text")

    assert events == ["assigned", "terminated", "closed"]


def test_strict_json_rejects_invalid_syntax_and_constants() -> None:
    with pytest.raises(fgraph.TypeError, match="column"):
        loads("{")
    with pytest.raises(fgraph.TypeError, match="non-finite"):
        loads("NaN", context="number")


def test_canonical_and_value_encoding_edge_paths() -> None:
    assert canonical_json(False) == "false"
    assert canonical_json(-1.2e-5) == "-0.000012"
    assert canonical_json(1.2e-4) == "0.00012"
    with pytest.raises(fgraph.TypeError):
        canonical_json(object())
    with pytest.raises(fgraph.TypeError):
        encode({"instant": 2**63})
    with pytest.raises(fgraph.TypeError):
        encode({"instant": []})
    with pytest.raises(fgraph.TypeError):
        encode({"bytes": []})
    with pytest.raises(fgraph.TypeError):
        encode({"vector": [1e100]})
    indirect = encode({"bytes": base64.b64encode(b"x" * 257).decode()})
    assert indirect.tag == BYTES_REF
    assert indirect.blob == b"x" * 257


def test_indirect_blob_invariants_cover_every_physical_domain() -> None:
    validate = store._valid_indirect_blob  # noqa: SLF001
    text = "x" * (BLOB_THRESHOLD + 1)
    binary = b"x" * (BLOB_THRESHOLD + 1)
    vector = b"\x00\x00\x80?"
    text_key = indirect_digest(TEXT_REF, text.encode())
    bytes_key = indirect_digest(BYTES_REF, binary)
    vector_key = indirect_digest(VECTOR, vector)

    assert validate(TEXT_REF, text_key, text)
    assert validate(BYTES_REF, bytes_key, binary)
    assert validate(VECTOR, vector_key, vector)
    assert not validate(TEXT_REF, "not-bytes", text)
    assert not validate(TEXT_REF, b"short", text)
    assert not validate(TEXT_REF, text_key, binary)
    assert not validate(TEXT_REF, text_key, "\ud800")
    assert not validate(TEXT_REF, indirect_digest(TEXT_REF, b"x"), "x")
    assert not validate(TEXT_REF, b"0" * 32, text)
    assert not validate(BYTES_REF, bytes_key, text)
    assert not validate(BYTES_REF, indirect_digest(BYTES_REF, b"x"), b"x")
    oversized = b"x" * (MAX_VALUE_BYTES + 1)
    assert not validate(BYTES_REF, indirect_digest(BYTES_REF, oversized), oversized)
    assert not validate(VECTOR, vector_key, text)
    assert not validate(VECTOR, indirect_digest(VECTOR, b""), b"")
    assert not validate(VECTOR, indirect_digest(VECTOR, b"bad"), b"bad")
    assert not validate(99, b"0" * 32, binary)

    with fgraph.connect(":memory:") as graph:
        cases = [
            (TEXT_REF, indirect_digest(TEXT_REF, b"short"), "short", "byte length"),
            (TEXT_REF, indirect_digest(TEXT_REF, binary), binary, "storage class"),
            (BYTES_REF, indirect_digest(BYTES_REF, b"short"), b"short", "byte length"),
            (BYTES_REF, indirect_digest(BYTES_REF, text.encode()), text, "storage class"),
            (VECTOR, indirect_digest(VECTOR, b"bad"), b"bad", "float32"),
        ]
        for tag, key, data, message in cases:
            graph._connection.execute(  # noqa: SLF001
                "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", (key, data)
            )
            with pytest.raises(fgraph.FormatError, match=message):
                graph._logical(tag, key)  # noqa: SLF001


def test_integer_and_environment_clocks_survive_reopen(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "clock.db"
    with fgraph.connect(path, clock=10) as graph:
        assert graph.transact({"id": "a", "x/value": 1}).at == 1_000_010
    with fgraph.connect(path, clock=10) as graph:
        assert graph.transact({"id": "b", "x/value": 2}).at == 2_000_010

    monkeypatch.setenv("FGRAPH_CLOCK", "invalid")
    with pytest.raises(fgraph.TypeError, match="FGRAPH_CLOCK"):
        fgraph.connect(path)
    fresh = tmp_path / "fresh.db"
    with pytest.raises(fgraph.TypeError, match="FGRAPH_CLOCK"):
        fgraph.connect(fresh)
    assert not fresh.exists()
    monkeypatch.delenv("FGRAPH_CLOCK")
    with pytest.raises(fgraph.TypeError, match="RFC 3339"):
        fgraph.connect(tmp_path / "out-of-range.db", clock=253_402_300_800_000_000)


def test_callable_clock_cannot_regress_transaction_receipts() -> None:
    base = 1_767_225_600_000_000
    with fgraph.connect(":memory:", clock=lambda: base) as graph:
        first = graph.transact({"id": "first", "item/value": 1})
        second = graph.transact({"id": "second", "item/value": 2})
    assert first.at == base + 1_000_000
    assert second.at == base + 2_000_000


def test_failed_write_does_not_consume_deterministic_clock_tick() -> None:
    with fgraph.connect(":memory:", clock=10) as graph:
        with pytest.raises(fgraph.TooLarge):
            graph.transact({}, meta="x" * 1_048_577)
        assert graph.transact({"id": "ok", "item/value": 1}).at == 1_000_010


def test_format_marker_read_only_and_initialize_rollback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(fgraph.FormatError, match="open"):
        fgraph.connect(tmp_path)

    invalid = tmp_path / "not-sqlite.db"
    invalid.write_bytes(b"this is not a SQLite file")
    with pytest.raises(fgraph.FormatError, match="SQLite"):
        fgraph.connect(invalid)

    mismatch = tmp_path / "mismatch.db"
    with fgraph.connect(mismatch, clock=Clock()):
        pass
    raw = sqlite3.connect(mismatch)
    raw.execute("PRAGMA user_version=1")
    raw.close()
    with pytest.raises(fgraph.FormatError):
        fgraph.connect(mismatch)

    blank = tmp_path / "blank.db"
    sqlite3.connect(blank).close()
    with pytest.raises(fgraph.FormatError):
        fgraph.connect(blank, read_only=True)

    failed = tmp_path / "failed.db"
    original = store.Db._insert_raw_fact  # noqa: SLF001

    def fail_once(_self: store.Db, *_args: Any, **_kwargs: Any):
        raise sqlite3.OperationalError("injected bootstrap failure")

    monkeypatch.setattr(store.Db, "_insert_raw_fact", fail_once)
    with pytest.raises(fgraph.FormatError, match="SQLite"):
        fgraph.connect(failed, clock=Clock())
    monkeypatch.setattr(store.Db, "_insert_raw_fact", original)
    raw = sqlite3.connect(failed)
    assert raw.execute("SELECT name FROM sqlite_master WHERE name LIKE 'fgraph_%'").fetchall() == []
    raw.close()
    with fgraph.connect(failed, clock=Clock()) as recovered:
        assert recovered.stats()["transactions"] == 1


def test_closed_and_nested_transaction_paths(db: fgraph.Db) -> None:
    with db.speculate():
        with pytest.raises(fgraph.Conflict):
            db.transact([["assert", "x", "x/value", 1], ["assert", "x", "x/value", 2]])
        assert db.transact({"id": "ok", "x/value": 1}).tx is not None
    assert "ok" not in db._names  # noqa: SLF001
    db.close()
    db.close()
    with pytest.raises(fgraph.FormatError, match="closed"):
        db.stats()


def test_historical_view_reports_closed_owner(db: fgraph.Db) -> None:
    report = db.transact({"id": "entity", "entity/value": 1})
    view = db.at(report.tx)
    db.close()

    with pytest.raises(fgraph.FormatError, match="closed"):
        view.entity("entity")


@pytest.mark.parametrize(
    ("data", "error"),
    [
        ({"id": ""}, fgraph.TypeError),
        ({"id": "bad\nname"}, fgraph.TypeError),
        ({"id": "bad\u0085name"}, fgraph.TypeError),
        ({"id": "\ud800"}, fgraph.TypeError),
        ({"id": "fgraph/private", "x/value": 1}, fgraph.SchemaError),
        ({"id": "x", "fgraph/private": 1}, fgraph.SchemaError),
        ({"id": "x", 1: "bad"}, fgraph.TypeError),
        (["assert"], fgraph.TypeError),
        (["replace", "x", "x/value", 1], fgraph.TypeError),
        (["retract"], fgraph.TypeError),
        (["retract", "x", "x/value", 1, 2], fgraph.TypeError),
        (["retract", "fgraph/at", 1], fgraph.Unsupported),
        ([1], fgraph.TypeError),
        (1, fgraph.TypeError),
    ],
)
def test_transaction_input_validation(db: fgraph.Db, data: Any, error: type[Exception]) -> None:
    with pytest.raises(error):
        db.transact(data)


def test_reference_lookup_and_selector_validation(db: fgraph.Db) -> None:
    db.declare("user/email", type="text", unique=True)
    first = db.transact({"id": "ada", "user/email": "ada@x"})
    entity = first.ids["ada"]
    assert db.entity(["user/email", "ada@x"])["user/email"] == "ada@x"
    assert db.transact(["assert", ["user/email", "ada@x"], "user/name", "Ada"]).tx is not None
    assert db.transact(["assert", entity, "user/age", 36]).tx is not None
    assert db.transact(["assert", {"tmp": "t"}, "user/name", "Temp"]).ids["t"]
    assert db.transact({"id": entity, "user/active": True}).tx is not None
    assert db.transact({"id": ["user/email", "ada@x"], "user/city": "Lyon"}).tx is not None
    assert db.transact({"id": {"tmp": "same"}, "user/value": 1}).ids["same"]

    with pytest.raises(fgraph.TypeError):
        db.entity(True)
    with pytest.raises(fgraph.TypeError):
        db.entity({"bad": 1})
    with pytest.raises(fgraph.SchemaError):
        db.entity([1, "x"])
    with pytest.raises(fgraph.NotFound):
        db.entity(["missing/attr", "x"])
    with pytest.raises(fgraph.NotFound):
        db.entity(["user/email", "missing"])
    with pytest.raises(fgraph.SchemaError):
        db.entity(["user/name", "Ada"])
    with pytest.raises(fgraph.TypeError):
        db.transact(["assert", True, "user/value", 1])
    with pytest.raises(fgraph.NotFound):
        db.transact(["assert", 999, "user/value", 1])
    with pytest.raises(fgraph.TypeError):
        db.transact(["assert", {"bad": "x"}, "user/value", 1])
    with pytest.raises(fgraph.TypeError):
        db.transact({"id": {"tmp": ""}, "user/value": 1})
    with pytest.raises(fgraph.TypeError):
        db.transact({"id": True, "user/value": 1})


def test_upsert_conflicts_tempids_and_plan_cancellation(db: fgraph.Db) -> None:
    db.declare("user/email", type="text", unique=True)
    db.declare("user/handle", type="text", unique=True)
    db.transact({"id": "ada", "user/email": "ada@x"})
    db.transact({"id": "grace", "user/handle": "grace"})
    with pytest.raises(fgraph.Conflict, match="different entities"):
        db.transact({"user/email": "ada@x", "user/handle": "grace"})
    with pytest.raises(fgraph.Conflict, match="tempid"):
        db.transact(
            [
                {"id": {"tmp": "same"}, "user/email": "ada@x"},
                {"id": {"tmp": "same"}, "user/handle": "grace"},
            ]
        )
    with pytest.raises(fgraph.Conflict, match="unique value"):
        db.transact(["assert", "other", "user/email", "ada@x"])
    cancelled = db.transact([["assert", "new", "user/email", "new@x"], ["retract", "new", "user/email", "new@x"]])
    assert cancelled.status == "applied"
    assert cancelled.tx is not None
    assert cancelled.ids == {"new": cancelled.tx - 1}
    assert db.entity("new") == {}


def test_schema_and_vector_edge_validation(db: fgraph.Db) -> None:
    with pytest.raises(fgraph.SchemaError, match="unknown type"):
        db.transact({"id": "raw/value", "fgraph/type": "unknown"})
    with pytest.raises(fgraph.SchemaError, match="positive integer"):
        db.transact({"id": "raw/vector", "fgraph/dims": 0})
    with pytest.raises(fgraph.SchemaError):
        db.declare("raw/vector", type="text", dims=2)
    with pytest.raises(fgraph.SchemaError):
        db.declare("raw/value", type="unknown")
    declared = db.declare("raw/doc", doc="Human description")
    assert declared.tx is not None

    db.declare("raw/vectors", type="vector", many=True)
    with pytest.raises(fgraph.TypeError, match="dimensions"):
        db.transact({"id": "v", "raw/vectors": [{"vector": [1, 2]}, {"vector": [1, 2, 3]}]})
    db.transact({"id": "v", "raw/vectors": [{"vector": [1, 2]}]})
    with pytest.raises(fgraph.SchemaError, match="already contains"):
        db.declare("raw/vectors", dims=3)
    with pytest.raises(fgraph.TypeError):
        db.retract("v", value=1)
    invalid_provenance: Any = 1
    with pytest.raises(fgraph.TypeError):
        db.transact({}, by=invalid_provenance)
    with pytest.raises(fgraph.TypeError):
        db.transact({}, source=invalid_provenance)
    with pytest.raises(fgraph.TypeError):
        db.transact({}, meta={"bad": "\ud800"})


def test_nested_map_cardinality_and_type_errors(db: fgraph.Db) -> None:
    with pytest.raises(fgraph.Conflict):
        db.transact({"id": "x", "x/value": [1, 2]})
    with pytest.raises(fgraph.TypeError, match="nested map"):
        db.transact({"id": "x", "x/value": {"child/value": 1}})
    db.declare("x/child", ref=True)
    with pytest.raises(fgraph.Conflict):
        db.transact({"id": "x", "x/child": {"id": "child", "child/value": [1, 2]}})
    with pytest.raises(fgraph.TypeError, match="nested map"):
        db._parse_map_for_entity(  # noqa: SLF001
            {"x/value": {"child/value": 1}},
            65,
            db._new_pending(),  # noqa: SLF001
            {},
        )


def test_corrupt_meta_and_blob_fail_fast(db: fgraph.Db) -> None:
    db._connection.execute("UPDATE fgraph_meta SET value='bad' WHERE key='next_id'")  # noqa: SLF001
    with pytest.raises(fgraph.FormatError):
        db.transact({"id": "x", "x/value": 1})
    db._connection.execute("UPDATE fgraph_meta SET value=65 WHERE key='next_id'")  # noqa: SLF001

    db.declare("x/vector", type="vector")
    db.transact({"id": "x", "x/vector": {"vector": [1, 2]}})
    digest = db._connection.execute("SELECT v FROM fgraph_facts WHERE t=?", (VECTOR,)).fetchone()[0]  # noqa: SLF001
    db._connection.execute("DELETE FROM fgraph_blobs WHERE hash=?", (digest,))  # noqa: SLF001
    with pytest.raises(fgraph.FormatError, match="missing blob"):
        db.entity("x")
    db._connection.execute("INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", (digest, b"bad"))  # noqa: SLF001
    with pytest.raises(fgraph.FormatError, match="float32"):
        db.entity("x")


def test_pull_cycles_reverse_unknown_and_query_call_validation(db: fgraph.Db) -> None:
    db.declare("node/link", ref=True)
    db.transact({"id": "a", "node/link": {"ref": "b"}})
    db.transact({"id": "b", "node/link": {"ref": "a"}})
    assert db.entity("a", depth=3)["node/link"]["node/link"] == {"ref": "a"}
    assert db.pull("a", ["missing/_link"])["missing/_link"] == []
    assert db.pull("a", ["node/_link"])["node/_link"] == [{"ref": "b"}]
    assert db.pull("a", ["node/_link"])["node/_link"]
    for pattern in ([42], [{"node/link": ["node/link"], "node/other": []}], [{"node/link": "node/link"}]):
        with pytest.raises(fgraph.QueryError):
            db.pull("a", pattern)
    with pytest.raises(fgraph.QueryError):
        db.entity("a", depth=-1)
    with pytest.raises(fgraph.QueryError):
        db.q({"find": ["?e"]}, find=["?e"])


def test_time_history_why_follow_and_empty_undo(db: fgraph.Db, monkeypatch: pytest.MonkeyPatch) -> None:
    first = db.transact({"id": "a", "a/value": 1})
    assert first.at is not None
    before = db.at(first.at - 1)
    with pytest.raises(fgraph.NotFound):
        before.entity("a")
    with pytest.raises(fgraph.NotFound, match="genesis"):
        db.at(1_767_225_599_999_999)
    assert db.at("2026-01-01T00:00:01Z").entity("a") == {"a/value": 1}
    with pytest.raises(fgraph.TypeError):
        db.at(True)
    with pytest.raises(fgraph.NotFound):
        db.history("a", "missing/value")
    with pytest.raises(fgraph.NotFound):
        db.why("a", "missing/value")
    with pytest.raises(fgraph.TypeError):
        next(db.follow(interval=0))

    stream = db.follow(64, interval=0.001)
    event = next(stream)
    assert event["event"] == first.event
    assert "tx" not in event
    monkeypatch.setattr(store.time, "sleep", lambda _interval: (_ for _ in ()).throw(RuntimeError("stop")))
    with pytest.raises(RuntimeError, match="stop"):
        next(stream)
    stream.close()

    metadata_only = db.transact([], source="audit")
    assert metadata_only.tx is not None
    compensated = db.undo(metadata_only.tx)
    assert compensated.tx is not None
    assert db.entity(compensated.tx)["fgraph/undoes"] == {"ref": metadata_only.tx}
    with db.speculate(), pytest.raises(fgraph.Unsupported, match="nested"), db.speculate():
        pass


def test_query_helper_and_public_error_paths(db: fgraph.Db) -> None:
    db.transact(
        [
            {"id": "a", "x/name": "Ada", "x/age": 1, "x/enabled": True, "x/data": {"json": {"z": 1}}},
            {"id": "b", "x/name": "Bob", "x/age": 2, "x/enabled": False, "x/data": {"json": {"a": 1}}},
        ]
    )
    assert _sort_key(False) < _sort_key(1) < _sort_key("a") < _sort_key({"a": 1})
    with pytest.raises(fgraph.QueryError):
        _column(1)
    with pytest.raises(fgraph.QueryError):
        _find_value(db, "?missing", {})
    with pytest.raises(fgraph.QueryError):
        _find_value(db, ["pull", "?x", ["*"]], {"?x": Cell(INT, 1)})
    with pytest.raises(fgraph.QueryError):
        _find_value(db, 1, {})
    with pytest.raises(fgraph.QueryError):
        _aggregate(["sum"], [])
    assert _aggregate(["sum", "?x"], []) is None
    with pytest.raises(fgraph.QueryError):
        _aggregate(["sum", "?x"], [{"?x": Cell(JSON, "x")}])
    with pytest.raises(fgraph.QueryError):
        _compare(">", Cell(INT, object()), Cell(INT, object()))

    bad_queries = [
        {"find": ["?x"], "where": [["?e", "x/name"]]},
        {"find": ["?x"], "where": [["?e", "x/name", "?x"], ["contains", "?x", 1]]},
        {"find": ["?x"], "where": [["?e", "x/name", "?x"], [">", "?x", 1]]},
        {"find": ["?x"], "where": [["contains", "?x"]]},
        {"find": ["?x"], "where": [1]},
        {"find": ["?x"], "where": [{"rule": []}]},
        {"find": ["?x"], "where": [{"rule": ["missing", "?x"]}]},
        {"find": ["?x"], "where": [{"rule": ["r", "?x", "?y"]}], "rules": {"head": ["r", "?x"], "body": []}},
        {"find": ["?x"], "where": [], "rules": "bad"},
        {"find": ["?x"], "where": [], "rules": {"head": [], "body": []}},
        {"find": ["?x"], "where": [], "rules": {"head": ["r", "?x"], "body": []}},
        {"find": [["pull", "?name", ["*"]]], "where": [["?e", "x/name", "?name"]]},
        {"find": [["sum", "?name"]], "where": [["?e", "x/name", "?name"]]},
        {"find": ["?x"], "where": [], "in": ["?x"], "order": "bad"},
        {"find": ["?name"], "where": [["?e", "x/name", "?name"]], "order": [["bad", "asc"]]},
        {"find": ["?name"], "where": [["?e", "x/name", "?name"]], "offset": True},
        {"find": ["?name"], "where": [["?e", "x/name", "?name"]], "limit": True},
    ]
    args = {"?x": {"ref": "missing"}}
    for query in bad_queries:
        with pytest.raises(fgraph.QueryError):
            db.q(query, args if query.get("in") else None)
    assert db.q(
        find=["?name"],
        where=[["?e", "x/name", "?name"]],
        order=[["?e", "desc"]],
    ).rows == [["Bob"], ["Ada"]]


def test_rule_graph_and_constant_edge_paths(db: fgraph.Db) -> None:
    db.transact({"id": "a", "x/name": "Ada"})
    with pytest.raises(fgraph.QueryError, match="not bound"):
        db.q(
            {
                "find": ["?x"],
                "where": [{"rule": ["r", "?x"]}],
                "rules": [{"head": ["r", "?x"], "body": [["a", "x/name", "?name"]]}],
            }
        )
    with pytest.raises(fgraph.QueryError, match="mutually recursive"):
        db.q(
            {
                "find": ["?x"],
                "where": [{"rule": ["a", "?x"]}],
                "rules": [
                    {"head": ["a", "?x"], "body": [{"rule": ["b", "?x"]}]},
                    {"head": ["b", "?x"], "body": [{"rule": ["c", "?x"]}]},
                    {"head": ["c", "?x"], "body": [{"rule": ["a", "?x"]}]},
                ],
            }
        )
    assert db.q(find=["?x"], where=[["?e", "x/name", "?x"], ["=", "?x", "Ada"]]).rows == [["Ada"]]
    with pytest.raises(fgraph.QueryError, match="cannot resolve"):
        db.q({"find": ["?x"], "where": [], "in": ["?x"]}, {"?x": {"ref": "missing"}})


def test_search_low_level_edges(db: fgraph.Db) -> None:
    assert _cosine([1], [1, 2]) == -float("inf")
    assert _cosine([1], [0]) == -float("inf")
    db.declare("x/vector", type="vector", many=True)
    db.transact(
        [
            {"id": "one", "x/text": "search", "x/vector": [{"vector": [1, 0]}, {"vector": [0, 0]}]},
        ]
    )
    assert db.search("!!!").hits == []
    assert db.search(vector=[1, 0], vector_attribute="x/vector").hits[0]["entity"] == "one"
    assert db.search(vector=[1, 0], vector_attribute="x/vector", filters=[["missing/filter", 1]]).hits == []
    with pytest.raises(fgraph.TypeError):
        db.search("x", k=True)
    with pytest.raises(fgraph.TypeError):
        db.search("x", expand=True)
    invalid: Any = 1
    with pytest.raises(fgraph.TypeError):
        db.search(vector=invalid)
    with pytest.raises(fgraph.TypeError):
        db.search("x", filters=invalid)
    invalid_attribute: Any = []
    with pytest.raises(fgraph.TypeError):
        db.search(vector=[1, 0], vector_attribute=invalid_attribute)

    db._connection.execute("INSERT INTO fgraph_fts(rowid,text) VALUES (999999,'stale')")  # noqa: SLF001
    assert db.search("stale").hits == []


def test_cli_file_embed_mcp_tail_and_main_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CliRunner()
    database = tmp_path / "cli.db"
    payload = tmp_path / "payload.json"
    payload.write_text(
        '{"id":"file","file/text":"from file","file/embedding":{"vector":[1,0]}}',
        encoding="utf-8",
    )
    result = runner.invoke(cli.app, ["add", f"@{payload}", "--db", str(database), "--json"])
    assert result.exit_code == 0
    empty = runner.invoke(cli.app, ["add", "-", "--db", str(database)], input="")
    assert isinstance(empty.exception, fgraph.TypeError)

    command = canonical_json([sys.executable, "-c", "print('[1, 0]')"])
    searched = runner.invoke(
        cli.app,
        [
            "search",
            "--text",
            "from file",
            "--embed-cmd",
            command,
            "--vector-attribute",
            "file/embedding",
            "--db",
            str(database),
            "--json",
        ],
    )
    assert searched.exit_code == 0

    class FakeDb:
        def __enter__(self):
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def follow(self, _since: int):
            return iter([{"event": "00000000-0000-4000-8000-000000000041", "fgraph": "event/1"}])

    monkeypatch.setattr(cli, "_open", lambda *_args, **_kwargs: FakeDb())
    assert runner.invoke(cli.app, ["tail", "--follow"]).stdout == (
        '{"event":"00000000-0000-4000-8000-000000000041","fgraph":"event/1"}\n'
    )
    called: list[tuple[bool, str | None]] = []
    monkeypatch.setattr(cli, "run_mcp", lambda _db, *, read_only, embed_cmd: called.append((read_only, embed_cmd)))
    assert runner.invoke(cli.app, ["mcp", "--embed-cmd", "embedder"]).exit_code == 0
    assert called == [(True, "embedder")]

    monkeypatch.setattr(sys, "argv", ["fgraph", "version"])
    cli.main()
    monkeypatch.setattr(sys, "argv", ["fgraph", "missing-command"])
    with pytest.raises(SystemExit) as usage:
        cli.main()
    assert usage.value.code == 2

    def typed_failure(*_args: Any, **_kwargs: Any) -> None:
        raise fgraph.NotFound("missing")

    monkeypatch.setattr(cli, "app", typed_failure)
    with pytest.raises(SystemExit) as typed:
        cli.main()
    assert typed.value.code == 1


def test_embed_process_failures_are_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(fgraph.TypeError, match="argv"):
        embed('[""]', "text")
    with pytest.raises(fgraph.TypeError, match="could not be started"):
        embed("/definitely/missing/fgraph-embedder", "text")

    monkeypatch.setattr("fgraph.mcp_server._EMBED_TIMEOUT_SECONDS", 0.01)
    timeout = canonical_json([sys.executable, "-c", "import time; time.sleep(1)"])
    with pytest.raises(fgraph.TypeError, match="timed out"):
        embed(timeout, "text")


def test_embed_timeout_terminates_inherited_stdout_processes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fgraph.mcp_server._EMBED_TIMEOUT_SECONDS", 0.5)
    ready = tmp_path / "descendant-started"
    marker = tmp_path / "descendant-survived"
    child = f"import pathlib,time; time.sleep(1); pathlib.Path({str(marker)!r}).touch()"
    launcher = (
        "import pathlib,subprocess,sys,time; "
        f"subprocess.Popen([sys.executable, '-c', {child!r}]); "
        f"pathlib.Path({str(ready)!r}).touch(); time.sleep(2)"
    )
    command = canonical_json([sys.executable, "-c", launcher])

    started = time.monotonic()
    with pytest.raises(fgraph.TypeError, match="timed out"):
        embed(command, "text")

    assert time.monotonic() - started < 1.5
    assert ready.exists()
    time.sleep(1.0)
    assert not marker.exists()


def test_mcp_embedding_and_context_paths(db: fgraph.Db) -> None:
    invalid = canonical_json([sys.executable, "-c", "print('[1, \"bad\"]')"])
    with pytest.raises(fgraph.TypeError, match="non-number"):
        embed(invalid, "text")
    command = canonical_json([sys.executable, "-c", "print('[1, 0]')"])

    async def scenario() -> None:
        server = create_server(db, read_only=False, embed_cmd=command)
        remembered = await server.call_tool(
            "remember",
            {"text": "embedded memory", "operation_id": "remember:embedded"},
        )
        assert isinstance(remembered, CallToolResult)
        assert remembered.structured_content["data"]["tx"] is not None
        recalled = await server.call_tool("recall", {"query": "embedded"})
        assert isinstance(recalled, CallToolResult)
        assert recalled.structured_content["data"]["hits"]

        recall = server._tool_manager._tools["recall"].fn  # noqa: SLF001
        with pytest.raises(ToolError, match="blank"):
            await recall(query="   ")

        remember = server._tool_manager._tools["remember"].fn  # noqa: SLF001
        context = SimpleNamespace(
            session=SimpleNamespace(client_params=SimpleNamespace(client_info=SimpleNamespace(name="tester")))
        )
        report = await remember(
            operation_id="remember:ctx",
            facts={"id": "ctx", "ctx/value": 1},
            ctx=context,
        )
        assert db.entity(report["data"]["tx"])["fgraph/by"] == "mcp:tester"

        class MissingParams:
            @property
            def client_params(self):
                raise ValueError("not initialized")

        report = await remember(
            operation_id="remember:fallback",
            facts={"id": "fallback", "ctx/value": 2},
            ctx=SimpleNamespace(session=MissingParams()),
        )
        assert db.entity(report["data"]["tx"])["fgraph/by"] == "mcp:unknown"

    asyncio.run(scenario())
