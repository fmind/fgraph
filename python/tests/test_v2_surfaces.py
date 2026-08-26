from __future__ import annotations

import asyncio
import hashlib
import re
from pathlib import Path

import pytest

import fgraph
from fgraph.mcp_server import create_server
from fgraph.values import canonical_json


def test_history_datoms_query_pagination_and_explain() -> None:
    with fgraph.connect(":memory:", clock=1_767_225_600_000_000) as db:
        first = db.transact({"id": "counter", "counter/value": 1})
        db.transact({"id": "counter", "counter/value": 2})

        current = db.q(
            find=["?a", "?value", "?tx", "?added"],
            where=[["counter", "?a", "?value", "?tx", "?added"]],
        )
        assert current.rows[0][0] == {"ref": "counter/value"}
        assert current.rows[0][1:] == [2, {"ref": current.rows[0][2]["ref"]}, True]

        history = db.q(
            {
                "source": "history",
                "find": ["?value", "?added"],
                "where": [["counter", "counter/value", "?value", "_", "?added"]],
                "order": [["?value", "asc"], ["?added", "desc"]],
            }
        )
        assert history.rows == [[1, True], [1, False], [2, True]]

        page = db.datoms("eavt", ["counter"], source="history", limit=1)
        assert page["basis_tx"] > first.tx
        assert page["next_cursor"] is not None
        pinned_basis = page["basis_tx"]
        db.transact({"id": "counter", "counter/value": 3})
        second = db.datoms("eavt", ["counter"], source="history", limit=1, cursor=page["next_cursor"])
        assert second["basis_tx"] == pinned_basis

        plan = db.explain({"find": ["?value"], "where": [["counter", "counter/value", "?value"]]})
        assert plan["clauses"] == [{"ordinal": 0, "kind": "pattern", "access": "eavt/ea", "bound": []}]


def test_schema_snapshot_shapes_and_vector_model() -> None:
    with fgraph.connect(":memory:") as db:
        db.declare("memory/embedding", type="vector", dims=2, vector_model="text-embedding-v1")
        db.declare_shape(
            "shape/person",
            required=["person/name"],
            allowed=["person/age"],
            closed=True,
        )
        with pytest.raises(fgraph.SchemaError, match="required attributes"):
            db.transact({"id": "ada", "fgraph/shape": {"ref": "shape/person"}, "person/age": 36})
        db.transact(
            {
                "id": "ada",
                "fgraph/shape": {"ref": "shape/person"},
                "person/name": "Ada",
                "person/age": 36,
            }
        )
        with pytest.raises(fgraph.SchemaError, match="closed shape"):
            db.transact({"id": "ada", "person/city": "London"})

        snapshot = db.schema()
        embedding = next(item for item in snapshot["attributes"] if item["name"] == "memory/embedding")
        assert embedding == {
            "name": "memory/embedding",
            "declared": {
                "type": "vector",
                "dims": 2,
                "vector_model": "text-embedding-v1",
            },
            "effective": {
                "type": "vector",
                "many": False,
                "unique": False,
                "nohistory": True,
                "dims": 2,
                "doc": None,
                "vector_model": "text-embedding-v1",
            },
            "observed": {"types": [], "live_facts": 0, "entities": 0},
        }
        assert snapshot["shapes"] == [
            {
                "name": "shape/person",
                "required": ["person/name"],
                "allowed": ["person/age", "person/name"],
                "closed": True,
            }
        ]
        digest_payload = {
            "attributes": [
                {
                    "name": item["name"],
                    "declared": item["declared"],
                    "effective": item["effective"],
                }
                for item in snapshot["attributes"]
            ],
            "shapes": snapshot["shapes"],
        }
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", snapshot["digest"])
        assert snapshot["digest"] == ("sha256:" + hashlib.sha256(canonical_json(digest_payload).encode()).hexdigest())
        digest = snapshot["digest"]
        db.transact({"id": "ada", "person/age": 37})
        assert db.schema()["digest"] == digest


def test_schema_observed_entities_are_distinct_across_physical_types() -> None:
    with fgraph.connect(":memory:") as db:
        db.declare("mixed/value", many=True)
        db.transact(
            [
                {"id": "mixed/one", "mixed/value": [1, "one"]},
                {"id": "mixed/two", "mixed/value": [2, True]},
            ]
        )

        attribute = next(item for item in db.schema("mixed/")["attributes"] if item["name"] == "mixed/value")
        assert attribute["observed"] == {
            "types": ["bool", "int", "text"],
            "live_facts": 4,
            "entities": 2,
        }


def test_shape_validation_and_mutation_helper_receipts() -> None:
    with fgraph.connect(":memory:") as db:
        declared = db.declare(
            "person/name",
            type="text",
            operation_id="declare:person-name",
            if_basis_tx=64,
        )
        retry = db.declare(
            "person/name",
            type="text",
            operation_id="declare:person-name",
            if_basis_tx=64,
        )
        assert retry.status == "already_applied"
        assert retry.tx == declared.tx

        shape = db.declare_shape(
            "shape/person",
            required=["person/name"],
            closed=True,
            operation_id="shape:person",
            if_basis_tx=declared.tx,
        )
        created = db.transact(
            {
                "id": "ada",
                "fgraph/shape": {"ref": "shape/person"},
                "person/name": "Ada",
            }
        )
        assert db.validate("ada") == {
            "basis_tx": created.tx,
            "valid": True,
            "violations": [],
        }

        name_attribute = db._names["person/name"]  # noqa: SLF001
        db._connection.execute(  # noqa: SLF001
            "DELETE FROM fgraph_facts WHERE e=? AND a=? AND rx IS NULL",
            (db._names["ada"], name_attribute),  # noqa: SLF001
        )
        invalid = db.validate("ada")
        assert invalid["basis_tx"] == created.tx
        assert invalid["valid"] is False
        assert invalid["violations"] == [
            {
                "code": "required",
                "entity": "ada",
                "shape": "shape/person",
                "attribute": "person/name",
                "message": "required attribute is missing",
            }
        ]
        assert shape.tx is not None


def test_undo_helper_forwards_idempotency_and_basis() -> None:
    with fgraph.connect(":memory:") as db:
        changed = db.transact({"id": "counter", "counter/value": 1})
        assert changed.tx is not None
        undone = db.undo(
            changed.tx,
            operation_id="undo:counter",
            if_basis_tx=changed.tx,
        )
        assert undone.status == "applied"
        db.transact({"id": "later", "later/value": True})
        retry = db.undo(
            changed.tx,
            operation_id="undo:counter",
            if_basis_tx=changed.tx,
        )
        assert retry.status == "already_applied"
        assert (retry.event, retry.tx, retry.basis_tx) == (undone.event, undone.tx, changed.tx)


def test_query_planner_reorders_pattern_blocks_and_pushes_bounds() -> None:
    with fgraph.connect(":memory:") as db:
        db.declare("person/name", type="text")
        db.declare("person/knows", type="ref", many=True)
        relation = db.transact(
            [
                {"id": "ada", "person/name": "Ada", "person/knows": {"ref": "grace"}},
                {"id": "grace", "person/name": "Grace"},
            ]
        )
        query = {
            "find": ["?name"],
            "where": [
                ["?friend", "person/name", "?name"],
                ["ada", "person/knows", "?friend", relation.tx, True],
            ],
        }
        explanation = db.explain(query)
        assert [clause["ordinal"] for clause in explanation["clauses"]] == [1, 0]
        assert [clause["access"] for clause in explanation["clauses"]] == ["eavt/ea", "eavt/batch"]

        statements: list[str] = []
        db._connection.set_trace_callback(statements.append)  # noqa: SLF001
        assert db.q(query).rows == [["Grace"]]
        db._connection.set_trace_callback(None)  # noqa: SLF001
        normalized = [" ".join(statement.lower().split()) for statement in statements]
        assert any(
            "where e=" in statement and "a=" in statement and "tx=" in statement
            for statement in normalized
            if statement.startswith("select * from fgraph_facts")
        )

        statements.clear()
        db._connection.set_trace_callback(statements.append)  # noqa: SLF001
        assert db.q(find=["?e"], where=[["?e", "person/name", "Grace"]]).rows == [[{"ref": "grace"}]]
        db._connection.set_trace_callback(None)  # noqa: SLF001
        normalized = [" ".join(statement.lower().split()) for statement in statements]
        assert any(
            "a=" in statement and "t=" in statement and "v=" in statement
            for statement in normalized
            if statement.startswith("select * from fgraph_facts")
        )

        barrier = db.explain(
            {
                "find": ["?name"],
                "where": [
                    ["?friend", "person/name", "?name"],
                    ["starts-with", "?name", "G"],
                    ["ada", "person/knows", "?friend"],
                ],
            }
        )
        assert [clause["ordinal"] for clause in barrier["clauses"]] == [0, 1, 2]


@pytest.mark.parametrize(
    ("pattern", "access"),
    [
        (["entity", "item/value", 1], "eavt/exact"),
        (["entity", "item/value", "?v"], "eavt/ea"),
        (["?e", "item/value", 1], "avet"),
        (["entity", "?a", "?v"], "eavt/e"),
        (["?e", "item/value", "?v"], "avet/a"),
        (["?e", "?a", 1], "value-scan"),
        (["?e", "?a", "?v"], "scan"),
    ],
)
def test_query_planner_uses_canonical_access_tokens(pattern: list[object], access: str) -> None:
    with fgraph.connect(":memory:") as db:
        explanation = db.explain({"find": ["?e"], "where": [pattern]})

    assert explanation["clauses"] == [{"ordinal": 0, "kind": "pattern", "access": access, "bound": []}]
    if access == "value-scan":
        assert explanation["warnings"] == ["clause 0 requires a value scan; bind entity or attribute earlier"]
    elif access == "scan":
        assert explanation["warnings"] == ["clause 0 requires a fact scan; bind entity, attribute, or value earlier"]
    else:
        assert explanation["warnings"] == []


def test_search_filters_before_candidate_cutoff_and_requires_vector_attribute() -> None:
    with fgraph.connect(":memory:") as db:
        db.transact(
            [{"id": f"noise-{index:02d}", "note/text": "shared token", "note/kind": "noise"} for index in range(60)]
            + [{"id": "wanted", "note/text": "shared token", "note/kind": "guide"}]
        )
        result = db.search("shared token", filters=[["note/kind", "guide"]], k=1)
        assert [hit["entity"] for hit in result.hits] == ["wanted"]

        db.transact({"id": "wanted", "note/embedding": {"vector": [1, 0]}})
        with pytest.raises(fgraph.TypeError, match="vector_attribute"):
            db.search(vector=[1, 0])
        assert db.search(vector=[1, 0], vector_attribute="note/embedding").hits[0]["entity"] == "wanted"


def test_search_is_bounded_deduplicated_and_budgeted() -> None:
    db = fgraph.connect(":memory:")
    db.declare("note/embedding", type="vector", many=True, vector_model="test/model")
    db.transact(
        {
            "id": "long",
            "note/text": "needle " + "x" * 3_000,
            "note/kind": "guide",
            "note/embedding": [{"vector": [1, 0]}, {"vector": [0.9, 0.1]}],
        }
    )
    semantic = db.search(vector=[1, 0], vector_attribute="note/embedding")
    assert semantic.basis_tx == db._latest_tx()  # noqa: SLF001
    vector_match = semantic.hits[0]["matched"][0]
    assert vector_match["v"] == {"vector_dims": 2}
    assert vector_match["value_truncated"] is True
    assert len(semantic.hits[0]["matched"]) == 1

    keyword = db.search("needle", text_attributes=["note/text"])
    text_match = keyword.hits[0]["matched"][0]
    assert text_match["value_truncated"] is True
    assert len(text_match["v"].encode()) <= 2_051
    assert len(canonical_json(keyword.to_dict()).encode()) <= 1024 * 1024
    with pytest.raises(fgraph.TypeError, match="unknown search options"):
        db.search(vector=[1, 0], attribute="note/embedding")
    with pytest.raises(fgraph.TypeError, match="not text"):
        db.search("needle", text_attributes=["note/embedding"])

    db.transact([{"id": f"candidate-{index}", "note/text": "common"} for index in range(51)])
    capped = db.search("common", k=1)
    assert capped.truncated is True
    assert len(capped.hits) == 1

    budgeted = fgraph.connect(":memory:", query_budget=1)
    budgeted.transact({"id": "one", "note/text": "needle", "note/kind": "guide"})
    with pytest.raises(fgraph.TooLarge, match="work budget"):
        budgeted.search("needle", filters=[["note/kind", "guide"]])
    budgeted.close()
    db.close()


def test_exact_snapshot_restore_and_mcp_read_only_default(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    snapshot = tmp_path / "snapshot.db"
    destination = tmp_path / "restored.db"
    with fgraph.connect(source) as db:
        db.transact({"id": "note", "note/text": "durable"})
        db.backup(snapshot)

    with fgraph.restore_backup(snapshot, destination) as restored:
        assert restored.entity("note") == {"note/text": "durable"}
        assert restored.doctor()["ok"] is True

        async def inspect_server() -> None:
            server = create_server(restored)
            tools = [tool.name for tool in await server.list_tools()]
            assert tools == [
                "recall",
                "about",
                "why",
                "history",
                "query",
                "datoms",
                "receipt",
                "schema",
                "explain",
            ]
            templates = {str(template.uri_template) for template in await server.list_resource_templates()}
            assert "fgraph://schema{?prefix,cursor}" in templates
            writable = create_server(restored, read_only=False)
            writable_tools = {tool.name for tool in await writable.list_tools()}
            assert "remember" in writable_tools
            assert "excise" not in writable_tools

        asyncio.run(inspect_server())
