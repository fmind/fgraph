# /// script
# requires-python = ">=3.12"
# dependencies = ["fgraph"]
# ///
"""fgraph quickstart: schema-light facts, guarded writes, and bounded reads."""

import sys

import fgraph


def emit(value: object) -> None:
    """Write one inspectable result without depending on a logging framework."""
    sys.stdout.write(f"{value}\n")


def require(condition: bool, message: str) -> None:
    """Keep the example executable as an acceptance check."""
    if not condition:
        raise RuntimeError(message)


def require_tx(report: fgraph.TxReport, action: str) -> int:
    """Return the committed transaction or fail if an expected write was a no-op."""
    if report.tx is None:
        raise RuntimeError(f"{action} did not commit")
    return report.tx


with fgraph.connect(":memory:") as db:  # Use a file path for durable storage.
    # No schema is needed for ordinary facts. The receipt makes this transaction
    # safely retryable and rejects a concurrent write at another basis.
    imported = db.transact(
        {"id": "ada", "person/name": "Ada Lovelace", "person/city": "London"},
        source="quickstart",
        by="example",
        operation_id="quickstart:ada:1",
        if_basis_tx=64,
    )
    retry = db.transact(
        {"id": "ada", "person/name": "Ada Lovelace", "person/city": "London"},
        source="quickstart",
        by="example",
        operation_id="quickstart:ada:1",
        if_basis_tx=64,
    )
    require(retry.status == "already_applied", "operation retry was not idempotent")
    require(retry.event == imported.event, "operation retry returned another receipt")

    # Declare only behavior that the store must enforce.
    db.declare("person/knows", ref=True, many=True)
    db.transact({"id": "grace", "person/name": "Grace Hopper"})
    before_move = db.transact({"id": "ada", "person/knows": {"ref": "grace"}})
    moved = db.transact({"id": "ada", "person/city": "Lyon"})
    before_move_tx = require_tx(before_move, "relationship write")
    moved_tx = require_tx(moved, "city move")
    guarded = db.transact(["cas", "ada", "person/city", "Lyon", "Paris"])
    guarded_tx = require_tx(guarded, "guarded city move")

    # Current, historical, and relational reads share one pinned fact model.
    require(db.entity("ada")["person/city"] == "Paris", "current read returned the wrong city")
    require(
        db.at(before_move_tx).entity("ada")["person/city"] == "London",
        "historical read returned the wrong city",
    )
    friends = db.q(
        find=["?friend"],
        where=[["ada", "person/knows", "?f"], ["?f", "person/name", "?friend"]],
    )
    require(friends.rows == [["Grace Hopper"]], "query returned the wrong relationship")
    require(bool(db.search("Grace Hopper", text_attributes=["person/name"]).hits), "search returned no match")

    # Inspect portable change records, the durable receipt, and query planning.
    emit(db.receipt(moved_tx))
    emit(db.receipt(guarded_tx))
    emit(db.event_records(since=before_move_tx))
    emit(db.explain({"find": ["?e"], "where": [["?e", "person/name", "_"]]}))
