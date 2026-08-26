# /// script
# requires-python = ">=3.12"
# dependencies = ["fgraph"]
# ///
"""Auditable agent memory with retry-safe writes and stale-basis protection."""

import sys

import fgraph


def emit(*values: object) -> None:
    """Write one inspectable result without depending on a logging framework."""
    sys.stdout.write(" ".join(str(value) for value in values) + "\n")


def require_tx(report: fgraph.TxReport, action: str) -> int:
    """Return the committed transaction or fail if an expected write was a no-op."""
    if report.tx is None:
        raise RuntimeError(f"{action} did not commit")
    return report.tx


with fgraph.connect("agent-memory.db") as db:
    # Operation ids make tool retries return the original receipt.
    initial = db.transact(
        {"id": "user", "user/editor": "vim", "user/language": "Go"},
        source="conversation:41",
        by="agent:assistant",
        operation_id="memory:user:conversation-41",
    )
    initial_tx = require_tx(initial, "initial memory write")
    update = db.transact(
        {"id": "user", "user/editor": "helix"},  # superseded, not overwritten
        source="conversation:42",
        by="agent:assistant",
        operation_id="memory:user:conversation-42",
        if_basis_tx=initial_tx,
    )
    update_tx = require_tx(update, "memory update")

    for fact in db.history("user", "user/editor"):
        emit(f"{fact['v']!r} believed from tx {fact['tx']} to {fact['rx'] or 'now'}")
    emit(db.why("user", "user/editor"))  # helix, because conversation:42
    emit(db.receipt(update_tx))

    note = db.transact(
        {"id": "note-1", "note/text": "User prefers small composable tools over frameworks."},
        source="conversation:42",
        operation_id="memory:note-1",
        if_basis_tx=update_tx,
    )
    for hit in db.search("composable tools", k=5, expand=1).hits:
        emit(hit["entity"], hit["score"])

    # Destructive decisions carry the basis the agent reviewed.
    db.transact(
        ["retract", "user", "user/language"],
        operation_id="memory:forget-user-language",
        if_basis_tx=require_tx(note, "note write"),
    )
