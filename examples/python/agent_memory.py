# /// script
# requires-python = ">=3.12"
# dependencies = ["fgraph"]
# ///
"""Auditable agent memory: supersession instead of silent overwrite, and hybrid recall.

This is usage mode 3 from SPEC.md §1 — the same operations the MCP tools
(remember / recall / why / forget) perform, driven directly from Python.
"""

import fgraph

db = fgraph.connect("agent-memory.db")

# An agent remembers facts extracted from conversations, each with provenance.
db.transact(
    {"id": "user", "user/editor": "vim", "user/language": "Go"},
    source="conversation:41",
    by="agent:assistant",
)
db.transact(
    {"id": "user", "user/editor": "helix"},  # belief changes: superseded, never overwritten
    source="conversation:42",
    by="agent:assistant",
)

# The audit trail every memory tool is missing:
for fact in db.history("user", "user/editor"):
    print(f"{fact['v']!r} believed from tx {fact['tx']} to {fact['rx'] or 'now'}")
print(db.why("user", "user/editor"))  # -> helix, because conversation:42

# Keyword recall over everything the agent knows (add vector=... for semantic recall).
db.transact(
    {"id": "note-1", "note/text": "User prefers small composable tools over frameworks."},
    source="conversation:42",
)
for hit in db.search("composable tools", k=5, expand=1).hits:
    print(hit["entity"], hit["score"])

# Forgetting is a retraction: recall stops, the audit trail remains.
db.retract("user", "user/language")
db.close()
