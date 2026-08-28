---
name: fgraph-mcp
description: Configure and operate fgraph as safe MCP memory or a project knowledge base. Use for recall, provenance, schema-aware queries, guarded writes, pagination, or agent-memory recovery.
license: MIT
---

# Use fgraph over MCP

Expose one local fgraph file to an MCP client as durable, attributable memory. Read-only mode is the default and should remain the default for project knowledge bases.

## Configure the boundary

1. Confirm fgraph 1.x is installed with `fgraph version`. Installing this skill does not install the server.
1. Choose a dedicated database path inside the intended project or data directory.
1. Start read-only access with `fgraph --db <file> mcp`.
1. Add `--write` only when the user explicitly wants the agent to mutate memory.
1. Configure the chosen MCP client to launch that exact command over stdio. Do not add an embedder, network service, or writable mode unless requested.

Example for Claude Code:

```bash
claude mcp add --scope project fgraph -- fgraph --db ./project.db mcp
```

## Read workflow

1. Inspect `schema` before composing structured queries or writes.
1. Use `recall` for bounded retrieval, `about` for an entity, `why` for provenance, `history` for a timeline, `query` for joins, and `datoms` for explicit pagination.
1. Preserve the returned `basis_tx` when a later decision depends on that read.
1. Follow returned cursors or `next_uri` values exactly. Never forge, reuse across requests, or silently restart a stale cursor.
1. When `changes` returns `oversized_event`, reconstruct it only through that integrity-pinned event URI and its chunk `next_uri` values; use the change `next_uri` separately to continue with later events.
1. Narrow the request when the server reports an output or work limit.

## Write workflow

- `remember`, `forget`, and `undo` require a stable `operation_id`; reuse it only for an exact retry.
- Guard destructive `forget` and `undo` calls with the basis reviewed by the agent.
- Prefer retraction or audited undo. Excision is intentionally unavailable over MCP.
- Treat the response basis as the state actually evaluated or established; do not substitute a newer unseen head.
- Keep memory records small and attributable. Use structured facts once a recurring text-memory shape is known.

## Operational limits

- Keep the SQLite file and backups outside an untrusted agent's direct administrative control when tampering matters.
- Event hashes detect corruption, not a malicious writer that can rewrite the file and hashes.
- MCP output is bounded, but direct entity/history/schema work can still depend on corpus size. Keep the file operationally bounded and prefer paged datoms or limited queries for large traversal.
- Run `fgraph --db <file> doctor --json` and make a backup before recovery or maintenance.

The MCP tool contract and safety limits are at <https://fmind.github.io/fgraph/docs/integrations/>.
