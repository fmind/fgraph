---
title: Integrations
weight: 6
---

`fgraph mcp` serves one database to MCP-capable agents over stdio. It opens the file read-only by default. A project knowledge base therefore needs no safety flag:

```bash
fgraph --db ./project.db mcp
```

Only an agent that should mutate its own memory gets the explicit opt-in:

```bash
fgraph --db ./agent-memory.db mcp --write
```

## Tool contract

Read-only mode exposes exactly:

- `recall` — bounded attributable keyword/semantic retrieval;
- `about` — bounded entity pull;
- `why` and `history` — provenance/timeline pages;
- `query` and `explain` — bounded canonical Datalog and its actual plan;
- `datoms` — basis-pinned EAVT/AVET/VAET pages;
- `receipt` — one durable transaction/operation receipt;
- `schema` — basis-pinned rich schema pages.

`--write` additionally exposes `remember`, `forget`, and `undo`. Every mutation requires `operation_id`; `forget` and `undo` also require `if_basis_tx`. Excision and operational maintenance are never agent tools.

Every successful result is both text and structured content:

```json
{ "ok": true, "basis_tx": 72, "data": {} }
```

Discovery is executable guidance, not a list of opaque tool names. During initialization the server tells clients to inspect schema before inventing attributes, use bounded pages, retain `basis_tx`, and guard writes with operation ids and basis checks. Every tool publishes input and output JSON Schema plus `readOnlyHint`, `destructiveHint`, `idempotentHint`, and `openWorldHint: false`, so an agent can select and constrain calls without runtime-specific prompt glue. Request cancellation is propagated where the SDK/runtime can interrupt work; deterministic query/search budgets remain the hard fallback for synchronous SQLite sections.

Responses are capped at 256 KiB. Search, depth, history, query, datom, schema, and cursor sizes have explicit limits. A tool returns an error instead of silently clamping an invalid request.

`basis_tx` is the read basis pinned before evaluation or the basis returned by a core search/page operation. A successful mutation reports the transaction it established (or the checked basis for a no-op); an idempotent retry reports its original transaction basis. This makes the envelope safe to feed into a later `if_basis_tx` decision without acknowledging an unseen concurrent write.

The item limits are output limits. Query/search work and datom/event pages have core budgets, while `about`, `why`, `history`, and schema construction may inspect corpus-sized local state before trimming the response. Keep an agent's database operationally bounded, and prefer query/datoms for large explicitly budgeted traversal.

## Claude Code

```bash
claude mcp add --scope project fgraph -- fgraph --db ./project.db mcp
```

For writable memory, add `--write` to the server arguments only after deciding that the agent owns that file.

## Codex

```bash
codex mcp add fgraph -- fgraph --db ./project.db mcp
```

## OpenCode

```json
{
  "mcp": {
    "fgraph": {
      "type": "local",
      "command": ["fgraph", "--db", "./project.db", "mcp"],
      "enabled": true
    }
  }
}
```

## Antigravity

```json
{
  "mcpServers": {
    "fgraph": {
      "command": "fgraph",
      "args": ["--db", "./project.db", "mcp"]
    }
  }
}
```

## GitHub Copilot

```bash
copilot mcp add fgraph -- fgraph --db ./project.db mcp
```

{{< callout type="info" >}} No installed binary yet? Substitute `uv run --project python fgraph`, `go/bin/fgraph`, or `node typescript/dist/cli.js` after its build. {{< /callout >}}

## Correctable memory

An unkeyed text memory is an append-only note. Give a decision or constraint a stable, opaque key when later corrections should supersede the current value while retaining history:

```text
remember(
  key="project:test-gate",
  text="Run mise run all before completion",
  source="AGENTS.md",
  operation_id="remember-project-test-gate-v1"
)
```

The server records `by = mcp:<client-name>`. `recall` returns matched facts with asserting provenance; `history` shows prior text; `why` explains the current belief; `receipt` provides stable event evidence. Retry the same operation id with the same request to get the original receipt.

Before a destructive memory correction, use the latest `basis_tx` from a tool response and pass it back:

```text
forget(
  entity="project:test-gate",
  attribute="memory/text",
  operation_id="forget-project-test-gate-v1",
  if_basis_tx=72
)
```

A stale basis is a conflict that requires rereading, not an invitation to retry blindly.

## Semantic memory

`--embed-cmd` configures one local executable or JSON argv array. It receives text on stdin and must return one JSON float vector on stdout within 60 seconds and 1 MiB. The process is invoked without a shell. Writable `remember` stores `memory/embedding`; `recall` embeds the query and searches that explicit attribute. Core never downloads a model or makes a network request.

## Resource URIs

Agents that prefer MCP resources can page:

- `fgraph://schema{?prefix,cursor}`;
- `fgraph://entity/{selector}{?at,cursor}`;
- `fgraph://tx/{tx}`;
- `fgraph://changes{?since,cursor}`;
- `fgraph://event/{event}{?basis,offset,digest}`.

Schema/entity/change continuations are pinned to the basis of the first page. Malformed, stale, invisible-basis, or cross-request cursors fail rather than mixing snapshots. Cursors are opaque continuation state, not authenticated security tokens.

Schema pages share one continuation across a combined maximum of 100 attributes and shapes. Entity pages expose EAVT datoms under `items`; change pages expose complete portable `event/1` records under `events`, with a 192 KiB canonical-event budget below the overall 256 KiB resource cap. When one event exceeds that budget, `oversized_event` points to integrity-checked 128 KiB base64 chunks and the change continuation advances to later events. Following the chunk `next_uri` values reconstructs the exact canonical event document.

## Make agents use it

Add a short project instruction such as:

> Durable project knowledge lives in fgraph. Use `recall` before re-deriving facts; use `schema` before inventing attributes; store correctable decisions with a stable key, source, and operation id; never retry a destructive write after a basis conflict without rereading.
