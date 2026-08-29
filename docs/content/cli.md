---
title: CLI Reference
weight: 9
description: The shared JSON-first command surface provided by the Python, Go, and TypeScript implementations.
---

Python, Go, and TypeScript expose the same v1 command vocabulary and wire behavior. The examples use an installed `fgraph`; [Getting Started](../getting-started/) lists the checkout commands for each runtime.

```text
fgraph [--db PATH] [--json] [--query-budget UNITS] COMMAND [ARGS...]
```

Run `fgraph COMMAND --help` for the exact options accepted by the installed version. A command that changes durable behavior is specified normatively in the [fgraph v1 specification](../spec/#62-cli).

## Global options

| Option                 | Default        | Meaning                                                                                 |
| ---------------------- | -------------- | --------------------------------------------------------------------------------------- |
| `--db PATH`            | `facts.fgraph` | SQLite file to open. `FGRAPH_DB` supplies the same value.                               |
| `--json`               | off            | Emit canonical machine-readable JSON instead of the human rendering.                    |
| `--query-budget UNITS` | `100000`       | Cap deterministic query work. `FGRAPH_QUERY_BUDGET` supplies the same positive integer. |
| `--help`               |                | Show root or command-specific help without opening a database.                          |

Machine-readable non-stream output is canonical JSON. Portable protocols are newline-delimited JSON and therefore stream one complete canonical record per line.

The database path resolves with one explicit precedence: `--db PATH`, then `FGRAPH_DB`, then the implicit local selection described below. A selected path must not be empty; unset `FGRAPH_DB` to use the implicit selection.

For upgrade safety, an implicit command keeps using a legacy-only `fgraph.db`; fresh directories use `facts.fgraph`; and an initialized `facts.fgraph` takes precedence once both files exist. If both exist but the new default is empty or unrelated, fgraph returns a typed error before mutation. Pass either path explicitly to override this transition. Help, version, and invalid usage do not inspect either file.

## Commands

| Area                  | Commands                                                                                  | Purpose                                                                                                      |
| --------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| File and statistics   | `init`, `info`, `version`                                                                 | Initialize a file, inspect format/count statistics, or print the implementation version.                     |
| Transactions          | `add`, `retract`, `undo`, `excise`                                                        | Assert or retract facts, compensate a retained transaction, or perform audited irreversible privacy erasure. |
| Entity and provenance | `get`, `tx`, `history`, `why`, `diff`                                                     | Pull an entity, inspect one receipt, or explain facts across transaction time.                               |
| Query and retrieval   | `q`, `explain`, `datoms`, `search`                                                        | Run bounded Datalog, inspect its actual plan, page an index, or run attributable hybrid search.              |
| Schema                | `declare`, `shape`, `validate`, `schema`, `schema-export`, `schema-check`, `schema-apply` | Declare behavior, manage gradual shapes, inspect schema, and exchange a portable `schema/1` manifest.        |
| Replication           | `tail`, `apply`                                                                           | Stream or idempotently consume portable `event/1` records.                                                   |
| Recovery              | `snapshot`, `restore`, `backup`, `doctor`                                                 | Exchange exact logical `snapshot/1` state, create a physical hot backup, or check integrity.                 |
| Agent integration     | `mcp`                                                                                     | Serve the bounded MCP surface over stdio; reads are the default and `--write` is explicit.                   |

There are no v1 `export` or `import` aliases. Use `tail`/`apply` for mergeable logical replication and `snapshot`/`restore` for exact retained-state recovery.

## JSON and stream inputs

- `add` accepts inline JSON, `@file` JSON, or `-` for stdin. With stdin it accepts one JSON value or NDJSON.
- `q` and `explain` accept canonical query JSON inline or from `@file`.
- `schema-check` and `schema-apply` accept inline JSON, `@file`, or `-` for stdin; `schema-export` writes the normalized manifest.
- `apply` and `restore` consume their respective NDJSON protocol streams incrementally. `tail` and `snapshot` write them incrementally.

For bounded resumable loading, combine `add -` with `--batch-size` and a stable `--operation-id-prefix`. Completed batches remain committed after interruption, and rerunning the same input is idempotent.

```bash
fgraph --db project.db add \
  --batch-size 500 \
  --operation-id-prefix import-project-v1 \
  - < entities.ndjson
```

## Safe writes

Mutations accept an operation id for retry safety and an expected basis for optimistic concurrency where the command exposes `--operation-id` and `--if-basis-tx`. Reusing an operation id with a different canonical request is an error.

`excise` always requires both options because it irreversibly removes every application fact that mentions the selected identity. It is intentionally unavailable through MCP.

```bash
basis="$(fgraph --db project.db schema --json | jq -r .basis_tx)"
fgraph --db project.db excise private-subject \
  --operation-id privacy-request-42 \
  --if-basis-tx "$basis"
```

`doctor` is read-only unless `--repair` is explicit. Repair only rebuilds derived FTS state, removes orphan blobs, and refreshes planner statistics; it does not invent or rewrite authoritative facts and receipts.

## Exit status and environment

| Status | Meaning                                               |
| -----: | ----------------------------------------------------- |
|      0 | Success, including a schema check that reports drift. |
|      1 | Typed fgraph runtime error.                           |
|      2 | Invalid CLI usage.                                    |

The supported environment settings are `FGRAPH_DB`, `FGRAPH_CLOCK`, and `FGRAPH_QUERY_BUDGET`. `FGRAPH_EVENT_SEED` exists only for tests and reproducible conformance runs; applications should not use it as an identity source.
