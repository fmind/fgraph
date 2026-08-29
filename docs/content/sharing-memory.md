---
title: Sharing and auditing memory
weight: 5
---

fgraph has three sharing surfaces with deliberately different guarantees:

| Surface                 | Use                               | Exact local rows | Mergeable                | Works after excision                |
| ----------------------- | --------------------------------- | ---------------- | ------------------------ | ----------------------------------- |
| SQLite online backup    | Operational copy                  | Yes              | No                       | Yes                                 |
| `snapshot` / `restore`  | Cross-runtime exact recovery      | Yes, logically   | No; pristine destination | Yes                                 |
| `tail` / `apply` events | Review, audit, replication, merge | No               | Yes                      | Redacted streams are non-applicable |

## Review portable events

```bash
fgraph --db project.db tail --since 64 > memory.events.ndjson
git diff -- memory.events.ndjson
```

Each canonical `event/1` line has a stable UUID, source timestamp, created identities, asserted facts, retracted facts, and optional provenance. The stream uses names or portable EIDs rather than local numeric ids, so Python, Go, and TypeScript reproduce it exactly from the same file.

Do not commit sensitive events merely because they are text. Keep both SQLite and NDJSON out of source control when the facts are private. fgraph does not encrypt values.

## Apply a logical stream

```bash
fgraph --db replica.db apply memory.events.ndjson
```

`apply` validates every individually capped event before committing the complete stream. A malformed, conflicting, oversized, or interrupted later line rolls back every earlier line in that call. Reapplying the same event UUID/hash returns the original receipt basis with empty change deltas, even if the receiver has since advanced.

There is deliberately no aggregate stream cap: reader/iterator APIs consume incrementally, but string callers may already hold the input. Detailed library `apply` surfaces return one receipt per event; CLI `apply` and compact summary surfaces retain only counters and the final basis. The caller owns total bytes, event count, wall time, and returned-report memory.

Applying is an ordered logical merge: names unify, ids reallocate, cardinality semantics follow event order, and each source time is retained as `fgraph/imported-at` while the receiver assigns a valid local monotonic transaction time. This is not a CRDT; review order when independent streams update the same cardinality-one attribute.

Local fact ids can differ after apply because the receiver records local replay provenance. Stable event ids, entity identities, values, transaction order, and public logical results are the portable contract.

## Restore exact retained state

```bash
fgraph --db project.db snapshot > memory.snapshot.ndjson
fgraph --db restored.db restore memory.snapshot.ndjson
```

The snapshot header pins format, basis event, and creation time. Receipt and fact records include retained history, transaction metadata, operation hashes, stable identities, retractions, and redaction state. The footer binds counts and a SHA-256 digest over every prior line.

`restore` accepts a pristine destination only and publishes nothing unless the complete stream validates. Every runtime must reproduce the same snapshot and ordered logical core rows.

Snapshot/restore is the safe recovery path after privacy excision. Prior event payloads that mentioned the target have been intentionally cleared, so a redacted tail cannot reconstruct the database and `apply` rejects it.

## Audit a running agent

```bash
fgraph --db agent.db tail --since 120 --follow | jq -c .
```

`tail --follow` streams new committed events. A collector can preserve evidence outside the agent's file and trust boundary. If the collector matters as an audit log, the agent must not control its process, destination, or retention.

Read-only MCP is the default agent-facing surface:

```bash
fgraph --db project.db mcp
```

It opens SQLite read-only and omits all mutation tools. Add `--write` only for a file the agent is authorized to change.

## Back up an active database

```bash
fgraph --db agent.db backup backups/agent-2026-08-25.db
fgraph --db backups/agent-2026-08-25.db doctor --json
```

The command uses SQLite's online backup API, verifies the temporary copy, fsyncs, and refuses overwrite. Run it—or an external replication system such as Litestream—outside an untrusted agent process and copy replicas off the VM.

## Deletion and retention

`nohistory` removes superseded fact rows but does not redact replayable event payloads. `excise` covers entity, attribute, and ref-value occurrences and redacts matching retained events, but it cannot reach copies already written to backups, snapshots, source control, or collectors.

Filesystem permissions, disk encryption, replica retention, secure deletion, and collector policy remain operator responsibilities. Event hashes detect corruption; they do not make a writer-controlled file tamper-evident.
