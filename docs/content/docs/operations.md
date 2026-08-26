---
title: Operations and safety boundaries
weight: 7
---

fgraph is embedded, not maintenance-free. The v1 contract makes integrity, recovery, concurrency, deletion, and resource limits explicit.

## Check before repairing

`doctor` is mutation-free by default and can run through a read-only handle:

```bash
fgraph --db memory.db doctor --json
```

Static files on read-only media open through the immutable fallback only when no WAL/SHM sidecar exists; ordinary read-only handles remain live WAL readers. In TypeScript, `better-sqlite3` fixes URI support at its first native constructor. If another package may construct it before fgraph, set `SQLITE_USE_URI=1` at process startup; an explicit `0` disables the static-media fallback.

It verifies:

- SQLite integrity, exact format markers, and the dedicated object layout;
- bootstrap, allocator, identity, transaction, and temporal invariants;
- physical value domains and content-addressed blob hashes;
- durable event/request hashes, idempotency receipts, and redaction proofs;
- schema, vector dimensions/models, and shape definitions;
- derived FTS rows and orphaned blobs.

Repair is explicit. Back up first:

```bash
fgraph --db memory.db backup memory-before-repair.db
fgraph --db memory.db doctor --repair --json
```

Repair can rebuild FTS, remove orphan blobs, and refresh SQLite planner statistics. It cannot invent or rewrite authoritative identities, facts, receipts, hashes, schema, or redaction evidence; restore a known-good snapshot or backup for those failures.

## Choose the right recovery surface

Use an online SQLite backup for a directly reopenable operational copy:

```bash
fgraph --db memory.db backup backups/memory.db
```

The command writes a temporary sibling with SQLite's online backup API, checks it, fsyncs it, and refuses to overwrite an existing destination.

Use snapshot/restore for exact logical retained-state recovery across runtimes:

```bash
fgraph --db memory.db snapshot > memory.snapshot.ndjson
fgraph --db restored.db restore memory.snapshot.ndjson
```

All three libraries expose streaming snapshot surfaces: Python `iter_snapshot()` or `snapshot(writer)`, Go `Snapshot(ctx, writer)`, and TypeScript `snapshotLines()` or `snapshot(writer)`. The Python and TypeScript convenience string-returning calls still materialize the complete stream. For very large local databases, use a writer/iterator or the SQLite online backup path, enforce an application-level size budget, and benchmark the actual corpus.

Use tail/apply for live logical replication or merge:

```bash
fgraph --db memory.db tail --since 64 > memory.events.ndjson
fgraph --db replica.db apply memory.events.ndjson
```

CLI `apply` and the compact library summary APIs consume input incrementally and retain only `events`, `applied`, `already_applied`, `noop`, and `basis_tx`. The detailed library `apply` retains one report per event. Both are whole-stream atomic but are not a physical restore: names unify and the receiver records replay provenance, so local fact ids can differ. Redacted events after excision are intentionally non-applicable; restore a snapshot instead.

## Load large datasets resumably

Use bounded transactions for application NDJSON rather than one corpus-sized transaction:

```bash
fgraph --db memory.db add \
  --batch-size 500 \
  --operation-id-prefix import-memory-v1 \
  - < entities.ndjson
```

The loader parses incrementally and commits at most 500 input values per transaction. Operation ids are derived as `import-memory-v1:00000000`, `import-memory-v1:00000001`, and so on. If a later line is malformed or the process stops, earlier batches remain committed and an exact retry reports them as `already_applied`. This deliberately trades whole-file atomicity for bounded memory, bounded lock duration, and safe restartability.

## Make writes retryable and basis-safe

Agents and distributed callers retry. Supply `operation_id` to get the original receipt back for the same canonical request and to reject accidental id reuse. Supply `if_basis_tx` when the decision depended on a state you read.

```python
basis = db.schema()["basis_tx"]
db.transact(
    {"id": "task-42", "task/status": "done"},
    operation_id="task-42-complete-v1",
    if_basis_tx=basis,
)
```

A basis conflict means reread and reconsider. Retrying the same stale decision under a new operation id defeats the protection.

Use value-level CAS when the decision depends on one known cardinality-one fact rather than the complete database basis:

```python
db.transact(["cas", "task-42", "task/status", "running", "done"])
db.transact(["cas", "task-42", "task/lease", "worker-7", {"missing": True}])
```

CAS requires an existing entity and attribute. A mismatch or another operation touching the same cell makes the whole transaction fail.

The CLI requires both controls for irreversible excision. Writable MCP requires operation ids for every mutation and a basis for `forget`/`undo`.

## Interpret statistics consistently

`info` and the library `stats()` method share one shape in all runtimes. `entities` excludes transaction identities but includes anonymous application identities. `facts` is retained history through the selected basis; `live_facts` is the visible current/as-of subset. `blobs` counts distinct indirect values referenced by retained facts, and `size` is the main SQLite file in bytes (zero for `:memory:` and excluding WAL bytes).

## Bound generated work

Every handle has a deterministic query work budget, default `100000`. It counts intermediate joins, predicates, rules, search candidates, and expansion—not only returned rows.

```bash
fgraph --query-budget 250000 --db memory.db q @query.json
```

Search additionally bounds retrieval lists, filters, text attributes, expanded nodes, matched evidence, and its complete 1 MiB result. MCP has tighter tool limits and a 256 KiB response cap. Exceeding a bound returns `TooLarge` or a typed input error; raise a budget only for a measured trusted workload.

One logical JSON fact can contain at most 64 nested arrays/objects. Complete wire documents have a separate depth limit of 80, leaving room for transaction, event, and snapshot envelopes around a maximum-depth fact. Both limits fail with `TooLarge`; cyclic in-memory structures are rejected rather than leaking a runtime recursion failure.

Response limits are not a universal database-work limit: direct entity, history, and schema introspection can scan matching corpus state, and complete apply/snapshot operations are corpus-sized. Put total database, stream, and wall-time budgets around untrusted workloads.

## Understand privacy deletion

`nohistory` physically removes superseded fact rows, but portable event payloads still retain the values for replay. It is not erasure.

`excise` removes every fact where the target appears as entity, attribute, or ref value. It removes derived FTS/blob state and nulls every retained canonical event payload that mentions the target. A new receipt records the redacted event UUIDs without retaining their values.

The identity registry name remains. Backups, snapshots, collectors, and duplicated scalar values on unrelated entities remain separate copies. For privacy-sensitive subjects:

- use opaque entity names;
- store sensitive identifiers as facts;
- require operation id + reviewed basis;
- apply deletion/retention policy to every external copy;
- keep disk encryption and filesystem access outside the application.

Excision is deliberately absent from MCP.

## Know the trust boundary

Event and snapshot hashes detect corruption, not a malicious writer. A process that controls the SQLite file can rewrite data and hashes. Read-only MCP helps only when the underlying file is actually read-only to that process. Put permissions, encryption, snapshots, and audit collection outside an untrusted agent's control.

## Benchmark the real envelope

`mise run benchmark` drives deterministic 1k/10k/100k-entity workloads and persists JSON observations plus accessible SVG charts under `benchmarks/`. It measures ingest, size, fresh-process queries, keyword/vector search, history, snapshots, restore, tail, and apply. It is opt-in and has no fragile wall-clock CI threshold. Use the corpus, vector width, hardware, and latency target that matter to your deployment.
