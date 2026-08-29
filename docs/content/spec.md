---
title: fgraph v1 specification — SQLite format v2
linkTitle: fgraph v1 specification
weight: 11
---

This document is the normative contract for fgraph 1.x. It defines the durable SQLite format, logical semantics, portable protocols, public surfaces, and cross-language conformance requirements. Python, Go, and TypeScript are peer implementations. When another project file disagrees with this document, this document wins.

The product version and file-format version are deliberately separate: fgraph **1.0.0** creates SQLite `user_version = 2`. The first public release has no earlier on-disk compatibility obligation.

## 1. Product and boundaries

fgraph is an embedded temporal fact store in one dedicated SQLite file. A fact is an immutable assertion `⟨entity, attribute, value, tx, rx?⟩`. `tx` identifies the transaction that made it true; `rx` identifies the transaction that ended it. Current state is the subset where `rx IS NULL`.

The same core serves three uses:

1. An embedded Python, Go, or TypeScript database.
1. A local temporal knowledge base controlled through the CLI.
1. A bounded, read-only-by-default memory and retrieval service over MCP.

The design commitments are:

- **One portable file.** SQLite and FTS5 are the only storage requirements; no loadable extension is required.
- **Schema optional, constraints explicit.** New attributes work immediately; declarations and shapes add invariants when the application needs them.
- **History by default.** Supersession and retraction preserve temporal facts.
- **Deterministic boundaries.** Canonical JSON, integer microseconds, ordered results, stable event identities, bounded protocol values, and query/search work budgets are cross-runtime contracts.
- **One writer, many readers.** WAL supports concurrent readers, but fgraph is not a multi-writer synchronization protocol.
- **No network in core.** Embeddings are caller-provided or produced by an explicitly configured local subprocess.

Non-goals for v1 are a hosted server, authentication, CRDT merge, an ANN index, billion-edge analytics, a Postgres backend, extraction pipelines, and tamper-evident storage.

## 2. Durable SQLite format

### 2.1 Markers, ownership, and connection policy

- SQLite must support STRICT tables and FTS5; the feature floor is SQLite 3.37.0.
- `PRAGMA application_id = 0x66677261` (`1718055521`) and `PRAGMA user_version = 2` claim the format.
- A new file must be empty except for SQLite-owned objects. fgraph owns a dedicated file: foreign application objects, partial `fgraph_*` layouts, wrong markers, or missing required objects are `FormatError`; they are never silently initialized or migrated.
- Writable file connections use WAL, `busy_timeout = 5000`, `synchronous = FULL`, `foreign_keys = OFF`, `trusted_schema = OFF`, and bounded automatic checkpoints. `:memory:` skips WAL. Read-only connections first use ordinary SQLite read-only mode plus `query_only = ON` where available, so they remain live WAL readers. If a static database is on storage that rejects SQLite sidecar creation, and neither `<file>-wal` nor `<file>-shm` exists, implementations retry it as an immutable read-only snapshot. The caller must keep that file unchanged while it is open; an existing or unreadable sidecar makes the fallback fail rather than hide uncheckpointed state.
- Every logical write is atomic under `BEGIN IMMEDIATE`. A failed write leaves facts, identities, events, blobs, FTS, timestamps, and allocation unchanged.
- `doctor` without `--repair`, reads, close, and read-only MCP must not mutate the database.

### 2.2 Required objects

The following SQL is the format. `sqlite_sequence` is SQLite-owned and appears because `fgraph_facts.id` uses `AUTOINCREMENT`.

```sql
CREATE TABLE fgraph_meta (
  key TEXT NOT NULL PRIMARY KEY,
  value ANY NOT NULL
) STRICT;

CREATE TABLE fgraph_ids (
  id INTEGER NOT NULL PRIMARY KEY,
  name TEXT UNIQUE,
  gid BLOB UNIQUE,
  created_tx INTEGER NOT NULL,
  CHECK ((name IS NULL) <> (gid IS NULL)),
  CHECK (gid IS NULL OR (typeof(gid) = 'blob' AND length(gid) = 16)),
  CHECK (created_tx >= 64)
) STRICT;
CREATE INDEX fgraph_ids_created ON fgraph_ids (created_tx, id);

CREATE TABLE fgraph_events (
  tx INTEGER NOT NULL PRIMARY KEY,
  event_hash BLOB NOT NULL,
  event_data TEXT,
  operation_id TEXT UNIQUE,
  request_hash BLOB,
  CHECK (typeof(event_hash) = 'blob' AND length(event_hash) = 32),
  CHECK (event_data IS NULL OR typeof(event_data) = 'text'),
  CHECK (request_hash IS NULL OR
         (typeof(request_hash) = 'blob' AND length(request_hash) = 32)),
  CHECK ((operation_id IS NULL) = (request_hash IS NULL))
) STRICT;

CREATE TABLE fgraph_facts (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  e INTEGER NOT NULL,
  a INTEGER NOT NULL,
  v ANY NOT NULL,
  t INTEGER NOT NULL,
  tx INTEGER NOT NULL,
  rx INTEGER,
  CHECK (t BETWEEN 0 AND 10),
  CHECK (rx IS NULL OR rx > tx)
) STRICT;

CREATE TABLE fgraph_blobs (
  hash BLOB NOT NULL PRIMARY KEY,
  data ANY NOT NULL
) STRICT;

CREATE VIRTUAL TABLE fgraph_fts USING fts5(
  text, tokenize = "unicode61 remove_diacritics 2"
);

CREATE UNIQUE INDEX fgraph_eavt
  ON fgraph_facts (e, a, v, t) WHERE rx IS NULL;
CREATE INDEX fgraph_avet
  ON fgraph_facts (a, t, v, e, tx, rx, id);
CREATE INDEX fgraph_vaet
  ON fgraph_facts (v, a, e) WHERE rx IS NULL AND t = 0;
CREATE INDEX fgraph_hist ON fgraph_facts (e, a, tx);
CREATE INDEX fgraph_txin ON fgraph_facts (tx);
CREATE INDEX fgraph_txout ON fgraph_facts (rx) WHERE rx IS NOT NULL;

CREATE VIEW fgraph_view AS
SELECT f.id, f.e, i.name AS attribute,
       CASE WHEN f.t IN (7, 8, 9)
            THEN (SELECT b.data FROM fgraph_blobs b WHERE b.hash = f.v)
            ELSE f.v END AS value,
       f.t AS tag, f.tx, f.rx
FROM fgraph_facts f JOIN fgraph_ids i ON i.id = f.a;

CREATE VIEW fgraph_now AS
SELECT * FROM fgraph_view WHERE rx IS NULL;
```

The five durable tables are `fgraph_meta`, `fgraph_ids`, `fgraph_events`, `fgraph_facts`, and `fgraph_blobs`. FTS is derived and rebuildable. Raw SQLite pager bytes are not normative because SQLite patch releases may choose different page layouts; ordered logical rows are normative.

### 2.3 Value tags and canonical values

| Tag | Logical type | SQLite storage | Rule                                           |
| --: | ------------ | -------------- | ---------------------------------------------- |
|   0 | `ref`        | INTEGER        | Positive entity id; indexed by VAET.           |
|   1 | `bool`       | INTEGER        | Exactly `0` or `1`.                            |
|   2 | `int`        | INTEGER        | Signed 64-bit integer.                         |
|   3 | `float`      | REAL           | Finite IEEE-754 binary64; no NaN or infinity.  |
|   4 | `text`       | TEXT           | Valid UTF-8 at most 256 bytes.                 |
|   5 | `instant`    | INTEGER        | UTC microseconds in RFC 3339 years 0001–9999.  |
|   6 | `bytes`      | BLOB           | At most 256 bytes.                             |
|   7 | `vector`     | BLOB hash      | Non-empty finite float32 little-endian vector. |
|   8 | `text_ref`   | BLOB hash      | UTF-8 text larger than 256 bytes.              |
|   9 | `bytes_ref`  | BLOB hash      | Bytes larger than 256 bytes.                   |
|  10 | `json`       | TEXT           | Canonical JSON.                                |

`BLOB_THRESHOLD = 256` bytes. Every logical value is at most `MAX_VALUE_BYTES = 1_048_576` bytes. Vectors are always indirect. An indirect key is the 32 raw bytes of `SHA-256(one physical tag byte || raw payload)`; reads recompute the hash and reject missing, malformed, or mismatched blobs. Unreferenced blobs are removed transactionally.

Canonical JSON sorts object keys by Unicode code point, preserves array order, uses UTF-8 without ASCII-only escaping, normalizes negative zero to `0`, rejects non-finite numbers and invalid Unicode, and emits the shortest deterministic round-tripping binary64 representation. Integral tokens in signed-int64 range remain integers. Other finite floats use fixed notation for absolute values in `[1e-6, 1e21)` and lowercase scientific notation otherwise, with no leading zero in the exponent and an explicit `+` for positive exponents. Separators have no whitespace.

JSON wire values use plain booleans, signed integers, finite floats, and strings. The remaining logical types use one-key wrappers:

```json
{"ref":"person/ada"}
{"instant":"2026-08-24T10:00:00Z"}
{"bytes":"AAEC"}
{"vector":[0.1,-0.2]}
{"json":{"nullable":null}}
```

`null` is not a bare fact value; use `{"json": null}`. References accept a name, integer id, lookup ref, tempid, or stable `{"eid":"<uuid>"}` where the specific operation permits it.

### 2.4 Bootstrap

Ids 1–63 are reserved. Genesis is transaction/id 64 with global id `00000000-0000-4000-8000-000000000040`; the first user allocation is 65. `fgraph_meta` starts with `next_id = 65` and `created_at = genesis at`.

The system identities and their exact declared types/docs are:

| Id | Name                    | Type      | Exact documentation                                                              |
| -: | ----------------------- | --------- | -------------------------------------------------------------------------------- |
|  1 | `fgraph/at`             | `instant` | Wall-clock time of the transaction (UTC microseconds).                           |
|  2 | `fgraph/by`             | `text`    | Author of the transaction (person or agent).                                     |
|  3 | `fgraph/source`         | `text`    | Provenance of the transaction (document, conversation, tool).                    |
|  4 | `fgraph/meta`           | `json`    | Free-form JSON metadata on the transaction.                                      |
|  5 | `fgraph/many`           | `bool`    | Schema: attribute holds multiple values per entity.                              |
|  6 | `fgraph/unique`         | `bool`    | Schema: live values of this attribute are unique; enables upsert.                |
|  7 | `fgraph/nohistory`      | `bool`    | Schema: superseded values are deleted instead of kept as history.                |
|  8 | `fgraph/type`           | `text`    | Schema: enforced value type (bool,int,float,text,instant,bytes,vector,json,ref). |
|  9 | `fgraph/dims`           | `int`     | Schema: vector dimensions for vector attributes.                                 |
| 10 | `fgraph/doc`            | `text`    | Schema: human/agent documentation for an attribute.                              |
| 11 | `fgraph/excised`        | `ref`     | Audit marker: entity was physically excised at this transaction.                 |
| 12 | `fgraph/undoes`         | `ref`     | Audit marker: this transaction undoes another transaction.                       |
| 13 | `fgraph/imported-at`    | `instant` | Original source timestamp retained when an import rebases transaction time.      |
| 14 | `fgraph/vector-model`   | `text`    | Schema: opaque identity of the embedding model used by a vector attribute.       |
| 15 | `fgraph/shape`          | `ref`     | Validation: shape assigned to an entity.                                         |
| 16 | `fgraph/shape-required` | `ref`     | Validation: attribute required by a shape.                                       |
| 17 | `fgraph/shape-allowed`  | `ref`     | Validation: attribute allowed by a closed shape.                                 |
| 18 | `fgraph/shape-closed`   | `bool`    | Validation: reject application attributes not allowed by the shape.              |

Genesis inserts, in order: its `fgraph/at` fact; 18 type declarations; 18 doc facts; and `fgraph/many = true` for `fgraph/shape-required` and `fgraph/shape-allowed`. Those are exact fact ids 1–39. The canonical genesis `event/1` receipt has no domain assertions/retractions and lists the 18 system names under `created`.

## 3. Identities, transactions, and schema

### 3.1 Identities

A named identity has `name` and no `gid`; an anonymous identity or transaction has a 16-byte RFC 4122 `gid` and no name. `created_tx` makes identity visibility temporal. Anonymous identities derive stable UUIDv5 ids from the transaction's event UUID and their deterministic allocation ordinal. Transaction `gid` is its event UUID.

Names are 1–512 UTF-8 bytes without control characters. `fgraph/` is reserved. Attribute names match exactly:

```text
^[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*$
```

Unknown names auto-create in writes. Reads never allocate. Names are permanent identity registry entries, including after excision; sensitive identifiers must therefore be stored as facts under opaque names. System and transaction entities cannot be mutated as ordinary application entities.

### 3.2 Transaction input and reports

`transact` accepts one record, one operation, or a list. A record uses optional `id` plus attribute keys. An operation is `['assert', e, a, v]`, `['retract', e]`, `['retract', e, a]`, `['retract', e, a, v]`, or `['cas', e, a, expected, desired]`. Nested records are allowed only for declared ref attributes. `{"tmp":"token"}` creates a transaction-local identity; report `ids` resolves tokens. A lookup `[unique-attribute, value]` resolves an existing unique owner.

CAS targets an existing entity and existing cardinality-one attribute. The exact sentinel `{"missing":true}` may be used as `expected` to create an absent fact or as `desired` to delete a present fact. A mismatch, a cardinality-many target, or another operation touching the same entity/attribute in one transaction fails atomically. CAS is normalized into ordinary event retractions/assertions, so request hashing, replay, history, and receipts need no separate wire form.

Transaction options are `source`, `by`, `meta`, arbitrary `tx` facts, `operation_id`, and `if_basis_tx` (camelCase in idiomatic APIs). All facts, metadata, inferred declarations, identities, and the receipt commit atomically.

Every committed transaction has a strictly increasing integer-microsecond `fgraph/at`, a fresh UUID event id, and a durable `fgraph_events` row. Normal allocation is deterministic: entities and attributes follow normalized first appearance and the transaction is allocated last. A true no-op allocates nothing and returns `status = 'noop'`, `tx = null`, `event = null`.

A transaction report contains exactly the logical fields:

```json
{
  "status": "applied",
  "event": "uuid",
  "basis_tx": 64,
  "tx": 67,
  "at": 1767225601000000,
  "ids": {},
  "asserted": [],
  "retracted": []
}
```

`operation_id` is 1–512 UTF-8 bytes without control characters. Its `request_hash` is SHA-256 over the canonical logical request. Direct `transact` uses exactly `{"data":<wire input>,"options":{...}}`; `options` contains only explicitly supplied `by`, `meta`, `source`, and `tx` fields. The operation id and basis guard are excluded because they identify and condition the request rather than change its logical effect. `undo(tx)` instead hashes `{"operation":"undo","tx":tx}` plus `by` when supplied; `excise(ref)` hashes `{"operation":"excise","ref":ref}`; schema apply is defined in §3.5. Retrying the same id and request returns the original transaction as `already_applied`, even if the supplied basis is now stale. Reusing the id for a different request is `Conflict`. Otherwise `if_basis_tx` must equal the current basis or the entire write fails with `Conflict`.

`receipt(tx)` returns `read_basis_tx`, the transaction's prior `basis_tx`, `tx`, event UUID, `sha256:<event hash>`, operation/request ids and hashes, timestamp, optional provenance, and custom transaction facts.

### 3.3 Cardinality, uniqueness, and history

Attributes are cardinality one by default. A new value retracts the prior live value; asserting two different values for a cardinality-one attribute in one transaction is `Conflict`. `many = true` keeps multiple distinct live values. An identical live assertion is a no-op.

`unique = true` requires a declared scalar type other than JSON/vector. A record without `id` that asserts an existing unique value upserts its owner; an explicit different owner conflicts. Schema changes validate existing live data.

Retraction normally sets `rx`. `nohistory = true` physically deletes matching fact rows; vectors default to nohistory unless explicitly overridden. This is a storage/history policy, not a privacy guarantee: canonical event payloads still retain replayable values until a covering excision redacts them.

### 3.4 Declarations, introspection, and shapes

Schema declarations are ordinary temporal facts on an attribute:

- `type`: one of `ref`, `bool`, `int`, `float`, `text`, `instant`, `bytes`, `vector`, or `json`.
- `many`, `unique`, and `nohistory`: boolean behavior.
- `dims`: positive vector dimensions. When an attribute has no declared type, its first vector write infers and asserts `type = vector`; that write also infers and asserts dimensions when absent. Partial declarations such as `doc` or `many` do not disable this inference.
- `doc`: human/agent description.
- `vector_model`: opaque model identity. Choosing an attribute binds retrieval to that embedding space; fgraph does not call a model.

Declarations are patches. Raw schema facts and `declare` have identical validation. A transaction's final working set must satisfy type, dimensions, cardinality, uniqueness, and shape rules regardless of input order.

`attributes(prefix?, include_system?)` is compact discovery. `schema(...)` returns the canonical rich snapshot:

```json
{
  "basis_tx": 70,
  "digest": "sha256:...",
  "attributes": [{
    "name": "person/name",
    "declared": {},
    "effective": {
      "type": null,
      "many": false,
      "unique": false,
      "nohistory": false,
      "dims": null,
      "doc": null,
      "vector_model": null
    },
    "observed": { "types": ["text"], "live_facts": 1, "entities": 1 }
  }],
  "shapes": []
}
```

The digest is SHA-256 of canonical JSON containing each attribute's `name/declared/effective` plus shapes; observation counts do not change it. Historical views report the schema visible at their basis.

A shape has a stable name, required attributes, allowed attributes, and a `closed` flag. Entities opt in with `fgraph/shape` refs. Required attributes must be present. A closed shape rejects application attributes outside `allowed`; every required attribute must therefore also be allowed. `validate` returns deterministic violations, while transactions fail with `SchemaError` instead of committing an invalid final state.

### 3.5 Portable schema manifests

`schema/1` is the portable application-schema control plane. Export always returns this exact shape:

```json
{
  "fgraph": "schema/1",
  "digest": "sha256:<64 lowercase hex digits>",
  "attributes": [
    {
      "name": "project/id",
      "declared": { "type": "text", "unique": true }
    }
  ],
  "shapes": [
    {
      "name": "shape/project",
      "required": ["project/id"],
      "allowed": ["project/id"],
      "closed": true
    }
  ]
}
```

Input requires `fgraph = "schema/1"`; `attributes` and `shapes` may be omitted and then mean empty arrays. `digest` is optional input metadata: implementations ignore its supplied value and recompute it. Unknown top-level fields are invalid. Each attribute item has exactly `name` and `declared`; the declaration may contain only `type`, `many`, `unique`, `nohistory`, `dims`, `doc`, and `vector_model`, with the types and constraints from §3.4. Each shape item has exactly `name`, `required`, `allowed`, and `closed`; the two attribute lists contain valid attribute names and `closed` is boolean. Duplicate attribute or shape names are invalid.

Normalization is deterministic:

- empty declarations are omitted, while explicit `false` declarations are preserved;
- attributes and shapes are sorted by name in Unicode code-point order;
- shape lists are deduplicated and sorted, and a closed shape's `allowed` list is unioned with `required`;
- `digest` is `sha256:` plus lowercase SHA-256 hex over canonical JSON of exactly `{"fgraph":"schema/1","attributes":[...],"shapes":[...]}` after normalization, without the digest field.

A normalized manifest is a **full replacement**, not a patch. An omitted declaration field retracts that field, an omitted attribute retracts all its declarations, and an omitted shape retracts its required, allowed, and closed definition. Replacement does not delete identity registry entries or application facts. The complete desired declaration/shape state is validated against live data and applied in one transaction; any failure leaves facts, schema, identities, allocation, time, and receipts unchanged.

`schema-check` is read-only and returns exactly `basis_tx`, `valid`, `current_digest`, `desired_digest`, and `changes`. Changes are sorted by `(kind, name)`, where `kind` is `attribute` or `shape`; `before` or `after` is `null` when that item is absent. `schema-apply` returns the transaction report from §3.2 and accepts `operation_id` and `if_basis_tx` with the same conflict rules as `transact`.

For idempotency, schema apply hashes the canonical request `{"operation":"schema-apply","manifest":<normalized desired manifest>}`, not the current schema or generated retract/assert operations. Retrying the same `operation_id` and normalized manifest returns `already_applied` with the original `basis_tx`, event, transaction, and timestamp plus empty `ids`, `asserted`, and `retracted`, even when the supplied basis is now stale. Reusing the id for a different normalized manifest or supplying a stale basis for a new id is `Conflict`.

Malformed JSON at a textual boundary is `TypeError`. A wrong manifest version, invalid shape, name, field, declaration, duplicate, or final schema state is `SchemaError`; mutation through a read-only or historical view is `ReadOnly`. Validation and basis/idempotency checks happen before any durable effect.

## 4. Reads and query

### 4.1 Temporal reads and provenance

- `entity(ref, depth)` / `pull` reads current facts with bounded nested refs.
- `at(tx)` returns an immutable historical view. Reads and schema are limited to identities/facts visible at that basis; search and follow are current-only.
- `history(ref, attr?)` returns the ordered fact timeline with asserting and retracting provenance.
- `why(ref, attr?)` returns current facts with full transaction metadata.
- `diff(t1, t2)` returns facts asserted/retracted in `(t1, t2]`.
- `receipt(tx)` exposes stable operation and event evidence.
- `undo(tx)` writes an audited compensating transaction with `fgraph/undoes = ref(tx)`; it never rewrites history. A valid target with no invertible domain delta still commits that audit fact instead of returning an unaudited no-op.

Rendered facts contain local fact `id`, rendered entity/attribute/value, `tx`, and `rx`; provenance surfaces additionally include `at`, `by`, and `source` as available. Local fact ids are stable within one file but are not portable across event replay because an importing store records its own provenance facts.

### 4.2 Canonical JSON Datalog

A query object supports `find`, `where`, `in`, `rules`, `source`, `order`, `limit`, and `offset`; unknown keys are errors. `source` is `current` (default) or `history`.

Patterns are:

```json
["?e", "person/name", "?name"]
["?e", "person/name", "?name", "?tx"]
["?e", "person/name", "?name", "?tx", "?added"]
```

The 4th/5th positions expose the event transaction and whether the datom was added or retracted. Entity, attribute, and value positions accept constants, variables (`?name`), or `_`; variables must be safely bound before predicates or negation. Predicates are `=`, `!=`, `<`, `<=`, `>`, `>=`, `contains`, and `starts-with`. Clause objects are `{"not":[...]}`, `{"or":[[...],[...]]}`, or `{"rule":["name", ...]}`. `or` branches bind identical outward variables; negation must be correlated.

Rules use `{ "head": ["name", "?arg"], "body": [...] }`. Direct self-recursion reaches a fixpoint; mutual recursion is unsupported. `find` accepts variables, `pull`, and aggregates `count`, `count-distinct`, `sum`, `avg`, `min`, and `max`. Pull cannot mix with aggregates. Unordered rows compare as multisets; a non-empty `order` is deterministic over rendered values.

Queries spend deterministic work units and abort with `TooLarge` at the database budget or with `QueryError` on cancellation. Candidate evaluation charges every examined fact-binding pair, including duplicate bindings that reuse one batched fact; a query `pull` projection additionally charges every visible fact examined while materializing the pull, including nested and reverse results. Standalone `entity` and `pull` reads are outside the query budget. `explain` validates without evaluation and reports basis/source plus one of these stable access labels per pattern: `eavt/exact`, `eavt/ea`, `avet`, `eavt/e`, `avet/a`, `value-scan`, or `scan`. Predicate/negation/rule barriers preserve semantic order.

### 4.3 Datom index API

`datoms(index, components, source, limit, cursor)` pages a basis-pinned index. Indexes are `eavt`, `avet`, and `vaet`; components must be a valid prefix of the selected logical order. `source` is current/history, library limit is 1–1000, and the opaque cursor binds format, basis, source, index, components, and last row. Malformed, invisible-basis, stale, mismatched, or cross-request cursors fail; they never silently restart. Cursors are opaque continuation state, not authenticated security tokens.

### 4.4 Bounded hybrid search

Search accepts optional text, optional vector, explicit `vector_attribute`, up to 16 `text_attributes`, up to 16 exact filters, `k` from 1–100, and `expand` from 0–3. At least one retrieval signal is required. A vector requires an explicit vector attribute so embedding spaces never mix.

Keyword candidates use FTS5 BM25 over live application facts. Input is reduced to quoted Unicode word tokens; blank/tokenless text produces no keyword list. Vector candidates use brute-force cosine similarity over the selected vector-typed attribute; zero query vectors, dimension conflicts, and unknown or non-vector attributes fail. Filters are applied before ranking.

Each list keeps at most `min(500, max(50, 5*k))` candidates. Entity ranks use their best fact. Reciprocal Rank Fusion uses `K = 60`; ties break by rendered entity identity. Expansion is deterministic bidirectional BFS over live ref facts, capped at 100 entities. Each hit contains `entity`, `score`, attributable `matched` facts, and a compact `pull` (32 attributes/32 many-values). Result also contains `basis_tx`, `expanded`, `truncated`, and `work_used`.

Matched strings are capped at 2 KiB and marked truncated; vector matches expose dimensions, not payload. The complete canonical result is capped at 1 MiB by deterministically dropping expanded nodes, then matched evidence, then trailing hits. Search also obeys the database work budget. v1 deliberately has no ANN index; measured workloads should justify one later.

## 5. Portable events, snapshots, and maintenance

### 5.1 `event/1`

Every committed transaction stores canonical event JSON and `event_hash = SHA-256(UTF-8 event_data)`. A normal event has exactly required fields `fgraph`, `event`, `at`, `created`, `asserted`, and `retracted`, plus optional `by`, `source`, `meta`, and `tx_facts`:

```json
{
  "fgraph": "event/1",
  "event": "8c5bd882-4cb0-4901-9af6-4027c1a588fb",
  "at": 1767225601000000,
  "created": ["ada", "person/name"],
  "asserted": [["ada", "person/name", "Ada", "text"]],
  "retracted": []
}
```

Fact tuples are `[entity selector, attribute name, logical wire value, logical
tag]`. A selector is a name or `{"eid":"uuid"}`. `created` excludes the transaction itself. `tx_facts` carries nonstandard facts on the transaction. One canonical event is capped at `8,454,144` bytes.

`tail(since)` emits canonical NDJSON in transaction order; `--follow` waits for later commits. `apply` validates and applies a complete event stream inside one outer transaction. The UTF-8 byte cap is checked before parsing each event. It is idempotent by event UUID/hash, preserves source event identity, and records source time under `fgraph/imported-at` while assigning a valid local transaction time. Reapplying the same UUID/hash returns `already_applied` with the original transaction's predecessor basis and empty ids/asserted/retracted deltas, even when the receiver has advanced. Any malformed, conflicting, oversized, cancelled, or short-I/O stream rolls back completely.

The event cap is per line, not an aggregate stream limit. Reader/iterator library inputs are consumed incrementally, while string inputs may already be materialized by the caller. Detailed library `apply` surfaces retain one report per event; CLI `apply` and compact summary surfaces retain only counters and the final basis. The caller therefore owns the total byte, event-count, time, and returned-report budget for a stream.

Event replay is logical merge, not exact physical restore: names unify, local ids may reallocate, and replay-provenance facts may shift fact row ids. The canonical event stream and resulting public logical state remain portable.

### 5.2 Snapshots and backups

`snapshot/1` is the exact logical recovery protocol. NDJSON contains one header (`format`, `basis`, `created_at`), ordered receipt records, retained fact records, and one `fgraph = 'end'` record with counts and SHA-256 over prior lines. It includes retractions, stable global identities, operation receipts, redaction state, and allocator-reconstructable ordering.

`restore` accepts only a pristine destination, validates the entire bounded stream and footer, and atomically reconstructs an equivalent format-v2 file. One snapshot record is capped at `16,973,824` UTF-8 bytes (`2 * event cap + 64 KiB`): a receipt embeds one event and repeats its created selectors so local ids remain reconstructable after later redaction. File readers enforce the cap before an unbounded line allocation; LF is the only record delimiter and is not part of the cap. Every runtime must reproduce the same snapshot and ordered core rows. Snapshots are the recovery path for an excised database because portable `apply` rejects redacted event records whose original payload is intentionally unavailable.

`backup(dest)` uses SQLite's online backup API into a temporary sibling, verifies it with `doctor`, fsyncs it, and publishes it without overwriting an existing destination. It is safe while the source has readers/writer activity.

### 5.3 Doctor and excision

`doctor` verifies SQLite integrity, exact markers/layout, identity and temporal domains, bootstrap, value storage, blob digests, FTS derivation, events and request hashes, operation uniqueness, redaction proofs, schema, and shapes. `--repair` is the only mutating mode: it rebuilds derived FTS, removes orphan blobs, and refreshes planner statistics; it cannot repair authoritative facts or receipts.

`excise` is irreversible privacy deletion for one application identity. It physically deletes every fact where the target appears as entity, attribute, or ref value; removes derived FTS/blob state; and retains an audited `fgraph/excised` marker. The stable identity registry row/name remains, so names must be opaque.

Every retained event payload that mentions the selector is set to NULL while its original hash remains. The new canonical excision event has `redacted = true` and a unique lexically sorted `redacts` list of prior event UUIDs. `tail` represents each cleared prior payload only as an event/hash stub, so deleted values cannot reappear. `doctor` reports those original hashes as intentionally unverifiable, not corrupt. The CLI requires both `--operation-id` and `--if-basis-tx`; excision is not exposed through MCP.

## 6. Public surfaces

### 6.1 Libraries

Public APIs are idiomatic per language but semantics and JSON shapes are identical. The required capabilities are:

- open/connect, read-only views, close, and stats;
- transact, retract, declare, shape, validate, undo, and excise;
- entity/pull, query/explain, datoms, search, attributes/schema, and schema manifest export/check/apply;
- at/history/why/diff/changes/follow and transaction receipt;
- tail/event records/apply, snapshot/restore, backup, and doctor.

Python uses snake_case, Go uses context plus option structs and typed errors, and TypeScript uses camelCase with lossless `bigint` where a wire integer exceeds its safe numeric range. No implementation delegates core behavior to another runtime.

`stats()` has one exact JSON shape in every runtime: `application_id`, `format_version`, `entities`, `attributes`, `facts`, `live_facts`, `transactions`, `blobs`, and `size`. `entities` counts visible non-transaction identities, including anonymous identities; `attributes` counts visible names matching the attribute grammar. `facts` counts retained fact rows asserted by the selected basis, while `live_facts` applies current/as-of visibility. `transactions` counts event receipts, and `blobs` counts distinct indirect values reachable from retained facts at that basis. `size` is the current main SQLite file size in bytes, or zero for an in-memory database; WAL bytes are not included.

### 6.2 CLI

All implementations expose the same canonical commands:

```text
init info add retract get tx q explain datoms search history why diff
declare shape validate schema schema-export schema-check schema-apply tail
apply snapshot restore undo excise backup doctor mcp version
```

Global options are `--db`, `--json`, and `--query-budget`. The database path resolves in this order: explicit `--db`, then `FGRAPH_DB`, then the default `facts.fgraph`. An explicitly selected empty path, including a present-but-empty `FGRAPH_DB`, MUST fail with `FormatError`; unsetting the environment variable selects the default. When the path selection is implicit and the legacy default `fgraph.db` exists, a database-opening command MUST verify that `facts.fgraph` is already an initialized fgraph database before using it. If the new default is missing, empty, or unrelated, the command MUST fail with `FormatError` before mutation and tell the caller to select either path explicitly. Help, version, and invalid usage MUST be handled without inspecting either database path. `add`, declarations, shapes, `schema-apply`, and undo accept `--operation-id`/`--if-basis-tx`; irreversible `excise` requires both. Machine output is canonical JSON; protocol streams are NDJSON. Exit status is 0 success, 1 typed runtime error, and 2 usage error. Other environment configuration is `FGRAPH_CLOCK`, `FGRAPH_QUERY_BUDGET`, and the test/reproducibility-only `FGRAPH_EVENT_SEED`.

`schema-export` accepts no positional argument and prints the normalized manifest. `schema-check <json|@file|->` compares inline JSON, a file, or standard input without mutation; drift is a successful result with `valid = false`. `schema-apply [--operation-id ID] [--if-basis-tx TX] <json|@file|->` atomically installs the full desired manifest.

Legacy `export`/`import` command aliases are not part of v1. Use `tail`/`apply` for portable event replication and `snapshot`/`restore` for exact recovery.

### 6.3 MCP

MCP uses the official SDK over stdio. It is read-only by default; `mcp --write` opts into three mutation tools. The exact inventory is:

| Mode            | Tools                                                                                  |
| --------------- | -------------------------------------------------------------------------------------- |
| Read            | `recall`, `about`, `why`, `history`, `query`, `datoms`, `receipt`, `schema`, `explain` |
| Write additions | `remember`, `forget`, `undo`                                                           |

Every successful tool result is both text and structured content with exactly `{"ok":true,"basis_tx":...,"data":...}`. A read is pinned before evaluation or uses the basis returned by the core search/page operation; a mutation reports the transaction it established, or the basis it checked for a no-op. An idempotent retry retains its original transaction basis rather than advertising a newer head it did not evaluate. Canonical responses are capped at 256 KiB; callers must narrow or continue when pagination exists. Output bounds are:

- `recall`: `k` 1–20, `expand` 0–2;
- `about`: depth 0–2;
- `why`/`history`: limit 1–100 with `{items,truncated}`;
- `query`: explicit/default limit 0–1000 (default 100), never silently clamped;
- `datoms`/`schema`: page limit at most 100 and opaque cursor at most 4096 bytes; one schema cursor continues across the combined ordered attribute-and-shape sequence.

`remember`, `forget`, and `undo` require `operation_id`; destructive `forget`/`undo` also require `if_basis_tx`. The server records `by = 'mcp:<negotiated-client-name>'`. `remember` accepts structured facts and/or a text note; an optional key provides stable superseding memory. Configured `--embed-cmd` stores/queries `memory/embedding` through a shell-free, 60-second, 1 MiB local process boundary.

These item limits bound agent-visible output. Query/search work and datom/event paging are bounded in the core, but `about`, `why`, `history`, and schema snapshot construction can still traverse corpus-sized local state before trimming a response. Run MCP only over a trusted, operationally bounded file and use query/datoms for explicitly budgeted large traversals.

The bounded resources are:

- `fgraph://schema{?prefix,cursor}` — `{basis_tx,digest,attributes,shapes,next_uri?}`, with at most 100 combined items;
- `fgraph://entity/{selector}{?at,cursor}` — `{basis_tx,items,next_uri?}` containing current/historical EAVT datoms;
- `fgraph://tx/{tx}` — receipt with at most 100 custom facts;
- `fgraph://changes{?since,cursor}` — `{basis_tx,events,oversized_event?,next_uri?}` containing at most 100 complete portable `event/1` records and at most 192 KiB of canonical event JSON per page. If the next event alone exceeds that page budget, `events` is empty and `oversized_event = {event,event_hash,bytes,uri}` points to its first chunk; the change cursor advances past that event so a later `next_uri` cannot be blocked by it.
- `fgraph://event/{event}{?basis,offset,digest}` — `{basis_tx,event,event_hash,offset,encoding:"base64",data,next_uri?}` containing at most 128 KiB of the canonical UTF-8 `event/1` document. `basis` and the lowercase 64-hex SHA-256 `digest` are required; `offset` defaults to zero. The server pins the basis, validates the digest against the durable receipt, rechecks canonical JSON and its hash before serving bytes, and rejects redacted payloads. Following `next_uri` values in order reconstructs the exact document without a trailing newline.

Excision, repair, restore, apply, and backup are deliberately absent from MCP.

## 7. Errors, conformance, and release proof

The stable typed error names are:

`NotFound`, `Conflict`, `SchemaError`, `TypeError`, `QueryError`, `FormatError`, `ReadOnly`, `TooLarge`, and `Unsupported`.

Boundary input is parsed into trusted logical types; errors are never swallowed. CLI and conformance report these names independent of language-specific wrapping.

Every normative behavior must land in all three implementations and, when portable, in `conformance/cases/`. Runners use clock `1767225600000000 + n*1_000_000` microseconds and event seed `fgraph-conformance-v2`. Expectations are exact unless an object explicitly has `"...": true`; arrays remain exact.

`scripts/crosscheck.sh` must prove:

1. Every runtime writes exact ordered core rows for the canonical scenario.
1. Every runtime reads every peer's file with identical events, snapshots, query, schema, keyword search, and vector search.
1. Every runtime restores every peer snapshot to exact logical rows.
1. Every runtime applies every peer event stream to identical portable state.
1. A malformed multi-event apply rolls back the complete stream.

`conformance/fixtures/format-v2.*` is immutable after the first public release. Every runtime must open the fixture read-only, pass doctor, and reproduce its events, snapshot, and exact core rows without migration.

Each runtime maintains at least 95% under its pinned native coverage gate: Python's branch-enabled aggregate, Go statements, and TypeScript statements, branches, functions, and lines. The release source gate is `mise run all`, which includes format, static/type/security checks, unit/property/conformance tests, minimum-runtime tests, cross-runtime differential and fixture tests, examples, installed package smoke tests, strict docs/link checks, and builds.

## 8. Security and scalability boundary

Event and snapshot hashes detect corruption and bind receipts, but a process that can rewrite the SQLite file can rewrite hashes too. fgraph is auditable only when file permissions, backups, snapshots, or a tail collector are controlled outside the writer's trust boundary. Read-only MCP is defense in depth, not authorization for an otherwise writable file.

Serialized values, individual events, cursors, MCP outputs, external embedder I/O, and query/search work have explicit caps. Complete apply/snapshot streams, retained report lists, and direct entity/history/schema scans are corpus-sized; callers must impose total input, time, and database-size budgets appropriate to their trust boundary. SQLite provides durable single-writer transactions and many readers. Scale vertically until measured FTS/vector/query costs justify a new index or backend; v1 does not hide linear vector search or promise distributed write scalability.
