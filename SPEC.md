# fgraph SPEC — format v1, release v0.1

This document is the **normative contract** for implementing fgraph. It defines the file format, semantics, query language, APIs, CLI, MCP server, and conformance suite. The Python and Go implementations are twins: both MUST implement everything here, verified by the shared `conformance/` suite. When this document and any other file disagree, this document wins.

**For the implementing agent:**

1. Read this file fully, then `AGENTS.md` and `README.md`.
1. Load and follow these skills for all conventions (do not re-invent them): `~/.agents/skills/python-stack/SKILL.md` for `python/`, `~/.agents/skills/go-stack/SKILL.md` for `go/`, plus `mise`, `dprint`, `lefthook`, `hugo`, `github-actions`, `conventional-commit`, and `readme-agents` skills as referenced.
1. Use the **latest stable** releases of every tool and dependency (verify online at implementation time; no RCs/betas).
1. Work milestone by milestone (§15), implementing each milestone in **both languages** with its conformance cases green before moving on.
1. Keep it simple. Prefer the smallest correct implementation; do not add features, options, or abstractions this spec does not require. Fix root causes; never weaken a test or gate to go green.

---

## 1. Product definition

**fgraph** (fact graph) is an embedded temporal fact store in a single SQLite file. Every piece of knowledge is an immutable **fact** `⟨entity, attribute, value⟩` that carries the transaction that asserted it and, when superseded or retracted, the transaction that ended it. Nothing is overwritten: the present is a view over the past, time travel is a query, and every fact can answer "when and why did I start (or stop) being true?".

Three usage modes, all served by the same library + CLI + MCP server:

1. **Standalone in an app** — a Python or Go library for facts with history (users, agents, domain knowledge).
1. **Project knowledge base over MCP** — expose a `.db` about a project to any coding agent (read-only supported).
1. **Agent memory over MCP** — an LLM stores and recalls its own memory with provenance and audit trail.

**Non-goals (v1)**: no server, no network calls in core (bring-your-own embeddings), no extraction pipeline, no ANN index, no multi-writer concurrency, no billion-edge analytics, no Postgres backend (the conformance suite keeps that possible later).

## 2. Design principles

- **Pure file**: the format is plain SQLite tables + FTS5 only. No loadable extension is ever required to read or write a file. Every accelerator (FTS index, caches) is derived and rebuildable.
- **File over server (the VM bet)**: the agentic landscape is heading toward one VM/sandbox per agent, where a database must be a file, not a service. A file means zero provisioning, snapshot/fork/restore as file operations, off-VM replication with stock tools (Litestream, `backup`), and **supervision without cooperation** — a host audits an agent's memory by tailing the file read-only (`follow`/`tail`); the agent cannot hide writes because the file is the interface.
- **Zero config**: `connect()` creates or opens; init is implicit and idempotent; schema is optional.
- **Friction budget**: each public concept must pay for itself. When in doubt, leave it out.
- **Correctness bar**: temporal semantics are property-tested against a reference model; timestamps are integer microseconds end-to-end (no float time, ever); the conformance suite is the contract between implementations.
- **Spec-first twins**: the SQLite file is the interchange; there is no FFI. Both implementations stay dependency-light (Python core: stdlib only; Go: `modernc.org/sqlite`, CGO-free).

## 3. Terminology

- **Entity** `e`: an `int64` identifier. Exists by having facts and/or a name.
- **Name**: a unique human-readable alias for an entity, stored in `fgraph_ids`. Attributes are named entities.
- **Attribute** `a`: a named entity whose name matches `^[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*$` (exactly one `/`). The `fgraph/` namespace is reserved for the system.
- **Value** `v`: a typed scalar (see tags, §4.4).
- **Transaction** `tx`: an entity representing one atomic write; its metadata are ordinary facts on it.
- **Fact**: row `⟨e, a, v, t, tx, rx⟩` — asserted by `tx`, retracted by `rx` (`NULL` = currently true).

## 4. File format (normative)

### 4.1 SQLite requirements

- Minimum SQLite **3.37.0** (STRICT tables). Feature floor only — target the latest bundled/embedded SQLite.
- On init: `PRAGMA application_id = 0x66677261;` ("fgra", decimal 1718055521) and `PRAGMA user_version = 1;` (format major version).
- On every connection: `PRAGMA journal_mode = WAL;` (persistent, skip for `:memory:`), `PRAGMA foreign_keys = OFF;`, `PRAGMA busy_timeout = 5000;`, `PRAGMA synchronous = NORMAL;`.
- Writes run inside `BEGIN IMMEDIATE … COMMIT` (single-writer; concurrent readers via WAL).
- Connections run `PRAGMA optimize;` on close (keeps planner statistics fresh, per sqlite.org guidance).
- Explicit read-only surfaces (`--read-only` CLI/MCP, followers tailing another process's file) open with the SQLite URI `mode=ro` for defense in depth.
- Opening a file whose `application_id`/`user_version` conflict with fgraph → typed error (never silently re-init). A file without fgraph tables gets them created (init on top of existing databases is supported; fgraph only touches `fgraph_*` objects).

### 4.2 Tables

```sql
CREATE TABLE fgraph_meta (
  key   TEXT NOT NULL PRIMARY KEY,
  value ANY  NOT NULL
) STRICT;
-- rows: ('next_id', <int>), ('created_at', <int µs>)

CREATE TABLE fgraph_ids (
  id   INTEGER NOT NULL PRIMARY KEY,
  name TEXT    NOT NULL UNIQUE
) STRICT;

CREATE TABLE fgraph_facts (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, -- stable fact id: survives VACUUM, never reused (citations stay valid after excision)
  e  INTEGER NOT NULL,              -- entity
  a  INTEGER NOT NULL,              -- attribute -> fgraph_ids.id
  v  ANY     NOT NULL,              -- value in its native SQLite storage class
  t  INTEGER NOT NULL,              -- value tag (§4.4)
  tx INTEGER NOT NULL,              -- asserting transaction
  rx INTEGER,                       -- retracting transaction; NULL = currently true
  CHECK (t BETWEEN 0 AND 10),
  CHECK (rx IS NULL OR rx > tx)
) STRICT;

CREATE TABLE fgraph_blobs (
  hash BLOB NOT NULL PRIMARY KEY,   -- SHA-256 of data
  data ANY  NOT NULL                -- original value (TEXT or BLOB storage class)
) STRICT;

CREATE VIRTUAL TABLE fgraph_fts USING fts5(
  text, tokenize = "unicode61 remove_diacritics 2"
);
-- rowid = fgraph_facts.id; holds the resolved text of every live fact with t IN (4, 8)
```

### 4.3 Indexes and views

```sql
-- The present is a partial index over the past.
CREATE UNIQUE INDEX fgraph_eavt ON fgraph_facts (e, a, v, t) WHERE rx IS NULL;
CREATE INDEX fgraph_avet ON fgraph_facts (a, v, e) WHERE rx IS NULL;
CREATE INDEX fgraph_vaet ON fgraph_facts (v, a, e) WHERE rx IS NULL AND t = 0;
-- History access: per-(entity, attribute) timeline, plus per-transaction receipts.
CREATE INDEX fgraph_hist ON fgraph_facts (e, a, tx);
CREATE INDEX fgraph_txin ON fgraph_facts (tx);
CREATE INDEX fgraph_txout ON fgraph_facts (rx) WHERE rx IS NOT NULL;

-- Read-only convenience views (part of the format; the SQL escape hatch).
CREATE VIEW fgraph_view AS
SELECT f.id, f.e, i.name AS attribute,
       CASE WHEN f.t IN (7, 8, 9)
            THEN (SELECT b.data FROM fgraph_blobs b WHERE b.hash = f.v)
            ELSE f.v END AS value,
       f.t AS tag, f.tx, f.rx
FROM fgraph_facts f JOIN fgraph_ids i ON i.id = f.a;

CREATE VIEW fgraph_now AS SELECT * FROM fgraph_view WHERE rx IS NULL;
```

Notes: `fgraph_facts` is a rowid table on purpose (fast inserts; sqlite.org's WITHOUT ROWID ~200-byte guidance would be violated by inline text values). AEVT is deliberately absent in v1; add only if a benchmark demands it. `AUTOINCREMENT` makes SQLite maintain its internal `sqlite_sequence` table — expected, not part of the fgraph format. In the SQL escape hatch, `json`-tagged values in `fgraph_view`/`fgraph_now` compose with SQLite's built-in JSON functions (`json_extract(value, …)`).

### 4.4 Value tags

| Tag | Name        | Storage class | Notes                                                                    |
| --- | ----------- | ------------- | ------------------------------------------------------------------------ |
| 0   | `ref`       | INTEGER       | Target entity id. Indexed in `fgraph_vaet`.                              |
| 1   | `bool`      | INTEGER       | 0 or 1.                                                                  |
| 2   | `int`       | INTEGER       | 64-bit signed.                                                           |
| 3   | `float`     | REAL          | IEEE 754 double. NaN and ±Inf are **rejected** at the boundary.          |
| 4   | `text`      | TEXT          | UTF-8, ≤ 256 bytes (larger becomes `text_ref`).                          |
| 5   | `instant`   | INTEGER       | Microseconds since Unix epoch, UTC. Never floats, never seconds.         |
| 6   | `bytes`     | BLOB          | ≤ 256 bytes (larger becomes `bytes_ref`).                                |
| 7   | `vector`    | BLOB          | **Indirect**: `v` = SHA-256 of the float32 little-endian array in blobs. |
| 8   | `text_ref`  | BLOB          | **Indirect**: `v` = SHA-256 of TEXT in `fgraph_blobs`.                   |
| 9   | `bytes_ref` | BLOB          | **Indirect**: `v` = SHA-256 of BLOB in `fgraph_blobs`.                   |
| 10  | `json`      | TEXT          | Canonical JSON (sorted keys, `,`/`:` separators, no spaces, no NaN/Inf). |

**Format constants** (MUST be identical across implementations — they determine physical bytes):

- `BLOB_THRESHOLD = 256` bytes: text/bytes values strictly larger are stored indirectly (content-addressed, deduplicated). Vectors are **always** indirect.
- `MAX_VALUE_BYTES = 1_048_576` (1 MiB): larger values are rejected with a typed error (fgraph is a fact store, not a blob store).
- Hash = SHA-256 (32 raw bytes).
- The API always presents logical values; indirection is invisible except in raw SQL.

### 4.5 System entities and bootstrap

Ids 1–63 are reserved for the system; `next_id` starts at 64. Init allocates the **genesis transaction = 64** (so the first user entity is 65) and writes:

`fgraph_ids`: (1, `fgraph/at`), (2, `fgraph/by`), (3, `fgraph/source`), (4, `fgraph/meta`), (5, `fgraph/many`), (6, `fgraph/unique`), (7, `fgraph/nohistory`), (8, `fgraph/type`), (9, `fgraph/dims`), (10, `fgraph/doc`), (11, `fgraph/excised`), (12, `fgraph/undoes`).

Genesis facts (all `tx = 64`, `rx = NULL`), in this exact insertion order (fact ids 1–25):

1. `(64, 1, <clock µs>, 5)` — the genesis timestamp.
1. Type declarations `(attr, 8, <type>, 4)` for attrs 1–12 in id order: `instant, text, text, json, bool, bool, bool, text, int, text, ref, ref`.
1. Docs `(attr, 10, <doc>, 4)` for attrs 1–12 in id order, exact strings:
   - `fgraph/at`: `Wall-clock time of the transaction (UTC microseconds).`
   - `fgraph/by`: `Author of the transaction (person or agent).`
   - `fgraph/source`: `Provenance of the transaction (document, conversation, tool).`
   - `fgraph/meta`: `Free-form JSON metadata on the transaction.`
   - `fgraph/many`: `Schema: attribute holds multiple values per entity.`
   - `fgraph/unique`: `Schema: live values of this attribute are unique; enables upsert.`
   - `fgraph/nohistory`: `Schema: superseded values are deleted instead of kept as history.`
   - `fgraph/type`: `Schema: enforced value type (bool,int,float,text,instant,bytes,vector,json,ref).`
   - `fgraph/dims`: `Schema: vector dimensions for vector attributes.`
   - `fgraph/doc`: `Schema: human/agent documentation for an attribute.`
   - `fgraph/excised`: `Audit marker: entity was physically excised at this transaction.`
   - `fgraph/undoes`: `Audit marker: this transaction undoes another transaction.`

## 5. Data model semantics

### 5.1 Names

- A bare string in an entity position (`"id"` key, `e` pattern position, `{"ref": "…"}`) denotes the entity with that name.
- In **write** contexts, unknown names auto-create (`fgraph_ids` row; no facts needed — a named entity with no facts exists and reads as empty). In **read** contexts, unknown names resolve to "no results" (queries) or a typed not-found error (`entity`, `history`).
- Names: 1–512 UTF-8 bytes, no control characters, `fgraph/` prefix reserved. Names are permanent in v1 (no rename).
- Anything named like an attribute (contains `/`) is addressable like any entity — schema introspection uses the normal API (`entity("person/name")` shows its flags and doc).

### 5.2 Schema (optional, per-attribute flags)

Attributes never need declaring; the first use auto-creates the named entity. Declare only special behavior, stored as ordinary facts on the attribute (so schema is queryable and time-traveled):

| Flag               | Meaning                                                                                                                                              |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fgraph/type`      | Enforced value type (one of the tag names; `ref` makes values entity references). Without it, any scalar type is accepted per fact.                  |
| `fgraph/many`      | Cardinality many. Default: cardinality one with auto-supersession.                                                                                   |
| `fgraph/unique`    | Live values must be unique across entities for this attribute; enables upsert (§6.4). Requires a declared `fgraph/type` other than `json`/`vector`.  |
| `fgraph/nohistory` | Superseded/retracted facts are physically deleted, not tombstoned. Implied default for `vector`-typed attributes (override by declaring it `false`). |
| `fgraph/dims`      | Vector dimensions. If absent, fixed by the first written vector; mismatches are errors.                                                              |
| `fgraph/doc`       | Documentation string.                                                                                                                                |

Declaration validation: enabling `unique` with existing duplicate live values, changing `many` → one with existing multi-values, or changing `type` over conflicting existing facts → typed error. Flags apply from their transaction onward; history is not rewritten.

### 5.3 Values in JSON (wire format)

Plain JSON scalars map directly: `true/false` → bool, integral number → int, other number → float, string → text. Typed wrappers (single-key objects) cover the rest:

```json
{"ref": "ada"}  {"ref": 65}  {"ref": ["person/email", "ada@x.io"]}
{"instant": "2026-08-24T10:00:00Z"}  {"instant": 1767225600000000}
{"bytes": "aGVsbG8="}          // standard base64, padded
{"vector": [0.1, -0.2, 0.3]}   // float32 array
{"json": {"any": ["nested", "value"]}}
{"tmp": "t1"}                  // transaction-local tempid (write contexts only)
```

Output rendering: instants as `{"instant": "<RFC 3339 UTC with microseconds>"}`, refs as `{"ref": <name-if-named-else-id>}`, bytes/vector/json as their wrappers, scalars bare. This **rendered fact** form — `{"id", "e", "a", "v", "tx", "rx"}` with names substituted — is reused by `history`, `diff`, `why`, and `export`.

### 5.4 Time

- One clock: the transactor stamps `fgraph/at` (int µs UTC) on every transaction. The clock is injectable for tests and deterministic runs (`FGRAPH_CLOCK=<µs>` env in CLIs: start value, +1_000_000 per subsequent transaction; library hook per language).
- A fact is visible at time `T` (a transaction id) iff `tx <= T AND (rx IS NULL OR rx > T)`.
- `at()` accepts a transaction id or a timestamp; a timestamp resolves to the greatest `tx` whose `fgraph/at ≤ timestamp` (via `fgraph_avet` on attribute 1).

## 6. Transactions (normative pipeline)

### 6.1 Input forms

**Map form** (one entity, the common case):

```json
{ "id": "ada", "person/name": "Ada Lovelace", "person/knows": { "ref": "grace" } }
```

- `"id"` (optional): name string | int id | lookup `["<unique-attr>", value]` | `{"tmp": "t1"}`. Omitted → new anonymous entity.
- Values may be a single value, a JSON array (each element asserted — **only** valid for `fgraph/many` attributes; arrays on cardinality-one are a typed error; a literal array value needs `{"json": [...]}`), or a nested map (creates/updates that entity and asserts a ref to it — the attribute must be `ref`-typed).

**Op form** (a JSON array of operations, mixable with maps in one transaction):

```json
["assert", "ada", "person/born", 1815]
["retract", "ada", "person/born", 1815]   // this exact fact
["retract", "ada", "person/born"]         // all live values of the attribute
["retract", "ada"]                        // whole entity: own facts + inbound refs
```

A transaction = one map, one op, or a JSON array of them. `transact` accepts optional provenance: `source` (→ `fgraph/source`), `by` (→ `fgraph/by`), `meta` (→ `fgraph/meta`), plus arbitrary extra facts on the tx entity via a `tx` map.

### 6.2 Pipeline

1. **Parse & validate**: attribute names against the regex; values against declared types/dims; NaN/Inf/oversize rejected. Unknown attributes auto-create.
1. **Resolve identities**: names (auto-vivify), lookups (must resolve unless the map itself asserts that unique attr — then upsert, §6.4), tempids unify within the transaction. **Allocation order is normative for determinism**: new entity ids (including auto-created attributes and names) are allocated in order of first appearance in the transaction data; the transaction id is allocated last (the highest id of its transaction). Retracting an unknown name is a no-op, not an error.
1. **Diff against current state**: asserting an already-live identical fact is a **no-op**; on a cardinality-one attribute with a different live value, emit a retraction of the old fact (supersession); `retract` of a non-live fact is a no-op. Two different values for the same cardinality-one `(e, a)` **within** one transaction → typed conflict error.
1. **Commit**: if nothing remains, allocate **no** transaction and return an empty report (`tx = null`). Otherwise allocate the next id as `tx` and write in this **normative order** (fact ids follow insertion order): tx metadata facts first, then assertions in input order (`tx` set, `rx NULL`); set `rx = tx` on superseded/retracted rows (or DELETE them for `nohistory` attributes, removing orphaned blobs), and maintain `fgraph_fts` (insert live text, delete retracted fact ids) — all in one `BEGIN IMMEDIATE` transaction, updating `next_id` once.

### 6.3 TxReport

```json
{"tx": 70, "at": 1767225605000000,
 "ids": {"t1": 69, "ada": 65},
 "asserted": [<rendered facts>], "retracted": [<rendered facts>]}
```

### 6.4 Upsert

If a transaction asserts a `unique` attribute value that already belongs to a live entity, the written entity **unifies** with that owner (Datomic-style upsert) instead of erroring — unless the map pinned a _different_ explicit id, which is a uniqueness conflict error. Two unique attrs resolving to different existing entities → conflict error.

### 6.5 Concurrency

Single writer per file, enforced by `BEGIN IMMEDIATE` + busy timeout. Implementations keep an in-memory `fgraph_ids` cache invalidated via `PRAGMA data_version` at each transaction start (multi-process safety). Connections are not safe for concurrent use by multiple threads/goroutines unless the implementation documents otherwise; the Go implementation MUST be safe via internal locking, the Python implementation documents one-connection-per-thread.

## 7. Time travel and history APIs

- `at(t)` → read-only view of the database at `t` (tx id | instant); all read APIs work on it (query compilation swaps the `rx IS NULL` predicate for the §5.4 visibility predicate). `search()` on a past view is a typed error in v1.
- `history(ref, attr?)` → rendered facts (live and retracted) for the entity (optionally one attribute), ascending `tx`, each augmented with `at`, `by`, `source` of its `tx` (and of `rx` when set).
- `diff(t1, t2)` → `{"asserted": [...], "retracted": [...]}`: facts with `tx` in `(t1, t2]` and facts with `rx` in `(t1, t2]`.
- `changes(since, until?)` → `diff` sugar for "what happened after `since`". `follow(since?)` → an iterator/callback stream of new transactions, polling `changes` gated on `PRAGMA data_version` (default interval 500 ms). Followers work **cross-process on a read-only connection**: an auditor or supervisor can tail an agent's memory file without the agent's cooperation — the file is the interface.
- `undo(tx)` → a **compensating transaction** applied through the normal pipeline: every fact asserted by `tx` (except `tx`'s own metadata facts, `e = tx`) is retracted if still live; every fact `tx` retracted (`rx = tx`) is re-asserted. The undo transaction records `(undo_tx, fgraph/undoes, tx, ref-tag)`. Later changes to the same attributes are superseded by the undo — visibly and audited. Undoing an undo works naturally; an empty compensation is a no-op (`tx = null`). This is "git revert" for memory: history never rewrites, it accretes.
- `why(ref, attr?)` → current rendered facts, each with the full provenance of its transaction (all facts on the tx entity). This is the audit answer: "why do you believe this?"
- `speculate()` → a scope that opens `SAVEPOINT`, allows transacting and querying, and always rolls back on exit (Python context manager; Go callback `db.Speculate(ctx, func(tx *DB) error)`).
- `excise(ref)` (destructive, explicit): physically DELETE all facts where `e = ref` or (`t = 0 AND v = ref`), delete newly-orphaned blobs and FTS rows, then record `(tx, fgraph/excised, <id>, ref-tag)` in a new transaction. Excising a system entity or a transaction entity is `Unsupported`. The GDPR escape hatch; never exposed over MCP.

## 8. Query language (normative)

One canonical JSON form everywhere (library, CLI, MCP). Language APIs may add idiomatic sugar; the JSON is what conformance tests.

```json
{
  "find": ["?name", ["count", "?friend"]],
  "where": [
    ["?e", "person/name", "?name"],
    ["?e", "person/knows", "?friend"],
    ["?e", "person/age", "?age"],
    { "not": [["?e", "person/status", "archived"]] },
    { "or": [[["?e", "person/city", "Lyon"]], [["?e", "person/city", "Metz"]]] },
    [">=", "?age", "?min"]
  ],
  "in": ["?min"],
  "order": [["?name", "asc"]],
  "limit": 20,
  "offset": 0
}
```

### 8.1 Clauses

- **Pattern** `[e, a, v]`: `e` is a variable (`?x`), name string, int id, or `"_"`; `a` is a **constant** attribute name (variables over attributes are out of scope v1); `v` is a variable, constant (bare scalar or wrapper), name string is NOT special in v position (use `{"ref": ...}`), or `"_"`.
- **Predicate** `[op, x, y]` with `op ∈ {"=", "!=", "<", "<=", ">", ">=", "contains", "starts-with"}` (the last two on text). Arguments: variables or constants. Distinguished from patterns structurally: attribute names contain `/`, operators never do.
- **Negation** `{"not": [<clauses>]}` → SQL `NOT EXISTS`; inner clauses see outer bindings.
- **Disjunction** `{"or": [<clause-list>, ...]}` → all branches MUST bind the same variables; compiles to `UNION`.
- **Rule invocation** `{"rule": ["<name>", args...]}` with definitions under `"rules"`: `{"head": ["<name>", "?x", ...], "body": [<clauses>]}`. Multiple bodies per head = OR. Self-recursion allowed (compiles to `WITH RECURSIVE`, `UNION` dedup); mutual recursion → typed error v1.
- **Inputs** `"in"`: variables bound from the `args` parameter (`{"?min": 30}`).

### 8.2 Find and results

- `find` items: variables, aggregates `["count"|"count-distinct"|"sum"|"min"|"max"|"avg", "?v"]`, or pull `["pull", "?e", <pull-pattern>]`.
- Set semantics: rows are distinct. Any aggregate present → `GROUP BY` all non-aggregate find items.
- Canonical result: `{"columns": ["?name", "count(?friend)"], "rows": [[...], ...]}` with values rendered per §5.3. Deterministic order only when `order` is given; conformance expectations compare as sorted sets otherwise.

### 8.3 Pull

Pull pattern = array of: attribute name | `"*"` (all attributes, depth 1) | `{"<ref-attr>": <sub-pattern>}` | reverse ref `"person/_knows"` (entities pointing here). Cardinality-many renders as arrays. `entity(ref, depth=n)` ≡ pull `["*"]` recursively to depth `n` (deeper refs render as `{"ref": ...}`). Cycles stop at the depth limit.

### 8.4 Compilation guidance (non-normative but strongly recommended)

Each pattern → an alias over `fgraph_facts` filtered `a = <id>` + visibility predicate; shared variables → equi-joins; order clauses by selectivity (bound `e` first, then unique `(a, v)`, then bound `(a, v)`, then bound `a`; predicates as early as their variables allow) and emit `CROSS JOIN` in that order to pin the plan (sqlite.org optoverview §7.1.2). Bind every constant as a SQL parameter. Run `ANALYZE` after bulk imports. Worked example:

```sql
-- [?e "person/name" ?n] [?e "person/knows" ?f] [?f "person/name" "Grace"]
SELECT d0.e, d0.v FROM fgraph_facts d0
CROSS JOIN fgraph_facts d2 CROSS JOIN fgraph_facts d1
WHERE d2.a = :a_name AND d2.v = 'Grace' AND d2.rx IS NULL      -- most selective first
  AND d1.a = :a_knows AND d1.v = d2.e  AND d1.rx IS NULL
  AND d0.a = :a_name AND d0.e = d1.e   AND d0.rx IS NULL
```

## 9. Search (normative algorithm)

`search(text?, vector?, k=10, expand=0, filters=[], attribute?)` over **current** facts:

1. **Keyword**: if `text` given, top-50 fact ids by FTS5 `bm25(fgraph_fts)` (query passed as an FTS5 string; implementations escape user input as a quoted phrase-set by default).
1. **Semantic**: if `vector` given, top-50 facts of vector-typed attributes (all, or just `attribute`) by cosine similarity, brute-force. Python may require the `fgraph[vector]` extra (NumPy); Go implements natively. Missing capability → typed error, never silent degradation.
1. **Fuse**: map each candidate fact to its entity; an entity's rank per list = its best fact. Reciprocal Rank Fusion with `K = 60`: `score(e) = Σ_lists 1/(K + rank_e)`.
1. **Filter**: keep entities matching every `[attr, value]` filter against live facts.
1. **Expand**: BFS up to `expand` hops over ref facts (both directions) from the top-`k` entities; expanded entities carry `"via"` (the path) and no score.
1. **Result**: `{"hits": [{"entity": <name|id>, "score": <float>, "matched": [<rendered facts>], "pull": <entity depth 1>}], "expanded": [...]}` — at most `k` hits. Keyword-matched facts additionally carry a `"snippet"` produced by FTS5 `snippet(fgraph_fts, 0, '[', ']', '…', 12)` — normative parameters so results render identically across implementations.

Embeddings are always caller-provided (library) or produced by an optional external command (`--embed-cmd` on CLI/MCP: receives the text on stdin, returns a JSON float array on stdout). Core never performs network I/O.

## 10. Public APIs

Idiomatic per language (follow the stack skills); semantics identical. Signatures (Python shown; Go mirrors with `ctx context.Context` first, options structs, `error` returns):

```python
fgraph.connect(path, *, clock=None) -> Db            # ":memory:" supported; Go: fgraph.Open
db.transact(data, *, source=None, by=None, meta=None, tx=None) -> TxReport   # db.add = alias
db.retract(ref, attr=None, value=None) -> TxReport
db.declare(attr, *, type=None, ref=False, many=False, unique=False,
           nohistory=None, dims=None, doc=None) -> TxReport   # ref=True ≡ type="ref"
db.entity(ref, depth=1) -> dict | None
db.q(query=None, args=None, **kw) -> Result           # kw sugar: find=, where=, ...
db.search(text=None, vector=None, k=10, expand=0, filters=(), attribute=None) -> SearchResult
db.at(t) -> Db (read-only)      db.history(ref, attr=None)   db.diff(t1, t2)
db.changes(since, until=None)   db.follow(since=None) -> iterator
db.why(ref, attr=None)          db.speculate() (context manager)
db.undo(tx) -> TxReport         db.excise(ref)               db.backup(path)
db.export(fp)                   db.import_(fp)
db.doctor() -> report           db.stats() -> dict            db.close()
```

**Errors**: one typed taxonomy in both languages: `NotFound`, `Conflict` (cardinality/uniqueness), `SchemaError`, `TypeError`/`ErrType`, `QueryError`, `FormatError` (bad file), `ReadOnly`, `TooLarge`, `Unsupported`. Every error message names the offending entity/attribute/value **and suggests the likely fix** (e.g., "`person/knows` holds one value per entity; declare it `many` to hold several") — for a schema-optional store, errors are the schema documentation.

**Export/import**: NDJSON, one transaction per line: `{"tx", "at", "by"?, "source"?, "meta"?, "asserted": [[e, a, v, tag]...], "retracted": [[e, a, v, tag]...]}` with names rendered. Import replays through the transactor preserving original timestamps and history (the migration/backup path; also the cross-implementation test surface). Implementations MAY batch many fgraph transactions into one SQLite transaction on import, and MAY use `ATTACH` internally — the replay semantics stay the contract. Because exports render names, importing another file's export **is a merge**: names unify, ids reallocate, cardinality-one conflicts resolve by replay order with both histories kept. Exports are deterministic and stable-ordered, so agent memory can live in git and memory changes can be reviewed in pull requests.

## 11. CLI

Same commands and flags in both implementations (Python: Typer, distribution `fgraph`; Go: `urfave/cli/v3`, binary `fgraph` from `go/cmd/fgraph`). Global flags: `--db <path>` (default `fgraph.db`), `--json` (machine output; default is human-readable). Env: `FGRAPH_DB`, `FGRAPH_CLOCK` (§5.4).

`init` (explicit init + file info) · `info` (stats: counts, size, format version) · `add` (args or stdin JSON/NDJSON; `-` reads stdin) · `retract` · `get <ref>` (entity pull) · `q` (inline JSON or `@file`) · `search` · `history <ref> [attr]` · `why <ref> [attr]` · `diff <t1> <t2>` · `declare` · `export` / `import` (NDJSON stdout/stdin) · `undo <tx>` · `tail` (stream transactions as export-format NDJSON; `--since <tx>`, `--follow` keeps polling — the audit stream to pipe into collectors, `jq`, or a supervisor on the host) · `backup <dest>` (safe hot backup via `VACUUM INTO`, valid while the file is in use) · `doctor` (integrity: `PRAGMA integrity_check`, FTS rebuild, orphan blob GC, `ANALYZE`, invariant checks) · `mcp` (serve MCP on stdio; `--read-only`, `--embed-cmd`) · `version`.

Exit codes: 0 ok, 1 error (typed error name on stderr), 2 usage.

## 12. MCP server

Stdio transport via the official MCP SDK of each language (verify the latest SDK at implementation time). Tools (names, params, and behavior are normative):

| Tool       | Params                              | Behavior                                                                                                                                                                                                  |
| ---------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `remember` | `facts?` (§6.1), `text?`, `source?` | At least one of `facts`/`text`. `text` stores a note entity `{"memory/text": <text>}` (FTS-indexed) — the zero-schema path; structured `facts` are the graduation path. Provenance `by = "mcp:<client>"`. |
| `recall`   | `query` (text), `k?`, `expand?`     | §9 search (semantic only when `--embed-cmd` is configured).                                                                                                                                               |
| `about`    | `entity`, `depth?`                  | Entity pull.                                                                                                                                                                                              |
| `why`      | `entity`, `attribute?`              | Provenance chain (§7).                                                                                                                                                                                    |
| `history`  | `entity`, `attribute?`              | Fact timeline.                                                                                                                                                                                            |
| `forget`   | `entity`, `attribute?`, `value?`    | Retraction (history preserved). Excision is never exposed over MCP.                                                                                                                                       |
| `undo`     | `tx`                                | Compensating transaction (§7) — the audited "revert that memory write".                                                                                                                                   |
| `query`    | `q` (§8 JSON), `args?`              | Read-only Datalog.                                                                                                                                                                                        |

`--read-only` disables `remember`/`forget`/`undo` and opens the file with SQLite `mode=ro`. When `--embed-cmd` is configured, `remember` additionally stores a `memory/embedding` vector for `text` notes and `recall` embeds the query — semantic memory end-to-end, while core stays network-free. Tool descriptions MUST include one worked example each (LLMs follow examples, not prose).

## 13. Conformance suite (`conformance/`)

### 13.1 Case format

One JSON file per case under `conformance/cases/<area>/<name>.json`:

```json
{
  "name": "transact/supersede-card-one",
  "comment": "why this case exists",
  "steps": [
    { "tx": { "id": "ada", "person/name": "Ada" } },
    { "tx": { "id": "ada", "person/name": "Ada L." }, "expect": { "tx": 68 } },
    {
      "q": { "find": ["?n"], "where": [["ada", "person/name", "?n"]] },
      "expect": { "columns": ["?n"], "rows": [["Ada L."]] }
    },
    { "entity": "ada", "expect": { "person/name": "Ada L." } },
    { "history": ["ada", "person/name"], "expect": "..." },
    { "tx": [["assert", "ada", "person/age", 40], ["assert", "ada", "person/age", 41]], "error": "Conflict" },
    { "facts": true, "expect": [[26, 67, 1, 1767225601000000, 5, 67, null], [27, 65, 66, "Ada", 4, 67, 68], "..."] }
  ]
}
```

Step kinds: `tx`, `declare`, `q`, `entity`, `history`, `diff`, `why`, `search`, `at` (wraps inner steps), `facts` (raw `fgraph_facts` rows `[id, e, a, v, t, tx, rx]` beyond the 25 genesis rows — the **format** assertion), `error` (expected typed error name on any step). Unordered expectations compare as sorted sets; `"..."` in seed files marks elisions to fill in during M1.

### 13.2 Determinism

Runners inject the conformance clock: start `1767225600000000` (2026-01-01T00:00:00Z), +1_000_000 µs per transaction. With it, ids, timestamps, and physical rows are byte-identical across implementations and runs.

### 13.3 Runners and cross-check

Each implementation ships a test that executes every case (pytest parametrized; Go table test). `scripts/crosscheck.sh` (already in the repo) replays `conformance/crosscheck.ndjson` through both CLIs and diffs physical rows and cross-read exports; it is wired as `mise run test:cross`. The seed cases in `conformance/cases/` pin the trickiest semantics; the implementer MUST extend the suite so every MUST in this spec has at least one case (target: every §4–§9 behavior, every typed error).

## 14. Testing and quality

- **Coverage gate ≥ 95%** in both implementations (already wired in `mise.toml`); do not lower it — write the missing test or delete the dead code.
- **Temporal property tests**: a randomized sequence of assert/retract/declare ops (fixed seeds) replayed against a naive in-memory reference model (dict of fact tuples); after every op, compare current state, and at the end compare `at(t)` for every past `t`, `history`, and `diff` against the model. Python: Hypothesis; Go: seeded `math/rand` table runs (keep it stdlib).
- **No network in tests**; `:memory:` databases everywhere except explicit file-format tests.
- Zero-warning bar: `mise run all` must pass clean (format, check with lint/types/vuln/leaks, test, build).

## 15. Milestones

Each milestone lands in **both** languages with its conformance cases green before the next begins.

- **M0 — Scaffold**: create `python/` (uv, library+CLI profile per python-stack; distribution `fgraph`, src layout) and `go/` (module `github.com/fmind/fgraph/go`, CLI per go-stack; `.golangci.yml`, tool directives) following the stack skills; `mise run install` and `mise run check` pass (empty-but-green suites); hooks installed; CI via the github-actions skill running `mise run all` (add `.github/workflows/` per that skill).
- **M1 — Format & store**: init/open (§4), pragmas, bootstrap, `fgraph_ids` cache with `data_version` invalidation, clock injection; `conformance/cases/format/` green (fill the `"..."` elisions in seeds while doing so).
- **M2 — Transact**: §6 complete (map/op forms, names, tempids, upsert, supersession, no-op elision, conflicts, provenance, nohistory, blobs, FTS maintenance).
- **M3 — Read**: `entity`/pull, `q()` with patterns, predicates, not/or, aggregates, in/args, order/limit; rendered forms.
- **M4 — Temporal**: `at`, `history`, `diff`, `changes`/`follow`, `why`, `speculate`, `undo`; property tests (§14); `excise` + `doctor` + `backup`.
- **M5 — Search**: FTS5 + vector + RRF + expand + filters; `fgraph[vector]` extra (NumPy) in Python; export/import.
- **M6 — Rules & CLI**: recursive rules; full CLI (§11); `test:cross` green; examples in `examples/` run as written (fix the implementation, not the examples — they are acceptance tests; wire them into CI).
- **M7 — MCP & docs**: MCP servers (§12); complete the `docs/` site (getting-started, concepts, guides, integrations pages exist — verify content against the implemented behavior, fill gaps, `mise run check:docs test:docs` green), adding three guide pages: **Modeling time & uncertainty** (transaction time answers "when did the system learn this"; real-world validity is domain data modeled with interval entities; contested facts become claim entities with confidence; enums are named entities referenced by `ref` attributes), **Sharing & auditing memory** (diffable NDJSON exports reviewed in git PRs; merge by replaying another file's export; `fgraph tail` audit streaming into host collectors; `backup`/Litestream replication for agent VMs), and **RAG with fgraph** (already seeded in `docs/` — verify against implemented behavior: chunks as entities, hybrid search, BYO embeddings, the ~10⁵-vector brute-force envelope and the ANN upgrade path); README/AGENTS sync via the readme-agents skill; Pages deploy live.

**Definition of done** (whole project): `mise run all` warning-free; every conformance case green in both implementations; `scripts/crosscheck.sh` green; coverage ≥ 95% both; examples runnable; docs deployed; no TODOs left in code.

## 16. Out of scope / future (do not build now)

Postgres sibling backend (`fgraph-pg`) · ANN acceleration (sqlite-vec/usearch feature-detect — promote to a milestone if the Chroma-tier RAG use case needs >10⁵ vectors) · framework adapters (LangChain/LlamaIndex vector store, Graphiti driver) · attribute-position variables and mutual recursion in queries · as-of search · name renames · compact text query syntax · CRDT/multi-writer sync · TypeScript implementation (invite via conformance suite).
