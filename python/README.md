# fgraph for Python

An embedded temporal fact store in one SQLite file. fgraph combines schema-light EAVT facts, immutable history, provenance, bounded Datalog, hybrid search, portable event streams, and an MCP server without operating a database service.

The core is standard-library-only and requires Python 3.12 or newer. The implementation starts at 1.0; PyPI 0.0.1 only reserved the name and does not contain fgraph. Require the stable line so a resolver can never select that placeholder:

```bash
uv add 'fgraph>=1,<2'
```

When contributing, install this checkout with `uv sync --project python` and run `uv run --project python fgraph version`.

## Facts first, schema when it matters

```python
import fgraph

with fgraph.connect("memory.db") as db:
    created = db.transact(
        {"id": "ada", "person/name": "Ada Lovelace", "person/city": "London"},
        source="wikipedia",
        by="importer",
        operation_id="person:ada:1",
        if_basis_tx=64,
    )
    assert created.status == "applied"
    assert db.entity("ada")["person/city"] == "London"

    moved = db.transact({"id": "ada", "person/city": "Lyon"})
    assert db.at(created.tx).entity("ada")["person/city"] == "London"
    assert db.entity("ada")["person/city"] == "Lyon"
    assert db.history("ada", "person/city")[-1]["tx"] == moved.tx
```

Names are stable identities. Anonymous entities and every transaction receive stable UUID identities in the temporal registry. `operation_id` makes a mutation retry-safe; an exact retry returns the original receipt, while reuse for another payload is rejected. `if_basis_tx` is an optimistic concurrency guard. Atomic compare-and-swap on an existing cardinality-one attribute uses `['cas', entity, attribute, expected, desired]`; exact `{"missing": True}` values support create and delete.

Declare only behavior the database must enforce:

```python
db.declare("person/email", type="text", unique=True)
db.declare("person/knows", ref=True, many=True)
db.declare(
    "note/embedding",
    type="vector",
    dims=3,
    vector_model="example-embedding-v1",
)
db.declare_shape(
    "shape/person",
    required=["person/email"],
    allowed=["person/knows"],
    closed=True,
)
```

`db.schema()` returns a basis-pinned, digestible schema snapshot containing declared, effective, and observed behavior plus shape definitions. `db.validate(entity)` reports all shape violations at the pinned basis. Closed shapes reject unexpected application attributes during writes; `db.doctor()` checks schema and shape invariants across the complete file.

## Bounded reads for applications and agents

Queries accept three-, four-, or five-position datom patterns: `[e,a,v]`, `[e,a,v,tx]`, and `[e,a,v,tx,added]`. Set `source: "history"` to include assertion and retraction events.

```python
result = db.q(
    {
        "find": ["?friend"],
        "where": [
            ["ada", "person/knows", "?person"],
            ["?person", "person/name", "?friend"],
        ],
    }
)
plan = db.explain({"find": ["?e"], "where": [["?e", "person/name", "_"]]})
page = db.datoms("avet", ["person/name"], limit=100)
```

Queries, datom pages, search, MCP tools, and MCP resources are work- or output-bounded. Datom cursors pin the basis and arguments, so later writes do not change an in-progress traversal.

Keyword, vector, and hybrid search rank entities rather than duplicate facts. Vector search always names its schema-checked vector attribute; exact filters are evaluated before candidate cutoff.

```python
hits = db.search(
    "analytical engine",
    vector=[0.1, 0.2, 0.3],
    vector_attribute="note/embedding",
    text_attributes=["note/text"],
    filters=[["note/kind", "reference"]],
    k=5,
    expand=1,
)
```

Embeddings are caller-provided. The store never makes network requests.

## Receipts, replication, and recovery

Every committed mutation has an event UUID, SHA-256 event hash, local transaction id, original basis, timestamp, optional operation receipt, provenance, and custom transaction facts:

```python
import json

receipt = db.receipt(created.tx)
events = db.event_records(since=64)  # portable event/1 records; no local tx ids
event_stream = "\n".join(json.dumps(event, sort_keys=True, separators=(",", ":")) for event in events)
summary = replica.apply_summary(event_stream)  # compact counters for large streams
# reports = replica.apply(event_stream)  # use instead when every receipt is needed
```

Canonical `event/1` JSON is retained with every ordinary receipt, including transactions that replace `nohistory` values, so tailing and restore remain replayable. A single event is capped at 8,454,144 bytes. Audited physical excision is the exception: it writes a redaction event and removes the affected prior payloads; redacted records are deliberately rejected by `apply()`.

Use each replication and recovery surface deliberately:

- `db.event_records()` / `db.apply()` exchange portable `event/1` records between databases.
- `db.apply_summary()` consumes large event iterables without retaining one report per event.
- `db.snapshot()` / `db.restore(text)` is the checksummed `snapshot/1` logical format for exact portable restore into a pristine database.
- `db.schema_manifest()` / `db.check_schema_manifest()` / `db.apply_schema_manifest()` exchange explicit declarations and shapes as portable `schema/1`.
- `db.backup(path)` creates and verifies a physical SQLite hot backup without overwriting an existing path. `fgraph.restore_backup(backup, destination)` safely installs that physical backup.

The public API is version 1. The dedicated on-disk SQLite format is version 2; `event/1` and `snapshot/1` identify their respective portable JSON protocols.

`iter_snapshot()` and `snapshot(writer)` stream snapshot output; the convenience `snapshot()` string result materializes it. `restore()` consumes an iterable or text stream incrementally inside one atomic restore. Use `backup()` for very large local files when logical portability is unnecessary, and size portable input/output explicitly at the application boundary. The per-event limit above applies inside a snapshot; there is no implicit whole-snapshot truncation.

`doctor()` is read-only by default. `doctor(repair=True)` only rebuilds derived FTS rows and removes unreferenced blobs; it refuses non-rebuildable logical or physical corruption.

## CLI and MCP

All CLI inputs and outputs are JSON-first:

```bash
fgraph init --db memory.db --json
fgraph add '{"id":"ada","person/name":"Ada"}' --db memory.db --json
fgraph declare note/embedding --type vector --dims 3 --vector-model example-embedding-v1 --db memory.db --json
fgraph shape shape/person --required person/name --allowed person/name --closed --db memory.db --json
fgraph validate ada --db memory.db --json
fgraph q '{"find":["?e"],"where":[["?e","person/name","Ada"]]}' --db memory.db --json
fgraph tx 67 --db memory.db --json
fgraph add --batch-size 500 --operation-id-prefix import-v1 - --db memory.db < entities.ndjson
fgraph schema-export --db memory.db > memory.schema.json
fgraph snapshot --db memory.db > memory.snapshot.ndjson
```

The MCP server is read-only by default and exposes bounded structured tools and resources for recall, entity reads, query, schema, datoms, explain, receipts, and changes:

```bash
fgraph mcp --db memory.db
```

Mutation tools require explicit opt-in and caller-supplied operation IDs. Destructive forget/undo operations also require a pinned basis. Physical excision is intentionally never exposed through MCP:

```bash
fgraph mcp --write --db memory.db
```

- [Documentation](https://fmind.github.io/fgraph/)
- [Source and examples](https://github.com/fmind/fgraph)
- [Issue tracker](https://github.com/fmind/fgraph/issues)
