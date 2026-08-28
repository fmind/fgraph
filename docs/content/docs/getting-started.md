---
title: Getting Started
weight: 1
---

## Install from the checkout

```bash
mise trust && mise install
mise run install
mise run build
```

You can also prepare one runtime:

```bash
uv sync --project python
go build -C go -o bin/fgraph ./cmd/fgraph
npm ci --prefix typescript && npm run build --prefix typescript
```

Stable installs are `uv add 'fgraph>=1,<2'`, `go get github.com/fmind/fgraph/go@v1.0.4`, and `npm add @fmind-dev/fgraph@^1.0.4`.

## Add facts without designing a schema

```python
import fgraph

db = fgraph.connect("memory.db")  # use ":memory:" in tests

initial = db.transact(
    {
        "id": "ada",
        "person/name": "Ada Lovelace",
        "person/city": "London",
    },
    source="wikipedia",
    by="importer",
    operation_id="person:ada:v1",
)

db.declare("person/knows", ref=True, many=True)
db.transact({"id": "grace", "person/name": "Grace Hopper"})
db.transact({"id": "ada", "person/knows": {"ref": "grace"}})
move = db.transact({"id": "ada", "person/city": "Lyon"})
```

The first assertion creates names and attributes. Declarations are only for behavior that must be enforced: refs, many values, uniqueness, types, vector models, docs, or shapes.

Vectors are the deliberate exception to completely heterogeneous undeclared values: the first vector assertion infers and records `type = vector` plus its dimensions, even after a partial declaration such as `doc` or `many`. This keeps embedding spaces dimension-safe across every runtime.

## Read, discover, and explain

```python
db.entity("ada")
db.attributes(prefix="person/")  # compact vocabulary discovery
db.schema(prefix="person/")      # rich declared/effective/observed snapshot
db.receipt(move.tx)               # durable event + operation evidence

result = db.q(
    find=["?friend"],
    where=[
        ["ada", "person/knows", "?entity"],
        ["?entity", "person/name", "?friend"],
    ],
)
assert result.rows == [["Grace Hopper"]]
```

`explain()` returns the actual bounded index plan without evaluating it. `datoms()` provides basis-pinned EAVT/AVET/VAET pagination for power users and generic tooling.

## Travel in time

```python
assert db.at(initial.tx).entity("ada")["person/city"] == "London"
db.history("ada", "person/city")
db.diff(initial.tx, move.tx)
db.why("ada", "person/city")
db.undo(move.tx, operation_id="undo-move", if_basis_tx=move.tx)
```

Transaction time says when fgraph learned something. Model real-world validity as domain facts; see [Modeling time and uncertainty](../modeling-time/).

## Use the CLI

From this checkout, substitute `uv run --project python fgraph`, `go/bin/fgraph`, or `node typescript/dist/cli.js` for `fgraph`.

```bash
fgraph --db notes.db add \
  --operation-id project-status-v1 \
  '{"id":"project","project/status":"v1 candidate"}'
fgraph --db notes.db schema project/ --json
fgraph --db notes.db why project project/status --json
fgraph --db notes.db explain \
  '{"find":["?status"],"where":[["project","project/status","?status"]]}'
fgraph --db notes.db datoms avet --components '["project/status"]'
```

For a larger NDJSON import, bound each transaction and make every batch retryable:

```bash
fgraph --db notes.db add \
  --batch-size 500 \
  --operation-id-prefix import-notes-v1 \
  - < notes.ndjson
```

Export the explicit schema contract for another database or generated tool, then check it without mutation:

```bash
fgraph --db notes.db schema-export > notes.schema.json
fgraph --db replica.db schema-check @notes.schema.json
fgraph --db replica.db schema-apply \
  --operation-id schema-notes-v1 \
  @notes.schema.json
```

## Replicate or recover

Use portable events for logical replication:

```bash
fgraph --db notes.db tail --since 64 > events.ndjson
fgraph --db replica.db apply events.ndjson
```

Use a snapshot for exact retained-state recovery:

```bash
fgraph --db notes.db snapshot > snapshot.ndjson
fgraph --db restored.db restore snapshot.ndjson
```

`apply` validates individually capped events and commits the complete stream atomically. Its CLI result is a compact status-count summary; use the detailed library API only when one receipt per source event is needed. The caller still owns the total stream/event budget. `restore` accepts a pristine destination and verifies the snapshot footer. See [Sharing and auditing memory](../sharing-memory/) for the proof boundary.

## Start MCP safely

```bash
fgraph --db notes.db mcp          # read-only tools
fgraph --db agent-memory.db mcp --write  # explicit remember/forget/undo
```

Read-only mode is the default. All successful tools carry the pinned/result basis actually used for the answer, and every response is bounded. See [Integrations](../integrations/).

## Back up an active file

```bash
fgraph --db notes.db backup notes-backup.db
fgraph --db notes-backup.db doctor --json
```

The online backup is verified and published without overwriting an existing destination. A plain `cp` is appropriate only while the database is offline.

```python
db.close()
```
