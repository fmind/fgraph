# fgraph

**fgraph means Facts Graph: a temporal fact graph in one SQLite file — schema-optional, attributable, portable, and built for local AI systems.**

fgraph stores knowledge as immutable EAV facts `⟨entity, attribute, value, asserted-tx, retracted-tx?⟩`. Current state is a view; history, provenance, and transaction receipts are first-class. It needs no server or loadable SQLite extension, yet provides bounded Datalog, keyword/vector search, exact snapshots, portable event replay, schema introspection, shapes, and a read-only-by-default MCP server.

## Why fgraph

- **Universal data model.** Add facts immediately; declare only the attributes that need types, refs, cardinality, uniqueness, vector-model identity, or validation shapes.
- **Time and provenance by construction.** `at`, `history`, `diff`, `why`, and durable receipts answer what changed, when, by whom, and from which source.
- **Agent-safe writes.** Operation ids make retries idempotent; basis checks reject stale decisions; cardinality-one CAS supports atomic create/replace/delete; `undo` writes an audited compensation.
- **Inspectable and portable schema.** Rich snapshots distinguish declared, effective, and observed behavior. A canonical `schema/1` manifest exports, checks, and atomically applies explicit declarations and shapes across runtimes.
- **Attributable retrieval.** FTS5 + caller-provided vectors + exact filters + graph expansion return matched facts and their asserting provenance. Search work and outputs are explicitly bounded.
- **Two honest portability paths.** `tail`/`apply` replicate logical `event/1` streams; `snapshot`/`restore` reproduce exact retained state, including history, receipts, and excision redactions.
- **One file, three native runtimes.** Python, Go, and TypeScript write the same logical rows and read each other's files. Shared conformance, differential traces, and an immutable format-v2 fixture enforce the contract.
- **Designed for local agents.** MCP is read-only unless `--write` is explicit, responses are capped, cursors are basis-pinned, and every tool publishes input/output schemas, safety annotations, and server instructions. No core code makes a network call.

## Python quickstart

From this checkout:

```bash
uv sync --project python
uv run --project python python examples/python/quickstart.py
```

The stable registry installation is `uv add 'fgraph>=1,<2'`.

```python
import fgraph

with fgraph.connect("memory.db") as db:
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

    assert db.entity("ada")["person/city"] == "Lyon"
    assert db.at(initial.tx).entity("ada")["person/city"] == "London"
    print(db.history("ada", "person/city"))
    print(db.why("ada", "person/city"))
    print(db.receipt(move.tx))

    result = db.q(
        find=["?friend"],
        where=[
            ["ada", "person/knows", "?entity"],
            ["?entity", "person/name", "?friend"],
        ],
    )
    assert result.rows == [["Grace Hopper"]]
```

`schema()` is the agent/tooling surface; it returns declared, effective, and observed behavior plus shapes and a digest. `attributes()` remains a compact human discovery API.

For a known cardinality-one attribute, `['cas', entity, attribute, expected, desired]` performs an atomic comparison. The exact `{ "missing": true }` sentinel creates an absent fact when used as `expected`, or deletes a present fact when used as `desired`.

## Go and TypeScript

The Go module is `github.com/fmind/fgraph/go` and is CGO-free. The strict ESM package is `@fmind-dev/fgraph` and targets the Node.js 24 LTS line (24.19+).

```bash
go get github.com/fmind/fgraph/go@v1.2.0
npm add @fmind-dev/fgraph@^1.2.0
```

```go
func storeAda(ctx context.Context) (result error) {
    db, err := fgraph.Open("memory.db")
    if err != nil {
        return fmt.Errorf("open fgraph: %w", err)
    }
    defer func() { result = errors.Join(result, db.Close()) }()

    report, err := db.Transact(ctx,
        fgraph.E{"id": "ada", "person/name": "Ada Lovelace"},
        fgraph.WithOperationID("person:ada:v1"),
    )
    if err != nil {
        return fmt.Errorf("store Ada: %w", err)
    }
    entity, err := db.Entity(ctx, "ada")
    if err != nil {
        return fmt.Errorf("read Ada: %w", err)
    }
    fmt.Println(report.Tx, entity)
    return nil
}
```

```typescript
import { connect } from "@fmind-dev/fgraph";

using db = connect("memory.db");
db.transact(
  { id: "ada", "person/name": "Ada Lovelace" },
  { operationId: "person:ada:v1" },
);
const rows = db.q({
  find: ["?name"],
  where: [["ada", "person/name", "?name"]],
}).rows;
```

Run the checked-in examples with `mise run test:examples`.

## CLI

All three implementations expose the same v1 commands. The examples below use an installed `fgraph`; from the checkout, substitute `uv run --project python fgraph`, `go/bin/fgraph`, or `node typescript/dist/cli.js` after building.

`--db` defaults to `facts.fgraph` in the current directory. `FGRAPH_DB` overrides that default, and an explicit `--db` takes precedence over the environment. An explicitly selected path must not be empty; unset `FGRAPH_DB` to use the default.

Upgrading from a release that used `fgraph.db` does not silently create a second database. An implicit command keeps using a legacy-only `fgraph.db`; fresh directories use `facts.fgraph`; and an initialized `facts.fgraph` takes precedence once both files exist. If both exist but the new default is empty or unrelated, fgraph fails before mutation. Pass either path explicitly to override this transition. Help, version, and invalid usage do not inspect either file.

```bash
fgraph --db project.db add \
  --operation-id project-status-v1 \
  '{"id":"project","project/status":"v1 candidate"}'

# Bounded, resumable NDJSON loading. Completed batches survive interruption.
fgraph --db project.db add \
  --batch-size 500 \
  --operation-id-prefix import-project-v1 \
  - < entities.ndjson

fgraph --db project.db schema project/
fgraph --db project.db schema-export > project.schema.json
fgraph --db project.db schema-check @project.schema.json
fgraph --db project.db explain \
  '{"find":["?status"],"where":[["project","project/status","?status"]]}'
fgraph --db project.db datoms avet --components '["project/status"]'
fgraph --db project.db tx 67

# Portable logical replication.
fgraph --db project.db tail --since 64 > events.ndjson
fgraph --db replica.db apply events.ndjson

# Exact retained-state recovery.
fgraph --db project.db snapshot > snapshot.ndjson
fgraph --db restored.db restore snapshot.ndjson

fgraph --db project.db backup project-backup.db
fgraph --db project-backup.db doctor --json
```

Irreversible excision requires an idempotency key and the basis you reviewed:

```bash
basis="$(fgraph --db project.db schema --json | jq -r .basis_tx)"
fgraph --db project.db excise private-subject \
  --operation-id privacy-request-42 \
  --if-basis-tx "$basis"
```

## MCP for AI agents

`fgraph mcp` opens the database read-only and exposes:

`recall`, `about`, `why`, `history`, `query`, `datoms`, `receipt`, `schema`, and `explain`.

Opting into `mcp --write` adds `remember`, `forget`, and `undo`. Writes require operation ids; destructive calls also require a current basis. Every successful tool response uses `{"ok":true,"basis_tx":...,"data":...}` and is capped at 256 KiB. MCP initialization tells agents to inspect schema, preserve read bases, paginate, and guard writes; tool discovery includes output schemas and read/destructive/idempotent/open-world annotations.

Read-only resources expose paginated schema, entity datoms, receipts, and portable changes. A change event too large for one bounded page is exposed as integrity-checked 128 KiB chunks, while its continuation still advances to later events.

```bash
# Read-only project knowledge base.
claude mcp add --scope project fgraph -- fgraph --db ./project.db mcp

# Explicitly writable agent memory.
claude mcp add --scope project fgraph-memory -- \
  fgraph --db ./agent-memory.db mcp --write
```

An optional `--embed-cmd` reads text on stdin and emits one JSON vector on stdout. It is shell-free and bounded; core never downloads models or calls a provider.

## Agent Skills

Install the small companion skills to teach a coding agent the fgraph model and its safe MCP workflow:

```bash
npx skills add fmind/fgraph --skill fgraph --skill fgraph-mcp
```

Use `fgraph` for data modeling, transactions, temporal reads, and queries. Use `fgraph-mcp` when configuring or operating an agent memory or project knowledge base over MCP.

## Benchmarks

The checked-in benchmark exercises each native CLI independently at 1k, 10k, and 100k named entities. The 100k workload writes three scalar application facts per entity plus 5,000 caller-provided 384-dimensional vectors: 305,000 application facts in 500-entity transactions. Read figures are medians of three fresh-process end-to-end CLI runs, so runtime and package startup are included. The harness invokes the installed Python entry point, compiled Go binary, and compiled Node.js CLI directly.

![Batched NDJSON import throughput across Python, Go, and TypeScript](benchmarks/ingest-throughput.svg)

![Grouped fresh-process CLI read latency by operation at 100,000 entities](benchmarks/read-latency.svg)

<!-- benchmark-results:start -->

At 100,000 entities:

| Runtime    | Ingest entities/s | Point get | Scalar filter | Connected join | Keyword search | Exact vector search |
| ---------- | ----------------: | --------: | ------------: | -------------: | -------------: | ------------------: |
| Python     |             4,280 |    176 ms |        186 ms |         224 ms |         600 ms |              573 ms |
| Go         |             5,324 |    102 ms |        109 ms |         160 ms |         516 ms |              481 ms |
| TypeScript |             4,908 |    148 ms |        216 ms |         233 ms |         547 ms |              430 ms |

| Runtime    | Snapshot | Restore | Event tail | Event apply |
| ---------- | -------: | ------: | ---------: | ----------: |
| Python     |   6.38 s | 25.11 s |     2.32 s |     34.15 s |
| Go         |   4.59 s | 17.34 s |     3.31 s |     22.01 s |
| TypeScript |  10.63 s | 15.40 s |     2.86 s |     16.79 s |

The common logical state occupied 92.02 MiB; its snapshot was 60.53 MiB and its event stream 22.52 MiB. Vector search is intentionally exact over the 5,000 vectors, not ANN. These measurements validate the tested 100k envelope, not millions of entities or a service-level objective; SQLite build, filesystem, runtime startup, and hardware affect the numbers.

This release run was generated on 2026-08-29 from clean source commit [`6ff1ef0`](https://github.com/fmind/fgraph/commit/6ff1ef04f2f86423bc955f7a16afcfca22cbba3e) and source digest `sha256:3af1d49318bccbd06b679a3ad01e212572be874625c6a57c59aeae60d2e17e18`. The raw metadata records the exact runtime, SQLite, platform, workload, and clean-tree provenance.

<!-- benchmark-results:end -->

Reproduce the complete run with `mise run benchmark`. The harness has no timing pass/fail threshold and writes the [raw NDJSON observations](benchmarks/latest.ndjson) plus both accessible SVG charts.

## Data deletion and trust boundary

`nohistory` removes superseded fact rows but does **not** erase their canonical event payloads. For privacy deletion, `excise` removes all facts where a target is entity, attribute, or ref value and redacts every retained event that mentions it. The identity registry name remains, so use opaque names and keep sensitive identifiers in facts.

Snapshots/backups and external collectors are separate copies with their own retention policy. Event hashes detect corruption but are not tamper evidence: a writer that controls the file can rewrite it. Put file permissions, encryption, snapshots, and audit collection outside an untrusted agent's control.

## When not to use fgraph

Use another system for high-concurrency distributed writers, tamper-evident logs, large blobs, approximate vector retrieval at scale, or billion-edge analytics. fgraph is one durable SQLite writer with many readers and exact linear vector search. Run `mise run benchmark` on your corpus and hardware.

## Repository

- [`docs/content/spec.md`](docs/content/spec.md) — normative fgraph v1 / SQLite format-v2 contract.
- [`python/`](python/), [`go/`](go/), [`typescript/`](typescript/) — native peers.
- [`conformance/`](conformance/) — shared behavior, differential scenario, and immutable format-v2 fixture.
- [`examples/`](examples/) — runnable acceptance examples.
- [`docs/`](docs/) — Hugo documentation site.
- [`skills/`](skills/) — installable Agent Skills for fgraph users.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — the cross-runtime contribution contract.
- [`SECURITY.md`](SECURITY.md) — private reporting and the supported trust boundary.
- [`CHANGELOG.md`](CHANGELOG.md) — candidate and released user-visible changes.
- [`RELEASING.md`](RELEASING.md) — exact proof, publication, and rollback procedure.

The source gate is `mise run all`. No commit, package publication, or hosted deployment is implied by a local green candidate.

## License

[MIT](LICENSE) — © 2026 Médéric Hurier (Fmind).
