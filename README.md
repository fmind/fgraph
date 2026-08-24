# fgraph

**A fact graph in a single SQLite file — every fact remembers when and why it became true, and when it stopped.**

fgraph is an embedded temporal fact store. Knowledge is stored as immutable facts `⟨entity, attribute, value⟩` with full history: nothing is ever overwritten, changes supersede with both moments kept, and the past is one query away. No server, no schema migrations, no configuration — one file you can `cp`, inspect with any SQLite tool, and trust as an audit trail.

> **Status**: the [specification](SPEC.md) is complete; the v0.1 implementation is in progress. The examples below describe the specified target API.

## Why

- **No upfront schema** — assert anything; declare an attribute only when it needs special behavior (reference, cardinality-many, unique, no-history). Perfect fit for LLM extraction output.
- **Time travel built in** — `at()`, `history()`, `diff()`: the database as it was, the timeline of any fact, and what changed between two moments.
- **Provenance as a feature** — every transaction records who/what/why (`source`, `by`, free metadata); `why()` answers "why do you believe this?" for any fact.
- **Auditable agent memory** — most memory tools silently overwrite or endlessly append conflicting facts; fgraph supersedes with history, so agents get memory you can inspect, explain, `undo`, and roll back through.
- **Built for the agent-VM era** — when every agent runs in its own VM or sandbox, memory must be a file, not a service: zero provisioning, snapshot/fork/restore are file operations, `fgraph tail --follow` gives the host an audit stream of the agent's memory without the agent's cooperation, and Litestream or `fgraph backup` replicate it off-VM.
- **Hybrid search & lightweight RAG** — FTS5 keyword search + bring-your-own-embeddings vector search, fused with reciprocal rank fusion, plus graph expansion around hits. For local RAG, chunks become entities with text, embeddings, metadata, _and relations_ in one file — a Chroma-class store that also remembers why (comfortable to ~100k vectors, zero extra dependencies).
- **One pure file** — plain SQLite tables, no extension needed to read your data, `:memory:` for tests, and read-only SQL views (`fgraph_now`, `fgraph_view`) as an escape hatch for pandas, Datasette, or any BI tool.
- **Python and Go, no compromise** — twin native implementations bound by a shared conformance suite; the SQLite file is the interchange format.

## Quickstart (Python)

```bash
pip install fgraph   # or: uv add fgraph
```

```python
import fgraph

db = fgraph.connect("memory.db")   # creates or opens; ":memory:" works too

db.transact({"id": "ada", "person/name": "Ada Lovelace", "person/city": "London"},
            source="wikipedia", by="importer")

db.declare("person/knows", ref=True, many=True)     # only special behavior needs declaring
db.transact({"id": "ada", "person/knows": {"ref": "grace"}})
db.transact({"id": "ada", "person/city": "Lyon"})   # supersedes London; both moments kept

db.entity("ada")                          # {"person/name": "Ada Lovelace", "person/city": "Lyon", ...}
db.q(find=["?friend"],
     where=[["ada", "person/knows", "?f"], ["?f", "person/name", "?friend"]])

db.history("ada", "person/city")          # London (tx…→tx…), Lyon (tx…→)
db.at(tx).entity("ada")                   # the world as it was
db.why("ada", "person/city")              # the fact plus its full provenance
db.search("mathematician in Lyon", k=8, expand=1)   # keyword + graph expansion
```

## Quickstart (Go)

```bash
go get github.com/fmind/fgraph/go
```

```go
db, err := fgraph.Open("memory.db") // error handling elided for brevity
defer db.Close()

_, err = db.Transact(ctx, fgraph.E{"id": "ada", "person/name": "Ada Lovelace"},
    fgraph.WithSource("wikipedia"))
entity, err := db.Entity(ctx, "ada")
fmt.Println(entity["person/name"])
```

## CLI and MCP

Both implementations ship the same `fgraph` CLI (`uv tool install fgraph` or `go install github.com/fmind/fgraph/go/cmd/fgraph@latest`):

```bash
fgraph add --db notes.db '{"id": "fgraph", "project/status": "v0.1"}'
fgraph why --db notes.db fgraph project/status
fgraph mcp --db notes.db            # MCP server on stdio (add --read-only to expose without writes)
```

Give any coding agent (Claude Code, Codex, OpenCode, Antigravity, Copilot, …) a durable, auditable memory or a project knowledge base:

```bash
claude mcp add fgraph --scope project -- fgraph mcp --db ./project.db
```

MCP tools: `remember`, `recall`, `about`, `why`, `history`, `forget`, `undo`, `query`. See the docs for per-harness setup.

## When not to use fgraph

Billion-edge analytics (use DuckDB or a server graph database), high-concurrency multi-writer services (fgraph is single-writer, many readers), or blob storage (values are capped at 1 MiB). fgraph is comfortable in the millions-of-facts range — honest numbers ship with the benchmarks.

## Project layout

- [`SPEC.md`](SPEC.md) — the normative specification (format, semantics, query language, conformance).
- [`python/`](python/) and [`go/`](go/) — the twin implementations.
- [`conformance/`](conformance/) — the shared test suite both implementations must pass.
- [`examples/`](examples/) — runnable Python and Go examples (they double as acceptance tests).
- [`docs/`](docs/) — documentation site (Hugo + Hextra), deployed to GitHub Pages.

## License

[MIT](LICENSE) — © 2026 Médéric Hurier (Fmind).
