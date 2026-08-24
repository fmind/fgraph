---
title: RAG with fgraph
weight: 3
---

fgraph is a lightweight local RAG store: chunks become entities carrying text (FTS-indexed), embeddings, metadata, and — unlike a pure vector store — _relations and provenance_. One file, zero services, bring-your-own embeddings.

## Coming from Chroma

| Chroma concept       | fgraph equivalent                                                    |
| -------------------- | -------------------------------------------------------------------- |
| Collection           | An attribute namespace (`chunk/…`) or a `ref` to a collection entity |
| Document + embedding | `chunk/text` (auto FTS-indexed) + `chunk/embedding` (vector)         |
| Metadata + `where`   | Ordinary facts + `filters=[["chunk/lang", "en"]]`                    |
| `query()`            | `search(text=…, vector=…)` — BM25 + cosine fused with RRF            |
| —                    | `expand=1`: pull graph neighbors of hits (chunk → document → author) |
| —                    | History and `why()` on every chunk: when it was ingested, from where |

```python
import fgraph

db = fgraph.connect("corpus.db")
db.declare("chunk/doc", ref=True)
db.declare("chunk/embedding", type="vector")   # nohistory by default

db.transact({
    "chunk/text": "SQLite is the most deployed database in the world.",
    "chunk/doc": {"id": "doc-sqlite", "doc/title": "SQLite notes"},
    "chunk/embedding": {"vector": embed("SQLite is the most deployed…")},
}, source="notes/sqlite.md")

hits = db.search(text="most deployed database",
                 vector=embed("widely used database"),
                 k=8, expand=1, filters=[])
```

Embeddings are always yours: call any model and pass the floats (`embed()` above is your function). On the CLI and MCP server, `--embed-cmd <command>` wires an external embedder (text on stdin, JSON float array on stdout) so `remember`/`recall` work semantically end-to-end — core never makes a network call.

## Honest envelope

Vector search is exact brute-force: fast and dependency-free up to roughly **100k vectors** (384–1536 dims), which covers agent memory and most local corpora. Beyond that — or if you need auto-embedding and million-vector ANN — a dedicated vector database like Chroma or LanceDB is the right tool today; an optional ANN accelerator is on fgraph's roadmap. What fgraph adds below that ceiling: keyword + vector fusion out of the box, metadata as first-class facts, graph expansion around hits, and an audit trail for every chunk.
