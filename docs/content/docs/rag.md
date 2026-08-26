---
title: RAG with fgraph
weight: 5
---

fgraph is a lightweight local RAG store: chunks become entities carrying text (FTS-indexed), embeddings, metadata, and — unlike a pure vector store — _relations and provenance_. Default search ranks only application attributes, and each match includes its asserting time and optional author/source. One file, zero services, bring-your-own embeddings.

## Coming from Chroma

| Chroma concept       | fgraph equivalent                                                    |
| -------------------- | -------------------------------------------------------------------- |
| Collection           | An attribute namespace (`chunk/…`) or a `ref` to a collection entity |
| Document + embedding | `chunk/text` (auto FTS-indexed) + `chunk/embedding` (vector)         |
| Metadata + `where`   | Ordinary facts + `filters=[["chunk/lang", "en"]]`                    |
| `query()`            | `search(text=…, vector=…, vector_attribute=…)` — BM25 + cosine/RRF   |
| —                    | `expand=1`: pull graph neighbors of hits (chunk → document → author) |
| —                    | History and `why()` on every chunk: when it was ingested, from where |

```python
import fgraph

db = fgraph.connect("corpus.db")
db.declare("chunk/doc", ref=True)
db.declare(
    "chunk/embedding",
    type="vector",
    dims=384,
    vector_model="provider/model@revision",
)  # vector values are nohistory by default

db.transact({
    "chunk/text": "SQLite is the most deployed database in the world.",
    "chunk/doc": {"id": "doc-sqlite", "doc/title": "SQLite notes"},
    "chunk/embedding": {"vector": embed("SQLite is the most deployed…")},
}, source="notes/sqlite.md")

hits = db.search(text="most deployed database",
                 vector=embed("widely used database"),
                 vector_attribute="chunk/embedding",
                 text_attributes=["chunk/text"],
                 k=8, expand=1, filters=[])
```

Embeddings are always yours: call any model and pass the floats (`embed()` above is your function). Vector search requires an explicit attribute so embeddings from different models never mix accidentally; declare `dims` and `vector_model` so agents can inspect that contract. On the CLI and MCP server, `--embed-cmd <command>` wires an external embedder (text on stdin, JSON float array on stdout) so `remember`/`recall` work semantically end-to-end — core never makes a network call.

## Honest envelope

Vector search is exact brute-force, so its work grows linearly with the number and width of stored vectors. One call is bounded to `k <= 100`, `expand <= 3`, at most 16 filters and 16 text attributes, at most 500 ranked candidates per retrieval list, 100 expanded entities, and a 1 MiB canonical result. Measure your corpus, hardware, and latency target before relying on a performance boundary. If you need automatic embedding or approximate nearest-neighbor retrieval at large scale, use a dedicated vector database. fgraph's deliberate trade-off is a dependency-free local path with keyword + vector fusion, metadata as first-class facts, graph expansion, and an audit trail for every chunk.
