---
title: Specification
weight: 5
---

The normative specification — file format, semantics, query language, APIs, CLI, MCP tools, and the conformance suite — lives in [`SPEC.md`](https://github.com/fmind/fgraph/blob/main/SPEC.md) at the repository root.

Highlights:

- **Format v1**: four plain STRICT tables (`fgraph_meta`, `fgraph_ids`, `fgraph_facts`, `fgraph_blobs`) plus a derived FTS5 index and read-only views. The present is a partial index over the past (`rx IS NULL`); time travel is a range predicate.
- **Determinism**: integer-microsecond timestamps, pinned allocation order, canonical JSON, and fixed format constants make files byte-identical across the Python and Go implementations — verified by the [conformance suite](https://github.com/fmind/fgraph/tree/main/conformance) and a cross-implementation file check.
- **Query language**: Datalog patterns as plain JSON — patterns, predicates, negation, disjunction, recursive rules, aggregates, and pull — compiled to SQLite SQL. SQL views remain as a read-only escape hatch.

Ports to other languages are welcome: implement the spec and run the conformance suite.
