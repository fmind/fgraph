# fgraph for Go

The Go implementation of fgraph is a Go 1.27+ library and CLI using `modernc.org/sqlite`, so builds remain CGO-free. It provides immutable temporal facts, provenance, bounded queries, hybrid search, and an MCP server in one SQLite file.

Install the stable module:

```bash
go get github.com/fmind/fgraph/go@v1.0.4
```

For contribution work, use the repository checkout instead.

From the repository root:

```bash
go -C go test ./...
go -C go build -o bin/fgraph ./cmd/fgraph
go/bin/fgraph --help
```

Application code imports `github.com/fmind/fgraph/go` and opens a database with `fgraph.Open(path)`. The v1 API includes:

- idempotent transactions with operation IDs, basis preconditions, and cardinality-one CAS with an exact `{"missing":true}` create/delete sentinel;
- current and historical Datalog, indexed datom pages, and actual-plan explanations;
- explicit attribute declarations, rich snapshots, portable `schema/1` manifests, gradual shapes, and validation;
- bounded keyword/vector search with attribute and fact filters;
- portable event streams with detailed or compact apply results, plus streaming snapshots, backup, and restore;
- a bounded MCP server that is read-only unless write tools are explicitly enabled.

Use `fgraph.DeclareShape` to replace a shape atomically, `fgraph.Schema` for rich introspection, `fgraph.SchemaManifest` / `fgraph.CheckSchemaManifest` / `fgraph.ApplySchemaManifest` for the portable control plane, and `fgraph.Validate` to check assigned entities. `fgraph.ApplySummary` consumes a large event reader without retaining detailed reports; `fgraph.Snapshot` writes incrementally. The CLI exposes the same surfaces and a bounded, resumable `add --batch-size N --operation-id-prefix PREFIX` loader.

The module lives in the `go/` subdirectory, so maintainers publish version `v1.0.4` with the repository tag `go/v1.0.4`.

- [Documentation](https://fmind.github.io/fgraph/)
- [Source, examples, and specification](https://github.com/fmind/fgraph)
- [Issue tracker](https://github.com/fmind/fgraph/issues)
