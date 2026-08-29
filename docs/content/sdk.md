---
title: SDK Reference
weight: 10
description: A cross-runtime map of the peer Python, Go, and TypeScript APIs and their shared contracts.
---

The three libraries are native peers over the same SQLite format and semantics. Python uses snake case, Go accepts `context.Context` and typed option values, and TypeScript uses camel case. None delegates core behavior to another runtime.

Use [Getting Started](../getting-started/) for installation. The package-specific READMEs provide complete runnable examples for [Python](https://github.com/fmind/fgraph/tree/main/python), [Go](https://github.com/fmind/fgraph/tree/main/go), and [TypeScript](https://github.com/fmind/fgraph/tree/main/typescript).

## Open and close

```python
import fgraph

with fgraph.connect("memory.db") as db:
    report = db.transact({"id": "ada", "person/name": "Ada Lovelace"})
    entity = db.entity("ada")
```

```go
func storeAda(ctx context.Context) (result error) {
    db, err := fgraph.Open("memory.db")
    if err != nil {
        return fmt.Errorf("open fgraph: %w", err)
    }
    defer func() { result = errors.Join(result, db.Close()) }()

    report, err := db.Transact(ctx, fgraph.E{"id": "ada", "person/name": "Ada Lovelace"})
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
const report = db.transact({ id: "ada", "person/name": "Ada Lovelace" });
const entity = db.entity("ada");
```

Open an existing file without write authority with `connect(path, read_only=True)`, `Open(path, WithReadOnly())`, or `connect(path, { readOnly: true })`. Historical views returned by `at`/`At` are also read-only.

## API crosswalk

| Capability                  | Python `Db`                                                         | Go `*DB`                                                       | TypeScript `Db`                                                |
| --------------------------- | ------------------------------------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------- |
| Open / close                | `connect`, context manager, `close`                                 | `Open`, `Close`                                                | `connect`, `close`, `using`                                    |
| Write facts                 | `transact`, `retract`                                               | `Transact`, `Add`, `Retract`                                   | `transact`, `add`, `retract`                                   |
| Attribute declarations      | `declare`                                                           | `Declare`                                                      | `declare`                                                      |
| Shapes and validation       | `declare_shape`, `validate`                                         | `DeclareShape`, `Validate`                                     | `defineShape`, `validate`                                      |
| Entity reads                | `entity`, `pull`                                                    | `Entity`, `Pull`                                               | `entity`, `pull`                                               |
| Datalog                     | `q`, `explain`                                                      | `Query`/`Qry`, `QueryJSON`, `Explain`/`ExplainJSON`            | `q`, `explain`                                                 |
| Ordered datoms              | `datoms`                                                            | `Datoms`                                                       | `datoms`                                                       |
| Search                      | `search`                                                            | `Search`                                                       | `search`                                                       |
| Schema discovery            | `attributes`, `schema`                                              | `Attributes`, `Schema`                                         | `attributes`, `schema`                                         |
| Portable schema             | `schema_manifest`, `check_schema_manifest`, `apply_schema_manifest` | `SchemaManifest`, `CheckSchemaManifest`, `ApplySchemaManifest` | `schemaManifest`, `checkSchemaManifest`, `applySchemaManifest` |
| Historical views            | `at`, `history`, `why`, `diff`, `changes`                           | `At`, `History`, `Why`, `Diff`, `Changes`                      | `at`, `history`, `why`, `diff`, `changes`                      |
| Receipts                    | `receipt`                                                           | `Receipt`                                                      | `receipt`                                                      |
| Portable events             | `event_records`, `follow`                                           | `EventRecords`, `Tail`, `Follow`                               | `eventRecords`, `tail`                                         |
| Event replay                | `apply`, `apply_summary`                                            | `Apply`, `ApplySummary`                                        | `apply`, `applySummary`                                        |
| Exact logical recovery      | `iter_snapshot`/`snapshot`, `restore`                               | `Snapshot`, `Restore`                                          | `snapshotLines`/`snapshot`, `restore`                          |
| Physical backup             | `backup`, module-level `restore_backup`                             | `Backup`                                                       | `backup`                                                       |
| Integrity                   | `doctor`                                                            | `Doctor`                                                       | `doctor`                                                       |
| Audited compensation/delete | `undo`, `excise`                                                    | `Undo`, `Excise`                                               | `undo`, `excise`                                               |
| Statistics                  | `stats`                                                             | `Stats`                                                        | `stats`                                                        |

The table maps names, not signatures. Go methods take `context.Context` first and return an error. Python and TypeScript raise typed exceptions. Consult the shipped types and package README for language-specific options.

## Shared mutation contract

`transact`/`Transact` accepts entity maps and explicit operations. A stable operation id makes a canonical request retry-safe; an expected basis rejects a stale write. Cardinality-one compare-and-swap uses `['cas', entity, attribute, expected, desired]`, including the exact `{"missing": true}` sentinel for create or delete.

Every successful mutation returns a transaction report with status, basis, transaction, event identity and hash, asserted/retracted facts, and any allocated identities. The same logical request and operation id returns the original receipt; the id cannot be reused for different data or options.

## Values and integers

The logical value set is shared: refs, booleans, signed 64-bit integers, finite binary64 floats, text, bytes, instants, JSON, and vectors. JSON values may contain null. Public JSON and protocol streams use the typed wrappers defined in the [specification](../spec/#23-value-tags-and-canonical-values).

- Python integers are arbitrary precision at the language boundary and are rejected outside signed 64-bit storage range.
- Go uses `int64` for transaction and storage integers.
- TypeScript uses `bigint` where lossless wire or database integers can exceed JavaScript's safe-number range.

Canonical JSON rejects non-finite floats, excessive nesting, invalid surrogate text, and out-of-range integers before a write reaches SQLite.

## Typed errors

Every runtime exposes the same stable taxonomy: `NotFound`, `Conflict`, `SchemaError`, `TypeError`, `QueryError`, `FormatError`, `ReadOnly`, `TooLarge`, and `Unsupported`.

- Python and TypeScript errors inherit from `FGraphError`.
- Go returns `*fgraph.Error`; use `errors.Is(err, fgraph.ErrConflict)` and the other exported sentinels rather than matching text.

## Streaming and bounded work

Prefer streaming APIs for data whose size is controlled by the database rather than the caller:

- Python `iter_snapshot`, Go `Snapshot(io.Writer)`, and TypeScript `snapshotLines`/`snapshot(writer)` avoid materializing a whole snapshot.
- `apply_summary`/`ApplySummary`/`applySummary` consumes event streams without retaining one report per event.
- Datom cursors bind basis, arguments, and index position. Query, search, portable events, MCP, values, and responses enforce the shared limits in the [specification](../spec/).

Embeddings are always caller-provided. The core libraries make no network calls.
