# Conformance suite

This directory is the executable contract between the native Python, Go, and TypeScript implementations.

## Shared cases

Each JSON file under `cases/<area>/` has a name, rationale, and ordered steps. Runners support:

- writes: `tx`, `declare`, and `shape`, with operation/basis options;
- reads: `stats`, `entity`, `history`, `diff`, `why`, `receipt`, `attributes`, `schema`, `validate`, `q`, `explain`, `datoms`, and `search`;
- historical wrapping through `at`;
- exact raw `facts` assertions beyond the 39 genesis facts;
- `expect` or one stable typed `error` name.

Arrays and objects are exact by default. `"...": true` permits additional object keys only; it never permits extra array values. Unordered query rows are compared as multisets only when the query has no non-empty `order`.

`portable-boundaries.json` is the shared adversarial stream corpus. Every runtime must reject its raw malformed-Unicode documents with the same typed error, preserve its valid Unicode line separators through event and snapshot replay, and reject its named snapshot-integrity mutations atomically.

## Determinism

Runners inject:

```text
clock = 1767225600000000 + transaction_index * 1000000 microseconds
event seed = fgraph-conformance-v2
```

This determines allocation, timestamps, stable UUIDs, hashes, encoded values, and ordered logical rows. Raw SQLite pager bytes are not normative.

## Cross-runtime proof

`crosscheck.ndjson` is the canonical compatibility scenario. `scripts/crosscheck.sh` proves:

1. Every CLI refuses to bypass a legacy-only `fgraph.db` when selecting the new `facts.fgraph` default implicitly.
1. All three writers produce exact ordered core rows.
1. Every runtime reads every peer file with identical events, snapshots, entity/schema/query results, and keyword/vector search.
1. Every runtime restores every peer snapshot to exact logical state.
1. Every runtime applies every peer event stream to identical portable state.
1. Reapplying every peer stream returns the original receipt basis with empty change deltas.
1. Malformed multi-event input rolls back as one atomic apply.

Event replay intentionally ignores local fact-row ids in matched search evidence because the receiving database records replay provenance. Stable event/entity ids, transactions, values, and public logical results remain exact.

## Immutable fixture

`fixtures/format-v2.db` plus its canonical events, snapshot, core rows, and checksums freezes the released file contract. Every runtime opens it read-only, passes `doctor`, and reproduces all expectations without migration.

Run the full proof through `mise run test`; run only the compatibility matrix with `mise run test:cross` and the frozen fixture with `mise run test:fixture`.
