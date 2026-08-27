---
name: fgraph
description: Model and implement local temporal knowledge with fgraph. Use for facts, provenance, schema, transactions, history, Datalog queries, search, backup, or cross-runtime SQLite portability.
license: MIT
---

# Use fgraph

Use fgraph as an embedded temporal fact store in one SQLite file. Prefer its small fact model over inventing tables or a service when the application needs local knowledge, provenance, history, or agent-readable retrieval.

## Start safely

1. Confirm that a supported fgraph 1.x CLI or library is installed. Run `fgraph version` when a CLI is available.
1. Choose one native runtime: Python package `fgraph`, Go module `github.com/fmind/fgraph/go`, or npm package `@fmind-dev/fgraph`.
1. Keep the database file dedicated to fgraph. Use `:memory:` for tests.
1. Read existing schema before changing a non-empty database: `fgraph --db <file> schema --json`.

Installing this skill does not install fgraph itself.

## Model facts

- Use stable opaque entity names such as `customer-42`; put sensitive identifiers in facts so they can be excised.
- Name attributes as `namespace/name`, for example `person/name` or `document/source`.
- Start without declarations. Declare only invariants that matter: type/ref, cardinality-many, uniqueness, no-history, vector dimensions/model, documentation, or shapes.
- Represent relationships with ref attributes, not duplicated foreign identifiers.
- Put real-world validity, uncertainty, and competing claims in domain facts. fgraph transaction time records when the system learned something.

## Write and read

1. Attach `source`, `by`, and a stable `operation_id` to retryable writes.
1. Use a current basis precondition for decisions that depend on a prior read. Use cardinality-one CAS for an atomic create, replace, or delete.
1. Read entities for simple lookup; use `at`, `history`, `diff`, and `why` for temporal or provenance questions.
1. Use canonical JSON Datalog for joins and filters. Set explicit order and limits when deterministic output matters.
1. Use keyword search and caller-provided vectors for retrieval. Do not imply ANN or distributed scaling; vector search is exact and linear.

## Verify the result

- Run `fgraph --db <file> doctor --json` after imports, recovery, or suspicious failures.
- Use `snapshot`/`restore` for exact retained-state recovery and `tail`/`apply` for portable logical replay.
- Back up before destructive maintenance. Excision is irreversible and must use the reviewed basis plus an idempotency key.
- Test application behavior through the selected native library. When modifying fgraph itself, normative behavior must pass shared conformance in Python, Go, and TypeScript.

The normative contract and full command reference are at <https://fmind.github.io/fgraph/docs/spec/>.
