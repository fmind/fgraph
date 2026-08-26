# AGENTS.md

Guidance for coding agents working on fgraph. Humans should start with `README.md`.

## Contract

- `docs/content/docs/spec.md` is normative for the file format, semantics, query language, APIs, CLI, MCP, and release proof. When another file disagrees, the specification wins.
- Python, Go, and TypeScript are equal native implementations. Normative behavior must land in all three and be covered by `conformance/`.
- Keep the embedded core simple: one SQLite file, deterministic behavior, no core network calls, and no speculative features.

## Coding rules

- Read the relevant specification section and existing tests before changing behavior.
- Parse external input into validated types at the boundary. Return the shared typed error taxonomy with useful context; never swallow an error.
- Preserve deterministic ids, integer-microsecond timestamps, canonical JSON, event identities, allocation order, and ordered logical rows.
- Prefer small functions and flat packages. Remove duplication, but do not create abstractions without a second concrete use.
- Comment the reason for a non-obvious invariant or trade-off, not the syntax.
- Keep core code and tests offline. Embeddings are always caller-provided or run through the explicit local subprocess boundary.
- Never weaken an assertion, coverage threshold, type, lint rule, or conformance expectation to make a gate pass.

## Language conventions

- **Python:** use uv, modern type annotations, Ruff, ty, and pytest. The core under `python/src/fgraph/` stays standard-library-only.
- **Go:** pass `context.Context` to I/O and long-running operations, wrap errors with `%w`, close resources promptly, and format with goimports plus gofumpt. The module is CGO-free.
- **TypeScript:** use strict ESM, explicit public types, `bigint` for lossless wire integers, Prettier, oxlint, strict `tsc`, and Vitest. Avoid `any` and unsafe casts.
- Verify dependency APIs against the installed source and current primary documentation before coding against them.

## Workflow

All development commands come from `mise.toml`; hooks and CI call the same tasks.

- `mise run install` installs locked dependencies and hooks.
- `mise run format` formats every language and document.
- `mise run check` runs static, type, security, workflow, version, and docs checks.
- `mise run test` runs native suites, shared conformance, differential traces, fixtures, examples, package smokes, and docs links.
- `mise run build` builds all packages, the Go CLI, and the docs site.
- `mise run all` is the complete local acceptance gate.

Run focused tests while iterating, then `mise run check` and `mise run test`. A release candidate must pass `mise run all` warning-free from a clean checkout.

## Repository layout

- `docs/content/docs/spec.md` — normative fgraph v1 / SQLite format-v2 contract.
- `python/`, `go/`, `typescript/` — peer libraries and CLIs.
- `conformance/` — shared cases, differential scenario, and immutable format-v2 fixture.
- `examples/` — runnable acceptance examples for every runtime.
- `docs/` — Hugo documentation site.
- `skills/` — installable Agent Skills for using fgraph.
- `scripts/` — cross-runtime, fixture, benchmark, and release helpers.
- `mise.toml`, `lefthook.yml`, `dprint.json` — canonical tasks, hooks, and formatting.

Do not commit, push, publish, or change the durable contract unless the task explicitly authorizes it. Use Conventional Commits and never add generated-code attribution or co-author trailers.
