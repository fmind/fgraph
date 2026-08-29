# Contributing to fgraph

Thank you for improving fgraph. The project favors small, complete changes that keep one clear contract across all three native runtimes.

## Before changing behavior

- Read the [specification](docs/content/spec.md). It is normative for the file format, semantics, APIs, CLI, MCP, and release proof.
- Open an issue before changing the durable format, canonical JSON, allocation order, event/snapshot protocols, error taxonomy, or public cross-runtime behavior.
- A normative change must land in Python, Go, and TypeScript together and add a shared case under `conformance/cases/`.
- Keep core storage and tests network-free. Embeddings remain caller-provided.

## Local workflow

```bash
mise trust && mise install
mise run install
mise run all
```

`mise run all` is the acceptance gate: formatting, static/type/security checks, 95% coverage suites, minimum-runtime tests, cross-runtime conformance, differential traces, the immutable fixture, examples, package smoke tests, documentation links, and builds.

Add focused tests first, then run the complete gate. Do not lower a coverage threshold, loosen a type, skip a test, or edit an acceptance example to hide an implementation defect.

## Pull requests

Keep each pull request reviewable and explain:

- what behavior changes and why;
- whether the specification changes;
- which shared conformance case proves portable behavior;
- the exact commands used for verification;
- any compatibility, privacy, performance, or recovery consequence.

Use Conventional Commits for commit subjects. Contributions are accepted under the repository's [MIT license](LICENSE); no CLA is required.
