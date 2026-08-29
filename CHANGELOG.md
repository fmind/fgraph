# Changelog

All notable user-visible changes are documented here. fgraph follows [Semantic Versioning](https://semver.org/); the [specification](docs/content/spec.md) defines the compatibility-bearing surfaces.

## Unreleased

## [1.2.0] - 2026-08-29

### Added

- Add flat Overview, CLI Reference, and cross-runtime SDK Reference documentation with rendered navigation invariants.

### Changed

- Default the Python, Go, and TypeScript CLIs to `facts.fgraph` when neither `--db` nor `FGRAPH_DB` is set, with consistent empty-path validation and a fail-closed migration guard that verifies the new default before bypassing an existing `fgraph.db`.
- Replace the misleading connected-line read-latency figure with grouped operation bars while retaining the exact raw observations.

## [1.1.0] - 2026-08-28

### Added

- Expose oversized portable MCP change events through basis- and digest-pinned 128 KiB chunks without blocking later events.
- Share adversarial Unicode, snapshot-integrity, and portability cases across Python, Go, and TypeScript conformance tests.

### Changed

- Stream query candidates, snapshots, event feeds, restore input, and CLI input/output where practical, with bounded buffers and exact work accounting.
- Split npm and PyPI Trusted Publishing into independently retryable jobs and publish the GitHub release only after both registries succeed.
- Verify benchmark documentation against raw evidence and expand release-version consistency checks from runtime metadata to public install docs and tests.

### Fixed

- Bind restored receipt timestamps, created identities, event identities, operation ids, hashes, and canonical payloads to the durable snapshot contract.
- Reject malformed Unicode, control-bearing operation ids, oversized records, invalid integer wire values, and corrupted schema/index declarations consistently across runtimes.
- Preserve Unicode line separators in differential NDJSON traces and charge duplicate batched query bindings against the configured work budget.

### Security

- Enforce bounded portable record reads, MCP page budgets, integrity-checked event chunks, and fail-closed release retry behavior.

## [1.0.4] - 2026-08-28

### Fixed

- Terminate a timed-out Python embedding command's process tree, even when a descendant holds stdout open.

### Security

- Replace the bootstrap npm token with tokenless Trusted Publishing for future releases.

## [1.0.3] - 2026-08-28

### Fixed

- Complete the registry-safe fix-forward release after the partial v1.0.2 publication.

## [1.0.2] - 2026-08-27

### Fixed

- Pin the PyPI publishing action to its container-backed commit so trusted publishing can start reliably.

## [1.0.1] - 2026-08-27

### Fixed

- Publish the local npm release artifact as a filesystem path so npm does not parse it as a Git repository.
- Use the npm namespace owned by the release account for the first TypeScript package publication.

## [1.0.0] - 2026-08-26

### Added

- Native Python, CGO-free Go, and strict ESM TypeScript implementations over one SQLite format-v2 contract.
- Temporal EAV facts with provenance, retained history, idempotent receipts, basis guards, CAS, undo, and audited excision.
- Bounded Datalog queries, EAVT/AVET/VAET datoms, FTS5 keyword retrieval, exact caller-provided vector search, and graph expansion.
- Portable `event/1` replay, exact `snapshot/1` recovery, online backup, integrity doctor, schema manifests, validation shapes, CLI parity, and read-only-by-default MCP.
- Shared conformance cases, seeded differential traces, an immutable format-v2 fixture, cross-runtime file compatibility, package smoke tests, and 95% native coverage gates.
- Provenance-bound benchmarks and deterministic, checksummed, attestable release archives for Python, Go, and TypeScript.

### Security

- Dedicated-file ownership checks, strict external input limits, bounded JSON recursion, safe typed errors, read-only MCP defaults, response caps, and complete-history secret/vulnerability scanning.

### Compatibility

- Version 1.0.0 is the first stable, compatibility-bearing implementation. The earlier PyPI 0.0.1 package was only a name reservation and exposed no database, CLI, or durable format.
