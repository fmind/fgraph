# Format-v2 compatibility fixture

`format-v2.db` is the immutable SQLite fixture for fgraph v1. It is generated from `conformance/crosscheck.ndjson` with the normative clock and event seed. The event stream, snapshot, exact logical core rows, and `SHA256SUMS` make the binary fixture reviewable without treating SQLite pager bytes as normative.

Every implementation must open the database read-only, pass `doctor`, and reproduce the event, snapshot, and core expectations without migration. Run `scripts/test-v2-fixture.sh` to verify it.

The fixture is part of the released format contract. Do not regenerate it for an implementation-only change. If a later release introduces a new format, retain this fixture and add an explicit migration boundary plus a new fixture. `scripts/generate-v2-fixture.sh` exists solely to reproduce the reviewed v2 candidate before its first public release.
