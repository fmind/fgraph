# Benchmarks

These artifacts are evidence for fgraph's documented v1 operating envelope, not a service-level objective or a comparison against unrelated databases.

- [`latest.ndjson`](latest.ndjson) is the machine-readable source. Its first record identifies the fgraph version, Git revision and tree state, a digest of the measured source and locked dependencies, runtime versions, and workload.
- [`ingest-throughput.svg`](ingest-throughput.svg) and [`read-latency.svg`](read-latency.svg) are generated from that NDJSON. `benchmark:verify` rejects charts that differ from the raw observations.
- The root [README](../README.md#benchmarks) explains the workload and summarizes the largest measured size.

Run the complete workload from the repository root:

```bash
mise run all
mise run benchmark
```

Review and commit only the generated benchmark evidence, then verify it from the resulting clean tree with `mise run benchmark:verify`. The verifier intentionally rejects uncommitted evidence because the benchmark metadata records the clean source candidate measured before those generated files changed.

The harness uses no timing threshold. It measures each native CLI independently at 1,000, 10,000, and 100,000 entities, including fresh-process startup for reads. Filesystem, hardware, runtime, and SQLite build differences affect results, so rerun it on the target environment before making a capacity decision.

Release measurements follow the complete clean-source sequence in [`RELEASING.md`](../RELEASING.md).
