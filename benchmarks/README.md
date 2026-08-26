# Benchmarks

These artifacts are evidence for fgraph's documented v1 operating envelope, not a service-level objective or a comparison against unrelated databases.

- [`latest.ndjson`](latest.ndjson) is the machine-readable source. Its first record identifies the fgraph version, Git revision and tree state, a digest of the measured source and locked dependencies, runtime versions, and workload.
- [`ingest-throughput.svg`](ingest-throughput.svg) and [`read-latency.svg`](read-latency.svg) are generated from that NDJSON. `benchmark:verify` rejects charts that differ from the raw observations.
- The root [README](../README.md#benchmarks) explains the workload and summarizes the largest measured size.

Run the complete workload from the repository root:

```bash
mise run all
mise run benchmark
mise run benchmark:verify
```

The harness uses no timing threshold. It measures each native CLI independently at 1,000, 10,000, and 100,000 entities, including fresh-process startup for reads. Filesystem, hardware, runtime, and SQLite build differences affect results, so rerun it on the target environment before making a capacity decision.

Release measurements follow the clean-source sequence in [`RELEASING.md`](../RELEASING.md): first commit the source candidate, then measure it, review and commit only the evidence, and finally verify the clean release candidate.
