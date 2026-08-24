# Conformance suite

The shared contract between the Python and Go implementations (and any future port). Every case in `cases/` MUST pass in every implementation; `crosscheck.ndjson` is the scenario replayed by `scripts/crosscheck.sh` to prove file-level compatibility.

- **Case format**: see `SPEC.md` §13.1 (steps: `tx`, `declare`, `q`, `entity`, `history`, `diff`, `why`, `search`, `at`, `facts`, with `expect`/`error`).
- **Determinism**: runners inject the conformance clock — start `1767225600000000` µs (2026-01-01T00:00:00Z), +1,000,000 µs per transaction (`SPEC.md` §13.2). Ids are then fully determined: genesis tx = 64, first user id = 65, entity ids in order of first appearance, tx id last.
- **`facts` steps** assert raw `fgraph_facts` rows `[id, e, a, v, t, tx, rx]` **beyond** the 25 genesis rows — they pin the physical file format.
- **Seeds**: the cases here pin the trickiest semantics and use `"..."` elisions where noted; the implementer fills them exactly during M1–M2 and extends the suite until every normative MUST in `SPEC.md` §4–§9 (and every typed error) has at least one case.
