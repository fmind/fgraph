---
title: Modeling time and uncertainty
weight: 3
---

fgraph records **transaction time**: when the store learned or stopped believing a fact. It does not guess when that fact was true in the real world. Keep those two clocks separate so an audit can answer both questions precisely.

## Transaction time

Every write receives a transaction and an integer-microsecond `fgraph/at` timestamp. Superseding a value closes the previous fact with `rx` and asserts the replacement. Use `history()`, `at()`, and `diff()` to inspect that knowledge timeline.

```python
opened = db.transact({"id": "bridge", "asset/status": "open"}, source="inspection:41")
report = db.transact({"id": "bridge", "asset/status": "closed"}, source="inspection:42")
assert opened.tx is not None

db.at(opened.tx).entity("bridge")     # status was open at this known transaction
db.history("bridge", "asset/status")  # both beliefs and their provenance
```

The CLI exposes the same read-only view with `fgraph get bridge --at <tx>` and `fgraph q '<json>' --at <tx>`.

Transaction ids and local `fgraph/at` values advance together. Applying portable `event/1` records assigns valid local monotonic transaction times and stores each original source timestamp as `fgraph/imported-at`; the retained event keeps its original `at` value. `snapshot`/`restore`, not event replay, is the exact retained-state recovery path.

## Real-world validity

Model domain validity as data. An interval entity keeps its own start, end, subject, and value; corrections to that interval still retain fgraph's transaction history.

```python
db.declare("interval/subject", ref=True)
db.transact({
    "id": "bridge-closure-2026-08",
    "interval/subject": {"ref": "bridge"},
    "interval/from": {"instant": "2026-08-24T08:00:00Z"},
    "interval/to": {"instant": "2026-08-24T12:00:00Z"},
    "interval/value": "closed",
})
```

This answers “when was the bridge closed?” without pretending that the transaction timestamp is the closure time.

## Contested facts

When sources disagree, store claims instead of forcing one scalar to win. A claim can reference its subject, carry a value, confidence, evidence, and source. Supersede a claim only when your belief about that claim changes.

```python
db.declare("claim/subject", ref=True)
db.transact({
    "id": "claim-forecast-a",
    "claim/subject": {"ref": "launch"},
    "claim/value": "Tuesday",
    "claim/confidence": 0.72,
    "claim/evidence": "forecast-a.json",
}, source="model:a")
```

## Enumerations

Use named entities for controlled values and declare the attribute as a reference. Names stay readable on the wire while references remain traversable.

```python
db.declare("task/status", ref=True)
db.transact({"id": "task-42", "task/status": {"ref": "status/in-progress"}})
```

This is preferable to hidden integer codes or duplicated validation lists: agents see meaningful names, and the enum entity can carry documentation as ordinary facts.
