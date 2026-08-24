---
title: Getting Started
weight: 1
---

{{< callout type="info" >}} v0.1 is in development — this page documents the specified target. {{< /callout >}}

## Install

```bash
pip install fgraph                                        # Python library + CLI
go get github.com/fmind/fgraph/go                         # Go library
go install github.com/fmind/fgraph/go/cmd/fgraph@latest   # single-binary CLI
```

## First facts

```python
import fgraph

db = fgraph.connect("memory.db")   # creates or opens; ":memory:" for tests

db.transact({"id": "ada", "person/name": "Ada Lovelace", "person/city": "London"},
            source="wikipedia", by="importer")

db.declare("person/knows", ref=True, many=True)   # declare only special behavior
db.transact({"id": "ada", "person/knows": {"ref": "grace"}})
db.transact({"id": "ada", "person/city": "Lyon"})  # supersedes; both moments kept
```

## Ask questions

```python
db.entity("ada")                              # pull the entity as a dict
db.q(find=["?n"], where=[["?e", "person/knows", "?f"], ["?f", "person/name", "?n"]])
db.search("mathematician in Lyon", k=8, expand=1)   # keyword + graph expansion
```

## Travel in time

```python
db.history("ada", "person/city")   # London (tx 67 → 68), Lyon (tx 68 → now)
db.at(67).entity("ada")            # the world as it was at transaction 67
db.diff(67, 68)                    # exactly what changed in between
db.why("ada", "person/city")       # the fact plus its transaction's provenance
db.undo(68)                        # compensating transaction: the audited revert
```

## From the terminal

```bash
fgraph add --db notes.db '{"id": "fgraph", "project/status": "v0.1"}'
fgraph get --db notes.db fgraph
fgraph why --db notes.db fgraph project/status
fgraph tail --db notes.db --follow   # live audit stream of every transaction (NDJSON)
fgraph backup --db notes.db notes-$(date +%F).db   # safe hot backup while in use
```

Everything lives in one plain SQLite file: back it up with `cp`, inspect it with any SQLite client via the read-only `fgraph_now` and `fgraph_view` views.
