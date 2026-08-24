#!/bin/sh
# Cross-implementation file compatibility check (SPEC.md §13).
#
# The SQLite file is the interchange format: a database written by the Python
# implementation must read identically in Go, and vice versa. This script runs
# the same deterministic scenario through both CLIs (FGRAPH_CLOCK pins time so
# transaction ids and timestamps match exactly), then compares:
#   1. the physical fact rows of the two written files (sqlite3 dump of fgraph_facts),
#   2. each implementation's export of the file written by the other.
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Deterministic clock: 2026-01-01T00:00:00Z in microseconds (SPEC.md §13.2).
export FGRAPH_CLOCK=1767225600000000

PY="uv run --project $ROOT/python fgraph"
GO="go run -C $ROOT/go ./cmd/fgraph"

# The scenario every implementation must replay identically.
SCENARIO="$ROOT/conformance/crosscheck.ndjson"

run_scenario() { # $1 = command, $2 = database path
  while IFS= read -r line; do
    printf '%s\n' "$line" | $1 add --db "$2" -
  done <"$SCENARIO"
}

echo "crosscheck: writing with Python, reading with Go"
run_scenario "$PY" "$WORK/py.db"
$PY export --db "$WORK/py.db" >"$WORK/py-by-py.ndjson"
$GO export --db "$WORK/py.db" >"$WORK/py-by-go.ndjson"
diff -u "$WORK/py-by-py.ndjson" "$WORK/py-by-go.ndjson"

echo "crosscheck: writing with Go, reading with Python"
run_scenario "$GO" "$WORK/go.db"
$GO export --db "$WORK/go.db" >"$WORK/go-by-go.ndjson"
$PY export --db "$WORK/go.db" >"$WORK/go-by-py.ndjson"
diff -u "$WORK/go-by-go.ndjson" "$WORK/go-by-py.ndjson"

echo "crosscheck: comparing physical fact rows of both files"
DUMP="SELECT id, e, a, quote(v), t, tx, ifnull(rx, 'NULL') FROM fgraph_facts ORDER BY id;"
sqlite3 "$WORK/py.db" "$DUMP" >"$WORK/py.rows"
sqlite3 "$WORK/go.db" "$DUMP" >"$WORK/go.rows"
diff -u "$WORK/py.rows" "$WORK/go.rows"

echo "crosscheck: OK"
