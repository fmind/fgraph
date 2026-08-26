#!/bin/sh
# Reproduce the immutable format-v2 compatibility fixture. Run only when adding
# the reviewed format version; normal tests consume but never rewrite it.
set -eu

ROOT="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
FIXTURES="${ROOT}/conformance/fixtures"
DATABASE="${FIXTURES}/format-v2.db"
EVENTS="${FIXTURES}/format-v2.events.ndjson"
SNAPSHOT="${FIXTURES}/format-v2.snapshot.ndjson"
CORE="${FIXTURES}/format-v2.core"
SCENARIO="${ROOT}/conformance/crosscheck.ndjson"

for output in "${DATABASE}" "${EVENTS}" "${SNAPSHOT}" "${CORE}"; do
  if [ -e "${output}" ]; then
    echo "refusing to overwrite immutable fixture: ${output}" >&2
    exit 1
  fi
done

export FGRAPH_CLOCK=1767225600000000
export FGRAPH_EVENT_SEED=fgraph-conformance-v2
while IFS= read -r line; do
  printf '%s\n' "${line}" | uv run --project "${ROOT}/python" fgraph add --db "${DATABASE}" - >/dev/null
done <"${SCENARIO}"

uv run --project "${ROOT}/python" fgraph tail --db "${DATABASE}" --since 64 >"${EVENTS}"
uv run --project "${ROOT}/python" fgraph snapshot --db "${DATABASE}" >"${SNAPSHOT}"
sqlite3 -init /dev/null -batch -noheader -separator '|' -cmd '.timer off' -cmd '.stats off' "${DATABASE}" >"${CORE}" <<'SQL'
SELECT 'meta', key, typeof(value), quote(value) FROM fgraph_meta ORDER BY key;
SELECT 'id', id, ifnull(quote(name),'NULL'), ifnull(hex(gid),'NULL'), created_tx FROM fgraph_ids ORDER BY id;
SELECT 'event', tx, hex(event_hash), ifnull(quote(event_data),'NULL'), ifnull(quote(operation_id),'NULL'), ifnull(hex(request_hash),'NULL') FROM fgraph_events ORDER BY tx;
SELECT 'fact', id, e, a, typeof(v), quote(v), t, tx, ifnull(rx, 'NULL') FROM fgraph_facts ORDER BY id;
SELECT 'blob', hex(hash), typeof(data), quote(data) FROM fgraph_blobs ORDER BY hash;
SELECT 'sequence', name, seq FROM sqlite_sequence WHERE name='fgraph_facts';
SQL

if command -v sha256sum >/dev/null 2>&1; then
  (cd "${FIXTURES}" && sha256sum format-v2.db format-v2.events.ndjson format-v2.snapshot.ndjson format-v2.core >SHA256SUMS)
else
  (cd "${FIXTURES}" && shasum --algorithm 256 format-v2.db format-v2.events.ndjson format-v2.snapshot.ndjson format-v2.core >SHA256SUMS)
fi
