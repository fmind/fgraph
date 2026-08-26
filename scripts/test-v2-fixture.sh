#!/bin/sh
# Prove that every runtime opens the immutable format-v2 fixture without
# migration and renders the same events, snapshot, and exact logical core.
set -eu

ROOT="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

FIXTURES="${ROOT}/conformance/fixtures"
EXPECTED_EVENTS="${FIXTURES}/format-v2.events.ndjson"
EXPECTED_SNAPSHOT="${FIXTURES}/format-v2.snapshot.ndjson"
EXPECTED_CORE="${FIXTURES}/format-v2.core"
PY="uv run --project ${ROOT}/python fgraph"
GO="${ROOT}/go/bin/fgraph"
TS="node ${ROOT}/typescript/dist/cli.js"

if command -v sha256sum >/dev/null 2>&1; then
  (cd "${FIXTURES}" && sha256sum --check SHA256SUMS)
else
  (cd "${FIXTURES}" && shasum --algorithm 256 --check SHA256SUMS)
fi
cp "${FIXTURES}/format-v2.db" "${WORK}/format-v2.db"
chmod 0444 "${WORK}/format-v2.db"
DATABASE="${WORK}/format-v2.db"

assert_portable() { # $1 command, $2 label
  $1 tail --db "${DATABASE}" --since 64 >"${WORK}/$2.events.ndjson"
  $1 snapshot --db "${DATABASE}" >"${WORK}/$2.snapshot.ndjson"
  diff -u "${EXPECTED_EVENTS}" "${WORK}/$2.events.ndjson"
  diff -u "${EXPECTED_SNAPSHOT}" "${WORK}/$2.snapshot.ndjson"
}

assert_portable "${PY}" python
assert_portable "${GO}" go
assert_portable "${TS}" typescript

assert_doctor() { # $1 command, $2 label
  $1 doctor --db "${DATABASE}" --json >"${WORK}/$2.doctor.json"
}

assert_doctor "${PY}" python
assert_doctor "${GO}" go
assert_doctor "${TS}" typescript
jq -e '.ok == true and .integrity == "ok" and (.problems | length) == 0' "${WORK}/python.doctor.json" >/dev/null
diff -u "${WORK}/python.doctor.json" "${WORK}/go.doctor.json"
diff -u "${WORK}/python.doctor.json" "${WORK}/typescript.doctor.json"

sqlite3 -init /dev/null -batch -noheader -separator '|' -cmd '.timer off' -cmd '.stats off' "${DATABASE}" >"${WORK}/format-v2.core" <<'SQL'
SELECT 'meta', key, typeof(value), quote(value) FROM fgraph_meta ORDER BY key;
SELECT 'id', id, ifnull(quote(name),'NULL'), ifnull(hex(gid),'NULL'), created_tx FROM fgraph_ids ORDER BY id;
SELECT 'event', tx, hex(event_hash), ifnull(quote(event_data),'NULL'), ifnull(quote(operation_id),'NULL'), ifnull(hex(request_hash),'NULL') FROM fgraph_events ORDER BY tx;
SELECT 'fact', id, e, a, typeof(v), quote(v), t, tx, ifnull(rx, 'NULL') FROM fgraph_facts ORDER BY id;
SELECT 'blob', hex(hash), typeof(data), quote(data) FROM fgraph_blobs ORDER BY hash;
SELECT 'sequence', name, seq FROM sqlite_sequence WHERE name='fgraph_facts';
SQL
diff -u "${EXPECTED_CORE}" "${WORK}/format-v2.core"

markers="$(sqlite3 -init /dev/null -batch -noheader "${DATABASE}" 'SELECT (SELECT application_id FROM pragma_application_id),(SELECT user_version FROM pragma_user_version);')"
[ "${markers}" = '1718055521|2' ]

echo "format-v2 fixture: OK"
