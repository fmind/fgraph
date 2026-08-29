#!/bin/sh
# Prove format-v2 compatibility across every writer, reader, event applier,
# and snapshot restorer. SQLite pager bytes are deliberately non-normative;
# exact logical rows and portable streams are the interchange contract.
set -eu

ROOT="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

export FGRAPH_CLOCK=1767225600000000
export FGRAPH_EVENT_SEED=fgraph-conformance-v2

SCENARIO="${ROOT}/conformance/crosscheck.ndjson"
RUNTIMES="python go typescript"

run_cli() { # $1 = runtime, remaining arguments = CLI arguments
  runtime=$1
  shift
  case "${runtime}" in
    python) uv run --project "${ROOT}/python" fgraph "$@" ;;
    go) "${ROOT}/go/bin/fgraph" "$@" ;;
    typescript) node "${ROOT}/typescript/dist/cli.js" "$@" ;;
    *) echo "crosscheck: unknown runtime: ${runtime}" >&2; exit 2 ;;
  esac
}

run_scenario() { # $1 = runtime, $2 = database path
  runtime=$1
  database=$2
  scenario_index=0
  while IFS= read -r line; do
    scenario_index=$((scenario_index + 1))
    printf '%s\n' "${line}" | run_cli "${runtime}" add --db "${database}" \
      --operation-id "crosscheck:${scenario_index}" - >/dev/null
  done <"${SCENARIO}"
}

# FTS is derived and is verified through public search below. The exact rows in
# the five durable tables, including stable UUIDs and receipt hashes, are the
# physical interoperability contract. Raw SQLite pages are not.
dump_core() { # $1 = database path, $2 = output path
  sqlite3 -init /dev/null -batch -noheader -separator '|' -cmd '.timer off' -cmd '.stats off' "$1" >"$2" <<'SQL'
SELECT 'meta', key, typeof(value), quote(value) FROM fgraph_meta ORDER BY key;
SELECT 'id', id, ifnull(quote(name),'NULL'), ifnull(hex(gid),'NULL'), created_tx FROM fgraph_ids ORDER BY id;
SELECT 'event', tx, hex(event_hash), ifnull(quote(event_data),'NULL'), ifnull(quote(operation_id),'NULL'), ifnull(hex(request_hash),'NULL') FROM fgraph_events ORDER BY tx;
SELECT 'fact', id, e, a, typeof(v), quote(v), t, tx, ifnull(rx, 'NULL') FROM fgraph_facts ORDER BY id;
SELECT 'blob', hex(hash), typeof(data), quote(data) FROM fgraph_blobs ORDER BY hash;
SELECT 'sequence', name, seq FROM sqlite_sequence WHERE name='fgraph_facts';
SQL
}

probe_public() { # $1 = runtime, $2 = database path, $3 = output prefix
  run_cli "$1" get --db "$2" --json ada >"$3.entity.json"
  run_cli "$1" schema --db "$2" --json person/ >"$3.schema.json"
  run_cli "$1" search --db "$2" --json --text Ada >"$3.keyword.json"
  run_cli "$1" search --db "$2" --json --vector '[0.1,-0.2]' --vector-attribute data/vector >"$3.vector.json"
  run_cli "$1" q --db "$2" --json '{"find":["?e","?name"],"where":[["?e","person/name","?name"]],"order":[["?name","asc"]]}' >"$3.query.json"
}

compare_public() { # $1 = expected prefix, $2 = actual prefix
  for surface in entity schema keyword vector query; do
    diff -u "$1.${surface}.json" "$2.${surface}.json"
  done
}

compare_replayed_public() { # $1 = expected prefix, $2 = replayed prefix
  for surface in entity schema query; do
    diff -u "$1.${surface}.json" "$2.${surface}.json"
  done
  # Applying a portable event records local replay provenance, so fact row ids
  # may shift even though stable identities, transaction ids, and values do not.
  for surface in keyword vector; do
    jq 'del(.hits[].matched[].id, .expanded[].via[].id)' "$1.${surface}.json" >"$1.${surface}.portable.json"
    jq 'del(.hits[].matched[].id, .expanded[].via[].id)' "$2.${surface}.json" >"$2.${surface}.portable.json"
    diff -u "$1.${surface}.portable.json" "$2.${surface}.portable.json"
  done
}

check_default_path_migration() { # $1 = runtime
  runtime=$1
  directory="${WORK}/${runtime}-default-path"
  mkdir -p "${directory}"
  (
    cd "${directory}"
    unset FGRAPH_DB
    run_cli "${runtime}" init --db fgraph.db --json >/dev/null
    run_cli "${runtime}" version >/dev/null
    run_cli "${runtime}" --help >/dev/null 2>&1
    if run_cli "${runtime}" init --json >implicit.stdout 2>implicit.stderr; then
      echo "crosscheck: ${runtime} silently bypassed legacy fgraph.db" >&2
      exit 1
    fi
    grep -F -- 'FormatError:' implicit.stderr >/dev/null
    grep -F -- '--db fgraph.db' implicit.stderr >/dev/null
    grep -F -- '--db facts.fgraph' implicit.stderr >/dev/null
    test ! -e facts.fgraph

    if (export FGRAPH_DB=''; run_cli "${runtime}" init --json) >empty-env.stdout 2>empty-env.stderr; then
      echo "crosscheck: ${runtime} accepted an empty FGRAPH_DB" >&2
      exit 1
    else
      empty_status=$?
    fi
    test "${empty_status}" -eq 1
    grep -F -- 'FormatError:' empty-env.stderr >/dev/null
    grep -F -- 'database path is empty' empty-env.stderr >/dev/null
    test ! -e facts.fgraph

    if run_cli "${runtime}" init --definitely-invalid >invalid.stdout 2>invalid.stderr; then
      echo "crosscheck: ${runtime} accepted an unknown init option" >&2
      exit 1
    else
      invalid_status=$?
    fi
    test "${invalid_status}" -eq 2

    if run_cli "${runtime}" get example --at definitely-not-an-int >invalid-at.stdout 2>invalid-at.stderr; then
      echo "crosscheck: ${runtime} accepted a non-integer historical selector" >&2
      exit 1
    else
      invalid_at_status=$?
    fi
    test "${invalid_at_status}" -eq 2

    touch facts.fgraph
    if run_cli "${runtime}" init --json >unclaimed.stdout 2>unclaimed.stderr; then
      echo "crosscheck: ${runtime} initialized an unclaimed facts.fgraph beside the legacy database" >&2
      exit 1
    else
      unclaimed_status=$?
    fi
    test "${unclaimed_status}" -eq 1
    grep -F -- 'not an initialized fgraph database' unclaimed.stderr >/dev/null
    test -e facts.fgraph
    test ! -s facts.fgraph
    rm facts.fgraph

    run_cli "${runtime}" init --db facts.fgraph --json >/dev/null
    run_cli "${runtime}" init --json >/dev/null
  )
}

echo "crosscheck: verifying the shared default-path migration guard"
for runtime in ${RUNTIMES}; do
  check_default_path_migration "${runtime}"
done

echo "crosscheck: writing the canonical scenario with every runtime"
for writer in ${RUNTIMES}; do
  run_scenario "${writer}" "${WORK}/${writer}.db"
  cp "${WORK}/${writer}.db" "${WORK}/${writer}.before-doctor.db"
  run_cli "${writer}" doctor --db "${WORK}/${writer}.db" --json >"${WORK}/${writer}.doctor.json"
  jq -e '.ok == true and .integrity == "ok" and (.problems | length) == 0' "${WORK}/${writer}.doctor.json" >/dev/null
  cmp "${WORK}/${writer}.before-doctor.db" "${WORK}/${writer}.db"
  run_cli "${writer}" tail --db "${WORK}/${writer}.db" --since 64 >"${WORK}/${writer}.events.ndjson"
  run_cli "${writer}" snapshot --db "${WORK}/${writer}.db" >"${WORK}/${writer}.snapshot.ndjson"
  dump_core "${WORK}/${writer}.db" "${WORK}/${writer}.core"
  probe_public "${writer}" "${WORK}/${writer}.db" "${WORK}/${writer}"
done

echo "crosscheck: comparing every reader against every writer"
for writer in ${RUNTIMES}; do
  for reader in ${RUNTIMES}; do
    run_cli "${reader}" tail --db "${WORK}/${writer}.db" --since 64 >"${WORK}/${writer}-by-${reader}.events.ndjson"
    run_cli "${reader}" snapshot --db "${WORK}/${writer}.db" >"${WORK}/${writer}-by-${reader}.snapshot.ndjson"
    diff -u "${WORK}/${writer}.events.ndjson" "${WORK}/${writer}-by-${reader}.events.ndjson"
    diff -u "${WORK}/${writer}.snapshot.ndjson" "${WORK}/${writer}-by-${reader}.snapshot.ndjson"
    probe_public "${reader}" "${WORK}/${writer}.db" "${WORK}/${writer}-by-${reader}"
    compare_public "${WORK}/${writer}" "${WORK}/${writer}-by-${reader}"
  done
done

echo "crosscheck: restoring every snapshot with every runtime"
for writer in ${RUNTIMES}; do
  for restorer in ${RUNTIMES}; do
    restored="${WORK}/${writer}-restored-by-${restorer}.db"
    run_cli "${restorer}" restore --db "${restored}" --json "${WORK}/${writer}.snapshot.ndjson" >/dev/null
    run_cli "${restorer}" tail --db "${restored}" --since 64 >"${WORK}/${writer}-restored-by-${restorer}.events.ndjson"
    run_cli "${restorer}" snapshot --db "${restored}" >"${WORK}/${writer}-restored-by-${restorer}.snapshot.ndjson"
    diff -u "${WORK}/${writer}.events.ndjson" "${WORK}/${writer}-restored-by-${restorer}.events.ndjson"
    diff -u "${WORK}/${writer}.snapshot.ndjson" "${WORK}/${writer}-restored-by-${restorer}.snapshot.ndjson"
    dump_core "${restored}" "${WORK}/${writer}-restored-by-${restorer}.core"
    diff -u "${WORK}/${writer}.core" "${WORK}/${writer}-restored-by-${restorer}.core"
    probe_public "${restorer}" "${restored}" "${WORK}/${writer}-restored-by-${restorer}"
    compare_public "${WORK}/${writer}" "${WORK}/${writer}-restored-by-${restorer}"
  done
done

echo "crosscheck: applying every event stream with every runtime"
for writer in ${RUNTIMES}; do
  for applier in ${RUNTIMES}; do
    applied="${WORK}/${writer}-applied-by-${applier}.db"
    applied_receipts="${WORK}/${writer}-applied-by-${applier}.json"
    duplicate_receipts="${WORK}/${writer}-reapplied-by-${applier}.json"
    run_cli "${applier}" apply --db "${applied}" --json "${WORK}/${writer}.events.ndjson" >"${applied_receipts}"
    run_cli "${applier}" apply --db "${applied}" --json "${WORK}/${writer}.events.ndjson" >"${duplicate_receipts}"
    event_count="$(wc -l <"${WORK}/${writer}.events.ndjson")"
    jq -e --argjson event_count "${event_count}" --slurpfile applied "${applied_receipts}" '
      ($applied | length) == 1 and
      ($applied[0] | keys) == ["already_applied", "applied", "basis_tx", "events", "noop"] and
      ($applied[0].events == $event_count) and
      ($applied[0].applied == $event_count) and
      ($applied[0].already_applied == 0) and
      ($applied[0].noop == 0) and
      (keys == ["already_applied", "applied", "basis_tx", "events", "noop"]) and
      (.events == $event_count) and
      (.applied == 0) and
      (.already_applied == $event_count) and
      (.noop == 0) and
      (.basis_tx == $applied[0].basis_tx)
    ' "${duplicate_receipts}" >/dev/null
    run_cli "${applier}" tail --db "${applied}" --since 64 >"${WORK}/${writer}-applied-by-${applier}.events.ndjson"
    diff -u "${WORK}/${writer}.events.ndjson" "${WORK}/${writer}-applied-by-${applier}.events.ndjson"
    probe_public "${applier}" "${applied}" "${WORK}/${writer}-applied-by-${applier}"
    compare_replayed_public "${WORK}/${writer}" "${WORK}/${writer}-applied-by-${applier}"
  done
done

echo "crosscheck: comparing exact logical core rows from every writer"
for runtime in go typescript; do
  diff -u "${WORK}/python.core" "${WORK}/${runtime}.core"
done

echo "crosscheck: proving whole-stream apply rollback for every runtime"
{
  sed -n '1p' "${WORK}/python.events.ndjson"
  printf '%s\n' '{"fgraph":"event/1"'
} >"${WORK}/invalid-events.ndjson"
for runtime in ${RUNTIMES}; do
  failed="${WORK}/${runtime}-failed-apply.db"
  clean="${WORK}/${runtime}-clean-control.db"
  if run_cli "${runtime}" apply --db "${failed}" "${WORK}/invalid-events.ndjson" >/dev/null 2>&1; then
    echo "crosscheck: ${runtime} accepted a malformed event stream" >&2
    exit 1
  fi
  run_cli "${runtime}" init --db "${clean}" --json >/dev/null
  run_cli "${runtime}" snapshot --db "${failed}" >"${WORK}/${runtime}-failed-apply.snapshot.ndjson"
  run_cli "${runtime}" snapshot --db "${clean}" >"${WORK}/${runtime}-clean-control.snapshot.ndjson"
  diff -u "${WORK}/${runtime}-clean-control.snapshot.ndjson" "${WORK}/${runtime}-failed-apply.snapshot.ndjson"
  dump_core "${failed}" "${WORK}/${runtime}-failed-apply.core"
  dump_core "${clean}" "${WORK}/${runtime}-clean-control.core"
  diff -u "${WORK}/${runtime}-clean-control.core" "${WORK}/${runtime}-failed-apply.core"
done

echo "crosscheck: OK"
