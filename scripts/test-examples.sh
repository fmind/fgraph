#!/bin/sh
# The examples are executable API contracts. Run them against the local peers in
# a disposable directory so file-backed examples never dirty the checkout.
set -eu

ROOT="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

uv run --project "${ROOT}/python" python "${ROOT}/examples/python/quickstart.py"
(
  cd "${WORK}"
  uv run --project "${ROOT}/python" python "${ROOT}/examples/python/agent_memory.py"
)

go build -C "${ROOT}/examples/go" -o "${WORK}/go-quickstart" ./quickstart
go build -C "${ROOT}/examples/go" -o "${WORK}/go-knowledgebase" ./knowledgebase
"${WORK}/go-quickstart"
(
  cd "${WORK}"
  "${WORK}/go-knowledgebase"
)

node "${ROOT}/examples/typescript/quickstart.mjs"

printf '%s\n' "examples: OK"
