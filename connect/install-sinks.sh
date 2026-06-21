#!/usr/bin/env bash
#
# Register (or update) every velop JDBC sink connector against a Kafka Connect
# cluster. Idempotent: uses PUT /connectors/<name>/config, which creates the
# connector if absent and updates it in place if it already exists (no 409 on
# re-run, unlike POST /connectors).
#
# Prereqs (see connect/README.md):
#   1. The velop.* CrateDB tables already exist (run `velop-watcher` once in
#      crate/both mode, or apply the DDL by hand) — the sinks use auto.create:false.
#   2. The Avro value schemas are registered — run the watcher once with
#      VELOP_SINK=kafka|both before the sinks start consuming.
#
# Usage:
#   CONNECT_URL=http://my-connect:8083 ./connect/install-sinks.sh
#   ./connect/install-sinks.sh            # defaults to http://badger:8083
#
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://badger:8083}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

command -v jq   >/dev/null || { echo "error: jq is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 1; }

echo "Registering velop sinks against ${CONNECT_URL}"

rc=0
for f in "${HERE}"/velop-sink-*.json; do
  name="$(jq -r '.name' "$f")"
  # The /config endpoint expects the bare config object, not the {name,config} wrapper.
  body="$(jq -c '.config' "$f")"

  code="$(curl -s -o /tmp/velop-sink-resp -w '%{http_code}' \
    -X PUT -H 'Content-Type: application/json' \
    --data "$body" \
    "${CONNECT_URL}/connectors/${name}/config")"

  if [[ "$code" == "200" || "$code" == "201" ]]; then
    echo "  ok   ${name} (HTTP ${code})"
  else
    echo "  FAIL ${name} (HTTP ${code}): $(cat /tmp/velop-sink-resp)" >&2
    rc=1
  fi
done

rm -f /tmp/velop-sink-resp
exit "$rc"
