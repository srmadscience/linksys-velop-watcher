#!/usr/bin/env bash
#
# Register (or update) every velop JDBC sink connector against a Kafka Connect
# cluster. Idempotent: uses PUT /connectors/<name>/config, which creates the
# connector if absent and updates it in place if it already exists (no 409 on
# re-run, unlike POST /connectors).
#
# TARGET DATABASE (see connect/sink-files.sh):
#   --target=postgres   PostgreSQL   (default) — connect/velop-sink-<table>-postgres.json
#   --target=crate      CrateDB                — connect/velop-sink-<table>.json
#   --target=all        both at once
#
# CREDENTIALS are NOT committed. The connector configs carry CHANGEME_<VAR>
# placeholders, and this script substitutes the environment variable of the same
# name, failing loudly if it is unset:
#   CHANGEME_CRATE_USER / CHANGEME_CRATE_PASSWORD  <- $CRATE_USER / $CRATE_PASSWORD
#   CHANGEME_PG_USER    / CHANGEME_PG_PASSWORD     <- $PG_USER    / $PG_PASSWORD
#
# Prereqs (see connect/README.md):
#   1. The velop.* tables already exist — apply sql/velop_schema.sql (CrateDB) or
#      sql/velop_schema_postgres.sql (PostgreSQL); the sinks use auto.create:false.
#   2. The Avro value schemas are registered — run `velop-watcher` once before
#      the sinks start consuming.
#
# Usage:
#   PG_USER=scott PG_PASSWORD=... ./connect/install-sinks.sh
#   CONNECT_URL=http://my-connect:8083 ./connect/install-sinks.sh
#
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://badger:8083}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${HERE}/sink-files.sh"

command -v jq   >/dev/null || { echo "error: jq is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 1; }

# Replace every CHANGEME_<VAR> in stdin with the value of $<VAR>.
fill_placeholders() {
  local body ph var val
  body="$(cat)"
  for ph in $(grep -o 'CHANGEME_[A-Z_]*' <<<"$body" | sort -u); do
    var="${ph#CHANGEME_}"
    val="${!var-}"
    if [[ -z "$val" ]]; then
      echo "error: \$${var} is unset — needed for the ${ph} placeholder" >&2
      return 1
    fi
    # jq -n does the JSON-escaping so a password with quotes/backslashes survives
    val="$(jq -rn --arg v "$val" '$v | @json | .[1:-1]')"
    body="${body//${ph}/${val}}"
  done
  # Never let a placeholder reach the cluster. An earlier version of this script
  # did no substitution at all and PUT the literal CHANGEME_CRATE_USER into the
  # live connectors; every task died with "password authentication failed for
  # user \"CHANGEME_CRATE_USER\"" and, because a FAILED task is silent unless
  # you poll for it, the sink stayed down for thirteen days.
  if grep -q 'CHANGEME_' <<<"$body"; then
    echo "error: config still contains $(grep -o 'CHANGEME_[A-Z_]*' <<<"$body" | sort -u | tr '\n' ' ')after substitution" >&2
    return 1
  fi
  printf '%s' "$body"
}

echo "Registering velop ${SINK_TARGET} sinks against ${CONNECT_URL}"

rc=0
while read -r f; do
  name="$(jq -r '.name' "$f")"
  # The /config endpoint expects the bare config object, not the {name,config} wrapper.
  body="$(jq -c '.config' "$f" | fill_placeholders)" || { rc=1; continue; }

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
done < <(sink_files)

rm -f /tmp/velop-sink-resp
exit "$rc"
