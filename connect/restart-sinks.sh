#!/usr/bin/env bash
#
# Restart every velop JDBC sink connector (and its tasks) on a Kafka Connect
# cluster. Useful after a CrateDB/Connect bounce or to clear FAILED tasks
# without re-POSTing config.
#
# By default it restarts only connectors/tasks that are currently FAILED. Pass
# --all to restart everything regardless of state.
#
# Usage:
#   ./connect/restart-sinks.sh                 # restart only FAILED tasks
#   ./connect/restart-sinks.sh --all           # restart all connectors + tasks
#   CONNECT_URL=http://my-connect:8083 ./connect/restart-sinks.sh
#
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://badger:8083}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ONLY_FAILED=true
[[ "${1:-}" == "--all" ]] && ONLY_FAILED=false

command -v jq   >/dev/null || { echo "error: jq is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 1; }

if $ONLY_FAILED; then
  echo "Restarting FAILED velop tasks against ${CONNECT_URL} (use --all for everything)"
else
  echo "Restarting ALL velop connectors + tasks against ${CONNECT_URL}"
fi

rc=0
for f in "${HERE}"/velop-sink-*.json; do
  name="$(jq -r '.name' "$f")"

  # POST /connectors/<name>/restart with includeTasks restarts the connector and
  # its tasks in one call; onlyFailed limits it to FAILED instances (Connect 2.3+).
  code="$(curl -s -o /tmp/velop-restart-resp -w '%{http_code}' \
    -X POST \
    "${CONNECT_URL}/connectors/${name}/restart?includeTasks=true&onlyFailed=${ONLY_FAILED}")"

  case "$code" in
    200|202|204) echo "  ok      ${name} (HTTP ${code})" ;;
    409)         echo "  skip    ${name} — rebalance in progress (HTTP 409)" >&2; rc=1 ;;
    404)         echo "  missing ${name} — not registered (HTTP 404); run install-sinks.sh" >&2; rc=1 ;;
    *)           echo "  FAIL    ${name} (HTTP ${code}): $(cat /tmp/velop-restart-resp)" >&2; rc=1 ;;
  esac
done

rm -f /tmp/velop-restart-resp
exit "$rc"
