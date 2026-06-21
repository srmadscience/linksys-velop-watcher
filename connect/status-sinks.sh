#!/usr/bin/env bash
#
# Show the state of every velop JDBC sink connector and its tasks. Handy after
# install-sinks.sh / restart-sinks.sh to confirm everything is RUNNING.
#
# Usage:
#   ./connect/status-sinks.sh
#   CONNECT_URL=http://my-connect:8083 ./connect/status-sinks.sh
#
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://badger:8083}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

command -v jq   >/dev/null || { echo "error: jq is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 1; }

rc=0
for f in "${HERE}"/velop-sink-*.json; do
  name="$(jq -r '.name' "$f")"
  status="$(curl -s "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || true)"

  if [[ -z "$status" ]] || ! echo "$status" | jq -e '.connector' >/dev/null 2>&1; then
    printf '  %-40s %s\n' "$name" "NOT REGISTERED"
    rc=1
    continue
  fi

  conn_state="$(echo "$status"  | jq -r '.connector.state')"
  task_states="$(echo "$status" | jq -r '[.tasks[]?.state] | join(",")')"
  [[ -z "$task_states" ]] && task_states="no-tasks"
  printf '  %-40s connector=%-8s tasks=%s\n' "$name" "$conn_state" "$task_states"

  [[ "$conn_state" != "RUNNING" ]] && rc=1
  echo "$task_states" | grep -q 'FAILED' && rc=1
done

exit "$rc"
