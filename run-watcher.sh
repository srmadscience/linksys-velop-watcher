#!/usr/bin/env bash
#
# Fetch one Velop sysinfo snapshot and store it in CrateDB.
#
# Usage: ./run-watcher.sh <VELOP_PASSWORD>
#
# Every setting except the router password is hard-coded below; the password
# is passed as the first argument so it stays out of the file and the
# environment of other processes.
set -euo pipefail

if [[ $# -lt 1 || -z "${1:-}" ]]; then
  echo "usage: $0 <VELOP_PASSWORD>" >&2
  exit 2
fi

# Resolve the directory this script lives in, so it works from anywhere.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Router / fetch ---
export VELOP_URL="https://10.13.1.1/sysinfo.cgi"
export VELOP_USER="admin"
export VELOP_PASSWORD="$1"
export VELOP_VERIFY_TLS="false"
export VELOP_SINK=kafka

# --- CrateDB ---
# The CrateDB password is a secret, so it is NOT hard-coded here. It is read
# from the gitignored .env file (CRATE_PASSWORD=...). Falls back to the
# environment if .env is absent.
export CRATE_URL="http://endowment:4200"
export CRATE_USER="scott"
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  CRATE_PASSWORD="$(grep -E '^CRATE_PASSWORD=' "${SCRIPT_DIR}/.env" | tail -n1 | cut -d= -f2-)"
fi
export CRATE_PASSWORD="${CRATE_PASSWORD:?CRATE_PASSWORD not set (add it to .env or the environment)}"

# --- OUI / vendor resolution ---
export OUI_MANUF_PATH="${SCRIPT_DIR}/manuf"
export OUI_MANUF_URL="https://www.wireshark.org/download/automated/data/manuf"

# Activate the project virtualenv and run.
source "${SCRIPT_DIR}/.venv/bin/activate"
exec velop-watcher
