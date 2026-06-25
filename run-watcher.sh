#!/usr/bin/env bash
#
# Fetch one Velop sysinfo snapshot and produce it to Kafka (Confluent-Avro).
# The Connect JDBC sinks land it in CrateDB; this script never talks to CrateDB.
#
# Usage: ./run-watcher.sh <VELOP_PASSWORD>
#
# Every setting except the router password is hard-coded below; the password
# is passed as the first argument so it stays out of the file and the
# environment of other processes.
set -euo pipefail

# Router password: first arg (manual use) or the VELOP_PASSWORD environment
# variable (e.g. systemd EnvironmentFile, which keeps it out of the process args
# visible in `ps`). The arg wins if both are set.
VELOP_PASSWORD="${1:-${VELOP_PASSWORD:-}}"
if [[ -z "$VELOP_PASSWORD" ]]; then
  echo "usage: $0 <VELOP_PASSWORD>   (or set VELOP_PASSWORD in the environment)" >&2
  exit 2
fi

# Resolve the directory this script lives in, so it works from anywhere.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Router / fetch ---
export VELOP_URL="https://10.13.1.1/sysinfo.cgi"
export VELOP_USER="admin"
export VELOP_PASSWORD
export VELOP_VERIFY_TLS="false"

# --- Kafka / schema registry ---
# Defaults (badger:9092 / http://badger:8081) live in config.py; override here
# only if your brokers differ.
export KAFKA_BOOTSTRAP="badger:9092"
export SCHEMA_REGISTRY_URL="http://badger:8081"

# --- Store-and-forward buffer ---
# When Kafka/registry are down a snapshot is buffered here and replayed on a
# later run. Pin it to an absolute path (anchored to the script dir, like the
# OUI file) so it never depends on the process working directory.
export VELOP_BUFFER_DIR="${SCRIPT_DIR}/buffer"

# --- OUI / vendor resolution ---
export OUI_MANUF_PATH="${SCRIPT_DIR}/manuf"
export OUI_MANUF_URL="https://www.wireshark.org/download/automated/data/manuf"

# Activate the project virtualenv and run.
source "${SCRIPT_DIR}/.venv/bin/activate"
exec velop-watcher
