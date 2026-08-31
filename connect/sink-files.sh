#!/usr/bin/env bash
#
# Shared helper for install-/restart-/status-sinks.sh: decides WHICH set of
# velop sink connector files the script acts on. There are two, one per target
# database, and they differ only in connection.url / credentials:
#
#   crate     connect/velop-sink-<table>.json           -> CrateDB   (pg-wire 5432)
#   postgres  connect/velop-sink-<table>-postgres.json  -> PostgreSQL (5433)
#   all       both
#
# The connector NAMES differ too (crate-jdbc-sink-velop-* vs
# postgres-jdbc-sink-velop-*), so both sets can run side by side against the
# same topics — each gets its own consumer group and lands the same records in
# both databases.
#
# Source this after setting HERE; it reads --target=<x> out of "$@" (leaving the
# script's own flags alone) or VELOP_SINK_TARGET from the environment, defaults
# to crate, and defines sink_files().

SINK_TARGET="${VELOP_SINK_TARGET:-crate}"

_sink_rest=()
for _arg in "$@"; do
  case "$_arg" in
    --target=*) SINK_TARGET="${_arg#--target=}" ;;
    *)          _sink_rest+=("$_arg") ;;
  esac
done
set -- ${_sink_rest[@]+"${_sink_rest[@]}"}
unset _arg _sink_rest

case "$SINK_TARGET" in
  crate|postgres|all) ;;
  *) echo "error: --target must be crate, postgres or all (got '${SINK_TARGET}')" >&2; exit 2 ;;
esac

# Print one connector-config path per line for the selected target.
sink_files() {
  local f
  for f in "${HERE}"/velop-sink-*.json; do
    case "$f" in
      *-postgres.json) [[ "$SINK_TARGET" == crate ]] && continue ;;
      *)               [[ "$SINK_TARGET" == postgres ]] && continue ;;
    esac
    echo "$f"
  done
}
