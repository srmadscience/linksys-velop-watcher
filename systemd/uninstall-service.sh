#!/usr/bin/env bash
#
# Remove the velop-watcher systemd timer + service. Run with sudo:
#   sudo ./systemd/uninstall-service.sh
#
# Leaves the secrets file (/etc/velop-watcher/velop-watcher.env), the repo, and
# the virtualenv in place — delete those by hand if you want a clean slate.
#
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "error: run with sudo" >&2
  exit 1
fi

UNIT_DIR=/etc/systemd/system

systemctl disable --now velop-watcher.timer 2>/dev/null || true
systemctl stop velop-watcher.service 2>/dev/null || true

rm -f "${UNIT_DIR}/velop-watcher.timer" "${UNIT_DIR}/velop-watcher.service"
systemctl daemon-reload

echo "Removed velop-watcher.timer and velop-watcher.service."
echo "Kept /etc/velop-watcher/velop-watcher.env (delete manually if desired)."
