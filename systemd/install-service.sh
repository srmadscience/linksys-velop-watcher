#!/usr/bin/env bash
#
# Install velop-watcher as a systemd timer-driven service on a Raspberry Pi
# (or any systemd Linux host). Run from a clone of this repo with sudo:
#
#   sudo ./systemd/install-service.sh
#
# It is idempotent — safe to re-run after a `git pull` to pick up unit changes.
#
# Overridable via environment:
#   SERVICE_USER=pi          who runs the watcher (default: invoking sudo user,
#                            else the owner of the repo)
#   VELOP_INTERVAL=10min     how often to fetch (systemd time span; default 10min)
#
# What it does:
#   1. creates the project virtualenv + installs the package (kafka extra)
#   2. fetches the OUI manuf file if missing (offline vendor lookups)
#   3. creates /etc/velop-watcher/velop-watcher.env (chmod 600) for the secrets
#   4. renders + installs the .service and .timer units
#   5. enables and starts the timer
#
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "error: run with sudo (needs to write ${UNIT_DIR:-/etc/systemd/system})" >&2
  exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${HERE}/.." && pwd)"

SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-$(stat -c '%U' "${REPO_DIR}")}}"
INTERVAL="${VELOP_INTERVAL:-10min}"
ENVDIR=/etc/velop-watcher
ENVFILE="${ENVDIR}/velop-watcher.env"
UNIT_DIR=/etc/systemd/system

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "error: user '$SERVICE_USER' does not exist (set SERVICE_USER=...)" >&2
  exit 1
fi

echo "Repo:     $REPO_DIR"
echo "User:     $SERVICE_USER"
echo "Interval: $INTERVAL"
echo

# 1. virtualenv + package (as the service user, never root)
#    PIP_INDEX_URL pins PyPI: Raspberry Pi OS adds piwheels in /etc/pip.conf,
#    which has served empty/broken wheels for some pure-Python packages.
echo "Creating virtualenv + installing package as ${SERVICE_USER}..."
sudo -u "$SERVICE_USER" PIP_INDEX_URL="https://pypi.org/simple" bash -c "
  cd '${REPO_DIR}' &&
  python3 -m venv .venv &&
  .venv/bin/pip install --upgrade pip &&
  .venv/bin/pip install --no-cache-dir -e .
"

# 1a. Assert the package actually imports before we enable anything -- a broken
#     wheel (empty install) must fail loudly here, not at the first timer tick.
echo "Verifying imports..."
if ! sudo -u "$SERVICE_USER" "${REPO_DIR}/.venv/bin/python" \
     -c "import velop_watcher.cli, requests, confluent_kafka" 2>/tmp/velop-import-err; then
  echo "error: the venv is missing required modules:" >&2
  cat /tmp/velop-import-err >&2
  echo "Fix the install (see README) before re-running; the timer was NOT enabled." >&2
  exit 1
fi
echo "  imports ok (velop_watcher, requests, confluent_kafka)"

# 2. OUI manuf file (best-effort; a missing file just means NULL vendor columns)
if [[ ! -f "${REPO_DIR}/manuf" ]]; then
  echo "Fetching OUI manuf file..."
  sudo -u "$SERVICE_USER" bash -c \
    "cd '${REPO_DIR}' && OUI_MANUF_PATH='${REPO_DIR}/manuf' .venv/bin/velop-oui-update" \
    || echo "warning: velop-oui-update failed; vendor columns stay NULL until it runs"
fi

# 3. secrets EnvironmentFile
mkdir -p "$ENVDIR"
if [[ ! -f "$ENVFILE" ]]; then
  install -m 600 "${HERE}/velop-watcher.env.example" "$ENVFILE"
  echo "Created ${ENVFILE} from template."
  NEEDS_SECRETS=1
else
  echo "${ENVFILE} already exists — leaving it untouched"
  chmod 600 "$ENVFILE"
fi

# 4. render + install units
for unit in velop-watcher.service velop-watcher.timer; do
  sed -e "s|@REPO_DIR@|${REPO_DIR}|g" \
      -e "s|@USER@|${SERVICE_USER}|g" \
      -e "s|@ENVFILE@|${ENVFILE}|g" \
      -e "s|@INTERVAL@|${INTERVAL}|g" \
      "${HERE}/${unit}" > "${UNIT_DIR}/${unit}"
  echo "Installed ${UNIT_DIR}/${unit}"
done

# 5. enable + start the timer
systemctl daemon-reload
systemctl enable --now velop-watcher.timer

echo
echo "=== Timer ==="
systemctl list-timers --no-pager velop-watcher.timer || true

if [[ "${NEEDS_SECRETS:-0}" == "1" ]]; then
  echo
  echo "!! ACTION REQUIRED: edit ${ENVFILE} and set VELOP_PASSWORD + CRATE_PASSWORD."
  echo "   Until then the service will fail. After editing, no restart is needed —"
  echo "   the next timer tick (or 'sudo systemctl start velop-watcher.service') picks it up."
fi

echo
echo "Useful commands:"
echo "  sudo systemctl start velop-watcher.service     # run one snapshot now"
echo "  journalctl -u velop-watcher.service -f         # follow logs"
echo "  systemctl list-timers velop-watcher.timer      # next scheduled run"
