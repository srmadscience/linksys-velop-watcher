# Running velop-watcher as a service (Raspberry Pi / systemd)

The watcher fetches **one snapshot per run** and exits, so it runs as a systemd
`oneshot` **service driven by a timer** rather than a long-lived daemon. The
timer gives you periodic runs, per-run logs in journald, and one catch-up run
after the Pi has been off (`Persistent=true`).

| File | Purpose |
|---|---|
| `velop-watcher.service` | one-shot unit that runs `../run-watcher.sh` (template) |
| `velop-watcher.timer` | schedule — every `VELOP_INTERVAL` (default 5min) (template) |
| `velop-watcher.env.example` | secrets template → `/etc/velop-watcher/velop-watcher.env` |
| `install-service.sh` | sets up venv, secrets, units; enables the timer |
| `uninstall-service.sh` | removes the units |

The `.service`/`.timer` files are **templates** with `@PLACEHOLDERS@`; the
installer fills in the repo path, user, env-file path, and interval.

## Install

On the Pi, clone the repo and run the installer with sudo:

```bash
git clone https://github.com/srmadscience/linksys-velop-watcher.git
cd linksys-velop-watcher
sudo ./systemd/install-service.sh
```

It will:
1. create `.venv` and `pip install -e .` (as the service user), then assert the
   package imports (it aborts before enabling the timer if a broken wheel left
   the venv incomplete),
2. fetch the OUI `manuf` file if missing,
3. create `/etc/velop-watcher/velop-watcher.env` (chmod 600),
4. install + enable the timer.

Then **edit the secrets** and you're done:

```bash
sudo nano /etc/velop-watcher/velop-watcher.env   # set VELOP_PASSWORD
sudo systemctl start velop-watcher.service       # trigger one run now
```

### Options

```bash
sudo SERVICE_USER=pi VELOP_INTERVAL=10min ./systemd/install-service.sh
```

- `SERVICE_USER` — account the watcher runs as (default: the invoking sudo user,
  else the repo owner).
- `VELOP_INTERVAL` — any systemd time span (`5min`, `1h`, `30s`). Re-run the
  installer after changing it.

## Operating

```bash
systemctl list-timers velop-watcher.timer      # when it next runs
journalctl -u velop-watcher.service -f         # follow logs
journalctl -u velop-watcher.service -e         # last run's output
sudo systemctl start velop-watcher.service     # run one snapshot immediately
sudo systemctl disable --now velop-watcher.timer   # pause scheduling
```

## Notes

- **Secrets** live only in `/etc/velop-watcher/velop-watcher.env` (root-owned,
  0600) and reach the process via `EnvironmentFile=` — never on the command line,
  so they don't show up in `ps`. All non-secret settings stay hard-coded in
  `run-watcher.sh`.
- Each run produces Avro to Kafka for the `connect/` JDBC sinks (Kafka is the
  only sink). Make sure those sinks are installed (`connect/install-sinks.sh`)
  and the `velop.*` CrateDB tables exist (`crash < sql/velop_schema.sql`).
- Re-run `install-service.sh` after a `git pull` to pick up unit or interval
  changes; it won't overwrite your secrets file.
