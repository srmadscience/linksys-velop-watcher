# linksys-velop-watcher

A watcher that periodically downloads the `sysinfo.cgi` diagnostic dump from a
Linksys Velop mesh router and archives each snapshot in [CrateDB](https://crate.io/),
to study the router and track how its state changes over time.

Each run stores the full page as raw text in `velop.sysinfo` (the source of
truth) and also parses it into structured, snapshot-linked tables (devices, wlan
clients, backhaul, nodes, ping, radio stats/config, nic counters, system, lldp).
Every MAC address is annotated with its vendor via an **offline** OUI lookup —
MAC addresses never leave the network.

## How it works

```
cli.main() → fetch_sysinfo(cfg) → parse.* → enrich(...) → store_*  (CrateDB)
```

- **fetch** — the CGI streams its output slowly, so the fetcher reads the
  response as a stream and stops only when the `End of Sysinfo Output`
  completion marker appears, never on connection close alone.
- **parse** — pure, defensive parsers turn the raw text into `list[dict]`
  records. No network or DB; unit-tested against `sampleoutput.txt`.
- **name enrich** — the CGI `Name` column is truncated to ~16 chars and often
  blank, so each device is also looked up via the Velop's **JNAP**
  `GetDevices3` API (`/JNAP/`) and its untruncated `friendlyName` stored in
  `velop.device.friendly_name`. Best-effort: a JNAP failure just leaves the
  column NULL.
- **vendor enrich** — each MAC's 24-bit OUI is resolved against a local
  Wireshark `manuf` file, with results (including misses) cached in `velop.oui`.
- **store** — one `velop.sysinfo` row per snapshot plus the structured tables.
  CrateDB has no autoincrement, so each row gets a Python-generated UUID key.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env          # then edit .env (see below)
```

### Configuration

All runtime settings come from environment variables (see `.env.example`).
`.env` is gitignored — keep secrets there, not in source.

| Variable          | Purpose                                  | Default                          |
| ----------------- | ---------------------------------------- | -------------------------------- |
| `VELOP_URL`       | Router sysinfo endpoint                  | `https://10.13.1.1/sysinfo.cgi`  |
| `VELOP_USER`      | Router HTTP Basic user                   | `admin`                          |
| `VELOP_PASSWORD`  | Router password (**required**)           | —                                |
| `VELOP_VERIFY_TLS`| Verify the router's TLS cert             | `false` (self-signed cert)       |
| `VELOP_JNAP_URL`  | JNAP device-name endpoint (optional)     | derived from `VELOP_URL` (`/JNAP/`) |
| `CRATE_URL`       | CrateDB **HTTP** endpoint (port 4200)    | `http://localhost:4200`          |
| `CRATE_USER`      | CrateDB user                             | `crate`                          |
| `CRATE_PASSWORD`  | CrateDB password                         | —                                |
| `OUI_MANUF_PATH`  | Local Wireshark `manuf` file path        | `manuf`                          |
| `OUI_MANUF_URL`   | Where `velop-oui-update` downloads it    | Wireshark automated data URL     |

> CrateDB is reached over its **HTTP API (port 4200)** using the official
> `crate` client — *not* the PostgreSQL wire protocol.

## Running

```bash
set -a; source .env; set +a   # load .env into the environment
velop-oui-update              # one-time: fetch the Wireshark manuf vendor file
velop-watcher                 # fetch one snapshot and store it
```

The schema is created automatically on first run, so there is nothing to set up
by hand in CrateDB. A missing `manuf` file is not fatal — the vendor columns
just stay NULL until you run `velop-oui-update`.

### Convenience wrapper

`run-watcher.sh` exports all non-secret config and takes the **router password
as its first argument**; the CrateDB password is read from the gitignored
`.env`:

```bash
./run-watcher.sh 'your-router-password'
```

## Tests

```bash
pytest                       # all tests
pytest tests/test_fetch.py   # one file
```

The unit tests cover only pure logic (config, timestamp/marker parsing, and the
parsers against `sampleoutput.txt`). The network and database paths require a
live router and CrateDB and are not exercised by the tests.
