# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A watcher that periodically downloads the `sysinfo.cgi` diagnostic dump from a
Linksys Velop mesh router and archives each snapshot in CrateDB, to study the
router and track its state over time. It stores the full page as raw text in
`velop.sysinfo` (the source of truth) and also parses it into structured,
snapshot-linked tables (devices, wlan clients, backhaul, nodes, ping, radio
stats/config, nic counters, system, lldp), with each MAC annotated with its
vendor via an offline OUI lookup.

## Commands

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # install package + pytest

pytest                       # run all tests
pytest tests/test_fetch.py::test_parse_generated_at   # single test

set -a; source .env; set +a  # load config from .env
velop-oui-update             # fetch the Wireshark manuf file to OUI_MANUF_PATH (do this once)
velop-watcher                # fetch one snapshot and store it (also: python -m velop_watcher.cli)
```

## Architecture

`src/velop_watcher/` (src layout — the package is only importable after an
editable install or with `src` on `PYTHONPATH`):

- `config.py` — `Config` dataclass built solely from environment variables via
  `Config.from_env()`. **All** runtime settings live here, including secrets.
- `fetch.py` — fetches and parses the page. The CGI **streams output slowly**,
  so `fetch_sysinfo` reads the response as a stream and `read_until_marker`
  stops only when the `End of Sysinfo Output` completion marker appears — never
  on connection close alone. `parse_generated_at` extracts the router's own
  `page generated on …` timestamp (router reports UTC). `fetch_jnap_devices`
  POSTs the **JNAP** `GetDevices3` action (`/JNAP/`, JSON API, auth via the
  `X-JNAP-Authorization` Basic header) to get untruncated device names; the
  endpoint is derived from `router_url` unless `VELOP_JNAP_URL` overrides it.
- `parse.py` — pure, defensive parsers that turn a snapshot's `raw_text` into
  structured `list[dict]` records (devices, wlan clients, backhaul, nodes, ping,
  radio stats/config, nic counters, system, lldp). No network or DB; unit-tested
  against `sampleoutput.txt`. `friendly_name_index` / `enrich_friendly_names`
  join a JNAP `GetDevices3` payload onto the device records (by UUID, then MAC)
  to fill `friendly_name` — the CGI `Name` column is capped at ~16 chars and
  often blank.
- `oui.py` — MAC→vendor resolution. `ManufDB` parses a local Wireshark `manuf`
  file; `VendorResolver` resolves each MAC's 24-bit OUI, caching results (incl.
  NULL misses) in `velop.oui`. `enrich()` adds a `*_vendor` field beside every
  MAC field before storage. `velop-oui-update` (→ `update_main`) downloads the
  manuf file. MAC addresses never leave the network.
- `store.py` — CrateDB persistence. One `velop.sysinfo` row per snapshot plus the
  structured tables and `velop.oui`; CrateDB has no autoincrement so each row
  gets a Python-generated UUID primary key.
- `cli.py` — wires fetch → parse → enrich → store for a single run. The JNAP
  name fetch is **best-effort**: a network/auth failure logs a note and leaves
  `friendly_name` NULL rather than losing the snapshot.

Data flow: `cli.main()` → `fetch_sysinfo(cfg)` → `parse.*` →
`enrich_friendly_names(...)` (JNAP) → `enrich(...)` (OUI) →
`store_sysinfo(...)` + `store_tier1(...)` into `velop.sysinfo` and the
structured tables.

## Key facts and gotchas

- **The router password is never committed or hard-coded.** It is read from
  `VELOP_PASSWORD` at runtime. Keep it out of source, tests, and memory.
- The router has a **self-signed TLS cert**, so `verify_tls` defaults to `False`
  (TLS warnings are suppressed). Auth is HTTP Basic.
- **CrateDB is reached over HTTP (port 4200)** using the official `crate`
  client — *not* the PostgreSQL wire protocol. Connection comes from
  `CRATE_URL` / `CRATE_USER` / `CRATE_PASSWORD`. The client uses the **qmark
  paramstyle** (`?`, not `%s`). CrateDB does not support real transactions and
  lacks autoincrement — keep DDL/SQL within that subset. It also has **no
  `ADD COLUMN IF NOT EXISTS`**; `store.MIGRATIONS` runs a plain `ALTER TABLE …
  ADD COLUMN` and swallows the "already has a column" error to stay idempotent.
- The completion marker is matched as a substring; in real output it appears as
  `**************** End of Sysinfo Output ******************`.
- `sampleoutput.txt` is a full real dump (~4800 lines) — the reference for the
  page format and any future parsing work.
- **OUI vendor lookups are offline.** They come from a local Wireshark `manuf`
  file (fetched by `velop-oui-update`), are cached in `velop.oui` keyed by the
  24-bit OUI, and never send MAC addresses off-network. A missing manuf file is
  not fatal — vendor columns just stay NULL. The cache keys on 24 bits, so the
  longer IEEE MA-M/MA-S blocks are not distinguished.
- Unit tests cover only pure logic (config, timestamp/marker parsing). The
  network and DB paths require a live router and CrateDB and are not tested.
- **Grafana's PostgreSQL datasource silently drops result columns whose pg
  type it can't convert — most commonly `NUMERIC` (OID 1700) — returning an
  empty frame (HTTP 200, zero rows, no error) for SQL that runs fine
  everywhere else.** The fault is in Grafana's frame converter, *not* CrateDB's
  pg-wire path: the same query returns its rows correctly over the HTTP endpoint
  (4200), over `psql` (simple protocol), and over `asyncpg` (extended
  Parse/Bind/Execute). The usual trigger is the **two-argument
  `ROUND(value, scale)`**, which CrateDB types as `NUMERIC`. **Fix: cast every
  computed numeric column to `DOUBLE PRECISION`** (`ROUND(…, 4)::DOUBLE`,
  float8/OID 701) or `REAL` before it leaves the query. (Query *shape* —
  window functions, multi-CTE/multi-join, `TIMESTAMP`-equality joins — is *not*
  the problem; all of those work over pg-wire. Keeping panel queries flat and
  pushing logic into a view is still good practice, but only the column-type
  cast is load-bearing.) Filter on epoch-ms (`fetched_at::BIGINT` vs Grafana's
  `${__from}`/`${__to}`) rather than timestamp literals (which the pg session
  timezone can shift). See `sql/grafana_radio_rates.sql` for the worked example
  (`velop.v_radio_rates` + its flat panel queries).
