# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A watcher that periodically downloads the `sysinfo.cgi` diagnostic dump from a
Linksys Velop mesh router and archives each snapshot in CrateDB, to study the
router and track its state over time. Currently it stores the page as raw text;
parsing the dump into structured columns is a planned future goal.

## Commands

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # install package + pytest

pytest                       # run all tests
pytest tests/test_fetch.py::test_parse_generated_at   # single test

set -a; source .env; set +a  # load config from .env
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
  `page generated on …` timestamp (router reports UTC).
- `store.py` — CrateDB persistence. One `velop.sysinfo` row per snapshot; CrateDB has
  no autoincrement so each row gets a Python-generated UUID primary key.
- `cli.py` — wires fetch → store for a single run.

Data flow: `cli.main()` → `fetch_sysinfo(cfg)` → `store_sysinfo(...)` into the
`velop.sysinfo` table.

## Key facts and gotchas

- **The router password is never committed or hard-coded.** It is read from
  `VELOP_PASSWORD` at runtime. Keep it out of source, tests, and memory.
- The router has a **self-signed TLS cert**, so `verify_tls` defaults to `False`
  (TLS warnings are suppressed). Auth is HTTP Basic.
- **CrateDB is reached over HTTP (port 4200)** using the official `crate`
  client — *not* the PostgreSQL wire protocol. Connection comes from
  `CRATE_URL` / `CRATE_USER` / `CRATE_PASSWORD`. The client uses the **qmark
  paramstyle** (`?`, not `%s`). CrateDB does not support real transactions and
  lacks autoincrement — keep DDL/SQL within that subset.
- The completion marker is matched as a substring; in real output it appears as
  `**************** End of Sysinfo Output ******************`.
- `sampleoutput.txt` is a full real dump (~4800 lines) — the reference for the
  page format and any future parsing work.
- Unit tests cover only pure logic (config, timestamp/marker parsing). The
  network and DB paths require a live router and CrateDB and are not tested.
