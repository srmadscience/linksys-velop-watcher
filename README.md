# linksys-velop-watcher

Fetches the `sysinfo.cgi` diagnostic page from a Linksys Velop mesh router and
archives each snapshot as raw text in [CrateDB](https://crate.io/) (which speaks
the PostgreSQL wire protocol). The goal is to understand the router's behaviour
and track how its state changes over time.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env   # then edit .env
```

## Running

Configuration comes from environment variables (see `.env.example`). The router
password must be supplied via `VELOP_PASSWORD` — it is never stored in the repo.

```bash
set -a; source .env; set +a   # load .env into the environment
velop-watcher                 # fetch one snapshot and store it
```

Each run downloads the full (slowly rendered) page, waiting until the
`End of Sysinfo Output` marker appears, then inserts one row into the
`velop.sysinfo` table.

## Tests

```bash
pytest                       # all tests
pytest tests/test_fetch.py   # one file
```

The network and database paths are not exercised by the unit tests; only pure
logic (config parsing, timestamp parsing, marker detection) is covered.
