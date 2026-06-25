# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A watcher that periodically downloads the `sysinfo.cgi` diagnostic dump from a
Linksys Velop mesh router and **produces each snapshot to Kafka as
Confluent-Avro** (one topic per table), to study the router and track its state
over time. It parses the dump into structured, snapshot-linked records (devices,
wlan clients, backhaul, nodes, ping, radio stats/config, nic counters, system,
ip neighbors, lldp), with each MAC annotated with its vendor via an offline OUI
lookup. It also fetches each **satellite node's** sysinfo (radio counters are
local to each node) and tags their radios by source node so WiFi throughput
reflects the whole mesh, not just the master.

**Kafka is the only sink.** The watcher does not talk to CrateDB; the Kafka
Connect JDBC sinks in `connect/` land the records into the `velop.*` CrateDB
tables (which must pre-exist — `sql/velop_schema.sql`). There is no raw_text
archive any more (`velop.sysinfo`/`node_sysinfo` are gone): only the 11
structured tables are produced. (History: the project used to write to CrateDB
directly via the `crate` Python client; that path and the `VELOP_SINK`
crate/both modes were removed — the `crate` PyPI wheel is broken/empty on
piwheels, which made it unusable on the Raspberry Pi target.)

## Commands

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # install package + pytest

pytest                       # run all tests
pytest tests/test_fetch.py::test_parse_generated_at   # single test

set -a; source .env; set +a  # load config from .env
velop-oui-update             # fetch the Wireshark manuf file to OUI_MANUF_PATH (do this once)
velop-watcher                # fetch one snapshot and produce it to Kafka (also: python -m velop_watcher.cli)

./run-watcher.sh <pw>        # one-shot wrapper: hard-codes config, runs velop-watcher
sudo ./systemd/install-service.sh   # install as a Pi timer-driven service (see systemd/README.md)
```

The watcher does **one snapshot per run** and exits, so on a Pi it runs as a
systemd `oneshot` service triggered by a `.timer` (every `VELOP_INTERVAL`,
default 5min) — *not* a long-lived daemon. `run-watcher.sh` takes the router
password as `$1` **or** the `VELOP_PASSWORD` env var (the service supplies it via
an `EnvironmentFile` so the secret never appears in `ps`). See `systemd/` for the
units and installer (the installer pins PyPI over piwheels and asserts the venv
imports before enabling the timer).

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
  `fetch_sysinfo` delegates to `fetch_sysinfo_url(url, cfg)` so satellite nodes
  reuse the same slow-stream/marker logic; `node_sysinfo_url(cfg, ip)` swaps the
  host of `router_url` to a node's LAN IP (every node serves the CGI).
- `parse.py` — pure, defensive parsers that turn a snapshot's `raw_text` into
  structured `list[dict]` records (devices, wlan clients, backhaul, nodes, ping,
  radio stats/config, nic counters, system, ip neighbors, lldp). No network or
  DB; unit-tested against `sampleoutput.txt`. `parse_ip_neighbors` reads the
  `ip neigh:` block (the ARP cache — a point-in-time IP↔MAC map + liveness
  signal, **not** a DHCP lease) into one row per IP with family (inet/inet6),
  bridge (`br0` main LAN / `br1` guest / `br2` Smart Connect / `eth0` WAN), MAC (NULL for
  an unresolved `FAILED` entry), an `is_router` flag, and the cache state.
  `friendly_name_index` / `enrich_friendly_names`
  join a JNAP `GetDevices3` payload onto the device records (by UUID, then MAC)
  to fill `friendly_name` — the CGI `Name` column is capped at ~16 chars and
  often blank.
- `oui.py` — MAC→vendor resolution. `ManufDB` parses a local Wireshark `manuf`
  file; `VendorResolver` resolves each MAC's 24-bit OUI. `enrich()` adds a
  `*_vendor` field beside every MAC field before producing. `velop-oui-update`
  (→ `update_main`) downloads the manuf file. MAC addresses never leave the
  network. (`VendorResolver` still has a dormant DB-cache path that takes a
  DBAPI `conn`; `cli` always passes `None`, so lookups resolve straight from the
  manuf file. The old `velop.oui` cache table is gone with the crate path.)
- `schema.py` — **single source of truth** for the 11 structured tables'
  CrateDB column order and types (`TABLES`). `kafka_sink.TABLE_SPECS` must mirror
  it (asserted by `test_specs_match_schema_columns`), and `schema_sql()`
  generates `sql/velop_schema.sql` (run `python -m velop_watcher.schema`). There
  is no Python that writes to CrateDB; the tables are created from that SQL and
  filled by the Connect sinks. (Replaced the old `store.py`.)
- `kafka_sink.py` — the only sink: produces each structured table to its own
  Kafka topic (`velop.<table>`) as Confluent-Avro, mirroring hcpy's
  `hc2kafka.py`. `TABLE_SPECS` declares one topic/Avro schema per table (column
  order mirrors `schema.TABLES`; a `test_kafka_sink` test asserts no drift).
  `OBJECT(IGNORED)` columns are sent as JSON strings, but `ARRAY(TEXT)` columns
  (only `device.extra_macs`/`extra_macs_vendor`) are sent as real Avro arrays
  (kind `array`) — a JSON string lands as TEXT and CrateDB rejects TEXT→ARRAY, so
  with the sink's `errors.tolerance=all` every device row was silently dropped.
  `fetched_at` is Avro `timestamp-millis`. `assign_ids` stamps each record's `id` up front (the
  CrateDB primary key) so a Connect sink upsert is stable on re-delivery.
  `confluent_kafka` is imported lazily inside `KafkaSink`. The matching JDBC
  sink connectors live in `connect/` (see `connect/README.md`), with helper
  scripts `connect/install-sinks.sh` (idempotent register/update via
  `PUT /connectors/<name>/config`), `connect/restart-sinks.sh` (restart
  connectors+tasks; FAILED-only or `--all`), and `connect/status-sinks.sh` — all
  honour `CONNECT_URL` (default `http://badger:8083`) and need `curl` + `jq`.
- `cli.py` — wires fetch → parse → enrich → produce for a single run. The JNAP
  name fetch is **best-effort**: a network/auth failure logs a note and leaves
  `friendly_name` NULL rather than losing the snapshot. After the master dump it
  discovers satellites from `parse_nodes` (role=slave + ip) and fetches each one's
  sysinfo for its radios — also **best-effort** per node (an offline node is
  skipped, not fatal). Fetches are **sequential**, so wall-clock time scales with
  node count (the CGI is slow).

Data flow: `cli.main()` → `fetch_sysinfo(cfg)` (master) → `parse.*` →
per-node `fetch_sysinfo_url(...)` + `parse_radio_stats` + `tag_radio_source`
(satellites) → `enrich_friendly_names(...)` (JNAP) → `enrich(...)` (OUI) →
`assign_ids(...)` → `KafkaSink.produce(...)` (Confluent-Avro, one topic per
table). The Connect JDBC sinks in `connect/` carry the records into the
`velop.*` CrateDB tables.

## Key facts and gotchas

- **The router password is never committed or hard-coded.** It is read from
  `VELOP_PASSWORD` at runtime. Keep it out of source, tests, and memory.
- The router has a **self-signed TLS cert**, so `verify_tls` defaults to `False`
  (TLS warnings are suppressed). Auth is HTTP Basic.
- **CrateDB is never reached by the watcher.** Records get there only via the
  Kafka Connect JDBC sinks (`connect/`), over pg-wire (port 5432). The `velop.*`
  tables must pre-exist — apply `sql/velop_schema.sql` (generated from
  `schema.py`). The sinks run `auto.create=false` and `insert.mode=upsert` on
  `id`. CrateDB lacks transactions/autoincrement, so the schema gives each row a
  Python-generated UUID `id` and there is no multi-row atomicity.
- The completion marker is matched as a substring; in real output it appears as
  `**************** End of Sysinfo Output ******************`.
- `sampleoutput.txt` is a full real dump (~4800 lines) — the reference for the
  page format and any future parsing work.
- **Kafka/Avro is the only sink.** The 11 structured tables are produced to
  `badger:9092` as Confluent-Avro (schema registry `http://badger:8081`);
  `connect/` holds one JDBC sink per topic that lands them in CrateDB over
  pg-wire (5432). Records carry an `id` (`assign_ids`) and sinks `upsert` on it,
  so Kafka re-delivery never duplicates rows. Needs `confluent-kafka` (a core
  dependency now: `pip install -e .`).
  Two caveats: `OBJECT(IGNORED)` columns are produced as JSON strings (verify the
  JDBC sink lands JSON→`OBJECT`, esp. `radio_stats.stats` which the Grafana views
  read) while `ARRAY(TEXT)` columns are produced as real Avro arrays (kind
  `array`) so they land natively — sending those as JSON strings instead made the
  JDBC sink drop every `device` row (TEXT→ARRAY is rejected, and
  `errors.tolerance=all` swallows it); and `connect/*.json` carry `scott`/`tiger` CrateDB creds (matching hcpy)
  — externalize via a Connect `ConfigProvider` if that matters.
- **Radio counters are per-node; a radio's identity is (node, band, radio).**
  `radio_stats` holds `wifi0/1/2` from every mesh node — names collide across
  nodes and bands differ by model (master MX42 vs satellite WHW03: master
  `wifi1`=2.4G but satellite `wifi0`=2.4G). So `velop.radio_stats` rows carry
  `source_node_mac` and the rate views (`v_radio_rates`, `v_wifi_vs_wired`)
  self-join on `source_node_mac` + band + radio, `COALESCE(source_node_mac,
  'master')` so legacy untagged rows stay joinable. WiFi-vs-wired now sums all
  nodes' radios for true mesh WiFi. The `source_node_*` columns are part of the
  base schema (`sql/velop_schema.sql`); **re-run the `CREATE OR REPLACE VIEW`s in
  the Crate UI after deploying** — views are not auto-updated.
- **The dump does NOT contain real DHCP leases.** `/tmp/dnsmasq.leases` (lease
  expiry, DHCP client-id, DHCP-supplied hostname) appears only as an `lsof`
  open-fd reference, never its contents. `velop.ip_neighbor` (from `ip neigh:`)
  is the closest per-IP source — the ARP cache, i.e. a point-in-time IP↔MAC map
  + reachability state, not a lease. True lease data would need a JNAP call.
- **OUI vendor lookups are offline.** They come from a local Wireshark `manuf`
  file (fetched by `velop-oui-update`), resolved by the 24-bit OUI, and never
  send MAC addresses off-network. A missing manuf file is not fatal — vendor
  columns just stay NULL. Resolution keys on 24 bits, so the longer IEEE
  MA-M/MA-S blocks are not distinguished.
- Unit tests cover only pure logic (config, timestamp/marker parsing, parsers,
  Avro spec/schema helpers). The network and Kafka paths require a live router
  and broker and are not tested.
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
