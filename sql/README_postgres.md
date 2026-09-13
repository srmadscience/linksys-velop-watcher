# PostgreSQL versions of the `sql/` DDL

**These are the files the live deployment uses.** PostgreSQL at
`endowment:5433/endowment_db` is the pipeline's target; the CrateDB sinks were
retired on 2026-09-13.

Every `sql/<name>.sql` targets CrateDB; its stock-PostgreSQL translation sits
beside it as `sql/<name>_postgres.sql`. Same tables, same views, same column
names and semantics — only the CrateDB-specific SQL differs. The CrateDB twins
are kept for anyone running against Crate; the watcher itself is unaffected
either way, since it only produces to Kafka.

| File | What it creates |
| --- | --- |
| [`velop_schema_postgres.sql`](velop_schema_postgres.sql) | schema, helper functions, the 11 structured tables, indexes — **apply first** |
| [`grafana_radio_rates_postgres.sql`](grafana_radio_rates_postgres.sql) | `velop.v_radio_rates` |
| [`grafana_nic_rates_postgres.sql`](grafana_nic_rates_postgres.sql) | `velop.v_nic_rates` |
| [`grafana_wifi_vs_wired_postgres.sql`](grafana_wifi_vs_wired_postgres.sql) | `velop.v_wifi_vs_wired` |
| [`grafana_node_wifi_postgres.sql`](grafana_node_wifi_postgres.sql) | `velop.v_node_wifi_rates` |
| [`grafana_ip_neighbors_postgres.sql`](grafana_ip_neighbors_postgres.sql) | `velop.v_ip_neighbor` |
| [`grafana_device_wlan_postgres.sql`](grafana_device_wlan_postgres.sql) | no view — panel queries only, so applying it is a no-op |

Every file is safe to run with `psql -f`: the DDL is `CREATE ... IF NOT EXISTS` /
`CREATE OR REPLACE`, and the Grafana panel queries at the bottom of each file are
commented out (they contain `${__from}`/`${__to}`, which are Grafana macros, not
SQL). Copy a panel query into Grafana and strip the leading `-- `.

```bash
psql -h endowment -p 5433 -U scott -d endowment_db -f sql/velop_schema_postgres.sql
psql -h endowment -p 5433 -U scott -d endowment_db -f sql/grafana_nic_rates_postgres.sql
```

`velop_schema_postgres.sql` is **generated** from `velop_watcher/schema.py`, the
same source as the CrateDB DDL (a test asserts both checked-in files are
current):

```bash
python -m velop_watcher.schema --dialect postgres > sql/velop_schema_postgres.sql
```

The `grafana_*_postgres.sql` views are hand-maintained — change one when you
change its CrateDB twin (`grafana_*.sql`), and vice versa.

They are **not** line-for-line translations in one respect: every rate view here
derives the previous snapshot with a `LAG()` **window function**, where the
CrateDB version self-joins (`JOIN ... ON b.t_ms < a.t_ms` with `MAX(b.t_ms)`).
PostgreSQL plans that self-join as a quadratic merge join — at a few tens of
thousands of rows it already materialises millions of intermediate rows and a
full scan of the view never finishes, so a Grafana panel just times out. With
`LAG()` all five views scan the whole history in a few seconds. Output columns
and semantics are unchanged; each view's header spells out the one edge case
(rows tied on `t_ms` within a partition are dropped rather than skipped over,
which a one-row-per-series snapshot never produces).

## How the translation works

| CrateDB | PostgreSQL |
| --- | --- |
| `DOUBLE` | `DOUBLE PRECISION` |
| `ARRAY(TEXT)` | `TEXT[]` (`device.extra_macs`, `extra_macs_vendor`) |
| `OBJECT(IGNORED)` | `JSONB` (`node.devinfo`, `radio_stats.stats`, …) |
| `fetched_at::BIGINT` (epoch-ms) | `velop.epoch_ms(fetched_at)` |
| `stats['tx_data_bytes']` | `stats->>'tx_data_bytes'` |
| `TRY_CAST(x AS BIGINT)` | `velop.try_bigint(x)` (also `try_double`) |
| everything indexed by default | explicit indexes (`PG_INDEXES` in `schema.py`) |

The three helper functions are created by `velop_schema.sql`, so apply it before
any view. `epoch_ms` is `IMMUTABLE`, so it can be indexed if a panel needs it.

## Gotchas that carry over — and one that gets worse

**Grafana still drops `NUMERIC`.** The empty-frame bug documented in
[`grafana_radio_rates.sql`](grafana_radio_rates.sql) and `CLAUDE.md` lives
in Grafana's PostgreSQL frame converter, not in CrateDB, so it hits stock
PostgreSQL exactly the same way: a result column typed `NUMERIC` (OID 1700) comes
back as zero rows with HTTP 200 and no error. Every computed numeric column here
is cast `::DOUBLE PRECISION`.

PostgreSQL makes that cast *mandatory* rather than merely advisable, in two ways:

- Two-argument `ROUND(value, scale)` is **`NUMERIC`-only** — there is no
  `round(double precision, integer)` — so the rounded value genuinely is
  `NUMERIC` and must be cast back out.
- Plain division of integers by a literal (`(cur_ms - prev_ms) / 1000.0`) yields
  `NUMERIC` too, where CrateDB typed it `DOUBLE`. That is why
  `v_nic_rates.elapsed_secs` carries a cast here and not in the CrateDB version.

A quick way to check a view before wiring a panel to it:

```sql
SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
WHERE n.nspname = 'velop' AND c.relname = 'v_radio_rates';
```

Nothing in the output should say `numeric`.

## Landing the records: the PostgreSQL Connect sinks

`connect/velop-sink-<table>-postgres.json` are the PostgreSQL twins of the
CrateDB sinks, and `connect/install-sinks.sh --target=postgres` registers them.
They can run at the same time as the CrateDB set — different connector names,
different consumer groups, same topics — so both databases stay populated. See
[`../connect/README.md`](../connect/README.md) for the operational side; the two
column types that need care are:

- **`OBJECT(IGNORED)` → `JSONB`.** Those columns are produced as JSON *strings*
  (see `kafka_sink`), and PostgreSQL will not implicitly coerce `TEXT` to
  `JSONB` on insert. The sink URLs carry **`?stringtype=unspecified`** so the
  driver sends them untyped and the server casts them. Drop that parameter and
  every row in `node`, `radio_stats`, `radio_config` and `lldp_neighbor` fails.
- **`ARRAY(TEXT)` → `TEXT[]`.** `device.extra_macs`/`extra_macs_vendor` are real
  Avro arrays and land natively — but only because `kafka_sink` sends an
  **empty** array as `null`. The JDBC sink binds a non-empty Avro array as a
  `varchar[]` parameter and stringifies an empty one to the literal `"[]"`,
  which PostgreSQL rejects (`malformed array literal: "[]"`). CrateDB accepts
  `[]` — its array literals are JSON-shaped — which is why this only ever showed
  up here. Records produced *before* that fix still carry `[]`; backfilling them
  needs the `ReplaceField` workaround in `connect/README.md`.
