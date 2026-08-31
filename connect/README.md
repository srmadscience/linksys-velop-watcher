# Kafka Connect JDBC sinks (Kafka → CrateDB / PostgreSQL)

These mirror the hcpy pattern: the watcher produces each structured table to its
own Kafka topic as Confluent-Avro (see `src/velop_watcher/kafka_sink.py`), and
one JDBC sink connector per topic lands the records in the database.

There are **two sets of connector configs**, one per target database. They are
identical apart from `connection.url` and the credential placeholders:

| Target | Files | Connector names | Database |
|---|---|---|---|
| CrateDB (default) | `velop-sink-<table>.json` | `crate-jdbc-sink-velop-*` | `endowment:5432/crate` (pg-wire) |
| PostgreSQL | `velop-sink-<table>-postgres.json` | `postgres-jdbc-sink-velop-*` | `endowment:5433/endowment_db` |

Because the connector names differ, both sets can run **at the same time**: each
gets its own consumer group and lands the same records in both databases.
`connect/sink-files.sh` is the shared `--target=` selector used by all three
helper scripts.

| Topic | Sink file (CrateDB / PostgreSQL) | Table |
|---|---|---|
| `velop.device` | `velop-sink-device.json` / `-postgres.json` | `velop.device` |
| `velop.wlan_client` | `velop-sink-wlan-client.json` / `-postgres.json` | `velop.wlan_client` |
| `velop.backhaul` | `velop-sink-backhaul.json` / `-postgres.json` | `velop.backhaul` |
| `velop.ping` | `velop-sink-ping.json` / `-postgres.json` | `velop.ping` |
| `velop.node` | `velop-sink-node.json` / `-postgres.json` | `velop.node` |
| `velop.radio_stats` | `velop-sink-radio-stats.json` / `-postgres.json` | `velop.radio_stats` |
| `velop.radio_config` | `velop-sink-radio-config.json` / `-postgres.json` | `velop.radio_config` |
| `velop.nic_counter` | `velop-sink-nic-counter.json` / `-postgres.json` | `velop.nic_counter` |
| `velop.system` | `velop-sink-system.json` / `-postgres.json` | `velop.system` |
| `velop.ip_neighbor` | `velop-sink-ip-neighbor.json` / `-postgres.json` | `velop.ip_neighbor` |
| `velop.lldp_neighbor` | `velop-sink-lldp-neighbor.json` / `-postgres.json` | `velop.lldp_neighbor` |

(The `sysinfo`/`node_sysinfo` raw_text dumps and the `oui` cache are not produced
to Kafka — see `kafka_sink.py`.)

## Producing

Kafka is the watcher's only sink, so just run it:

```bash
velop-watcher
# defaults: KAFKA_BOOTSTRAP=badger:9092  SCHEMA_REGISTRY_URL=http://badger:8081
```

The Avro value schemas register themselves under each `velop.<table>-value`
subject on the first produce, so run the watcher once before starting the sinks.

## Deploying the sinks

1. **Tables must exist first** — the sinks run `auto.create:false`, and the
   watcher no longer creates them (it only produces to Kafka). Apply the DDL
   once, for whichever database you are targeting:
   ```bash
   crash < sql/velop_schema.sql     # CrateDB (or psql, or the CrateDB admin UI)
   psql -h endowment -p 5433 -U scott -d endowment_db \
        -f sql/velop_schema_postgres.sql          # PostgreSQL
   ```
   Both are generated from `velop_watcher/schema.py`; see
   [`../sql/README_postgres.md`](../sql/README_postgres.md). On CrateDB the
   tables are **plain (not partitioned)**: the Confluent JDBC sink checks table
   existence via JDBC metadata, and an empty *partitioned* CrateDB table is
   invisible to that check (it fails with "table is missing").
2. **Register the connectors** with the helper script (idempotent — it PUTs each
   config, so re-running updates in place rather than 409-ing). Credentials come
   from the environment, never from the repo:
   ```bash
   CRATE_USER=... CRATE_PASSWORD=... ./connect/install-sinks.sh
   PG_USER=scott PG_PASSWORD=... ./connect/install-sinks.sh --target=postgres
   PG_USER=... CRATE_USER=... ...  ./connect/install-sinks.sh --target=all
   # CONNECT_URL defaults to http://badger:8083 if unset
   ```
3. **Check status** (connector + task states for the selected set):
   ```bash
   ./connect/status-sinks.sh
   ./connect/status-sinks.sh --target=postgres
   ```

### Restarting

After a CrateDB/Connect bounce or to clear FAILED tasks without re-applying config:

```bash
./connect/restart-sinks.sh                      # restart only FAILED connectors/tasks
./connect/restart-sinks.sh --all                # restart every connector + its tasks
./connect/restart-sinks.sh --target=postgres    # the PostgreSQL set
```

All three scripts honour `CONNECT_URL`, take `--target=crate|postgres|all`
(default `crate`, or `VELOP_SINK_TARGET` from the environment) and need
`curl` + `jq`.

## Caveats

- **Credentials are never committed.** `connection.user`/`connection.password`
  ship as `CHANGEME_CRATE_USER`/`CHANGEME_CRATE_PASSWORD` (CrateDB) and
  `CHANGEME_PG_USER`/`CHANGEME_PG_PASSWORD` (PostgreSQL). `install-sinks.sh`
  substitutes the environment variable of the same name and fails loudly if it
  is unset, so export `CRATE_USER`/`CRATE_PASSWORD` or `PG_USER`/`PG_PASSWORD`
  before registering. For a hands-off setup, swap them for a Connect
  `ConfigProvider` (e.g. `FileConfigProvider`) that reads an external file.
- **`OBJECT(IGNORED)` columns** (`node.devinfo`, `radio_stats.stats`,
  `radio_config.settings`, `lldp_neighbor.capabilities`) are produced as **JSON
  strings**. CrateDB coerces those into `OBJECT` on its own. PostgreSQL will
  *not* implicitly coerce `TEXT` to `JSONB`, so the PostgreSQL sink URLs carry
  **`?stringtype=unspecified`** — the driver then sends the parameter untyped
  and the server casts it. Remove that parameter and all four of those tables
  fail every row.
- **`ARRAY(TEXT)` columns** (`device.extra_macs`, `device.extra_macs_vendor`)
  are produced as **real Avro arrays** and land natively in both `ARRAY(TEXT)`
  and `TEXT[]`. There is one sharp edge: the JDBC sink binds a *non-empty* Avro
  array as a `varchar[]` parameter but stringifies an *empty* one to the literal
  `"[]"`. CrateDB accepts that (its array literals are JSON-shaped); PostgreSQL
  rejects it with `malformed array literal: "[]"` and — with
  `errors.tolerance=all` — silently drops the whole `device` row. `kafka_sink`
  therefore sends an empty array as `null`.

  **Backfilling `velop.device` into PostgreSQL** replays records produced
  *before* that fix, which still carry `[]`. To land them, register the device
  sink once with an SMT that drops the two array columns, let it catch up, then
  re-register from the checked-in config so new records keep their extra MACs:

  ```bash
  "transforms": "dropArrays",
  "transforms.dropArrays.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
  "transforms.dropArrays.exclude": "extra_macs,extra_macs_vendor"
  ```

  Reset the connector's offsets first (`PUT /connectors/<name>/stop`,
  `DELETE /connectors/<name>/offsets`, `PUT /connectors/<name>/resume`) so the
  whole topic replays.
- **Idempotency**: sinks use `insert.mode=upsert` with `pk.fields=id`. Each
  record's `id` is stamped once by the watcher (`kafka_sink.assign_ids`), so
  Kafka re-delivery upserts the same row rather than duplicating it. On
  PostgreSQL that becomes a real `ON CONFLICT (id) DO UPDATE` against the
  table's `PRIMARY KEY`.
