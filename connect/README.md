# Kafka Connect JDBC sinks (Kafka → CrateDB)

These mirror the hcpy pattern: the watcher produces each structured table to its
own Kafka topic as Confluent-Avro (see `src/velop_watcher/kafka_sink.py`), and
one JDBC sink connector per topic lands the records in CrateDB over pg-wire.

| Topic | Sink file | CrateDB table |
|---|---|---|
| `velop.device` | `velop-sink-device.json` | `velop.device` |
| `velop.wlan_client` | `velop-sink-wlan-client.json` | `velop.wlan_client` |
| `velop.backhaul` | `velop-sink-backhaul.json` | `velop.backhaul` |
| `velop.ping` | `velop-sink-ping.json` | `velop.ping` |
| `velop.node` | `velop-sink-node.json` | `velop.node` |
| `velop.radio_stats` | `velop-sink-radio-stats.json` | `velop.radio_stats` |
| `velop.radio_config` | `velop-sink-radio-config.json` | `velop.radio_config` |
| `velop.nic_counter` | `velop-sink-nic-counter.json` | `velop.nic_counter` |
| `velop.system` | `velop-sink-system.json` | `velop.system` |
| `velop.ip_neighbor` | `velop-sink-ip-neighbor.json` | `velop.ip_neighbor` |
| `velop.lldp_neighbor` | `velop-sink-lldp-neighbor.json` | `velop.lldp_neighbor` |

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
   once:
   ```bash
   crash < sql/velop_schema.sql     # or psql, or paste into the CrateDB admin UI
   ```
   (`sql/velop_schema.sql` is generated from `velop_watcher/schema.py`.) The
   tables are **plain (not partitioned)**: the Confluent JDBC sink checks table
   existence via JDBC metadata, and an empty *partitioned* CrateDB table is
   invisible to that check (it fails with "table is missing").
2. **Register the connectors** with the helper script (idempotent — it PUTs each
   config, so re-running updates in place rather than 409-ing):
   ```bash
   CONNECT_URL=http://<connect-host>:8083 ./connect/install-sinks.sh
   # CONNECT_URL defaults to http://badger:8083 if unset
   ```
3. **Check status** (connector + task states for all sinks):
   ```bash
   ./connect/status-sinks.sh
   ```

### Restarting

After a CrateDB/Connect bounce or to clear FAILED tasks without re-applying config:

```bash
./connect/restart-sinks.sh          # restart only FAILED connectors/tasks
./connect/restart-sinks.sh --all    # restart every connector + its tasks
```

All three scripts honour `CONNECT_URL` and need `curl` + `jq`.

## Caveats

- **Credentials**: `connection.user`/`connection.password` are `scott`/`tiger`
  here to match the existing hcpy sinks against the same CrateDB. If you want to
  keep secrets out of the repo, swap them for a Connect `ConfigProvider`
  (e.g. `FileConfigProvider`) and reference an external file.
- **OBJECT/ARRAY columns** (`node.devinfo`, `radio_stats.stats`,
  `radio_config.settings`, `lldp_neighbor.capabilities`, `device.extra_macs*`)
  are produced as **JSON strings**. Confirm the JDBC sink lands a JSON string
  into the CrateDB `OBJECT`/`ARRAY` column as expected (test on
  `velop.radio_stats` first, since its `stats` object is read by the Grafana
  views). If CrateDB rejects the string, those columns may need to be `TEXT` in
  the sink target, or `cast` via an SMT.
- **Idempotency**: sinks use `insert.mode=upsert` with `pk.fields=id`. Each
  record's `id` is stamped once by the watcher (`kafka_sink.assign_ids`), so
  Kafka re-delivery upserts the same row rather than duplicating it.
