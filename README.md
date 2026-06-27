# linksys-velop-watcher

A watcher that periodically downloads the `sysinfo.cgi` diagnostic dump from a
Linksys Velop mesh router and produces each snapshot to Kafka as
Confluent-Avro, to study the router and track how its state changes over time.
[Kafka Connect JDBC sinks](connect/) land the records in
[CrateDB](https://crate.io/).

Each run parses the dump into structured, snapshot-linked records (devices, wlan
clients, backhaul, nodes, ping, radio stats/config, nic counters, system, ip
neighbors, lldp) — one Kafka topic (`velop.<table>`) per table. Every MAC
address is annotated with its vendor via an **offline** OUI lookup — MAC
addresses never leave the network.

## How it works

```
cli.main() → fetch_sysinfo(cfg) → parse.* → enrich(...) → KafkaSink (Avro)
                                                              ↓
                              Kafka topics → Connect JDBC sinks → CrateDB
```

- **fetch** — the CGI streams its output slowly, so the fetcher reads the
  response as a stream and stops only when the `End of Sysinfo Output`
  completion marker appears, never on connection close alone.
- **parse** — pure, defensive parsers turn the raw text into `list[dict]`
  records. No network or DB; unit-tested against `sampleoutput.txt`.
- **name enrich** — the CGI `Name` column is truncated to ~16 chars and often
  blank, so each device is also looked up via the Velop's **JNAP**
  `GetDevices3` API (`/JNAP/`) and its untruncated `friendlyName` stored in
  `friendly_name`. Best-effort: a JNAP failure just leaves the column NULL.
- **vendor enrich** — each MAC's 24-bit OUI is resolved against a local
  Wireshark `manuf` file (offline, no cache DB needed).
- **produce** — each structured record is produced to its `velop.<table>` topic
  as Confluent-Avro; the schemas auto-register in the Schema Registry. A
  per-record `id` is the CrateDB primary key, so a Connect sink upsert never
  duplicates a row on re-delivery. See [`connect/`](connect/) for the sinks and
  `sql/velop_schema.sql` for the CrateDB DDL.

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

> **The defaults below are the author's home setup** — the router at
> `10.13.1.1` and Kafka/registry/CrateDB on hosts named `badger`/`endowment`.
> Change them to match your own network. Likewise, the `connect/*.json` sink
> configs ship `CHANGEME_CRATE_USER`/`CHANGEME_CRATE_PASSWORD` placeholders you
> must set (see [`connect/`](connect/)). The router password is **never** stored
> in the repo — it is read from `VELOP_PASSWORD` at runtime only.

| Variable          | Purpose                                  | Default                          |
| ----------------- | ---------------------------------------- | -------------------------------- |
| `VELOP_URL`       | Router sysinfo endpoint                  | `https://10.13.1.1/sysinfo.cgi`  |
| `VELOP_USER`      | Router HTTP Basic user                   | `admin`                          |
| `VELOP_PASSWORD`  | Router password (**required**)           | —                                |
| `VELOP_VERIFY_TLS`| Verify the router's TLS cert             | `false` (self-signed cert)       |
| `VELOP_JNAP_URL`  | JNAP device-name endpoint (optional)     | derived from `VELOP_URL` (`/JNAP/`) |
| `KAFKA_BOOTSTRAP` | Kafka broker(s)                          | `badger:9092`                    |
| `SCHEMA_REGISTRY_URL` | Confluent Schema Registry            | `http://badger:8081`             |
| `OUI_MANUF_PATH`  | Local Wireshark `manuf` file path        | `manuf`                          |
| `OUI_MANUF_URL`   | Where `velop-oui-update` downloads it    | Wireshark automated data URL     |

> The watcher only produces to Kafka. The [`connect/`](connect/) Kafka Connect
> JDBC sinks land the records in CrateDB over pg-wire; the `velop.*` tables must
> exist first (`crash < sql/velop_schema.sql`).

## Running

```bash
set -a; source .env; set +a   # load .env into the environment
velop-oui-update              # one-time: fetch the Wireshark manuf vendor file
velop-watcher                 # fetch one snapshot and produce it to Kafka
```

Create the CrateDB tables once (`crash < sql/velop_schema.sql`) and install the
Connect sinks (see [`connect/`](connect/)) so the produced records land in
CrateDB. A missing `manuf` file is not fatal — the vendor columns just stay NULL
until you run `velop-oui-update`.

### Convenience wrapper

`run-watcher.sh` exports all non-secret config and takes the **router password
as its first argument** (or the `VELOP_PASSWORD` env var):

```bash
./run-watcher.sh 'your-router-password'
```

To run it as a service on a Raspberry Pi, see [`systemd/`](systemd/).

## Tests

```bash
pytest                       # all tests
pytest tests/test_fetch.py   # one file
```

The unit tests cover only pure logic (config, timestamp/marker parsing, the
parsers against `sampleoutput.txt`, and the Avro spec/schema helpers). The
network and Kafka paths require a live router and broker and are not exercised
by the tests.
