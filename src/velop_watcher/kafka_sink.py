"""Produce the parsed structured records to Kafka as Confluent-Avro.

Mirrors hcpy's ``hc2kafka.py`` pattern: each record is serialized with a
Confluent ``AvroSerializer`` (schema registered in the Schema Registry) and
produced to Kafka, where a JDBC sink connector lands it in CrateDB. Unlike hcpy
(one record shape) the watcher has one shape per structured table, so this
module declares one topic + Avro schema + serializer per table.

Design notes:
- One topic per table, ``<kafka_topic_prefix><table>`` (e.g. ``velop.device``);
  key is the snapshot id so all of a snapshot's rows for a table share a key.
- Every record carries ``id`` (its CrateDB primary key, generated up front so a
  Connect JDBC sink upsert is stable on re-delivery -- see ``cli``/``assign_ids``),
  ``snapshot_id`` and ``fetched_at`` (Avro ``timestamp-millis``).
- CrateDB ``OBJECT(IGNORED)`` columns (``devinfo``/``stats``/``settings``/
  ``capabilities``) and ``ARRAY(TEXT)`` columns are encoded as JSON strings --
  Avro/JDBC has no clean dynamic-object mapping. The matching JDBC sink lands
  the JSON into the CrateDB OBJECT/ARRAY column.
- ``raw_text`` tables (``sysinfo``/``node_sysinfo``) are deliberately NOT
  produced: the dumps are large and the structured tables carry the value.
- ``confluent_kafka`` is imported lazily (only when a producer is built) so the
  pure schema/record helpers and the rest of the package import without it.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime


# Column kinds -> Avro (non-null branch of a nullable union). "json" covers the
# CrateDB OBJECT/ARRAY columns, sent as a JSON string.
_KIND_TO_AVRO = {
    "str": "string",
    "int": "int",
    "long": "long",
    "double": "double",
    "bool": "boolean",
    "json": "string",
}


class TableSpec:
    """One Kafka topic / Avro schema for a structured table.

    ``parsed_key`` is the key under which ``cli`` collects this table's records
    in the ``parsed`` dict; ``table`` is the CrateDB table (also the topic
    suffix); ``columns`` is an ordered ``[(name, kind)]`` list mirroring the
    record dict keys (kinds in ``_KIND_TO_AVRO``; ``json`` columns are JSON-
    encoded). ``id``/``snapshot_id``/``fetched_at`` are added to every schema.
    """

    def __init__(self, parsed_key: str, table: str, columns: list[tuple[str, str]]):
        self.parsed_key = parsed_key
        self.table = table
        self.columns = columns

    def json_columns(self) -> set[str]:
        return {name for name, kind in self.columns if kind == "json"}


# The 11 structured tables produced to Kafka (the raw_text sysinfo dumps are
# intentionally excluded). Column order mirrors schema.TABLES (asserted in tests).
TABLE_SPECS: list[TableSpec] = [
    TableSpec("devices", "device", [
        ("uuid", "str"), ("mac", "str"), ("mac_vendor", "str"), ("ip", "str"),
        ("conn", "str"), ("status", "str"), ("name", "str"), ("friendly_name", "str"),
        ("fw_ver", "str"), ("role", "str"),
        ("extra_macs", "json"), ("extra_macs_vendor", "json"),
    ]),
    TableSpec("wlan_clients", "wlan_client", [
        ("client_mac", "str"), ("client_mac_vendor", "str"), ("stat", "str"),
        ("net", "str"), ("node", "str"), ("mcs", "str"), ("rssi", "int"),
        ("last_seen", "str"),
    ]),
    TableSpec("backhaul", "backhaul", [
        ("node_mac", "str"), ("node_mac_vendor", "str"), ("node_ip", "str"),
        ("parent_ip", "str"), ("intf", "str"), ("chan", "str"), ("rssi", "str"),
        ("speed", "double"), ("state", "str"), ("timestamp", "long"),
    ]),
    TableSpec("ping", "ping", [
        ("target", "str"), ("transmitted", "int"), ("received", "int"),
        ("loss_pct", "double"), ("rtt_min", "double"), ("rtt_avg", "double"),
        ("rtt_max", "double"),
    ]),
    TableSpec("nodes", "node", [
        ("uuid", "str"), ("mac", "str"), ("mac_vendor", "str"), ("ip", "str"),
        ("name", "str"), ("role", "str"), ("sku", "str"), ("serial_number", "str"),
        ("fw_ver", "str"), ("mode", "str"), ("model_base", "str"),
        ("model_number", "str"), ("hw_version", "str"),
        ("userap2g_bssid", "str"), ("userap2g_bssid_vendor", "str"), ("userap2g_channel", "str"),
        ("userap5gl_bssid", "str"), ("userap5gl_bssid_vendor", "str"), ("userap5gl_channel", "str"),
        ("userap5gh_bssid", "str"), ("userap5gh_bssid_vendor", "str"), ("userap5gh_channel", "str"),
        ("devinfo", "json"),
    ]),
    TableSpec("radio_stats", "radio_stats", [
        ("radio", "str"), ("band", "str"), ("source_node_mac", "str"),
        ("source_node_name", "str"), ("source_node_ip", "str"), ("source_role", "str"),
        ("stats", "json"),
    ]),
    TableSpec("radio_config", "radio_config", [
        ("interface", "str"), ("ssid", "str"), ("mac", "str"), ("mac_vendor", "str"),
        ("frequency", "str"), ("settings", "json"),
    ]),
    TableSpec("nic_counters", "nic_counter", [
        ("intf", "str"), ("rx_bytes", "long"), ("tx_bytes", "long"),
    ]),
    TableSpec("system", "system", [
        ("uptime_secs", "long"), ("load_1", "double"), ("load_5", "double"),
        ("load_15", "double"), ("mem_total", "long"), ("mem_used", "long"),
        ("mem_free", "long"), ("mem_shared", "long"), ("mem_buffers", "long"),
        ("mem_cached", "long"), ("cpu_idle_pct", "double"),
    ]),
    TableSpec("ip_neighbors", "ip_neighbor", [
        ("ip", "str"), ("family", "str"), ("iface", "str"), ("mac", "str"),
        ("mac_vendor", "str"), ("is_router", "bool"), ("state", "str"),
    ]),
    TableSpec("lldp", "lldp_neighbor", [
        ("interface", "str"), ("rid", "str"), ("chassis_id", "str"),
        ("chassis_id_vendor", "str"), ("sys_name", "str"), ("sys_descr", "str"),
        ("mgmt_ip", "str"), ("port_id", "str"), ("port_id_vendor", "str"),
        ("port_descr", "str"), ("capabilities", "json"),
    ]),
]


def assign_ids(parsed: dict) -> None:
    """Stamp every structured record with an ``id`` (its CrateDB primary key).

    Generated up front, in place, so a Connect JDBC sink upsert keys on a stable
    id -- Kafka re-delivery then never duplicates a row. Idempotent per record.
    """
    for spec in TABLE_SPECS:
        for rec in parsed.get(spec.parsed_key, []) or []:
            rec.setdefault("id", str(uuid.uuid4()))


def value_schema(spec: TableSpec) -> str:
    """Build the Avro value schema (JSON string) for ``spec``.

    Every field except id/snapshot_id/fetched_at is a nullable union so a missing
    parsed value serializes as null. ``fetched_at`` is ``timestamp-millis``.
    """
    fields = [
        {"name": "id", "type": "string"},
        {"name": "snapshot_id", "type": "string"},
        {"name": "fetched_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ]
    for name, kind in spec.columns:
        fields.append({"name": name, "type": ["null", _KIND_TO_AVRO[kind]], "default": None})
    return json.dumps({
        "type": "record",
        "name": spec.table,
        "namespace": "velop",
        "fields": fields,
    })


def record_value(spec: TableSpec, rec: dict, row_id: str, snapshot_id: str,
                 fetched_at: datetime) -> dict:
    """Build the Avro value dict for one record (pure; no Kafka).

    ``json`` columns are JSON-encoded (or null); other columns pass through by
    key from the parse.py record. ``fetched_at`` stays a datetime for the Avro
    ``timestamp-millis`` logical type.
    """
    json_cols = spec.json_columns()
    value = {"id": row_id, "snapshot_id": snapshot_id, "fetched_at": fetched_at}
    for name, _kind in spec.columns:
        raw = rec.get(name)
        if name in json_cols:
            value[name] = None if raw is None else json.dumps(raw)
        else:
            value[name] = raw
    return value


class KafkaSink:
    """Confluent producer + per-table Avro serializers for the velop tables."""

    def __init__(self, cfg):
        # Lazy import so the package (and tests) work without confluent_kafka.
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer

        self._cfg = cfg
        self._producer = Producer({
            "bootstrap.servers": cfg.kafka_bootstrap,
            "client.id": cfg.kafka_client_id,
        })
        registry = SchemaRegistryClient({"url": cfg.schema_registry_url})
        self._key_serializer = StringSerializer("utf_8")
        # One serializer per table; each registers "<topic>-value" on first use.
        self._serializers = {
            spec.table: AvroSerializer(registry, value_schema(spec))
            for spec in TABLE_SPECS
        }

    def topic_for(self, table: str) -> str:
        return f"{self._cfg.kafka_topic_prefix}{table}"

    def produce(self, parsed: dict, snapshot_id: str,
                fetched_at: datetime) -> dict[str, int]:
        """Produce every structured record in ``parsed`` to its topic.

        Each record must already carry an ``id`` (see ``assign_ids``) so Kafka
        rows reuse the CrateDB primary keys. Returns a per-table produced count.
        """
        from confluent_kafka.serialization import (
            MessageField,
            SerializationContext,
        )

        counts: dict[str, int] = {}
        for spec in TABLE_SPECS:
            records = parsed.get(spec.parsed_key, []) or []
            topic = self.topic_for(spec.table)
            serializer = self._serializers[spec.table]
            n = 0
            for rec in records:
                row_id = rec.get("id") or str(uuid.uuid4())
                value = record_value(spec, rec, row_id, snapshot_id, fetched_at)
                self._producer.produce(
                    topic=topic,
                    key=self._key_serializer(
                        snapshot_id, SerializationContext(topic, MessageField.KEY)),
                    value=serializer(
                        value, SerializationContext(topic, MessageField.VALUE)),
                )
                self._producer.poll(0)
                n += 1
            if n:
                counts[spec.table] = n
        return counts

    def flush(self, timeout: float = 30.0) -> int:
        """Block until queued messages are delivered; returns # still in queue."""
        return self._producer.flush(timeout)
