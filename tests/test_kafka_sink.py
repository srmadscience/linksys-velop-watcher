"""Tests for the pure (no-Kafka) parts of kafka_sink: specs, schema, records.

The Producer/serializer path needs confluent_kafka + a live broker and is not
covered here, matching the project's network/DB test boundary.
"""

import json
from datetime import datetime, timezone

from velop_watcher import kafka_sink, schema
from velop_watcher.kafka_sink import TABLE_SPECS, assign_ids, record_value, value_schema


def test_specs_match_schema_columns():
    """Each topic's columns must mirror its CrateDB table's columns (same order).

    schema.TABLES is the single source of truth (it also generates the DDL the
    Connect JDBC sinks land into), so the Avro specs must not drift from it.
    """
    by_table = {spec.table: spec for spec in TABLE_SPECS}
    assert set(by_table) == set(schema.TABLES)
    for table in schema.TABLES:
        spec_cols = [name for name, _kind in by_table[table].columns]
        assert spec_cols == schema.column_names(table), table


def test_value_schema_is_valid_avro_with_meta_fields():
    spec = next(s for s in TABLE_SPECS if s.table == "ip_neighbor")
    schema = json.loads(value_schema(spec))
    assert schema["name"] == "ip_neighbor"
    assert schema["namespace"] == "velop"
    names = [f["name"] for f in schema["fields"]]
    # id/snapshot_id/fetched_at prepended, then the table columns.
    assert names[:3] == ["id", "snapshot_id", "fetched_at"]
    assert "is_router" in names
    fetched = next(f for f in schema["fields"] if f["name"] == "fetched_at")
    assert fetched["type"] == {"type": "long", "logicalType": "timestamp-millis"}
    is_router = next(f for f in schema["fields"] if f["name"] == "is_router")
    assert is_router["type"] == ["null", "boolean"]  # nullable union


def test_array_columns_serialize_as_avro_arrays():
    # device.extra_macs is ARRAY(TEXT): a nullable Avro array of nullable strings,
    # not a string (a JSON string lands as TEXT and CrateDB drops the row).
    spec = next(s for s in TABLE_SPECS if s.table == "device")
    schema = json.loads(value_schema(spec))
    extra = next(f for f in schema["fields"] if f["name"] == "extra_macs")
    assert extra["type"] == ["null", {"type": "array", "items": ["null", "string"]}]


def test_record_value_passthrough_and_meta():
    spec = next(s for s in TABLE_SPECS if s.table == "ip_neighbor")
    ts = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc)
    rec = {"ip": "10.13.1.5", "family": "inet", "iface": "br0",
           "mac": "aa:bb:cc:dd:ee:ff", "mac_vendor": "Acme",
           "is_router": False, "state": "REACHABLE"}
    value = record_value(spec, rec, "row-1", "snap-1", ts)
    assert value["id"] == "row-1"
    assert value["snapshot_id"] == "snap-1"
    assert value["fetched_at"] is ts
    assert value["ip"] == "10.13.1.5"
    assert value["is_router"] is False


def test_record_value_json_encodes_object_columns():
    # radio_stats.stats is OBJECT(IGNORED) -> JSON string.
    radio_spec = next(s for s in TABLE_SPECS if s.table == "radio_stats")
    rec = {"radio": "wifi0", "band": "2.4G", "stats": {"tx_data_bytes": 42}}
    value = record_value(radio_spec, rec, "r", "s", datetime.now(timezone.utc))
    assert value["stats"] == '{"tx_data_bytes": 42}'


def test_record_value_passes_array_columns_through_as_lists():
    # device.extra_macs is ARRAY(TEXT) -> a real Avro array, not a JSON string,
    # so the JDBC sink lands ARRAY(TEXT) instead of dropping the row.
    dev_spec = next(s for s in TABLE_SPECS if s.table == "device")
    drec = {"mac": "a", "extra_macs": ["b", "c"], "extra_macs_vendor": [None, "X"]}
    dval = record_value(dev_spec, drec, "r", "s", datetime.now(timezone.utc))
    assert dval["extra_macs"] == ["b", "c"]
    assert dval["extra_macs_vendor"] == [None, "X"]
    # An absent array column serializes as null, not an empty array.
    missing = record_value(dev_spec, {"mac": "a"}, "r", "s", datetime.now(timezone.utc))
    assert missing["extra_macs"] is None


def test_record_value_missing_fields_are_none():
    spec = next(s for s in TABLE_SPECS if s.table == "radio_stats")
    value = record_value(spec, {}, "r", "s", datetime.now(timezone.utc))
    assert value["stats"] is None          # absent OBJECT -> null, not "null"
    assert value["source_node_mac"] is None


def test_assign_ids_stamps_every_record_once():
    parsed = {
        "ip_neighbors": [{"ip": "1"}, {"ip": "2"}],
        "radio_stats": [{"radio": "wifi0", "id": "pre-existing"}],
        "devices": [],
    }
    assign_ids(parsed)
    ids = [r["id"] for r in parsed["ip_neighbors"]]
    assert all(ids) and len(set(ids)) == 2          # unique, populated
    assert parsed["radio_stats"][0]["id"] == "pre-existing"  # idempotent
