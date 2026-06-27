"""Tier-1 parser tests, run against the real ``sampleoutput.txt`` reference dump."""

from pathlib import Path

import pytest

from velop_watcher import parse

SAMPLE = Path(__file__).resolve().parent.parent / "sampleoutput.txt"


@pytest.fixture(scope="module")
def dump() -> str:
    return SAMPLE.read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# show_devices -> devices
# --------------------------------------------------------------------------


def test_parse_devices_master_row(dump):
    devices = parse.parse_devices(dump)
    master = next(d for d in devices if d["role"] == "master")
    assert master["uuid"] == "BB9B3BEC-7224-299E-5178-D8EC5E8EED9E"
    assert master["mac"] == "D8:EC:5E:8E:ED:9E"
    assert master["ip"] == "10.13.1.1"
    assert master["conn"] == "wired"
    assert master["status"] == "Up"
    assert master["name"] == "Router"
    assert master["fw_ver"] == "1.0.13.216903"


def test_parse_devices_ditto_rows_attach_extra_macs(dump):
    """Ditto continuation rows add MACs to the device above, not new records."""
    devices = parse.parse_devices(dump)
    dev = next(d for d in devices if d["uuid"].endswith("C4411EEC4888"))
    assert dev["name"] == "LINKSYS_return1"
    assert dev["extra_macs"] == ["ca:41:1e:ec:48:8b"]
    # No record's UUID is a ditto quote mark.
    assert all(not d["uuid"].startswith('"') for d in devices)


def test_parse_devices_keeps_mojibake_and_truncated_names(dump):
    devices = parse.parse_devices(dump)
    names = {d["name"] for d in devices if d["name"]}
    assert "Celetonâ€™s iM.." in names  # mojibake preserved verbatim
    assert "LINKSYS-Return.." in names  # truncated name preserved


def test_parse_devices_empty_optional_fields_are_none(dump):
    devices = parse.parse_devices(dump)
    # A down client with no IP / name / fw.
    dev = next(d for d in devices if d["mac"] == "d8:43:ae:24:39:a6")
    assert dev["status"] == "Down"
    assert dev["ip"] is None
    assert dev["name"] is None
    assert dev["fw_ver"] is None
    assert dev["role"] == "client"


# --------------------------------------------------------------------------
# JNAP GetDevices3 -> friendly_name enrichment
# --------------------------------------------------------------------------

_JNAP = {
    "result": "OK",
    "output": {
        "devices": [
            {
                "deviceID": "595567DF-B624-361D-D10F-E89F804C57DF",  # mixed case
                "friendlyName": "LINKSYS-Return",
                "knownInterfaces": [{"macAddress": "E8:9F:80:4C:57:DF"}],
            },
            {
                "deviceID": "aaaaaaaa-0000-0000-0000-000000000000",
                "friendlyName": "endowment",
                "knownInterfaces": [{"macAddress": "30:9C:23:27:12:6F"}],
            },
            {"deviceID": "no-name", "friendlyName": "", "knownInterfaces": []},
        ]
    },
}


def test_friendly_name_index_keys_uuid_and_mac():
    idx = parse.friendly_name_index(_JNAP)
    assert idx["595567df-b624-361d-d10f-e89f804c57df"] == "LINKSYS-Return"
    assert idx["E8:9F:80:4C:57:DF"] == "LINKSYS-Return"
    assert idx["30:9C:23:27:12:6F"] == "endowment"
    assert "no-name" not in idx  # blank friendlyName skipped


def test_friendly_name_index_tolerates_junk():
    assert parse.friendly_name_index({}) == {}
    assert parse.friendly_name_index({"output": {"devices": ["x", None]}}) == {}


def test_enrich_friendly_names_matches_uuid_then_mac():
    idx = parse.friendly_name_index(_JNAP)
    devices = [
        {"uuid": "595567DF-B624-361D-D10F-E89F804C57DF", "mac": "e8:9f:80:4c:57:df"},
        {"uuid": "UNKNOWN-UUID", "mac": "30:9c:23:27:12:6f"},  # falls back to MAC
        {"uuid": "MISSING", "mac": "ff:ff:ff:ff:ff:ff", "extra_macs": []},
    ]
    parse.enrich_friendly_names(devices, idx)
    assert devices[0]["friendly_name"] == "LINKSYS-Return"  # by UUID
    assert devices[1]["friendly_name"] == "endowment"  # by MAC (case-insensitive)
    assert devices[2]["friendly_name"] is None  # absent -> column still present


def test_enrich_friendly_names_uses_extra_macs():
    idx = parse.friendly_name_index(_JNAP)
    devices = [{"uuid": "X", "mac": "00:00:00:00:00:00",
                "extra_macs": ["30:9c:23:27:12:6f"]}]
    parse.enrich_friendly_names(devices, idx)
    assert devices[0]["friendly_name"] == "endowment"


# --------------------------------------------------------------------------
# wlan_report -> wlan_clients
# --------------------------------------------------------------------------


def test_parse_wlan_clients_count(dump):
    assert len(parse.parse_wlan_clients(dump)) == 10


def test_parse_wlan_client_numeric_mcs(dump):
    clients = parse.parse_wlan_clients(dump)
    c = next(c for c in clients if c["client_mac"] == "20:3d:bd:4a:e9:b6")
    assert c["node"] == "C4411EEC4360"  # node value overflows its column
    assert c["mcs"] == "211"
    assert c["rssi"] == -69
    assert c["last_seen"] == "2026-06-15T00:29:24Z"


def test_parse_wlan_client_rsn_mcs_kept_as_text(dump):
    clients = parse.parse_wlan_clients(dump)
    c = next(c for c in clients if c["client_mac"] == "00:09:b0:ba:1c:43")
    assert c["mcs"] == "RSN"  # not numeric, kept verbatim
    assert c["node"] == "master"
    assert c["rssi"] == -76


# --------------------------------------------------------------------------
# bh_report -> backhaul
# --------------------------------------------------------------------------


def test_parse_backhaul_count(dump):
    assert len(parse.parse_backhaul(dump)) == 4


def test_parse_backhaul_wired_row(dump):
    rows = parse.parse_backhaul(dump)
    r = next(r for r in rows if r["node_mac"] == "C4411EEC4275")
    assert r["node_ip"] == "10.13.1.9"
    assert r["parent_ip"] == "10.13.1.1"
    assert r["intf"] == "eth1"
    assert r["chan"] == "wired"
    assert r["speed"] == 1024.0
    assert r["state"] == "up"
    assert r["timestamp"] == 1781794631


def test_parse_backhaul_wireless_row_missing_rssi(dump):
    """The wireless node's RSSI(AP/STA) column is empty -> None, not a shift."""
    rows = parse.parse_backhaul(dump)
    r = next(r for r in rows if r["node_mac"] == "C4411EEC4360")
    assert r["intf"] == "5GL"
    assert r["chan"] == "44"
    assert r["rssi"] is None
    assert r["speed"] == 146.84
    assert r["state"] == "down"


# --------------------------------------------------------------------------
# DEVINFO + show_devices -> nodes
# --------------------------------------------------------------------------


def test_parse_nodes_count_and_roles(dump):
    nodes = parse.parse_nodes(dump)
    assert len(nodes) == 5
    assert sum(n["role"] == "master" for n in nodes) == 1
    assert sum(n["role"] == "slave" for n in nodes) == 4


def test_parse_node_master_devinfo_fields(dump):
    nodes = parse.parse_nodes(dump)
    master = next(n for n in nodes if n["role"] == "master")
    assert master["sku"] == "MX42-EU"
    assert master["serial_number"] == "38U10M57C03110"
    assert master["fw_ver"] == "1.0.13.216903"
    assert master["mode"] == "master"
    assert master["model_base"] == "MX42"
    # Per-radio columns promoted from DEVINFO.
    assert master["userap5gh_channel"] == "116"
    assert master["userap2g_bssid"] == "D8:EC:5E:8E:ED:9F"
    # Full DEVINFO data dict retained for the OBJECT column.
    assert master["devinfo"]["manufacturer"] == "Linksys"
    assert isinstance(master["devinfo"]["extra_macs"], list)


def test_parse_node_slave_matches_devinfo_by_uuid(dump):
    nodes = parse.parse_nodes(dump)
    slave = next(n for n in nodes if n["uuid"].endswith("C4411EEC4360"))
    assert slave["sku"] == "WHW03-UK"
    assert slave["serial_number"] == "20J20M38A14594"
    assert slave["mode"] == "slave"


def test_parse_devinfo_keyed_by_uuid(dump):
    info = parse.parse_devinfo(dump)
    assert len(info) == 5
    assert "BB9B3BEC-7224-299E-5178-D8EC5E8EED9E" in info
    assert info["BB9B3BEC-7224-299E-5178-D8EC5E8EED9E"]["name"] == "Router"


# --------------------------------------------------------------------------
# ping statistics -> ping (Tier 7)
# --------------------------------------------------------------------------


def test_parse_ping_single_record(dump):
    rows = parse.parse_ping(dump)
    assert len(rows) == 1
    r = rows[0]
    assert r["target"] == "www.linksys.com"
    assert r["transmitted"] == 2
    assert r["received"] == 2
    assert r["loss_pct"] == 0.0
    assert r["rtt_min"] == 9.297
    assert r["rtt_avg"] == 11.709
    assert r["rtt_max"] == 14.122


def test_parse_ping_total_loss_has_no_rtt():
    """On 100% loss the router omits the round-trip line; counts still parse."""
    text = (
        "--- example.com ping statistics ---\n"
        "3 packets transmitted, 0 packets received, 100% packet loss\n"
    )
    rows = parse.parse_ping(text)
    assert len(rows) == 1
    r = rows[0]
    assert r["target"] == "example.com"
    assert r["transmitted"] == 3
    assert r["received"] == 0
    assert r["loss_pct"] == 100.0
    assert r["rtt_min"] is None
    assert r["rtt_avg"] is None
    assert r["rtt_max"] is None


def test_parse_ping_missing_section():
    assert parse.parse_ping("no ping here") == []


# --------------------------------------------------------------------------
# defensive behaviour
# --------------------------------------------------------------------------


def test_parsers_return_empty_on_missing_sections():
    assert parse.parse_devices("nothing here") == []
    assert parse.parse_wlan_clients("nothing here") == []
    assert parse.parse_backhaul("nothing here") == []
    assert parse.parse_nodes("nothing here") == []
    assert parse.parse_devinfo("nothing here") == {}


# --------------------------------------------------------------------------
# wifi apstats -> radio_stats (Tier 5)
# --------------------------------------------------------------------------


def test_parse_radio_stats_per_radio(dump):
    rows = parse.parse_radio_stats(dump)
    assert [(r["radio"], r["band"]) for r in rows] == [
        ("wifi1", "2.4G"),
        ("wifi0", "5G low"),
        ("wifi2", "5G high"),
    ]


def test_parse_radio_stats_counters_and_nested(dump):
    rows = parse.parse_radio_stats(dump)
    wifi1 = next(r for r in rows if r["radio"] == "wifi1")
    assert wifi1["stats"]["tx_data_packets"] == 5473615
    assert wifi1["stats"]["rx_rssi"] == 27
    assert wifi1["stats"]["channel_utilization_0_255"] == "<DISABLED>"  # non-numeric kept
    # Nested per-AC counters are prefixed so the duplicate "Best effort" labels
    # under Tx and Rx do not collide.
    assert wifi1["stats"]["tx_data_packets_per_ac_best_effort"] == 5072156
    assert wifi1["stats"]["rx_data_packets_per_ac_best_effort"] == 1229862


def test_tag_radio_source_stamps_node_identity(dump):
    rows = parse.parse_radio_stats(dump)
    node = {"mac": "C4:41:1E:EC:42:75", "name": "LINKSYS-Hall",
            "ip": "10.13.1.9", "role": "slave"}
    tagged = parse.tag_radio_source(rows, node)
    assert tagged is rows  # tags in place, returns same list
    for r in rows:
        assert r["source_node_mac"] == "C4:41:1E:EC:42:75"
        assert r["source_node_name"] == "LINKSYS-Hall"
        assert r["source_node_ip"] == "10.13.1.9"
        assert r["source_role"] == "slave"


def test_tag_radio_source_tolerates_missing_node_fields():
    radios = [{"radio": "wifi0", "band": "2.4G", "stats": {}}]
    parse.tag_radio_source(radios, {})  # no KeyError
    assert radios[0]["source_node_mac"] is None
    assert radios[0]["source_role"] is None


# --------------------------------------------------------------------------
# athN Settings -> radio_config (Tier 6)
# --------------------------------------------------------------------------


def test_parse_radio_config_vaps(dump):
    rows = parse.parse_radio_config(dump)
    assert [r["interface"] for r in rows] == ["ath0", "ath2", "ath1", "ath3", "ath10"]


def test_parse_radio_config_fields_and_settings(dump):
    rows = parse.parse_radio_config(dump)
    ath0 = next(r for r in rows if r["interface"] == "ath0")
    assert ath0["ssid"] == "MyWiFi"
    assert ath0["mac"] == "D8:EC:5E:8E:ED:9F"
    assert ath0["frequency"] == "2.472GHz"
    assert ath0["settings"]["chwidth"] == 0
    assert ath0["settings"]["mode"] == "11GHE20"
    assert ath0["settings"]["disablecoext"] == 0  # the g_ prefixed token


# --------------------------------------------------------------------------
# NIC Counters -> nic_counters (Tier 8)
# --------------------------------------------------------------------------


def test_parse_nic_counters(dump):
    rows = parse.parse_nic_counters(dump)
    assert [r["intf"] for r in rows] == ["br0", "eth0", "eth1", "eth2"]
    br0 = rows[0]
    assert br0["rx_bytes"] == 36301150454
    assert br0["tx_bytes"] == 166679092879
    assert rows[3] == {"intf": "eth2", "rx_bytes": 0, "tx_bytes": 0}


# --------------------------------------------------------------------------
# uptime / memory / cpu -> system (Tier 9)
# --------------------------------------------------------------------------


def test_parse_system_single_record(dump):
    rows = parse.parse_system(dump)
    assert len(rows) == 1
    r = rows[0]
    assert r["uptime_secs"] == 5 * 86400 + 12 * 3600 + 13 * 60  # "5 days, 12:13"
    assert (r["load_1"], r["load_5"], r["load_15"]) == (1.17, 1.16, 1.18)
    assert r["mem_total"] == 424156
    assert r["mem_free"] == 149808
    assert r["cpu_idle_pct"] == 97.6


def test_parse_system_minutes_only_uptime():
    rows = parse.parse_system(" 09:10:11 up 42 min,  0 users,  load average: 0.5, 0.4, 0.3\n")
    assert rows[0]["uptime_secs"] == 42 * 60
    assert rows[0]["load_1"] == 0.5


# --------------------------------------------------------------------------
# LLDP Information -> lldp (Tier 10)
# --------------------------------------------------------------------------


def test_parse_lldp_neighbors(dump):
    rows = parse.parse_lldp(dump)
    assert len(rows) == 3
    n = next(r for r in rows if r["rid"] == "11")
    assert n["interface"] == "eth1"
    assert n["chassis_id"] == "c4:41:1e:ec:42:75"  # "mac " prefix stripped
    assert n["sys_name"] == "Linksys14547"
    assert n["sys_descr"] == "Velop"
    assert n["mgmt_ip"] == "10.13.1.9"
    assert n["port_descr"] == "eth1"
    assert n["capabilities"] == {
        "Bridge": True,
        "Router": True,
        "Wlan": True,
        "Station": False,
    }


# --------------------------------------------------------------------------
# ip neigh -> ip_neighbors
# --------------------------------------------------------------------------


def test_parse_ip_neighbors_counts(dump):
    rows = parse.parse_ip_neighbors(dump)
    # The block mixes IPv4 and IPv6 rows; both are captured.
    assert len(rows) == 105
    assert sum(1 for r in rows if r["family"] == "inet") == 62
    assert sum(1 for r in rows if r["family"] == "inet6") == 43
    assert {r["iface"] for r in rows} == {"br0", "br2", "eth0", "eth1"}


def test_parse_ip_neighbors_reachable_row(dump):
    rows = parse.parse_ip_neighbors(dump)
    r = next(r for r in rows if r["ip"] == "10.13.1.30")
    assert r == {
        "ip": "10.13.1.30",
        "family": "inet",
        "iface": "br0",
        "mac": "1c:1b:0d:76:ef:1f",
        "is_router": False,
        "state": "DELAY",
    }


def test_parse_ip_neighbors_failed_row_has_no_mac(dump):
    rows = parse.parse_ip_neighbors(dump)
    r = next(r for r in rows if r["ip"] == "10.13.1.193")
    assert r["mac"] is None
    assert r["state"] == "FAILED"


def test_parse_ip_neighbors_router_flag(dump):
    rows = parse.parse_ip_neighbors(dump)
    r = next(r for r in rows if r["ip"] == "fe80::c641:1eff:feec:4275")
    assert r["is_router"] is True
    assert r["family"] == "inet6"
    assert r["mac"] == "c4:41:1e:ec:42:75"
    # A plain (non-router) neighbour is False, not None.
    plain = next(r for r in rows if r["ip"] == "192.168.20.101")
    assert plain["is_router"] is False
    assert plain["iface"] == "br2"  # IoT/smart-connect VLAN


def test_parse_ip_neighbors_missing_section():
    assert parse.parse_ip_neighbors("no neighbours here") == []
