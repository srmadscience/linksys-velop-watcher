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


# --------------------------------------------------------------------------
# athN Settings -> radio_config (Tier 6)
# --------------------------------------------------------------------------


def test_parse_radio_config_vaps(dump):
    rows = parse.parse_radio_config(dump)
    assert [r["interface"] for r in rows] == ["ath0", "ath2", "ath1", "ath3", "ath10"]


def test_parse_radio_config_fields_and_settings(dump):
    rows = parse.parse_radio_config(dump)
    ath0 = next(r for r in rows if r["interface"] == "ath0")
    assert ath0["ssid"] == "CodeSpooks7"
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
