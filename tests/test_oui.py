"""Tests for OUI/vendor resolution (manuf parsing, cache resolver, enrichment)."""

import pytest

from velop_watcher import oui


# --------------------------------------------------------------------------
# Fake CrateDB cursor/connection for the velop.oui cache
# --------------------------------------------------------------------------


class FakeCursor:
    def __init__(self, db, log):
        self.db = db
        self.log = log
        self._result = None

    def execute(self, sql, params=None):
        verb = sql.strip().split()[0].upper()
        self.log.append(verb)
        if verb == "SELECT":
            oui_key = params[0]
            self._result = (self.db[oui_key],) if oui_key in self.db else None
        else:  # INSERT
            self.db[params[0]] = params[1]

    def fetchone(self):
        return self._result

    def close(self):
        pass


class FakeConn:
    def __init__(self, db=None):
        self.db = {} if db is None else db
        self.log = []

    def cursor(self):
        return FakeCursor(self.db, self.log)


# --------------------------------------------------------------------------
# oui_of
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mac,expected",
    [
        ("D8:EC:5E:8E:ED:9E", "D8:EC:5E"),
        ("d8:ec:5e:8e:ed:9e", "D8:EC:5E"),
        ("c4-41-1e-ec-42-75", "C4:41:1E"),
        ("d8ec5e8eed9e", "D8:EC:5E"),
        ("", None),
        (None, None),
        ("not-a-mac", None),
        ("ab:cd", None),
    ],
)
def test_oui_of(mac, expected):
    assert oui.oui_of(mac) == expected


# --------------------------------------------------------------------------
# ManufDB
# --------------------------------------------------------------------------

SAMPLE_MANUF = [
    "# Wireshark manuf sample",
    "",
    "D8:EC:5E\tLinksys\tLinksys (Belkin)",
    "00:09:B0\tOnkyo\tOnkyo Corporation",
    "BADLINE_NO_TAB",
    "70:B3:D5\tIeeeReg\tIEEE Registration Authority",
    "70:B3:D5:12:30:00/36\tSmallCo\tSmall Co 36 Block",
    "00:55:DA:00:00:00/28\tMidCo\tMid Co 28 Block",
    "AA:BB:CC\tShortOnly",  # no long name -> short used
]


def test_manufdb_basic_lookup():
    db = oui.ManufDB.from_lines(SAMPLE_MANUF)
    assert db.lookup("D8:EC:5E:8E:ED:9E") == "Linksys (Belkin)"
    assert db.lookup("00:09:b0:ba:1c:43") == "Onkyo Corporation"
    assert db.lookup("AA:BB:CC:00:00:01") == "ShortOnly"  # falls back to short name
    assert db.lookup("12:34:56:78:9a:bc") is None  # unknown
    assert db.lookup("") is None


def test_manufdb_longest_prefix_wins():
    db = oui.ManufDB.from_lines(SAMPLE_MANUF)
    # 36-bit block matches a more specific prefix than the 24-bit IEEE entry.
    assert db.lookup("70:B3:D5:12:3F:FF") == "Small Co 36 Block"
    # Outside the 36-bit block, falls back to the 24-bit entry.
    assert db.lookup("70:B3:D5:99:99:99") == "IEEE Registration Authority"


def test_manufdb_28bit_block():
    db = oui.ManufDB.from_lines(SAMPLE_MANUF)
    assert db.lookup("00:55:DA:0F:11:22") == "Mid Co 28 Block"
    assert db.lookup("00:55:DA:F0:11:22") is None  # outside the /28 range


def test_manufdb_skips_junk_lines():
    db = oui.ManufDB.from_lines(SAMPLE_MANUF)
    # 6 valid entries: 4x /24, 1x /28, 1x /36 (comment, blank, no-tab skipped).
    assert len(db) == 6


# --------------------------------------------------------------------------
# VendorResolver (velop.oui cache)
# --------------------------------------------------------------------------


def test_resolver_miss_then_caches_in_db_and_process():
    db = oui.ManufDB.from_lines(SAMPLE_MANUF)
    conn = FakeConn()
    resolver = oui.VendorResolver(conn, db)

    assert resolver.vendor_for("D8:EC:5E:8E:ED:9E") == "Linksys (Belkin)"
    assert conn.db["D8:EC:5E"] == "Linksys (Belkin)"  # written to cache table
    assert conn.log == ["SELECT", "INSERT"]

    # Same OUI again -> served from the in-process cache, no further DB calls.
    assert resolver.vendor_for("d8:ec:5e:00:00:99") == "Linksys (Belkin)"
    assert conn.log == ["SELECT", "INSERT"]


def test_resolver_caches_known_miss():
    conn = FakeConn()
    resolver = oui.VendorResolver(conn, oui.ManufDB.from_lines(SAMPLE_MANUF))
    assert resolver.vendor_for("12:34:56:78:9a:bc") is None
    # The miss is cached as a NULL vendor so it is not retried.
    assert "12:34:56" in conn.db
    assert conn.db["12:34:56"] is None


def test_resolver_uses_existing_db_row_without_manuf():
    conn = FakeConn({"AA:BB:CC": "PreCached Inc"})
    resolver = oui.VendorResolver(conn, manuf=None)
    assert resolver.vendor_for("aa:bb:cc:dd:ee:ff") == "PreCached Inc"
    assert conn.log == ["SELECT"]  # no INSERT, no manuf needed


def test_resolver_none_manuf_yields_none():
    conn = FakeConn()
    resolver = oui.VendorResolver(conn, manuf=None)
    assert resolver.vendor_for("11:22:33:44:55:66") is None
    assert conn.db["11:22:33"] is None


def test_resolver_non_mac_is_none_without_db_call():
    conn = FakeConn()
    resolver = oui.VendorResolver(conn, oui.ManufDB.from_lines(SAMPLE_MANUF))
    assert resolver.vendor_for("not-a-mac") is None
    assert conn.log == []  # nothing to look up


# --------------------------------------------------------------------------
# enrich
# --------------------------------------------------------------------------


def test_enrich_fills_all_mac_fields():
    db = oui.ManufDB.from_lines(SAMPLE_MANUF)
    resolver = oui.VendorResolver(FakeConn(), db)
    parsed = {
        "devices": [
            {"mac": "D8:EC:5E:8E:ED:9E", "extra_macs": ["00:09:b0:ba:1c:43", "zz:zz"]}
        ],
        "wlan_clients": [{"client_mac": "00:09:b0:ba:1c:43"}],
        "backhaul": [{"node_mac": "D8EC5E8EED9E"}],
        "nodes": [
            {
                "mac": "D8:EC:5E:8E:ED:9E",
                "userap2g_bssid": "00:09:B0:00:00:01",
                "userap5gl_bssid": None,
                "userap5gh_bssid": "garbage",
            }
        ],
        "radio_config": [{"mac": "D8:EC:5E:8E:ED:9F"}],
        "lldp": [{"chassis_id": "00:09:b0:11:22:33", "port_id": None}],
    }
    oui.enrich(parsed, resolver)

    assert parsed["devices"][0]["mac_vendor"] == "Linksys (Belkin)"
    # Parallel vendor array, including a None for the non-MAC extra entry.
    assert parsed["devices"][0]["extra_macs_vendor"] == ["Onkyo Corporation", None]
    assert parsed["wlan_clients"][0]["client_mac_vendor"] == "Onkyo Corporation"
    assert parsed["backhaul"][0]["node_mac_vendor"] == "Linksys (Belkin)"
    assert parsed["nodes"][0]["mac_vendor"] == "Linksys (Belkin)"
    assert parsed["nodes"][0]["userap2g_bssid_vendor"] == "Onkyo Corporation"
    assert parsed["nodes"][0]["userap5gl_bssid_vendor"] is None  # None MAC
    assert parsed["nodes"][0]["userap5gh_bssid_vendor"] is None  # non-MAC value
    assert parsed["radio_config"][0]["mac_vendor"] == "Linksys (Belkin)"
    assert parsed["lldp"][0]["chassis_id_vendor"] == "Onkyo Corporation"
    assert parsed["lldp"][0]["port_id_vendor"] is None
