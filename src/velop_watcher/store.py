"""Persist sysinfo snapshots into CrateDB via its HTTP endpoint (port 4200)."""

from __future__ import annotations

import uuid
from datetime import datetime

from crate import client

from .config import Config

# CrateDB requires an explicit primary key and has no autoincrement, so we
# generate a UUID per snapshot. raw_text keeps the unparsed page verbatim --
# parsing into structured columns is a later goal. The "velop" schema is
# created implicitly by the first CREATE TABLE.
DDL = """
CREATE TABLE IF NOT EXISTS velop.sysinfo (
    id TEXT PRIMARY KEY,
    router_host TEXT,
    fetched_at TIMESTAMP WITH TIME ZONE,
    generated_at TIMESTAMP WITH TIME ZONE,
    -- The dump is one ~250 KB blob. CrateDB's default full-text index AND its
    -- columnstore (doc values) both reject values over 32766 bytes, so disable
    -- both -- we store the page verbatim and don't query it by content.
    raw_text TEXT INDEX OFF STORAGE WITH (columnstore = false)
)
"""

# The crate client uses the qmark paramstyle.
INSERT = """
INSERT INTO velop.sysinfo (id, router_host, fetched_at, generated_at, raw_text)
VALUES (?, ?, ?, ?, ?)
"""

# Tier-1 structured tables. Each row is parsed out of a snapshot's raw_text and
# carries snapshot_id + fetched_at so it can be tracked over time and joined
# back to velop.sysinfo. CrateDB has no autoincrement, so every row gets a
# Python-generated UUID primary key.
TIER1_DDL = (
    # OUI/vendor cache: one row per 24-bit OUI, populated lazily from the manuf
    # file the first time a MAC with that prefix is seen (vendor may be NULL to
    # cache a known miss). See oui.py.
    """
    CREATE TABLE IF NOT EXISTS velop.oui (
        oui TEXT PRIMARY KEY,
        vendor TEXT,
        source TEXT,
        looked_up_at TIMESTAMP WITH TIME ZONE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.device (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        uuid TEXT,
        mac TEXT,
        mac_vendor TEXT,
        ip TEXT,
        conn TEXT,
        status TEXT,
        name TEXT,
        -- Untruncated name from the JNAP GetDevices3 API (see fetch/parse);
        -- the CGI 'name' is capped at ~16 chars and often blank.
        friendly_name TEXT,
        fw_ver TEXT,
        role TEXT,
        extra_macs ARRAY(TEXT),
        extra_macs_vendor ARRAY(TEXT)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.wlan_client (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        client_mac TEXT,
        client_mac_vendor TEXT,
        stat TEXT,
        net TEXT,
        node TEXT,
        mcs TEXT,
        rssi INTEGER,
        last_seen TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.backhaul (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        node_mac TEXT,
        node_mac_vendor TEXT,
        node_ip TEXT,
        parent_ip TEXT,
        intf TEXT,
        chan TEXT,
        rssi TEXT,
        speed DOUBLE,
        state TEXT,
        "timestamp" BIGINT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.ping (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        target TEXT,
        transmitted INTEGER,
        received INTEGER,
        loss_pct DOUBLE,
        rtt_min DOUBLE,
        rtt_avg DOUBLE,
        rtt_max DOUBLE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.node (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        uuid TEXT,
        mac TEXT,
        mac_vendor TEXT,
        ip TEXT,
        name TEXT,
        role TEXT,
        sku TEXT,
        serial_number TEXT,
        fw_ver TEXT,
        mode TEXT,
        model_base TEXT,
        model_number TEXT,
        hw_version TEXT,
        userap2g_bssid TEXT,
        userap2g_bssid_vendor TEXT,
        userap2g_channel TEXT,
        userap5gl_bssid TEXT,
        userap5gl_bssid_vendor TEXT,
        userap5gl_channel TEXT,
        userap5gh_bssid TEXT,
        userap5gh_bssid_vendor TEXT,
        userap5gh_channel TEXT,
        -- Full DEVINFO 'data' blob kept verbatim; CrateDB infers sub-columns.
        devinfo OBJECT(IGNORED)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.radio_stats (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        radio TEXT,
        band TEXT,
        -- Which mesh node these radio counters came from. radio names (wifi0/1/2)
        -- repeat across nodes and bands differ by model, so the source node is
        -- part of a radio's identity; legacy (master-only) rows have it NULL.
        source_node_mac TEXT,
        source_node_name TEXT,
        source_node_ip TEXT,
        source_role TEXT,
        -- ~54 apstats counters per radio; columns vary, so keep them as a blob.
        stats OBJECT(IGNORED)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.radio_config (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        interface TEXT,
        ssid TEXT,
        mac TEXT,
        mac_vendor TEXT,
        frequency TEXT,
        settings OBJECT(IGNORED)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.nic_counter (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        intf TEXT,
        rx_bytes BIGINT,
        tx_bytes BIGINT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.system (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        uptime_secs BIGINT,
        load_1 DOUBLE,
        load_5 DOUBLE,
        load_15 DOUBLE,
        mem_total BIGINT,
        mem_used BIGINT,
        mem_free BIGINT,
        mem_shared BIGINT,
        mem_buffers BIGINT,
        mem_cached BIGINT,
        cpu_idle_pct DOUBLE
    )
    """,
    # Per-node raw sysinfo archive. The master dump lives in velop.sysinfo (the
    # snapshot); each satellite's full dump is archived here under the same
    # snapshot_id so the "raw_text is source of truth" principle extends to the
    # whole mesh and satellite-local sections can be parsed later.
    """
    CREATE TABLE IF NOT EXISTS velop.node_sysinfo (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        generated_at TIMESTAMP WITH TIME ZONE,
        node_mac TEXT,
        node_name TEXT,
        node_ip TEXT,
        role TEXT,
        raw_text TEXT INDEX OFF STORAGE WITH (columnstore = false)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.ip_neighbor (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        ip TEXT,
        -- inet / inet6: the ip neigh block mixes IPv4 and IPv6 (fe80::) rows.
        family TEXT,
        -- br0 main LAN, br1 guest, br2 IoT, eth0 WAN -- the subnet/VLAN the
        -- neighbour was seen on.
        iface TEXT,
        -- NULL for an unresolved (FAILED) entry.
        mac TEXT,
        mac_vendor TEXT,
        is_router BOOLEAN,
        state TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS velop.lldp_neighbor (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT,
        fetched_at TIMESTAMP WITH TIME ZONE,
        interface TEXT,
        rid TEXT,
        chassis_id TEXT,
        chassis_id_vendor TEXT,
        sys_name TEXT,
        sys_descr TEXT,
        mgmt_ip TEXT,
        port_id TEXT,
        port_id_vendor TEXT,
        port_descr TEXT,
        capabilities OBJECT(IGNORED)
    )
    """,
)

# Column order per table for executemany inserts. id/snapshot_id/fetched_at are
# prepended for every row; the rest mirror the parse.py record keys.
_DEVICE_COLS = (
    "uuid", "mac", "mac_vendor", "ip", "conn", "status", "name", "friendly_name",
    "fw_ver", "role", "extra_macs", "extra_macs_vendor",
)
_WLAN_COLS = (
    "client_mac", "client_mac_vendor", "stat", "net", "node", "mcs", "rssi", "last_seen",
)
_BACKHAUL_COLS = (
    "node_mac", "node_mac_vendor", "node_ip", "parent_ip", "intf", "chan", "rssi",
    "speed", "state", "timestamp",
)
_PING_COLS = (
    "target", "transmitted", "received", "loss_pct", "rtt_min", "rtt_avg", "rtt_max",
)
_NODE_COLS = (
    "uuid", "mac", "mac_vendor", "ip", "name", "role", "sku", "serial_number", "fw_ver",
    "mode", "model_base", "model_number", "hw_version",
    "userap2g_bssid", "userap2g_bssid_vendor", "userap2g_channel",
    "userap5gl_bssid", "userap5gl_bssid_vendor", "userap5gl_channel",
    "userap5gh_bssid", "userap5gh_bssid_vendor", "userap5gh_channel", "devinfo",
)
_RADIO_STATS_COLS = (
    "radio", "band", "source_node_mac", "source_node_name", "source_node_ip",
    "source_role", "stats",
)
_RADIO_CONFIG_COLS = ("interface", "ssid", "mac", "mac_vendor", "frequency", "settings")
_NIC_COUNTER_COLS = ("intf", "rx_bytes", "tx_bytes")
_SYSTEM_COLS = (
    "uptime_secs", "load_1", "load_5", "load_15", "mem_total", "mem_used",
    "mem_free", "mem_shared", "mem_buffers", "mem_cached", "cpu_idle_pct",
)
_IP_NEIGHBOR_COLS = (
    "ip", "family", "iface", "mac", "mac_vendor", "is_router", "state",
)
_LLDP_COLS = (
    "interface", "rid", "chassis_id", "chassis_id_vendor", "sys_name", "sys_descr",
    "mgmt_ip", "port_id", "port_id_vendor", "port_descr", "capabilities",
)


def connect(cfg: Config):
    """Open a CrateDB connection over HTTP."""
    return client.connect(
        cfg.crate_url,
        username=cfg.crate_user,
        password=cfg.crate_password or None,
    )


# Idempotent migrations for tables that may already exist from earlier runs --
# CREATE TABLE IF NOT EXISTS never adds columns to an existing table. CrateDB
# has no ADD COLUMN IF NOT EXISTS, so we run the plain ALTER and ignore the
# "already has a column" error on tables that already carry the column.
MIGRATIONS = (
    "ALTER TABLE velop.device ADD COLUMN friendly_name TEXT",
    "ALTER TABLE velop.radio_stats ADD COLUMN source_node_mac TEXT",
    "ALTER TABLE velop.radio_stats ADD COLUMN source_node_name TEXT",
    "ALTER TABLE velop.radio_stats ADD COLUMN source_node_ip TEXT",
    "ALTER TABLE velop.radio_stats ADD COLUMN source_role TEXT",
)


def ensure_schema(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(DDL)
        for ddl in TIER1_DDL:
            cur.execute(ddl)
        for migration in MIGRATIONS:
            try:
                cur.execute(migration)
            except Exception as exc:  # crate ProgrammingError on a re-run
                if "already" not in str(exc).lower():
                    raise
    finally:
        cur.close()


def _insert_rows(cur, table: str, columns: tuple, records: list[dict],
                 snapshot_id: str, fetched_at: datetime) -> int:
    """Bulk-insert parsed records into a Tier-1 table.

    Each record contributes id/snapshot_id/fetched_at plus the named columns,
    pulled by key from the parse.py dict. Returns the number of rows inserted.
    """
    if not records:
        return 0
    cols = ("id", "snapshot_id", "fetched_at") + columns
    placeholders = ", ".join("?" for _ in cols)
    sql = f'INSERT INTO {table} ({", ".join(col_sql(c) for c in cols)}) VALUES ({placeholders})'
    params = [
        [str(uuid.uuid4()), snapshot_id, fetched_at, *(rec.get(c) for c in columns)]
        for rec in records
    ]
    cur.executemany(sql, params)
    return len(params)


def col_sql(name: str) -> str:
    """Quote ``timestamp`` (a CrateDB reserved word); pass other names through."""
    return '"timestamp"' if name == "timestamp" else name


def store_tier1(conn, parsed: dict, snapshot_id: str, fetched_at: datetime) -> dict[str, int]:
    """Persist the Tier-1 parsed sections for one snapshot.

    ``parsed`` maps ``devices``/``wlan_clients``/``backhaul``/``nodes`` to the
    record lists returned by ``parse.py``. Returns a per-table inserted-row
    count. CrateDB has no transactions, so a partial failure can leave some
    tables populated and others not.
    """
    cur = conn.cursor()
    try:
        return {
            "device": _insert_rows(cur, "velop.device", _DEVICE_COLS,
                                   parsed.get("devices", []), snapshot_id, fetched_at),
            "wlan_client": _insert_rows(cur, "velop.wlan_client", _WLAN_COLS,
                                        parsed.get("wlan_clients", []), snapshot_id, fetched_at),
            "backhaul": _insert_rows(cur, "velop.backhaul", _BACKHAUL_COLS,
                                     parsed.get("backhaul", []), snapshot_id, fetched_at),
            "ping": _insert_rows(cur, "velop.ping", _PING_COLS,
                                 parsed.get("ping", []), snapshot_id, fetched_at),
            "node": _insert_rows(cur, "velop.node", _NODE_COLS,
                                 parsed.get("nodes", []), snapshot_id, fetched_at),
            "radio_stats": _insert_rows(cur, "velop.radio_stats", _RADIO_STATS_COLS,
                                        parsed.get("radio_stats", []), snapshot_id, fetched_at),
            "radio_config": _insert_rows(cur, "velop.radio_config", _RADIO_CONFIG_COLS,
                                         parsed.get("radio_config", []), snapshot_id, fetched_at),
            "nic_counter": _insert_rows(cur, "velop.nic_counter", _NIC_COUNTER_COLS,
                                        parsed.get("nic_counters", []), snapshot_id, fetched_at),
            "system": _insert_rows(cur, "velop.system", _SYSTEM_COLS,
                                   parsed.get("system", []), snapshot_id, fetched_at),
            "ip_neighbor": _insert_rows(cur, "velop.ip_neighbor", _IP_NEIGHBOR_COLS,
                                        parsed.get("ip_neighbors", []), snapshot_id, fetched_at),
            "lldp_neighbor": _insert_rows(cur, "velop.lldp_neighbor", _LLDP_COLS,
                                          parsed.get("lldp", []), snapshot_id, fetched_at),
        }
    finally:
        cur.close()


def store_sysinfo(
    conn,
    raw_text: str,
    fetched_at: datetime,
    generated_at: datetime | None,
    router_host: str,
) -> str:
    row_id = str(uuid.uuid4())
    cur = conn.cursor()
    try:
        cur.execute(INSERT, (row_id, router_host, fetched_at, generated_at, raw_text))
    finally:
        cur.close()
    return row_id


NODE_SYSINFO_INSERT = """
INSERT INTO velop.node_sysinfo
    (id, snapshot_id, fetched_at, generated_at, node_mac, node_name, node_ip, role, raw_text)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def store_node_sysinfo(conn, nodes: list[dict], snapshot_id: str,
                       fetched_at: datetime) -> int:
    """Archive each satellite's raw sysinfo dump under the master's snapshot_id.

    ``nodes`` is a list of dicts with ``raw_text``/``generated_at`` and the node
    identity (``node_mac``/``node_name``/``node_ip``/``role``). Returns the
    number of rows written.
    """
    if not nodes:
        return 0
    cur = conn.cursor()
    try:
        params = [
            [
                str(uuid.uuid4()), snapshot_id, fetched_at, n.get("generated_at"),
                n.get("node_mac"), n.get("node_name"), n.get("node_ip"),
                n.get("role"), n.get("raw_text"),
            ]
            for n in nodes
        ]
        cur.executemany(NODE_SYSINFO_INSERT, params)
        return len(params)
    finally:
        cur.close()
