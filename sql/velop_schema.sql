-- velop structured-table schema -- GENERATED from velop_watcher/schema.py.
-- Regenerate with:  python -m velop_watcher.schema > sql/velop_schema.sql
--
-- The watcher is Kafka-only; the Connect JDBC sinks land records into these
-- tables, which must pre-exist (the sinks run auto.create=false). Apply once,
-- e.g.  crash < sql/velop_schema.sql  (or psql, or the CrateDB admin UI).

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
    friendly_name TEXT,
    fw_ver TEXT,
    role TEXT,
    extra_macs ARRAY(TEXT),
    extra_macs_vendor ARRAY(TEXT)
);

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
);

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
);

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
);

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
    devinfo OBJECT(IGNORED)
);

CREATE TABLE IF NOT EXISTS velop.radio_stats (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMP WITH TIME ZONE,
    radio TEXT,
    band TEXT,
    source_node_mac TEXT,
    source_node_name TEXT,
    source_node_ip TEXT,
    source_role TEXT,
    stats OBJECT(IGNORED)
);

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
);

CREATE TABLE IF NOT EXISTS velop.nic_counter (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMP WITH TIME ZONE,
    intf TEXT,
    rx_bytes BIGINT,
    tx_bytes BIGINT
);

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
);

CREATE TABLE IF NOT EXISTS velop.ip_neighbor (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMP WITH TIME ZONE,
    ip TEXT,
    family TEXT,
    iface TEXT,
    mac TEXT,
    mac_vendor TEXT,
    is_router BOOLEAN,
    state TEXT
);

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
);
