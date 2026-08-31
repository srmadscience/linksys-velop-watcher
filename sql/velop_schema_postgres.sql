-- velop structured-table schema, PostgreSQL -- GENERATED from velop_watcher/schema.py.
-- Regenerate with:
--   python -m velop_watcher.schema --dialect postgres > sql/velop_schema_postgres.sql
--
-- A stock-PostgreSQL translation of sql/velop_schema.sql, for running the
-- pipeline against PostgreSQL instead of CrateDB. Same tables, same column
-- order, same id/upsert contract; only the CrateDB-only types change:
--     DOUBLE          -> DOUBLE PRECISION
--     ARRAY(TEXT)     -> TEXT[]    (device.extra_macs / extra_macs_vendor)
--     OBJECT(IGNORED) -> JSONB     (node.devinfo, radio_stats.stats, ...)
--
-- Apply once:  psql -f sql/velop_schema_postgres.sql
-- Then the views:  psql -f sql/grafana_<name>.sql
--
-- ALSO EMITTED HERE (CrateDB needs neither):
--   * velop.epoch_ms()/try_bigint()/try_double() -- stand-ins for CrateDB's
--     fetched_at::BIGINT epoch-ms cast and TRY_CAST, used by the postgres
--     grafana_*.sql views.
--   * indexes -- CrateDB indexes every column by default; PostgreSQL does not,
--     and every rate view self-joins a table to its own previous snapshot.
--
-- CONNECT JDBC SINK NOTES -- the PostgreSQL sinks are connect/*-postgres.json
-- (register them with `./connect/install-sinks.sh --target=postgres`):
--   * They use org.postgresql.Driver and a jdbc:postgresql:// URL, and keep
--     auto.create=false and insert.mode=upsert / pk.fields=id -- on PostgreSQL
--     that becomes a real ON CONFLICT (id) DO UPDATE against the PRIMARY KEY.
--   * The OBJECT(IGNORED)->JSONB columns are produced as JSON *strings* (see
--     kafka_sink), and PostgreSQL will not implicitly coerce TEXT to JSONB on
--     insert. The sink URLs therefore carry ?stringtype=unspecified, which makes
--     the driver send them untyped so the server casts them. Do not drop it.
--   * extra_macs/extra_macs_vendor are real Avro arrays and land natively in
--     TEXT[] -- but ONLY because kafka_sink sends an EMPTY array as null. The
--     JDBC sink binds a non-empty Avro array as a varchar[] parameter and
--     stringifies an empty one to the literal "[]", which PostgreSQL rejects
--     ("malformed array literal"); CrateDB accepts it, so this only ever bit
--     PostgreSQL. Records produced before that fix carry [] and are dropped by
--     the sink's errors.tolerance=all -- see connect/README.md for the
--     ReplaceField backfill workaround.

CREATE SCHEMA IF NOT EXISTS velop;

CREATE OR REPLACE FUNCTION velop.epoch_ms(ts TIMESTAMPTZ) RETURNS BIGINT
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
$$ SELECT (EXTRACT(EPOCH FROM ts) * 1000)::BIGINT $$;

CREATE OR REPLACE FUNCTION velop.try_bigint(txt TEXT) RETURNS BIGINT
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE
       WHEN txt ~ '^\s*[-+]?[0-9]+\s*$' AND length(btrim(txt)) <= 18
       THEN btrim(txt)::BIGINT
   END $$;

CREATE OR REPLACE FUNCTION velop.try_double(txt TEXT) RETURNS DOUBLE PRECISION
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE
       WHEN txt ~ '^\s*[-+]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][-+]?[0-9]{1,3})?\s*$'
       THEN btrim(txt)::DOUBLE PRECISION
   END $$;

CREATE TABLE IF NOT EXISTS velop.device (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
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
    extra_macs TEXT[],
    extra_macs_vendor TEXT[]
);

CREATE TABLE IF NOT EXISTS velop.wlan_client (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
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
    fetched_at TIMESTAMPTZ,
    node_mac TEXT,
    node_mac_vendor TEXT,
    node_ip TEXT,
    parent_ip TEXT,
    intf TEXT,
    chan TEXT,
    rssi TEXT,
    speed DOUBLE PRECISION,
    state TEXT,
    "timestamp" BIGINT
);

CREATE TABLE IF NOT EXISTS velop.ping (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
    target TEXT,
    transmitted INTEGER,
    received INTEGER,
    loss_pct DOUBLE PRECISION,
    rtt_min DOUBLE PRECISION,
    rtt_avg DOUBLE PRECISION,
    rtt_max DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS velop.node (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
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
    devinfo JSONB
);

CREATE TABLE IF NOT EXISTS velop.radio_stats (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
    radio TEXT,
    band TEXT,
    source_node_mac TEXT,
    source_node_name TEXT,
    source_node_ip TEXT,
    source_role TEXT,
    stats JSONB
);

CREATE TABLE IF NOT EXISTS velop.radio_config (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
    interface TEXT,
    ssid TEXT,
    mac TEXT,
    mac_vendor TEXT,
    frequency TEXT,
    settings JSONB
);

CREATE TABLE IF NOT EXISTS velop.nic_counter (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
    intf TEXT,
    rx_bytes BIGINT,
    tx_bytes BIGINT
);

CREATE TABLE IF NOT EXISTS velop.system (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
    uptime_secs BIGINT,
    load_1 DOUBLE PRECISION,
    load_5 DOUBLE PRECISION,
    load_15 DOUBLE PRECISION,
    mem_total BIGINT,
    mem_used BIGINT,
    mem_free BIGINT,
    mem_shared BIGINT,
    mem_buffers BIGINT,
    mem_cached BIGINT,
    cpu_idle_pct DOUBLE PRECISION,
    source_node_mac TEXT,
    source_node_name TEXT,
    source_node_ip TEXT,
    source_role TEXT
);

CREATE TABLE IF NOT EXISTS velop.ip_neighbor (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT,
    fetched_at TIMESTAMPTZ,
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
    fetched_at TIMESTAMPTZ,
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
    capabilities JSONB
);

CREATE INDEX IF NOT EXISTS idx_device_fetched_at ON velop.device (fetched_at);
CREATE INDEX IF NOT EXISTS idx_wlan_client_fetched_at ON velop.wlan_client (fetched_at);
CREATE INDEX IF NOT EXISTS idx_backhaul_fetched_at ON velop.backhaul (fetched_at);
CREATE INDEX IF NOT EXISTS idx_ping_fetched_at ON velop.ping (fetched_at);
CREATE INDEX IF NOT EXISTS idx_node_fetched_at ON velop.node (fetched_at);
CREATE INDEX IF NOT EXISTS idx_radio_stats_fetched_at ON velop.radio_stats (fetched_at);
CREATE INDEX IF NOT EXISTS idx_radio_config_fetched_at ON velop.radio_config (fetched_at);
CREATE INDEX IF NOT EXISTS idx_nic_counter_fetched_at ON velop.nic_counter (fetched_at);
CREATE INDEX IF NOT EXISTS idx_system_fetched_at ON velop.system (fetched_at);
CREATE INDEX IF NOT EXISTS idx_ip_neighbor_fetched_at ON velop.ip_neighbor (fetched_at);
CREATE INDEX IF NOT EXISTS idx_lldp_neighbor_fetched_at ON velop.lldp_neighbor (fetched_at);
CREATE INDEX IF NOT EXISTS idx_device_snapshot_id ON velop.device (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_wlan_client_snapshot_id ON velop.wlan_client (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_nic_counter_intf_fetched_at ON velop.nic_counter (intf, fetched_at);
CREATE INDEX IF NOT EXISTS idx_radio_stats_source_node_mac_band_radio_fetched_at ON velop.radio_stats (source_node_mac, band, radio, fetched_at);
CREATE INDEX IF NOT EXISTS idx_ip_neighbor_family_fetched_at ON velop.ip_neighbor (family, fetched_at);
