-- grafana_node_wifi_postgres.sql (PostgreSQL)
--
-- PostgreSQL translation of sql/grafana_node_wifi.sql. Same view, same columns;
-- only the CrateDB-specific SQL changes:
--     fetched_at::BIGINT       ->  velop.epoch_ms(fetched_at)   (epoch-ms)
--     stats['tx_data_bytes']   ->  stats->>'tx_data_bytes'      (JSONB -> text)
--     TRY_CAST(x AS BIGINT)    ->  velop.try_bigint(x)
--     ROUND(<double>, 4)       ->  ROUND(<numeric>, 4)::DOUBLE PRECISION
-- epoch_ms()/try_bigint() are created by sql/velop_schema_postgres.sql; apply
-- that first.
--
-- DIVERGENCE FROM THE CrateDB TWIN: this view derives the previous snapshot with
-- a LAG() WINDOW FUNCTION, where the CrateDB version uses a self-join
-- (`JOIN ... ON b.t_ms < a.t_ms` + `MAX(b.t_ms)`). PostgreSQL plans that
-- self-join as a quadratic merge join -- on a few tens of thousands of rows it
-- already builds millions of intermediate rows and a full scan of the view never
-- finishes, so a Grafana panel just times out. LAG() is one sort per partition.
-- The output columns and their meaning are identical; the only behavioural
-- difference is that two rows sharing a t_ms within a partition are dropped
-- (`WHERE prev_ms < t_ms`) instead of skipped over, which cannot happen here
-- since a snapshot writes one row per series.
-- WiFi throughput per mesh node over time -- one time series per node.
--
-- Each node's radio byte counters (tx_data_bytes + rx_data_bytes, cumulative
-- since boot) are diffed against that radio's immediate predecessor snapshot,
-- summed across the node's radios (wifi0/1/2), and converted to Mbps. The result
-- is one row per (node, snapshot), so a Grafana time-series panel shows one line
-- per node (Router / each satellite).
--
-- DEPENDS ON per-node capture: velop.radio_stats must carry source_node_mac
-- (the watcher fetches each satellite's sysinfo and tags its radios -- see
-- CLAUDE.md / sql/grafana_wifi_vs_wired_postgres.sql). Before that data exists
-- the view still works but shows only the master ('master'), since legacy rows
-- are COALESCEd to one node. A satellite unreachable at capture time
-- contributes no row for that snapshot.
--
-- GRAFANA GOTCHA (see sql/grafana_radio_rates_postgres.sql and CLAUDE.md):
--   Grafana's PostgreSQL frame converter silently drops NUMERIC (OID 1700)
--   columns -- a Grafana-side fault that hits stock PostgreSQL too. Every Mbps
--   column is cast ::DOUBLE PRECISION (float8, OID 701) so Grafana renders it.
--
-- USAGE:
--   1. psql -f sql/velop_schema_postgres.sql   (tables + helper functions)
--   2. psql -f sql/grafana_node_wifi_postgres.sql   (this view)
--   3. Point a Grafana Time series panel at the flat SELECT at the bottom;
--      `node` becomes the series label -> one line per mesh node.


-- ===========================================================================
-- STEP 1 -- create the view
-- ===========================================================================
CREATE OR REPLACE VIEW velop.v_node_wifi_rates AS
WITH rad AS (
  -- one row per radio per node; (node,radio) is the identity (wifi0/1/2 repeat
  -- across nodes). COALESCE keeps legacy master-only rows under one 'master' key.
  SELECT
    COALESCE(source_node_mac, 'master')   AS node,
    -- legacy rows captured before per-node tagging are ALWAYS the master (the
    -- only node the watcher fetched then), whose name is 'Router' -- label them
    -- so they form one continuous 'Router' series rather than a separate 'master'.
    COALESCE(source_node_name, 'Router')  AS node_name,
    radio, fetched_at, velop.epoch_ms(fetched_at) AS t_ms,
    COALESCE(velop.try_bigint(stats->>'tx_data_bytes'), 0) AS tx_bytes,
    COALESCE(velop.try_bigint(stats->>'rx_data_bytes'), 0) AS rx_bytes
  FROM velop.radio_stats
),
d AS (
  -- each snapshot beside its immediate predecessor, per node+radio
  SELECT rad.*,
         LAG(t_ms)    OVER w AS prev_ms,
         LAG(tx_bytes) OVER w AS prev_tx,
         LAG(rx_bytes) OVER w AS prev_rx
  FROM rad
  WINDOW w AS (PARTITION BY node, radio ORDER BY t_ms)
)
SELECT
  fetched_at,
  t_ms,
  node_name AS node,                                                 -- series label
  -- sum the per-interval byte delta across the node's radios -> Mbps.
  -- ::DOUBLE PRECISION is mandatory (2-arg ROUND -> NUMERIC -> Grafana drops it).
  ROUND(SUM((tx_bytes - prev_tx) + (rx_bytes - prev_rx)) * 8.0
        / (MAX(t_ms - prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS wifi_mbps,
  ROUND(SUM(tx_bytes - prev_tx) * 8.0
        / (MAX(t_ms - prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS tx_mbps,
  ROUND(SUM(rx_bytes - prev_rx) * 8.0
        / (MAX(t_ms - prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS rx_mbps
FROM d
WHERE prev_ms < t_ms                                                 -- drops the first snapshot
  AND tx_bytes >= prev_tx AND rx_bytes >= prev_rx                    -- drop reboot intervals
GROUP BY fetched_at, t_ms, node, node_name;


-- NOTE: every panel query below is COMMENTED OUT on purpose. ${__from} /
-- ${__to} are Grafana macros, not SQL, so an uncommented panel query makes
-- this file fail under `psql -f`. Copy one into a panel and drop the `-- `.
--
-- ===========================================================================
-- STEP 2 -- Grafana panel query (flat select against the view)
-- ===========================================================================
-- ${__from}/${__to} render as epoch-ms, matching t_ms. `node` is the series
-- label, so the panel draws one WiFi line per mesh node. Order by time AND node
-- so the per-node rows at each snapshot come back deterministically.

-- Total WiFi (tx+rx) per node:
-- SELECT fetched_at AS "time", node, wifi_mbps
-- FROM velop.v_node_wifi_rates
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC, 2 ASC;

-- TX only per node (downstream-to-clients):
-- SELECT fetched_at AS "time", node, tx_mbps
-- FROM velop.v_node_wifi_rates
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC, 2 ASC;

-- RX only per node (upstream-from-clients):
-- SELECT fetched_at AS "time", node, rx_mbps
-- FROM velop.v_node_wifi_rates
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC, 2 ASC;

-- Ad-hoc inspection in psql (no Grafana vars):
-- SELECT fetched_at, node, wifi_mbps FROM velop.v_node_wifi_rates ORDER BY t_ms, node;
