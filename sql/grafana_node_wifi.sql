-- grafana_node_wifi.sql
--
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
-- CLAUDE.md / sql/grafana_wifi_vs_wired.sql). Before that data exists the view
-- still works but shows only the master ('master'), since legacy rows are
-- COALESCEd to one node. A satellite unreachable at capture time contributes no
-- row for that snapshot.
--
-- GRAFANA GOTCHA (see CLAUDE.md / sql/grafana_radio_rates.sql):
--   Grafana's PostgreSQL frame converter silently drops NUMERIC (OID 1700)
--   columns, which the two-argument ROUND(value, scale) produces. Every Mbps
--   column is cast ::DOUBLE PRECISION (float8, OID 701) so Grafana renders it.
--
-- USAGE:
--   1. Run the CREATE OR REPLACE VIEW once in the Crate Admin UI (HTTP :4200).
--   2. Point a Grafana Time series panel at the flat SELECT at the bottom;
--      `node` becomes the series label -> one line per mesh node.


-- ===========================================================================
-- STEP 1 -- create the view (run in the Crate UI, not Grafana)
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
    radio, fetched_at, fetched_at::BIGINT AS t_ms,
    COALESCE(TRY_CAST(stats['tx_data_bytes'] AS BIGINT), 0) AS tx_bytes,
    COALESCE(TRY_CAST(stats['rx_data_bytes'] AS BIGINT), 0) AS rx_bytes
  FROM velop.radio_stats
),
rad_pairs AS (
  -- each snapshot paired with its immediate predecessor, per node+radio
  SELECT a.node, a.radio, a.t_ms AS cur_ms, MAX(b.t_ms) AS prev_ms
  FROM rad a JOIN rad b ON a.node = b.node AND a.radio = b.radio AND b.t_ms < a.t_ms
  GROUP BY a.node, a.radio, a.t_ms
)
SELECT
  cur.fetched_at,
  cur.t_ms,
  cur.node_name AS node,                                             -- series label
  -- sum the per-interval byte delta across the node's radios -> Mbps.
  -- ::DOUBLE PRECISION is mandatory (2-arg ROUND -> NUMERIC -> Grafana drops it).
  ROUND(SUM((cur.tx_bytes - prv.tx_bytes) + (cur.rx_bytes - prv.rx_bytes)) * 8.0
        / (MAX(p.cur_ms - p.prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS wifi_mbps,
  ROUND(SUM(cur.tx_bytes - prv.tx_bytes) * 8.0
        / (MAX(p.cur_ms - p.prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS tx_mbps,
  ROUND(SUM(cur.rx_bytes - prv.rx_bytes) * 8.0
        / (MAX(p.cur_ms - p.prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS rx_mbps
FROM rad_pairs p
JOIN rad cur ON cur.node = p.node AND cur.radio = p.radio AND cur.t_ms = p.cur_ms
JOIN rad prv ON prv.node = p.node AND prv.radio = p.radio AND prv.t_ms = p.prev_ms
WHERE cur.tx_bytes >= prv.tx_bytes AND cur.rx_bytes >= prv.rx_bytes  -- drop reboot intervals
GROUP BY cur.fetched_at, cur.t_ms, cur.node, cur.node_name;


-- ===========================================================================
-- STEP 2 -- Grafana panel query (flat select against the view)
-- ===========================================================================
-- ${__from}/${__to} render as epoch-ms, matching t_ms. `node` is the series
-- label, so the panel draws one WiFi line per mesh node. Order by time AND node
-- so the per-node rows at each snapshot come back deterministically.

-- Total WiFi (tx+rx) per node:
SELECT fetched_at AS "time", node, wifi_mbps
FROM velop.v_node_wifi_rates
WHERE t_ms BETWEEN ${__from} AND ${__to}
ORDER BY 1 ASC, 2 ASC;

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

-- Ad-hoc inspection in the Crate UI (no Grafana vars):
-- SELECT fetched_at, node, wifi_mbps FROM velop.v_node_wifi_rates ORDER BY t_ms, node;
