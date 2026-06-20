-- grafana_radio_rates.sql
--
-- Per-snapshot throughput & link-quality rates derived from velop.radio_stats.
--
-- WHY THIS EXISTS / THE GOTCHA:
--   Grafana's PostgreSQL datasource silently returns an EMPTY frame (HTTP 200,
--   zero rows, no error) when a result column has a pg type its frame converter
--   can't handle -- most commonly NUMERIC (OID 1700). This is a Grafana-side
--   conversion fault, NOT a CrateDB pg-wire limitation: the very same query
--   returns its rows correctly over the HTTP endpoint (4200, the Crate UI /
--   `crate` client), over psql (simple protocol), and over asyncpg (extended
--   Parse/Bind/Execute). The usual trigger is the two-argument
--   ROUND(value, scale), which CrateDB types as NUMERIC.
--
--   THE FIX: cast every computed numeric column to DOUBLE PRECISION (float8,
--   OID 701) -- e.g. ROUND(..., 4)::DOUBLE PRECISION -- which Grafana renders.
--   Query *shape* is not the issue: window functions, multi-CTE/multi-join, and
--   TIMESTAMP-equality joins all work fine over pg-wire. (A full reproduction of
--   the NUMERIC/Grafana behaviour lives in crate_issue/, which is git-ignored.)
--
--   Keeping the rate logic in a server-side VIEW and letting Grafana send only a
--   flat SELECT against it is still good practice for readability, but the
--   load-bearing change is the ::DOUBLE PRECISION cast on the computed columns.
--
-- radio_stats counters are cumulative since the node booted, so a "rate" is the
-- delta between a snapshot and its immediate predecessor for the same radio,
-- divided by the elapsed time. We compute that with a self-join; joins are on
-- text (node/band/radio) or BIGINT epoch-ms (t_ms) keys. `fetched_at` is carried
-- only for display on the time axis.
--
-- radio_stats now holds one row per radio PER NODE (the watcher fetches each
-- satellite's sysinfo too), so the self-join key includes source_node_mac --
-- otherwise the satellites' wifi0/1/2 would collide with the master's. The
-- metric label is prefixed with the node name.
--
-- USAGE:
--   1. Run the CREATE VIEW below ONCE in the Crate Admin UI (HTTP endpoint),
--      where complex DDL/SQL is reliable. Re-running needs CREATE OR REPLACE.
--   2. Point each Grafana panel at one of the flat SELECTs at the bottom.


-- ===========================================================================
-- STEP 1 -- create the view (run in the Crate UI, not Grafana)
-- ===========================================================================
CREATE OR REPLACE VIEW velop.v_radio_rates AS
WITH s AS (
  -- radio names (wifi0/1/2) repeat across mesh nodes and bands differ by model,
  -- so a radio's identity is (node, band, radio). COALESCE keeps legacy
  -- master-only rows (captured before per-node tagging) joinable under one key.
  SELECT
    COALESCE(source_node_mac, 'master')  AS node,
    COALESCE(source_node_name, 'master') AS node_name,
    band, radio, fetched_at, fetched_at::BIGINT AS t_ms,
    TRY_CAST(stats['tx_data_bytes']      AS BIGINT) AS tx_bytes,
    TRY_CAST(stats['rx_data_bytes']      AS BIGINT) AS rx_bytes,
    TRY_CAST(stats['tx_data_packets']    AS BIGINT) AS tx_pkts,
    TRY_CAST(stats['tx_failures']        AS BIGINT) AS tx_failures,
    TRY_CAST(stats['rx_rssi']            AS BIGINT) AS rx_rssi,
    TRY_CAST(stats['self_bss_chan_util'] AS BIGINT) AS self_bss_util,
    TRY_CAST(stats['obss_chan_util']     AS BIGINT) AS obss_util,
    TRY_CAST(stats['lithium_cycle_cnt_chan_nf_bdf_averaged_nf_dbm'] AS BIGINT) AS noise_floor_dbm
  FROM velop.radio_stats
),
pairs AS (
  -- each snapshot paired with its immediate predecessor (per node+radio) via self-join
  SELECT a.node, a.band, a.radio, a.t_ms AS cur_ms, MAX(b.t_ms) AS prev_ms
  FROM s a
  JOIN s b ON a.node = b.node AND a.band = b.band AND a.radio = b.radio AND b.t_ms < a.t_ms
  GROUP BY a.node, a.band, a.radio, a.t_ms
)
SELECT
  cur.fetched_at,
  cur.t_ms,
  cur.node_name || ' ' || p.band || ' / ' || p.radio AS metric,
  -- throughput = delta bytes * 8 bits / elapsed seconds / 1e6 -> Mbps.
  -- ::DOUBLE PRECISION on each ROUND() is mandatory: the 2-arg ROUND yields
  -- NUMERIC, which Grafana's frame converter drops (empty panel). See header.
  ROUND((cur.tx_bytes - prv.tx_bytes) * 8.0 / ((p.cur_ms - p.prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS tx_mbps,
  ROUND((cur.rx_bytes - prv.rx_bytes) * 8.0 / ((p.cur_ms - p.prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS rx_mbps,
  cur.rx_rssi,
  cur.noise_floor_dbm,
  -- rough SNR margin: rx_rssi minus the (negative) noise floor
  (cur.rx_rssi + cur.noise_floor_dbm)                              AS snr_db_approx,
  cur.self_bss_util,
  cur.obss_util,
  -- tx failure rate over the interval, as a percentage of tx packets
  -- (::DOUBLE PRECISION for the same Grafana-drops-NUMERIC reason as above)
  ROUND(100.0 * (cur.tx_failures - prv.tx_failures) / NULLIF(cur.tx_pkts - prv.tx_pkts, 0), 3)::DOUBLE PRECISION AS tx_fail_pct
FROM pairs p
JOIN s cur ON cur.node = p.node AND cur.band = p.band AND cur.radio = p.radio AND cur.t_ms = p.cur_ms
JOIN s prv ON prv.node = p.node AND prv.band = p.band AND prv.radio = p.radio AND prv.t_ms = p.prev_ms
WHERE (cur.tx_bytes - prv.tx_bytes) >= 0;   -- drop reboot intervals (counter reset)


-- ===========================================================================
-- STEP 2 -- Grafana panel queries (flat selects against the view)
-- ===========================================================================
-- ${__from}/${__to} are Grafana global vars; they render as epoch-ms, matching
-- t_ms exactly (integer vs integer -- no timezone interpretation). Keep these
-- flat: no CTEs, joins, or window functions in what Grafana sends.

-- TX throughput per radio (Time series panel; one value column + a label):
SELECT fetched_at AS "time", metric, tx_mbps
FROM velop.v_radio_rates
WHERE t_ms BETWEEN ${__from} AND ${__to}
ORDER BY 1 ASC;

-- RX throughput per radio:
-- SELECT fetched_at AS "time", metric, rx_mbps
-- FROM velop.v_radio_rates
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC;

-- TX failure rate (%) per radio:
-- SELECT fetched_at AS "time", metric, tx_fail_pct
-- FROM velop.v_radio_rates
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC;

-- Link quality gauges (RSSI / noise floor / SNR / channel utilization).
-- Multiple numeric columns + a string label -> use a Table panel, or split
-- into one Time series panel per measure:
-- SELECT fetched_at AS "time", metric,
--        rx_rssi, noise_floor_dbm, snr_db_approx, self_bss_util, obss_util
-- FROM velop.v_radio_rates
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC;
