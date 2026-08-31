-- grafana_radio_rates_postgres.sql (PostgreSQL)
--
-- PostgreSQL translation of sql/grafana_radio_rates.sql. Same view, same
-- columns, same Grafana panel queries; only the CrateDB-specific SQL changes:
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
--   CrateDB                      PostgreSQL (this file)
--   ---------------------------  -------------------------------------------
--   fetched_at::BIGINT           velop.epoch_ms(fetched_at)   -- epoch-ms
--   stats['tx_data_bytes']       stats->>'tx_data_bytes'      -- JSONB -> text
--   TRY_CAST(x AS BIGINT)        velop.try_bigint(x)
--   ROUND(<double>, 4)           ROUND(<numeric>, 4)          -- see below
--
-- epoch_ms()/try_bigint() are created by sql/velop_schema_postgres.sql; apply
-- that first.
--
-- Per-snapshot throughput & link-quality rates derived from velop.radio_stats.
--
-- THE GRAFANA GOTCHA STILL APPLIES:
--   Grafana's PostgreSQL datasource silently returns an EMPTY frame (HTTP 200,
--   zero rows, no error) when a result column has a pg type its frame converter
--   can't handle -- most commonly NUMERIC (OID 1700). That is a Grafana-side
--   fault, so it bites stock PostgreSQL exactly as it bites CrateDB.
--   THE FIX IS THE SAME: cast every computed numeric column to DOUBLE PRECISION
--   (float8, OID 701) before it leaves the query.
--
--   PostgreSQL makes the NUMERIC even harder to avoid: two-argument ROUND is
--   only defined for NUMERIC (there is no round(double precision, integer)), so
--   the argument must BE numeric and the result must then be cast back out with
--   ::DOUBLE PRECISION. Untyped literals like 8.0 and 1e6 are already numeric in
--   PostgreSQL, so bigint * 8.0 / ... is numeric and ROUND(..., 4) type-checks;
--   the trailing ::DOUBLE PRECISION is the load-bearing part.
--
-- radio_stats counters are cumulative since the node booted, so a "rate" is the
-- delta between a snapshot and its immediate predecessor for the same radio,
-- divided by the elapsed time. We compute that with a self-join; joins are on
-- text (node/band/radio) or BIGINT epoch-ms (t_ms) keys. `fetched_at` is carried
-- only for display on the time axis.
--
-- radio_stats holds one row per radio PER NODE (the watcher fetches each
-- satellite's sysinfo too), so the self-join key includes source_node_mac --
-- otherwise the satellites' wifi0/1/2 would collide with the master's. The
-- metric label is prefixed with the node name.
--
-- USAGE:
--   1. psql -f sql/velop_schema_postgres.sql   (tables + helper functions)
--   2. psql -f sql/grafana_radio_rates_postgres.sql   (this view)
--   3. Point each Grafana panel at one of the flat SELECTs at the bottom.


-- ===========================================================================
-- STEP 1 -- create the view
-- ===========================================================================
CREATE OR REPLACE VIEW velop.v_radio_rates AS
WITH s AS (
  -- radio names (wifi0/1/2) repeat across mesh nodes and bands differ by model,
  -- so a radio's identity is (node, band, radio). COALESCE keeps legacy
  -- master-only rows (captured before per-node tagging) joinable under one key.
  SELECT
    COALESCE(source_node_mac, 'master')  AS node,
    COALESCE(source_node_name, 'master') AS node_name,
    band, radio, fetched_at, velop.epoch_ms(fetched_at) AS t_ms,
    velop.try_bigint(stats->>'tx_data_bytes')      AS tx_bytes,
    velop.try_bigint(stats->>'rx_data_bytes')      AS rx_bytes,
    velop.try_bigint(stats->>'tx_data_packets')    AS tx_pkts,
    velop.try_bigint(stats->>'tx_failures')        AS tx_failures,
    velop.try_bigint(stats->>'rx_rssi')            AS rx_rssi,
    velop.try_bigint(stats->>'self_bss_chan_util') AS self_bss_util,
    velop.try_bigint(stats->>'obss_chan_util')     AS obss_util,
    velop.try_bigint(stats->>'lithium_cycle_cnt_chan_nf_bdf_averaged_nf_dbm') AS noise_floor_dbm
  FROM velop.radio_stats
),
d AS (
  -- each snapshot beside its immediate predecessor for the same (node,band,radio)
  SELECT s.*,
         LAG(t_ms)        OVER w AS prev_ms,
         LAG(tx_bytes)    OVER w AS prev_tx_bytes,
         LAG(rx_bytes)    OVER w AS prev_rx_bytes,
         LAG(tx_pkts)     OVER w AS prev_tx_pkts,
         LAG(tx_failures) OVER w AS prev_tx_failures
  FROM s
  WINDOW w AS (PARTITION BY node, band, radio ORDER BY t_ms)
)
SELECT
  fetched_at,
  t_ms,
  node_name || ' ' || band || ' / ' || radio AS metric,
  -- throughput = delta bytes * 8 bits / elapsed seconds / 1e6 -> Mbps.
  -- ::DOUBLE PRECISION on each ROUND() is mandatory: 2-arg ROUND is NUMERIC-only
  -- in PostgreSQL, and Grafana's frame converter drops NUMERIC (empty panel).
  ROUND((tx_bytes - prev_tx_bytes) * 8.0 / ((t_ms - prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS tx_mbps,
  ROUND((rx_bytes - prev_rx_bytes) * 8.0 / ((t_ms - prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS rx_mbps,
  rx_rssi,
  noise_floor_dbm,
  -- rough SNR margin: rx_rssi minus the (negative) noise floor
  (rx_rssi + noise_floor_dbm)                                      AS snr_db_approx,
  self_bss_util,
  obss_util,
  -- tx failure rate over the interval, as a percentage of tx packets
  -- (::DOUBLE PRECISION for the same Grafana-drops-NUMERIC reason as above)
  ROUND(100.0 * (tx_failures - prev_tx_failures) / NULLIF(tx_pkts - prev_tx_pkts, 0), 3)::DOUBLE PRECISION AS tx_fail_pct
FROM d
WHERE prev_ms < t_ms                          -- drops the first snapshot per radio
  AND (tx_bytes - prev_tx_bytes) >= 0;        -- drop reboot intervals (counter reset)

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
