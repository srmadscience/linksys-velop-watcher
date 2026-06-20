-- grafana_nic_rates.sql
--
-- Per-snapshot byte-rate (throughput) per network interface, derived from
-- velop.nic_counter.
--
-- velop.nic_counter holds the kernel NIC byte counters (br0, eth0, eth1, ...),
-- which are CUMULATIVE since the node booted. A "rate" is therefore the delta
-- between a snapshot and its immediate predecessor for the SAME interface,
-- divided by the elapsed time. We compute that with a self-join (not a window
-- function); all joins are on text (intf) or BIGINT epoch-ms (t_ms) keys.
--
-- GRAFANA GOTCHA (see also sql/grafana_radio_rates.sql and CLAUDE.md):
--   Grafana's PostgreSQL frame converter silently drops result columns typed
--   NUMERIC (OID 1700), which CrateDB's two-argument ROUND(value, scale)
--   produces -> empty panel, HTTP 200, no error. Each rate column below is cast
--   ::DOUBLE PRECISION (float8, OID 701) so Grafana renders it.
--
-- USAGE:
--   1. Run the CREATE OR REPLACE VIEW once in the Crate Admin UI (HTTP :4200).
--   2. Point each Grafana panel at the flat SELECT at the bottom.


-- ===========================================================================
-- STEP 1 -- create the view (run in the Crate UI, not Grafana)
-- ===========================================================================
CREATE OR REPLACE VIEW velop.v_nic_rates AS
WITH s AS (
  SELECT intf,
         fetched_at,
         fetched_at::BIGINT AS t_ms,
         rx_bytes,
         tx_bytes
  FROM velop.nic_counter
),
pairs AS (
  -- each snapshot paired with its immediate predecessor (per interface)
  SELECT a.intf, a.t_ms AS cur_ms, MAX(b.t_ms) AS prev_ms
  FROM s a
  JOIN s b ON a.intf = b.intf AND b.t_ms < a.t_ms
  GROUP BY a.intf, a.t_ms
)
SELECT
  cur.fetched_at,
  cur.t_ms,
  p.intf,
  cur.rx_bytes,
  cur.tx_bytes,
  (cur.rx_bytes - prv.rx_bytes)                   AS d_rx_bytes,
  (cur.tx_bytes - prv.tx_bytes)                   AS d_tx_bytes,
  (p.cur_ms - p.prev_ms) / 1000.0                 AS elapsed_secs,
  -- throughput over the interval, Mbps. ::DOUBLE PRECISION is mandatory: the
  -- 2-arg ROUND yields NUMERIC, which Grafana's frame converter drops.
  ROUND((cur.rx_bytes - prv.rx_bytes) * 8.0 / ((p.cur_ms - p.prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS rx_mbps,
  ROUND((cur.tx_bytes - prv.tx_bytes) * 8.0 / ((p.cur_ms - p.prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS tx_mbps
FROM pairs p
JOIN s cur ON cur.intf = p.intf AND cur.t_ms = p.cur_ms
JOIN s prv ON prv.intf = p.intf AND prv.t_ms = p.prev_ms
WHERE (cur.rx_bytes - prv.rx_bytes) >= 0    -- drop reboot intervals (counter reset)
  AND (cur.tx_bytes - prv.tx_bytes) >= 0;


-- ===========================================================================
-- STEP 2 -- Grafana panel queries (flat selects against the view)
-- ===========================================================================
-- ${__from}/${__to} are Grafana global vars; they render as epoch-ms, matching
-- t_ms exactly (integer vs integer). Keep these flat: no CTEs/joins/windows.

-- RX + TX throughput per interface (Time series panel; intf is the series label):
SELECT fetched_at AS "time", intf, rx_mbps, tx_mbps
FROM velop.v_nic_rates
WHERE t_ms BETWEEN ${__from} AND ${__to}
ORDER BY 1 ASC;

-- Ad-hoc inspection in the Crate UI (no Grafana vars):
-- SELECT fetched_at, intf, d_rx_bytes, d_tx_bytes, elapsed_secs, rx_mbps, tx_mbps
-- FROM velop.v_nic_rates
-- ORDER BY intf, fetched_at;
