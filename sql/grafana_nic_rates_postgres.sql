-- grafana_nic_rates_postgres.sql (PostgreSQL)
--
-- PostgreSQL translation of sql/grafana_nic_rates.sql. Same view, same columns;
-- only the CrateDB-specific SQL changes:
--     fetched_at::BIGINT  ->  velop.epoch_ms(fetched_at)      (epoch-ms)
--     ROUND(<double>, 4)  ->  ROUND(<numeric>, 4)::DOUBLE PRECISION
-- velop.epoch_ms() is created by sql/velop_schema_postgres.sql; apply that first.
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
-- GRAFANA GOTCHA (see also sql/grafana_radio_rates_postgres.sql and CLAUDE.md):
--   Grafana's PostgreSQL frame converter silently drops result columns typed
--   NUMERIC (OID 1700) -> empty panel, HTTP 200, no error. It is a Grafana-side
--   fault, so it applies to stock PostgreSQL as much as to CrateDB. In
--   PostgreSQL two-argument ROUND is NUMERIC-only, so every rate column here is
--   necessarily NUMERIC inside the view and MUST be cast ::DOUBLE PRECISION
--   (float8, OID 701) on the way out. Note elapsed_secs is cast too: it is a
--   plain division, which PostgreSQL also types NUMERIC (CrateDB typed it
--   DOUBLE, so the CrateDB version gets away without the cast).
--
-- USAGE:
--   1. psql -f sql/velop_schema_postgres.sql   (tables + helper functions)
--   2. psql -f sql/grafana_nic_rates_postgres.sql   (this view)
--   3. Point each Grafana panel at the flat SELECT at the bottom.


-- ===========================================================================
-- STEP 1 -- create the view
-- ===========================================================================
CREATE OR REPLACE VIEW velop.v_nic_rates AS
WITH s AS (
  SELECT intf,
         fetched_at,
         velop.epoch_ms(fetched_at) AS t_ms,
         rx_bytes,
         tx_bytes
  FROM velop.nic_counter
),
d AS (
  -- each snapshot beside its immediate predecessor for the SAME interface
  SELECT intf, fetched_at, t_ms, rx_bytes, tx_bytes,
         LAG(t_ms)     OVER w AS prev_ms,
         LAG(rx_bytes) OVER w AS prev_rx,
         LAG(tx_bytes) OVER w AS prev_tx
  FROM s
  WINDOW w AS (PARTITION BY intf ORDER BY t_ms)
)
SELECT
  fetched_at,
  t_ms,
  intf,
  rx_bytes,
  tx_bytes,
  (rx_bytes - prev_rx)                    AS d_rx_bytes,
  (tx_bytes - prev_tx)                    AS d_tx_bytes,
  ((t_ms - prev_ms) / 1000.0)::DOUBLE PRECISION AS elapsed_secs,
  -- throughput over the interval, Mbps. ::DOUBLE PRECISION is mandatory: 2-arg
  -- ROUND is NUMERIC-only here, and Grafana's frame converter drops NUMERIC.
  ROUND((rx_bytes - prev_rx) * 8.0 / ((t_ms - prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS rx_mbps,
  ROUND((tx_bytes - prev_tx) * 8.0 / ((t_ms - prev_ms) / 1000.0) / 1e6, 4)::DOUBLE PRECISION AS tx_mbps
FROM d
WHERE prev_ms < t_ms                 -- drops the first snapshot (and any t_ms tie)
  AND (rx_bytes - prev_rx) >= 0      -- drop reboot intervals (counter reset)
  AND (tx_bytes - prev_tx) >= 0;


-- NOTE: every panel query below is COMMENTED OUT on purpose. ${__from} /
-- ${__to} are Grafana macros, not SQL, so an uncommented panel query makes
-- this file fail under `psql -f`. Copy one into a panel and drop the `-- `.
--
-- ===========================================================================
-- STEP 2 -- Grafana panel queries (flat selects against the view)
-- ===========================================================================
-- ${__from}/${__to} are Grafana global vars; they render as epoch-ms, matching
-- t_ms exactly (integer vs integer). Keep these flat: no CTEs/joins/windows.

-- RX + TX throughput per interface (Time series panel; intf is the series label):
-- SELECT fetched_at AS "time", intf, rx_mbps, tx_mbps
-- FROM velop.v_nic_rates
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC;

-- Ad-hoc inspection in psql (no Grafana vars):
-- SELECT fetched_at, intf, d_rx_bytes, d_tx_bytes, elapsed_secs, rx_mbps, tx_mbps
-- FROM velop.v_nic_rates
-- ORDER BY intf, fetched_at;
