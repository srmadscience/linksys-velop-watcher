-- grafana_wifi_vs_wired_postgres.sql (PostgreSQL)
--
-- PostgreSQL translation of sql/grafana_wifi_vs_wired.sql. Same view, same
-- columns; only the CrateDB-specific SQL changes:
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
-- WiFi vs Wired throughput per snapshot, as two time series ('WiFi' / 'Wired').
--
-- Two cumulative-counter sources are diffed against each snapshot's immediate
-- predecessor (self-join, per interface/radio), summed per snapshot, converted
-- to Mbps, and unioned:
--   * WIRED -> velop.nic_counter, physical Ethernet ports eth0 + eth1.
--             br0 is the software bridge (aggregate; carries WiFi-side traffic
--             too) so it is EXCLUDED to avoid double counting; eth2 is idle.
--   * WIFI  -> velop.radio_stats, per-radio tx_data_bytes + rx_data_bytes,
--             summed across EVERY mesh node's radios (the watcher fetches each
--             satellite's sysinfo, so radio_stats holds all nodes' radios,
--             tagged by source_node_mac). This is whole-mesh WiFi airtime.
-- Both tables share fetched_at per snapshot, so the two series line up in time.
--
-- HISTORY / CAVEAT:
--   Before per-node capture, radio_stats held only the MASTER's 3 radios, which
--   serve few direct clients (most client traffic rides the satellites), so the
--   WiFi line was ~100x smaller than Wired and looked missing. It is now summed
--   over all nodes' radios and is comparable to Wired. Snapshots captured before
--   the per-node change still hold master-only radios (COALESCEd to 'master'),
--   so the WiFi series understates those earlier periods. A satellite that was
--   unreachable at capture time contributes no radios for that snapshot.
--
-- GRAFANA GOTCHA (see sql/grafana_radio_rates_postgres.sql and CLAUDE.md):
--   Grafana's PostgreSQL frame converter silently drops NUMERIC (OID 1700)
--   columns -- a Grafana-side fault that hits stock PostgreSQL too. Here the
--   two-argument ROUND is NUMERIC-only anyway, so mbps is cast
--   ::DOUBLE PRECISION (float8, OID 701) on the way out.
--
-- USAGE:
--   1. psql -f sql/velop_schema_postgres.sql   (tables + helper functions)
--   2. psql -f sql/grafana_wifi_vs_wired_postgres.sql   (this view)
--   3. Point each Grafana panel at the flat SELECT at the bottom.


-- ===========================================================================
-- STEP 1 -- create the view
-- ===========================================================================
CREATE OR REPLACE VIEW velop.v_wifi_vs_wired AS
WITH
nic AS (
  SELECT intf, fetched_at, velop.epoch_ms(fetched_at) AS t_ms, (rx_bytes + tx_bytes) AS bytes
  FROM velop.nic_counter
  WHERE intf IN ('eth0','eth1')          -- physical wired ports; br0=bridge(aggregate), eth2 idle
),
nic_d AS (
  SELECT nic.*,
         LAG(t_ms)  OVER w AS prev_ms,
         LAG(bytes) OVER w AS prev_bytes
  FROM nic
  WINDOW w AS (PARTITION BY intf ORDER BY t_ms)
),
wired AS (
  SELECT fetched_at, t_ms, 'Wired' AS category,
         SUM(bytes - prev_bytes)      AS d_bytes,
         MAX(t_ms - prev_ms)/1000.0   AS secs   -- interval (shared across intf per snapshot)
  FROM nic_d
  WHERE prev_ms < t_ms                  -- drops the first snapshot
    AND bytes >= prev_bytes             -- drop reboot intervals (counter reset)
  GROUP BY fetched_at, t_ms
),
rad AS (
  -- one row per radio PER NODE; (node,radio) is the identity (wifi0/1/2 repeat
  -- across nodes). COALESCE keeps legacy master-only rows joinable under 'master'.
  SELECT COALESCE(source_node_mac, 'master') AS node, radio,
         fetched_at, velop.epoch_ms(fetched_at) AS t_ms,
         COALESCE(velop.try_bigint(stats->>'tx_data_bytes'),0)
       + COALESCE(velop.try_bigint(stats->>'rx_data_bytes'),0) AS bytes
  FROM velop.radio_stats
),
rad_d AS (
  SELECT rad.*,
         LAG(t_ms)  OVER w AS prev_ms,
         LAG(bytes) OVER w AS prev_bytes
  FROM rad
  WINDOW w AS (PARTITION BY node, radio ORDER BY t_ms)
),
wifi AS (
  -- sum the per-interval delta across EVERY node's radios -> whole-mesh WiFi.
  SELECT fetched_at, t_ms, 'WiFi' AS category,
         SUM(bytes - prev_bytes)      AS d_bytes,
         MAX(t_ms - prev_ms)/1000.0   AS secs
  FROM rad_d
  WHERE prev_ms < t_ms
    AND bytes >= prev_bytes
  GROUP BY fetched_at, t_ms
)
SELECT
  u.fetched_at,
  u.t_ms,
  u.category,                                                       -- 'WiFi' / 'Wired' (series label)
  ROUND(u.d_bytes * 8.0 / u.secs / 1e6, 4)::DOUBLE PRECISION AS mbps
FROM (SELECT * FROM wired UNION ALL SELECT * FROM wifi) u
WHERE u.secs > 0;


-- NOTE: every panel query below is COMMENTED OUT on purpose. ${__from} /
-- ${__to} are Grafana macros, not SQL, so an uncommented panel query makes
-- this file fail under `psql -f`. Copy one into a panel and drop the `-- `.
--
-- ===========================================================================
-- STEP 2 -- Grafana panel query (flat select against the view)
-- ===========================================================================
-- ${__from}/${__to} render as epoch-ms, matching t_ms. category becomes the
-- series label, so the panel shows one WiFi line and one Wired line. WiFi is
-- whole-mesh (all nodes' radios), so it is comparable to Wired; for snapshots
-- captured before per-node fetch it is master-only and will look small. Order by
-- time AND category so the two rows per snapshot come back deterministically
-- rather than wired-first on a tie.
--     SELECT fetched_at AS "time", category, mbps
--     FROM velop.v_wifi_vs_wired
--     WHERE t_ms BETWEEN ${__from} AND ${__to}
--     ORDER BY 1 ASC, 2 ASC;

-- Ad-hoc inspection in psql (no Grafana vars): both series interleaved, so you
-- can confirm WiFi is present (small) at every snapshot, not dropped.
-- SELECT fetched_at, category, mbps FROM velop.v_wifi_vs_wired ORDER BY t_ms, category;
