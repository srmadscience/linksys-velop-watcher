-- grafana_wifi_vs_wired.sql
--
-- WiFi vs Wired throughput per snapshot, as two time series ('WiFi' / 'Wired').
--
-- Two cumulative-counter sources are diffed against each snapshot's immediate
-- predecessor (self-join, per interface/radio), summed per snapshot, converted
-- to Mbps, and unioned:
--   * WIRED -> velop.nic_counter, physical Ethernet ports eth0 + eth1.
--             br0 is the software bridge (aggregate; carries WiFi-side traffic
--             too) so it is EXCLUDED to avoid double counting; eth2 is idle.
--   * WIFI  -> velop.radio_stats, per-radio tx_data_bytes + rx_data_bytes,
--             summed across EVERY mesh node's radios (the watcher now fetches
--             each satellite's sysinfo, so radio_stats holds all nodes' radios,
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
-- GRAFANA GOTCHA (see CLAUDE.md / sql/grafana_radio_rates.sql):
--   Grafana's PostgreSQL frame converter silently drops NUMERIC (OID 1700)
--   columns, which the two-argument ROUND(value, scale) produces. The mbps
--   column is cast ::DOUBLE PRECISION (float8, OID 701) so Grafana renders it.
--
-- USAGE:
--   1. Run the CREATE OR REPLACE VIEW once in the Crate Admin UI (HTTP :4200).
--   2. Point each Grafana panel at the flat SELECT at the bottom.


-- ===========================================================================
-- STEP 1 -- create the view (run in the Crate UI, not Grafana)
-- ===========================================================================
CREATE OR REPLACE VIEW velop.v_wifi_vs_wired AS
WITH
nic AS (
  SELECT intf, fetched_at, fetched_at::BIGINT AS t_ms, (rx_bytes + tx_bytes) AS bytes
  FROM velop.nic_counter
  WHERE intf IN ('eth0','eth1')          -- physical wired ports; br0=bridge(aggregate), eth2 idle
),
nic_pairs AS (
  SELECT a.intf, a.t_ms AS cur_ms, MAX(b.t_ms) AS prev_ms
  FROM nic a JOIN nic b ON a.intf = b.intf AND b.t_ms < a.t_ms
  GROUP BY a.intf, a.t_ms
),
wired AS (
  SELECT cur.fetched_at, cur.t_ms, 'Wired' AS category,
         SUM(cur.bytes - prv.bytes)        AS d_bytes,
         MAX(p.cur_ms - p.prev_ms)/1000.0  AS secs   -- interval (shared across intf per snapshot)
  FROM nic_pairs p
  JOIN nic cur ON cur.intf = p.intf AND cur.t_ms = p.cur_ms
  JOIN nic prv ON prv.intf = p.intf AND prv.t_ms = p.prev_ms
  WHERE cur.bytes >= prv.bytes             -- drop reboot intervals (counter reset)
  GROUP BY cur.fetched_at, cur.t_ms
),
rad AS (
  -- one row per radio PER NODE; (node,radio) is the identity (wifi0/1/2 repeat
  -- across nodes). COALESCE keeps legacy master-only rows joinable under 'master'.
  SELECT COALESCE(source_node_mac, 'master') AS node, radio,
         fetched_at, fetched_at::BIGINT AS t_ms,
         COALESCE(TRY_CAST(stats['tx_data_bytes'] AS BIGINT),0)
       + COALESCE(TRY_CAST(stats['rx_data_bytes'] AS BIGINT),0) AS bytes
  FROM velop.radio_stats
),
rad_pairs AS (
  SELECT a.node, a.radio, a.t_ms AS cur_ms, MAX(b.t_ms) AS prev_ms
  FROM rad a JOIN rad b ON a.node = b.node AND a.radio = b.radio AND b.t_ms < a.t_ms
  GROUP BY a.node, a.radio, a.t_ms
),
wifi AS (
  -- sum the per-interval delta across EVERY node's radios -> whole-mesh WiFi.
  SELECT cur.fetched_at, cur.t_ms, 'WiFi' AS category,
         SUM(cur.bytes - prv.bytes)        AS d_bytes,
         MAX(p.cur_ms - p.prev_ms)/1000.0  AS secs
  FROM rad_pairs p
  JOIN rad cur ON cur.node = p.node AND cur.radio = p.radio AND cur.t_ms = p.cur_ms
  JOIN rad prv ON prv.node = p.node AND prv.radio = p.radio AND prv.t_ms = p.prev_ms
  WHERE cur.bytes >= prv.bytes
  GROUP BY cur.fetched_at, cur.t_ms
)
SELECT
  u.fetched_at,
  u.t_ms,
  u.category,                                                       -- 'WiFi' / 'Wired' (series label)
  ROUND(u.d_bytes * 8.0 / u.secs / 1e6, 4)::DOUBLE PRECISION AS mbps
FROM (SELECT * FROM wired UNION ALL SELECT * FROM wifi) u
WHERE u.secs > 0;


-- ===========================================================================
-- STEP 2 -- Grafana panel query (flat select against the view)
-- ===========================================================================
-- ${__from}/${__to} render as epoch-ms, matching t_ms. category becomes the
-- series label, so the panel shows one WiFi line and one Wired line. WiFi is now
-- whole-mesh (all nodes' radios), so it is comparable to Wired; for snapshots
-- captured before per-node fetch it is master-only and will look small. Order by
-- time AND category so the two rows per snapshot come back deterministically
-- rather than wired-first on a tie.
    SELECT fetched_at AS "time", category, mbps
    FROM velop.v_wifi_vs_wired
    WHERE t_ms BETWEEN ${__from} AND ${__to}
    ORDER BY 1 ASC, 2 ASC;

-- Ad-hoc inspection in the Crate UI (no Grafana vars): both series interleaved,
-- so you can confirm WiFi is present (small) at every snapshot, not dropped.
-- SELECT fetched_at, category, mbps FROM velop.v_wifi_vs_wired ORDER BY t_ms, category;
