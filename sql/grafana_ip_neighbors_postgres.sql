-- grafana_ip_neighbors_postgres.sql (PostgreSQL)
--
-- PostgreSQL translation of sql/grafana_ip_neighbors.sql. Same view, same
-- columns; the only CrateDB-specific SQL here is the epoch-ms cast:
--     fetched_at::BIGINT  ->  velop.epoch_ms(fetched_at)
-- velop.epoch_ms() is created by sql/velop_schema_postgres.sql; apply that first.
--
-- Active-host counts per VLAN over time, derived from velop.ip_neighbor (the
-- router's `ip neigh` ARP cache, one row per IP per snapshot).
--
-- WHAT THIS SHOWS:
--   velop.ip_neighbor is a point-in-time IP<->MAC map + reachability state, NOT
--   a DHCP lease table (the dump never exposes /tmp/dnsmasq.leases -- see
--   CLAUDE.md). Each snapshot we count, per bridge/VLAN, how many hosts the
--   router currently has a neighbour entry for:
--     * br0 -> Main LAN, br1 -> Guest, eth0/eth1 -> WAN. br2 (192.168.20.x) is
--       the Velop "Smart Connect" inter-node network -- the only hosts on it are
--       the mesh nodes' own second interfaces, not user/IoT devices.
--     * A resolved entry (mac IS NOT NULL) means a host answered ARP; a FAILED
--       entry (mac NULL) is a stale probe for an IP nobody answered, so it is
--       counted separately as `unresolved`, not as a live host.
--   Restricted to IPv4 (family='inet'): a host has one IPv4 here, but many
--   IPv6 link-local (fe80::) entries can share one MAC, which would inflate
--   counts. The base table keeps both families if you want the v6 detail.
--
-- GRAFANA GOTCHA (see sql/grafana_radio_rates_postgres.sql and CLAUDE.md):
--   Grafana's PostgreSQL frame converter silently drops NUMERIC (OID 1700)
--   columns. There is nothing to cast here: COUNT/SUM over integers return
--   BIGINT, and the rest is TEXT/TIMESTAMPTZ, all of which Grafana renders.
--
-- USAGE:
--   1. psql -f sql/velop_schema_postgres.sql   (tables + helper functions)
--   2. psql -f sql/grafana_ip_neighbors_postgres.sql   (this view)
--   3. Point each Grafana panel at one of the flat SELECTs at the bottom.


-- ===========================================================================
-- STEP 1 -- create the view
-- ===========================================================================
CREATE OR REPLACE VIEW velop.v_ip_neighbor AS
WITH n AS (
  SELECT
    fetched_at,
    velop.epoch_ms(fetched_at) AS t_ms,
    CASE iface
      WHEN 'br0'  THEN 'Main LAN'
      WHEN 'br1'  THEN 'Guest'
      WHEN 'br2'  THEN 'Smart Connect'
      WHEN 'eth0' THEN 'WAN'
      WHEN 'eth1' THEN 'WAN'
      ELSE iface
    END AS subnet,
    mac,
    state
  FROM velop.ip_neighbor
  WHERE family = 'inet'        -- IPv4 only; see header (IPv6 has many fe80 per MAC)
)
SELECT
  fetched_at,
  t_ms,
  subnet,                                                            -- series label
  COUNT(DISTINCT mac) AS live_hosts,                                 -- resolved (host answered ARP)
  SUM(CASE WHEN state = 'REACHABLE'        THEN 1 ELSE 0 END) AS reachable,
  SUM(CASE WHEN state = 'STALE'            THEN 1 ELSE 0 END) AS stale,
  SUM(CASE WHEN state IN ('DELAY','PROBE') THEN 1 ELSE 0 END) AS probing,
  SUM(CASE WHEN mac IS NULL                THEN 1 ELSE 0 END) AS unresolved  -- FAILED/INCOMPLETE
FROM n
GROUP BY fetched_at, t_ms, subnet;


-- NOTE: every panel query below is COMMENTED OUT on purpose. ${__from} /
-- ${__to} are Grafana macros, not SQL, so an uncommented panel query makes
-- this file fail under `psql -f`. Copy one into a panel and drop the `-- `.
--
-- ===========================================================================
-- STEP 2 -- Grafana panel queries (flat selects against the view)
-- ===========================================================================
-- ${__from}/${__to} render as epoch-ms, matching t_ms exactly (integer vs
-- integer -- no timezone interpretation). Keep these flat.

-- Live hosts per VLAN over time (Time series panel; subnet is the series label):
-- SELECT fetched_at AS "time", subnet, live_hosts
-- FROM velop.v_ip_neighbor
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC;

-- Reachable hosts per VLAN (the ones the router talked to most recently):
-- SELECT fetched_at AS "time", subnet, reachable
-- FROM velop.v_ip_neighbor
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC;

-- Unresolved (FAILED) ARP entries per VLAN -- noise / departed hosts:
-- SELECT fetched_at AS "time", subnet, unresolved
-- FROM velop.v_ip_neighbor
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC;

-- All count columns at once (Table panel):
-- SELECT fetched_at AS "time", subnet, live_hosts, reachable, stale, probing, unresolved
-- FROM velop.v_ip_neighbor
-- WHERE t_ms BETWEEN ${__from} AND ${__to}
-- ORDER BY 1 ASC, subnet;


-- ===========================================================================
-- Detail: who is on the network in the latest snapshot (Table panel)
-- ===========================================================================
-- Flat select straight off the base table (no view needed): the resolved IPv4
-- neighbours from the most recent snapshot, with MAC + offline-OUI vendor and
-- VLAN. ${__to} bounds it to the dashboard's time window; the subquery picks
-- the newest snapshot within that window.
-- SELECT
--   ip,
--   CASE iface WHEN 'br0' THEN 'Main LAN' WHEN 'br1' THEN 'Guest'
--              WHEN 'br2' THEN 'Smart Connect' WHEN 'eth0' THEN 'WAN' WHEN 'eth1' THEN 'WAN'
--              ELSE iface END AS subnet,
--   mac, mac_vendor, is_router, state
-- FROM velop.ip_neighbor
-- WHERE family = 'inet' AND mac IS NOT NULL
--   AND velop.epoch_ms(fetched_at) = (
--     SELECT MAX(velop.epoch_ms(fetched_at)) FROM velop.ip_neighbor
--     WHERE velop.epoch_ms(fetched_at) <= ${__to}
--   )
-- ORDER BY subnet, ip;
