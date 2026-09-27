-- grafana_wired_backhaul_postgres.sql (PostgreSQL)
--
-- "Is every node that SHOULD be cabled actually on its cable?" -- the query
-- behind the wired-backhaul alert (grafana/alerts/velop-wired-backhaul.yaml)
-- and the "Wired Backhaul Status" dashboard panel. CrateDB twin:
-- sql/grafana_wired_backhaul.sql -- change a query in BOTH files.
-- No DDL here; applying this file is a no-op.
--
-- WHY THIS EXISTS: a Velop satellite whose Ethernet uplink fails does not go
-- offline. It silently falls back to a 5 GHz wireless backhaul through another
-- node, at ~150-170 Mbps link rate instead of 1024, sharing airtime with that
-- node's clients. Nothing breaks, the WiFi just gets slow. In Aug-Sep 2026:
--
--   * LINKSYS_return1 (10.13.1.6) ran wireless for all of 13-20 Sep, and
--     flipped wired <-> wireless on 21, 23 and 25 Sep.
--   * LINKSYS-Return.. (10.13.1.7) was wireless on most days from 31 Aug; its
--     longest episode was ~15 days.
--   * Both also dropped off the mesh entirely (no IP) 7-8% of the time.
--
-- velop.backhaul records this, as one row per non-master node per snapshot:
-- intf 'eth1' / chan 'wired' / state 'up' when cabled, '5GL' or '5GH' when not.
-- A node can also be MISSING from backhaul altogether when the master can't
-- resolve its link (10.13.1.6 was absent for three days in Sep 2026), so
-- "absent" counts as off-the-wire too, not as "no data".
--
-- WHICH NODES ARE "EXPECTED WIRED" IS DECLARED, NOT INFERRED. Inferring it
-- from history fails for exactly the case that matters: 10.13.1.7 spent more
-- of Sep 2026 on wireless than on its cable, so a "usually wired" heuristic
-- would have classed it as a wireless node and never fired. Nodes are keyed
-- by backhaul.node_mac (uppercase, no colons) because IPs and names drift:
-- Dadroom has been both 10.13.1.171 and 10.13.1.8, and the CGI truncates
-- names to ~16 chars ("LINKSYS-Return.."). LINKSYS-Dadroom (C4411EEC4360) is
-- deliberately NOT listed -- it has no cable and is wireless by design.
-- Edit the list in the queries below AND in the alert YAML together.
--
-- Checked against 60 days of history (to 2026-09-27), per node:
--
--   node              off-wire episodes  1 snapshot  >=4 snapshots  longest
--   LINKSYS-Hall                      2           2              0   1 snap
--   LINKSYS-Return..                  6           0              6   2177 snaps (~15 d)
--   LINKSYS_return1                   8           0              8   1344 snaps (~9 d)
--
-- The split is clean: one-snapshot blips, or real episodes lasting hours to
-- days, with nothing in between. The alert's `for: 30m` (~3 ticks) ignores
-- every blip and would have fired on all 14 real episodes.
--
-- THE LATEST SNAPSHOT IS TAKEN FROM velop.system, not velop.backhaul, for the
-- same reason the feed-staleness alert reads it: system always gets rows (the
-- master's at minimum), so "latest snapshot" is well-defined even when the
-- backhaul section came back empty. If the whole feed stops, this rule keeps
-- evaluating the last snapshot it has -- the feed-staleness alert covers that
-- case, so the two don't both fire for one outage.
--
-- The ::DOUBLE PRECISION casts are there for the usual reason: Grafana's
-- PostgreSQL frame converter silently drops NUMERIC columns (see CLAUDE.md).
-- The CASE yields INTEGER, which is safe today, but the cast costs nothing and
-- keeps a later edit from turning the alert into one that can never fire.

-- ===========================================================================
-- Alert query: one row per expected-wired node, 0 = on its cable, 1 = not
-- (wireless backhaul, link down, or absent from the backhaul table).
-- The string column `node` becomes the alert's label, so each node fires and
-- resolves independently.
-- ===========================================================================
WITH expected (node_mac, node) AS (VALUES
       ('C4411EEC4275', 'LINKSYS-Hall'),
       ('C4411EEC4888', 'LINKSYS_return1'),
       ('E89F804C57DF', 'LINKSYS-Return..')),
latest AS (
  SELECT snapshot_id FROM velop.system ORDER BY fetched_at DESC LIMIT 1)
SELECT e.node,
       (CASE WHEN b.chan = 'wired' AND b.state = 'up' THEN 0 ELSE 1 END)::DOUBLE PRECISION
         AS off_wire
FROM expected e
LEFT JOIN velop.backhaul b
       ON b.node_mac = e.node_mac
      AND b.snapshot_id = (SELECT snapshot_id FROM latest)
ORDER BY e.node;

-- Expected value in normal operation: 0 for every node.

-- ===========================================================================
-- Wired Backhaul Status (State timeline; format: Time series)
-- ===========================================================================
-- One series per expected-wired node, one point per snapshot:
--   0 = wired, 1 = wireless (5GL/5GH), 2 = absent from velop.backhaul.
-- The panel maps those to green "wired" / orange "wireless" / red "absent".
-- Snapshots come from velop.system (CROSS JOIN) so an absent node shows as a
-- red bar rather than as a gap, which would look like missing data.
--
-- WITH expected (node_mac, node) AS (VALUES
--        ('C4411EEC4275', 'LINKSYS-Hall'),
--        ('C4411EEC4888', 'LINKSYS_return1'),
--        ('E89F804C57DF', 'LINKSYS-Return..')),
-- snaps AS (
--   SELECT DISTINCT snapshot_id, fetched_at FROM velop.system
--   WHERE velop.epoch_ms(fetched_at) BETWEEN ${__from} AND ${__to}),
-- bh AS (
--   SELECT snapshot_id, node_mac, chan, state FROM velop.backhaul
--   WHERE velop.epoch_ms(fetched_at) BETWEEN ${__from} AND ${__to})
-- SELECT s.fetched_at AS time,
--        e.node AS metric,
--        (CASE WHEN b.node_mac IS NULL                    THEN 2
--              WHEN b.chan = 'wired' AND b.state = 'up'  THEN 0
--              ELSE 1 END)::DOUBLE PRECISION AS backhaul
-- FROM snaps s
-- CROSS JOIN expected e
-- LEFT JOIN bh b ON b.snapshot_id = s.snapshot_id AND b.node_mac = e.node_mac
-- ORDER BY 1;
