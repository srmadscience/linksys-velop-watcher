-- grafana_wired_backhaul.sql (CrateDB)
--
-- "Is every node that SHOULD be cabled actually on its cable?" -- the query
-- behind the wired-backhaul alert (grafana/alerts/velop-wired-backhaul.yaml)
-- and the "Wired Backhaul Status" dashboard panel. PostgreSQL twin:
-- sql/grafana_wired_backhaul_postgres.sql -- change a query in BOTH files.
-- No DDL here; applying this file is a no-op.
--
-- The rationale (why a satellite losing its cable is silent, why the
-- expected-wired set is declared by MAC rather than inferred, and the 60-day
-- backtest behind `for: 30m`) is in the PostgreSQL twin. Differences here:
--
--   * The expected-wired list is built with unnest(array, array), CrateDB's
--     documented multi-column table function, instead of a VALUES list.
--   * fetched_at::BIGINT instead of velop.epoch_ms(fetched_at).
--   * ::DOUBLE instead of ::DOUBLE PRECISION.
--
-- UNTESTED against a live CrateDB: it was retired (2026-09-13) before this
-- was written. The PostgreSQL twin is the one that has been run.

-- ===========================================================================
-- Alert query: one row per expected-wired node, 0 = on its cable, 1 = not
-- (wireless backhaul, link down, or absent from the backhaul table).
-- ===========================================================================
SELECT e.node,
       (CASE WHEN b.chan = 'wired' AND b.state = 'up' THEN 0 ELSE 1 END)::DOUBLE
         AS off_wire
FROM (SELECT col1 AS node_mac, col2 AS node
      FROM unnest(['C4411EEC4275', 'C4411EEC4888', 'E89F804C57DF'],
                  ['LINKSYS-Hall', 'LINKSYS_return1', 'LINKSYS-Return..'])) e
LEFT JOIN velop.backhaul b
       ON b.node_mac = e.node_mac
      AND b.snapshot_id = (SELECT snapshot_id FROM velop.system
                           ORDER BY fetched_at DESC LIMIT 1)
ORDER BY e.node;

-- ===========================================================================
-- Wired Backhaul Status (State timeline; format: Time series)
-- 0 = wired, 1 = wireless (5GL/5GH), 2 = absent from velop.backhaul.
-- ===========================================================================
-- SELECT s.fetched_at AS time,
--        e.node AS metric,
--        (CASE WHEN b.node_mac IS NULL                    THEN 2
--              WHEN b.chan = 'wired' AND b.state = 'up'  THEN 0
--              ELSE 1 END)::DOUBLE AS backhaul
-- FROM (SELECT DISTINCT snapshot_id, fetched_at FROM velop.system
--       WHERE fetched_at::BIGINT BETWEEN ${__from} AND ${__to}) s
-- CROSS JOIN (SELECT col1 AS node_mac, col2 AS node
--             FROM unnest(['C4411EEC4275', 'C4411EEC4888', 'E89F804C57DF'],
--                         ['LINKSYS-Hall', 'LINKSYS_return1', 'LINKSYS-Return..'])) e
-- LEFT JOIN (SELECT snapshot_id, node_mac, chan, state FROM velop.backhaul
--            WHERE fetched_at::BIGINT BETWEEN ${__from} AND ${__to}) b
--        ON b.snapshot_id = s.snapshot_id AND b.node_mac = e.node_mac
-- ORDER BY 1;
