-- grafana_device_wlan.sql
--
-- Devices joined to their Wi-Fi link stats: one row per device with a display
-- name, IP, MCS rate index and RSSI.
--
-- WHAT THIS SHOWS:
--   velop.device is the router's device table (every known host: wired + Wi-Fi),
--   velop.wlan_client is the per-AP wireless association table (MCS/RSSI only
--   exist for associated Wi-Fi clients). We drive from velop.device (there is
--   always a device row) and LEFT JOIN the Wi-Fi stats on, so:
--     * Wi-Fi clients get their mcs/rssi
--     * wired hosts (and Wi-Fi clients not currently associated) keep their row
--       with mcs/rssi NULL
--   The join is scoped to a single snapshot (d.snapshot_id = w.snapshot_id) so a
--   device is only ever paired with a client from the SAME dump, then matched on
--   MAC (device.mac = wlan_client.client_mac, lowercased to be case-safe).
--
--   `name` falls back: friendly_name (JNAP, full) -> CGI name (capped ~16 chars,
--   often blank) -> the device's OUI vendor -> raw MAC. So every row has a label
--   even when the router never learned a hostname.
--
-- GRAFANA GOTCHA (see CLAUDE.md / sql/grafana_radio_rates.sql):
--   Grafana's PostgreSQL frame converter silently drops NUMERIC (OID 1700)
--   columns. Nothing here is computed/ROUND()ed: rssi is INTEGER, mcs is TEXT,
--   so both render fine. If you want to graph mcs as a numeric line, cast it
--   explicitly with TRY_CAST(w.mcs AS DOUBLE) (float8/OID 701), not NUMERIC.
--
-- USAGE:
--   This is a flat panel query (no view). Time series panels want a numeric
--   value per series (rssi graphs cleanly); for the full name/IP/MCS/RSSI view
--   use a Table panel. ${__from}/${__to} render as epoch-ms, matching
--   fetched_at::BIGINT exactly (integer vs integer -- no timezone shift).


-- ===========================================================================
-- Devices + Wi-Fi link stats over the dashboard window (Table panel)
-- ===========================================================================
SELECT
  d.fetched_at                                                AS "time",
  COALESCE(
    NULLIF(d.friendly_name, ''),
    NULLIF(d.name, ''),
    d.mac_vendor,
    d.mac
  )                                                           AS name,
  d.ip                                                        AS ip,
  w.mcs                                                       AS mcs,
  w.rssi                                                      AS rssi
FROM velop.device d
LEFT JOIN velop.wlan_client w
       ON d.snapshot_id = w.snapshot_id
      AND lower(d.mac) = lower(w.client_mac)
WHERE d.fetched_at::BIGINT BETWEEN ${__from} AND ${__to}
ORDER BY "time" ASC, name;


-- ===========================================================================
-- RSSI per device over time (Time series panel; name is the series label)
-- ===========================================================================
-- SELECT
--   d.fetched_at AS "time",
--   COALESCE(NULLIF(d.friendly_name, ''), NULLIF(d.name, ''), d.mac_vendor, d.mac) AS name,
--   w.rssi
-- FROM velop.device d
-- LEFT JOIN velop.wlan_client w
--        ON d.snapshot_id = w.snapshot_id
--       AND lower(d.mac) = lower(w.client_mac)
-- WHERE w.rssi IS NOT NULL
--   AND d.fetched_at::BIGINT BETWEEN ${__from} AND ${__to}
-- ORDER BY "time" ASC;


-- ===========================================================================
-- Latest snapshot only (Table panel) -- swap the time-range filter for the
-- newest snapshot within the dashboard window.
-- ===========================================================================
-- SELECT
--   COALESCE(NULLIF(d.friendly_name, ''), NULLIF(d.name, ''), d.mac_vendor, d.mac) AS name,
--   d.ip, w.mcs, w.rssi
-- FROM velop.device d
-- LEFT JOIN velop.wlan_client w
--        ON d.snapshot_id = w.snapshot_id
--       AND lower(d.mac) = lower(w.client_mac)
-- WHERE d.fetched_at::BIGINT = (
--         SELECT MAX(fetched_at::BIGINT) FROM velop.device
--         WHERE fetched_at::BIGINT <= ${__to}
--       )
-- ORDER BY name;
