"""Command-line entry point: fetch one snapshot and store it."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

import requests

from .config import Config
from .fetch import (
    fetch_jnap_devices,
    fetch_sysinfo,
    fetch_sysinfo_url,
    node_sysinfo_url,
    parse_generated_at,
    router_host,
)
from .parse import (
    enrich_friendly_names,
    friendly_name_index,
    parse_backhaul,
    parse_devices,
    parse_ip_neighbors,
    parse_lldp,
    parse_nic_counters,
    parse_nodes,
    parse_ping,
    parse_radio_config,
    parse_radio_stats,
    parse_system,
    parse_wlan_clients,
    tag_radio_source,
)
from .oui import VendorResolver, enrich, load_manuf
from .store import connect, ensure_schema, store_node_sysinfo, store_sysinfo, store_tier1


def main(argv: list[str] | None = None) -> int:
    cfg = Config.from_env()

    if not cfg.password:
        print(
            "error: router password not set. Export VELOP_PASSWORD before running.",
            file=sys.stderr,
        )
        return 2

    print(f"Fetching {cfg.router_url} (this can take a while) ...", file=sys.stderr)
    text = fetch_sysinfo(cfg)
    fetched_at = datetime.now(timezone.utc)
    generated_at = parse_generated_at(text)
    host = router_host(cfg.router_url)
    print(
        f"Fetched {len(text)} chars (router generated_at={generated_at}); storing ...",
        file=sys.stderr,
    )

    parsed = {
        "devices": parse_devices(text),
        "wlan_clients": parse_wlan_clients(text),
        "backhaul": parse_backhaul(text),
        "ping": parse_ping(text),
        "nodes": parse_nodes(text),
        "radio_stats": parse_radio_stats(text),
        "radio_config": parse_radio_config(text),
        "nic_counters": parse_nic_counters(text),
        "system": parse_system(text),
        "ip_neighbors": parse_ip_neighbors(text),
        "lldp": parse_lldp(text),
    }

    # Whole-mesh WiFi: the master dump only carries the master's own radios, so
    # client WiFi traffic (served mostly by satellites) is missing. Tag the
    # master's radios, then fetch each satellite's dump for ITS radios. Each
    # node fetch is best-effort -- an offline/unreachable node logs a note and is
    # skipped rather than losing the whole capture. (Fetches are sequential; the
    # CGI is slow, so this multiplies wall-clock time by the node count.)
    nodes = parsed["nodes"]
    master = next((n for n in nodes if n["role"] == "master"), None)
    if master:
        tag_radio_source(parsed["radio_stats"], master)
    satellite_dumps: list[dict] = []
    for node in nodes:
        if node["role"] != "slave" or not node.get("ip"):
            continue
        try:
            node_text = fetch_sysinfo_url(node_sysinfo_url(cfg, node["ip"]), cfg)
        except (requests.RequestException, ValueError, TimeoutError) as exc:
            print(f"note: sysinfo fetch from {node['name']} ({node['ip']}) failed "
                  f"({exc}); its radios are skipped", file=sys.stderr)
            continue
        radios = tag_radio_source(parse_radio_stats(node_text), node)
        parsed["radio_stats"].extend(radios)
        satellite_dumps.append({
            "node_mac": node["mac"],
            "node_name": node["name"],
            "node_ip": node["ip"],
            "role": node["role"],
            "raw_text": node_text,
            "generated_at": parse_generated_at(node_text),
        })
        print(f"Captured {len(radios)} radios from {node['name']} ({node['ip']})",
              file=sys.stderr)

    # Best-effort: enrich devices with their untruncated names from the JNAP
    # API. A failure here (network/auth) must not lose the snapshot.
    try:
        jnap = fetch_jnap_devices(cfg)
        enrich_friendly_names(parsed["devices"], friendly_name_index(jnap))
        named = sum(1 for d in parsed["devices"] if d.get("friendly_name"))
        print(f"Enriched {named}/{len(parsed['devices'])} device names from JNAP",
              file=sys.stderr)
    except (requests.RequestException, ValueError) as exc:
        for d in parsed["devices"]:
            d.setdefault("friendly_name", None)
        print(f"note: JNAP device-name fetch failed ({exc}); friendly_name stays null",
              file=sys.stderr)

    manuf = load_manuf(cfg.oui_manuf_path)
    if manuf is None:
        print(
            f"note: OUI manuf file not found at {cfg.oui_manuf_path}; vendor columns "
            "will be null. Run velop-oui-update to fetch it.",
            file=sys.stderr,
        )

    conn = connect(cfg)
    try:
        ensure_schema(conn)
        # Annotate every MAC with its vendor, caching lookups in velop.oui.
        enrich(parsed, VendorResolver(conn, manuf))
        row_id = store_sysinfo(conn, text, fetched_at, generated_at, host)
        counts = store_tier1(conn, parsed, row_id, fetched_at)
        counts["node_sysinfo"] = store_node_sysinfo(conn, satellite_dumps, row_id, fetched_at)
    finally:
        conn.close()

    summary = ", ".join(f"{n} {table}" for table, n in counts.items())
    print(f"Stored snapshot {row_id} ({summary})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
