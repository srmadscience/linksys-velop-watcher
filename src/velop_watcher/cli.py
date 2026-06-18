"""Command-line entry point: fetch one snapshot and store it."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from .config import Config
from .fetch import fetch_sysinfo, parse_generated_at, router_host
from .parse import (
    parse_backhaul,
    parse_devices,
    parse_lldp,
    parse_nic_counters,
    parse_nodes,
    parse_ping,
    parse_radio_config,
    parse_radio_stats,
    parse_system,
    parse_wlan_clients,
)
from .store import connect, ensure_schema, store_sysinfo, store_tier1


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
        "lldp": parse_lldp(text),
    }

    conn = connect(cfg)
    try:
        ensure_schema(conn)
        row_id = store_sysinfo(conn, text, fetched_at, generated_at, host)
        counts = store_tier1(conn, parsed, row_id, fetched_at)
    finally:
        conn.close()

    summary = ", ".join(f"{n} {table}" for table, n in counts.items())
    print(f"Stored snapshot {row_id} ({summary})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
