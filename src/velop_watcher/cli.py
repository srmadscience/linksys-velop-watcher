"""Command-line entry point: fetch one snapshot and produce it to Kafka."""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone

import requests

from .config import Config
from .fetch import (
    fetch_jnap_devices,
    fetch_sysinfo,
    fetch_sysinfo_url,
    node_sysinfo_url,
    parse_generated_at,
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
    tag_node_source,
)
from .oui import VendorResolver, enrich, load_manuf
from .kafka_sink import KafkaSink, assign_ids
from .outbox import Outbox, buffer_snapshot, drain


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
    print(
        f"Fetched {len(text)} chars (router generated_at={generated_at}); producing ...",
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

    # Whole-mesh capture: the master dump only carries the master's own radios
    # and its own system health, so client WiFi traffic (served mostly by
    # satellites) and per-node load/memory are missing. Tag the master's rows,
    # then fetch each satellite's dump for ITS radios and system stats. Each node
    # fetch is best-effort -- an offline/unreachable node logs a note and is
    # skipped rather than losing the whole capture. (Fetches are sequential; the
    # CGI is slow, so this multiplies wall-clock time by the node count.)
    nodes = parsed["nodes"]
    master = next((n for n in nodes if n["role"] == "master"), None)
    if master:
        tag_node_source(parsed["radio_stats"], master)
        tag_node_source(parsed["system"], master)
    for node in nodes:
        if node["role"] != "slave" or not node.get("ip"):
            continue
        try:
            node_text = fetch_sysinfo_url(node_sysinfo_url(cfg, node["ip"]), cfg)
        except (requests.RequestException, ValueError, TimeoutError) as exc:
            print(f"note: sysinfo fetch from {node['name']} ({node['ip']}) failed "
                  f"({exc}); it is skipped", file=sys.stderr)
            continue
        radios = tag_node_source(parse_radio_stats(node_text), node)
        parsed["radio_stats"].extend(radios)
        system = tag_node_source(parse_system(node_text), node)
        parsed["system"].extend(system)
        print(f"Captured {len(radios)} radios + {len(system)} system row from "
              f"{node['name']} ({node['ip']})", file=sys.stderr)

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

    # One snapshot id ties this capture's rows together; each record is stamped
    # with its own id up front so the Connect JDBC sinks upsert on a stable
    # primary key (re-delivery never duplicates a row).
    snapshot_id = str(uuid.uuid4())
    assign_ids(parsed)

    # Annotate every MAC with its vendor, resolved offline from the manuf file
    # (no DB cache now that the direct CrateDB write is gone).
    enrich(parsed, VendorResolver(None, manuf))

    try:
        sink = KafkaSink(cfg)
    except ImportError:
        print("error: confluent-kafka is not installed. pip install -e .",
              file=sys.stderr)
        return 2

    # Store-and-forward: probe Kafka once. If it's up, first replay any snapshots
    # buffered while it was down, then produce this one. If it's down, buffer this
    # snapshot to disk (one file per topic) and exit cleanly so the timer's next
    # run can drain it. The Pi runs oneshot per timer tick, so this is the retry.
    outbox = Outbox(cfg.buffer_dir)
    if not sink.kafka_up(cfg.kafka_probe_timeout):
        buffered = buffer_snapshot(sink, outbox, parsed, snapshot_id, fetched_at)
        bsummary = ", ".join(f"{n} {t}" for t, n in buffered.items()) or "0 records"
        print(f"Kafka {cfg.kafka_bootstrap} unreachable; buffered snapshot "
              f"{snapshot_id} to {cfg.buffer_dir} ({bsummary})", file=sys.stderr)
        return 0

    sent = drain(sink, outbox, time_limit=cfg.drain_time_limit)
    if sent:
        print(f"Drained {len(sent)} buffered file(s) ({sum(sent.values())} "
              f"messages) before this snapshot", file=sys.stderr)

    produced = sink.produce(parsed, snapshot_id, fetched_at)
    undelivered = sink.flush()
    psummary = ", ".join(f"{n} {t}" for t, n in produced.items()) or "0 records"
    print(f"Produced snapshot {snapshot_id} to Kafka {cfg.kafka_bootstrap} "
          f"({psummary})", file=sys.stderr)
    if undelivered:
        print(f"WARN {undelivered} Kafka message(s) still undelivered after flush",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
