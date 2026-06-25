"""Runtime configuration, sourced entirely from environment variables.

Secrets (notably the router password) are never hard-coded or persisted to
disk by this project -- they must be supplied via the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Config:
    # Router / fetch
    router_url: str = "https://10.13.1.1/sysinfo.cgi"
    username: str = "admin"
    password: str = ""
    # The router uses a self-signed certificate, so TLS verification is off
    # by default. Set VELOP_VERIFY_TLS=true once a trusted cert is in place.
    verify_tls: bool = False
    connect_timeout: float = 10.0
    # Max gap between streamed chunks before requests gives up.
    read_timeout: float = 60.0
    # Hard ceiling on the whole (slow) render.
    overall_timeout: float = 600.0
    # Fetch is only considered complete once this substring is seen.
    completion_marker: str = "End of Sysinfo Output"
    # JNAP device-list endpoint (POST/JSON API) used to enrich device records
    # with untruncated friendly names. Empty means "derive from router_url".
    jnap_url: str = ""

    # OUI/vendor resolution. The manuf file is fetched once by velop-oui-update
    # and read locally; MAC addresses are never sent anywhere.
    oui_manuf_path: str = "manuf"
    oui_manuf_url: str = "https://www.wireshark.org/download/automated/data/manuf"

    # Kafka / Avro (Confluent wire format) -- the only sink. The structured tables
    # are produced as one topic each, "<prefix><table>"; raw_text dumps are never
    # produced. Connect JDBC sinks land the records in CrateDB (see connect/).
    kafka_bootstrap: str = "badger:9092"
    schema_registry_url: str = "http://badger:8081"
    kafka_topic_prefix: str = "velop."
    kafka_client_id: str = "velop-watcher"

    # Store-and-forward outbox: when Kafka/registry are unreachable a snapshot's
    # messages are buffered as files under buffer_dir (one file per topic, named
    # "<topic>.<yyyymmdd_HHMMSS>") and replayed on a later run once Kafka is back.
    # kafka_probe_timeout bounds the up/down reachability check (seconds);
    # drain_time_limit caps how long a run spends replaying+gzipping the backlog
    # (seconds, between files) so a oneshot run is never blocked indefinitely.
    buffer_dir: str = "buffer"
    kafka_probe_timeout: float = 5.0
    drain_time_limit: float = 120.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Config":
        env = os.environ if env is None else env
        d = cls()
        return cls(
            router_url=env.get("VELOP_URL", d.router_url),
            username=env.get("VELOP_USER", d.username),
            password=env.get("VELOP_PASSWORD", d.password),
            verify_tls=_as_bool(env.get("VELOP_VERIFY_TLS", "false")),
            connect_timeout=float(env.get("VELOP_CONNECT_TIMEOUT", d.connect_timeout)),
            read_timeout=float(env.get("VELOP_READ_TIMEOUT", d.read_timeout)),
            overall_timeout=float(env.get("VELOP_OVERALL_TIMEOUT", d.overall_timeout)),
            completion_marker=env.get("VELOP_MARKER", d.completion_marker),
            jnap_url=env.get("VELOP_JNAP_URL", d.jnap_url),
            oui_manuf_path=env.get("OUI_MANUF_PATH", d.oui_manuf_path),
            oui_manuf_url=env.get("OUI_MANUF_URL", d.oui_manuf_url),
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", d.kafka_bootstrap),
            schema_registry_url=env.get("SCHEMA_REGISTRY_URL", d.schema_registry_url),
            kafka_topic_prefix=env.get("KAFKA_TOPIC_PREFIX", d.kafka_topic_prefix),
            kafka_client_id=env.get("KAFKA_CLIENT_ID", d.kafka_client_id),
            buffer_dir=env.get("VELOP_BUFFER_DIR", d.buffer_dir),
            kafka_probe_timeout=float(
                env.get("VELOP_KAFKA_PROBE_TIMEOUT", d.kafka_probe_timeout)),
            drain_time_limit=float(
                env.get("VELOP_DRAIN_TIME_LIMIT", d.drain_time_limit)),
        )
