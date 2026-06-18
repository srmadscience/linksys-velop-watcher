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

    # Storage. We talk to CrateDB over its HTTP endpoint (port 4200) using the
    # official crate client, not the PostgreSQL wire protocol.
    crate_url: str = "http://localhost:4200"
    crate_user: str = "crate"
    crate_password: str = ""

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
            crate_url=env.get("CRATE_URL", d.crate_url),
            crate_user=env.get("CRATE_USER", d.crate_user),
            crate_password=env.get("CRATE_PASSWORD", d.crate_password),
        )
