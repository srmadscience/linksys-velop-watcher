"""Fetch the (slowly rendered) sysinfo.cgi page from the Velop router.

The CGI streams its output progressively and can take a long time to finish.
We treat the response as a stream and stop only once the completion marker is
seen, rather than relying on the connection closing.
"""

from __future__ import annotations

import base64
import time
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import urlparse

import requests
import urllib3
from requests.auth import HTTPBasicAuth

from .config import Config

# JNAP action that returns the full device list (incl. untruncated friendlyName).
JNAP_GET_DEVICES = "http://linksys.com/jnap/devicelist/GetDevices3"


def router_host(url: str) -> str:
    """The bare hostname/IP of the router, for tagging stored rows."""
    return urlparse(url).hostname or ""


def jnap_url(cfg: Config) -> str:
    """The JNAP endpoint: ``cfg.jnap_url`` if set, else derived from the CGI URL.

    The Velop serves JNAP at ``/JNAP/`` on the same host/port as ``sysinfo.cgi``.
    """
    if cfg.jnap_url:
        return cfg.jnap_url
    parts = urlparse(cfg.router_url)
    return f"{parts.scheme}://{parts.netloc}/JNAP/"


def parse_generated_at(text: str) -> datetime | None:
    """Parse the router's own timestamp from the first 'page generated on' line.

    Example: 'page generated on Thu Jun 18 14:58:22 UTC 2026'. The router
    reports UTC, so we attach UTC explicitly (strptime %Z does not reliably
    populate tzinfo across platforms).
    """
    for line in text.splitlines():
        line = line.strip()
        if line.lower().startswith("page generated on"):
            stamp = line[len("page generated on"):].strip()
            try:
                naive = datetime.strptime(stamp, "%a %b %d %H:%M:%S %Z %Y")
            except ValueError:
                return None
            return naive.replace(tzinfo=timezone.utc)
    return None


def read_until_marker(
    chunks: Iterable[str],
    marker: str,
    deadline: float | None = None,
) -> str:
    """Accumulate streamed text chunks until ``marker`` appears.

    Raises TimeoutError if ``deadline`` (a time.monotonic() value) passes first,
    or ValueError if the stream ends before the marker is seen.
    """
    parts: list[str] = []
    seen = ""
    for chunk in chunks:
        if not chunk:
            continue
        parts.append(chunk)
        seen += chunk
        if marker in seen:
            return "".join(parts)
        if deadline is not None and time.monotonic() > deadline:
            raise TimeoutError(f"completion marker {marker!r} not seen before deadline")
    raise ValueError(f"completion marker {marker!r} not found before stream ended")


def fetch_sysinfo(cfg: Config, session: requests.Session | None = None) -> str:
    """Download the full sysinfo.cgi output, returning it as text."""
    if not cfg.verify_tls:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    sess = session or requests.Session()
    resp = sess.get(
        cfg.router_url,
        auth=HTTPBasicAuth(cfg.username, cfg.password),
        verify=cfg.verify_tls,
        stream=True,
        timeout=(cfg.connect_timeout, cfg.read_timeout),
    )
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"

    deadline = time.monotonic() + cfg.overall_timeout
    chunks = resp.iter_content(chunk_size=4096, decode_unicode=True)
    try:
        return read_until_marker(chunks, cfg.completion_marker, deadline)
    finally:
        resp.close()


def fetch_jnap_devices(cfg: Config, session: requests.Session | None = None) -> dict:
    """POST the JNAP ``GetDevices3`` action and return the decoded JSON payload.

    Uses the same Basic credentials as the CGI fetch, passed in the
    ``X-JNAP-Authorization`` header the Velop expects (not a normal ``auth=``).
    Unlike ``sysinfo.cgi`` this responds quickly, so no streaming is needed.
    Raises ``requests`` errors on transport failure and ``ValueError`` if JNAP
    reports a non-OK result, so the caller can treat name enrichment as
    best-effort.
    """
    if not cfg.verify_tls:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    sess = session or requests.Session()
    token = base64.b64encode(f"{cfg.username}:{cfg.password}".encode()).decode()
    resp = sess.post(
        jnap_url(cfg),
        headers={
            "Content-Type": "application/json",
            "X-JNAP-Action": JNAP_GET_DEVICES,
            "X-JNAP-Authorization": f"Basic {token}",
        },
        data="{}",
        verify=cfg.verify_tls,
        timeout=(cfg.connect_timeout, cfg.read_timeout),
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("result") != "OK":
        raise ValueError(f"JNAP {JNAP_GET_DEVICES} returned result={payload.get('result')!r}")
    return payload
