"""MAC -> vendor (OUI) resolution, backed by the Wireshark ``manuf`` database.

Every MAC seen in a snapshot is annotated with its manufacturer. Lookups are
resolved offline from a local copy of Wireshark's ``manuf`` file (see
https://www.wireshark.org/tools/oui-lookup.html) -- MAC addresses never leave
the network. Results are cached in the ``velop.oui`` table keyed by the 24-bit
OUI (first three octets), so each prefix is resolved from ``manuf`` at most once
and is queryable later as a plain table.

Flow per MAC: ``oui_of`` -> check ``velop.oui`` -> on miss, ``ManufDB.lookup``
the OUI and insert the result (vendor or NULL) so the miss is cached too.

Note: keying the cache on the 24-bit OUI means the longer IEEE MA-M (/28) and
MA-S (/36) assignments are not distinguished -- acceptable for this use and in
line with the "OUI = first three octets" model. ``ManufDB`` itself still
understands the longer prefixes for callers that pass a full MAC.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterable, Optional

import requests

# Wireshark's automatically rebuilt manuf database.
MANUF_URL = "https://www.wireshark.org/download/automated/data/manuf"

_HEX_RE = re.compile(r"[^0-9a-fA-F]")


def _hex(value: Optional[str]) -> str:
    """Strip a MAC/prefix down to its lowercase hex nibbles."""
    return _HEX_RE.sub("", value or "").lower()


def oui_of(mac: Optional[str]) -> Optional[str]:
    """The 24-bit OUI of ``mac`` as ``AA:BB:CC`` (upper-case), or ``None``.

    Accepts any common separator (``:``/``-``/none). Returns ``None`` for empty
    or non-MAC values (e.g. an LLDP chassis id that is not a MAC), which the
    resolver treats as "no vendor".
    """
    h = _hex(mac)
    if len(h) < 6:
        return None
    h = h[:6].upper()
    return f"{h[0:2]}:{h[2:4]}:{h[4:6]}"


# --------------------------------------------------------------------------
# manuf database
# --------------------------------------------------------------------------


class ManufDB:
    """Lookup table parsed from Wireshark's ``manuf`` file.

    The file is tab-separated: ``<prefix>[/<bits>]<TAB><short><TAB>[<long>]``.
    Prefixes are 24-bit by default; longer IEEE blocks carry an explicit
    ``/28`` or ``/36`` mask. ``lookup`` matches the longest prefix first.
    """

    def __init__(self, by_mask: Optional[dict[int, dict[str, str]]] = None):
        # {mask_bits: {prefix_hex: vendor}}; masks are multiples of 4 (nibbles).
        self.by_mask = by_mask or {}

    @classmethod
    def from_lines(cls, lines: Iterable[str]) -> "ManufDB":
        by_mask: dict[int, dict[str, str]] = {}
        for line in lines:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            prefix = parts[0].strip()
            short = parts[1].strip()
            long = parts[2].strip() if len(parts) > 2 else ""
            vendor = long or short
            if not vendor:
                continue
            if "/" in prefix:
                addr, _, mask = prefix.partition("/")
                try:
                    bits = int(mask)
                except ValueError:
                    continue
            else:
                addr, bits = prefix, 24
            nibbles = bits // 4
            ph = _hex(addr)[:nibbles]
            if len(ph) < nibbles:
                continue
            by_mask.setdefault(bits, {})[ph] = vendor
        return cls(by_mask)

    @classmethod
    def from_file(cls, path: str) -> "ManufDB":
        with open(path, encoding="utf-8", errors="replace") as fh:
            return cls.from_lines(fh)

    def lookup(self, mac: Optional[str]) -> Optional[str]:
        """Vendor for ``mac`` (full MAC or shorter prefix), longest match first."""
        h = _hex(mac)
        if not h:
            return None
        for bits in sorted(self.by_mask, reverse=True):
            nibbles = bits // 4
            if len(h) >= nibbles:
                vendor = self.by_mask[bits].get(h[:nibbles])
                if vendor:
                    return vendor
        return None

    def __len__(self) -> int:
        return sum(len(table) for table in self.by_mask.values())


def load_manuf(path: str) -> Optional[ManufDB]:
    """Load a ``manuf`` file, or ``None`` if it is missing/unreadable.

    A missing file is not fatal: the resolver simply yields ``None`` vendors
    until ``velop-oui-update`` has fetched the database.
    """
    try:
        return ManufDB.from_file(path)
    except OSError:
        return None


def download_manuf(path: str, url: str = MANUF_URL,
                   session: Optional[requests.Session] = None) -> int:
    """Download the ``manuf`` database to ``path``; return the entry count."""
    sess = session or requests.Session()
    resp = sess.get(url, timeout=60)
    resp.raise_for_status()
    text = resp.text
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return len(ManufDB.from_lines(text.splitlines()))


# --------------------------------------------------------------------------
# velop.oui cache resolver
# --------------------------------------------------------------------------

_SELECT_OUI = "SELECT vendor FROM velop.oui WHERE oui = ?"
_INSERT_OUI = "INSERT INTO velop.oui (oui, vendor, source, looked_up_at) VALUES (?, ?, ?, ?)"


class VendorResolver:
    """Resolve MAC -> vendor, caching results in ``velop.oui``.

    On a cache miss the OUI is resolved from ``manuf`` and the result (vendor
    or ``None``) is inserted so the miss is not retried. An in-process cache
    avoids re-querying the DB for a prefix already seen this run. If ``manuf``
    is ``None`` every lookup is a (cached) ``None``.
    """

    def __init__(self, conn, manuf: Optional[ManufDB], source: str = "manuf"):
        self.conn = conn
        self.manuf = manuf
        self.source = source
        self._seen: dict[str, Optional[str]] = {}

    def vendor_for(self, mac: Optional[str]) -> Optional[str]:
        oui = oui_of(mac)
        if oui is None:
            return None
        if oui in self._seen:
            return self._seen[oui]
        vendor = self._resolve(oui)
        self._seen[oui] = vendor
        return vendor

    def _resolve(self, oui: str) -> Optional[str]:
        cur = self.conn.cursor()
        try:
            cur.execute(_SELECT_OUI, (oui,))
            row = cur.fetchone()
            if row is not None:
                return row[0]
            # Look the OUI up by its 3 octets so the cache key and the resolved
            # vendor stay consistent regardless of which full MAC first hit it.
            vendor = self.manuf.lookup(oui) if self.manuf else None
            try:
                cur.execute(_INSERT_OUI, (oui, vendor, self.source,
                                          datetime.now(timezone.utc)))
            except Exception:
                # A concurrent run may have inserted the same OUI; harmless.
                pass
            return vendor
        finally:
            cur.close()


# --------------------------------------------------------------------------
# enrichment of parsed records
# --------------------------------------------------------------------------

# For each parsed-section key, the (mac_field -> vendor_field) columns to fill.
VENDOR_FIELDS: dict[str, list[tuple[str, str]]] = {
    "devices": [("mac", "mac_vendor")],
    "wlan_clients": [("client_mac", "client_mac_vendor")],
    "backhaul": [("node_mac", "node_mac_vendor")],
    "nodes": [
        ("mac", "mac_vendor"),
        ("userap2g_bssid", "userap2g_bssid_vendor"),
        ("userap5gl_bssid", "userap5gl_bssid_vendor"),
        ("userap5gh_bssid", "userap5gh_bssid_vendor"),
    ],
    "radio_config": [("mac", "mac_vendor")],
    "ip_neighbors": [("mac", "mac_vendor")],
    "lldp": [("chassis_id", "chassis_id_vendor"), ("port_id", "port_id_vendor")],
}

# Array MAC fields -> a parallel array of vendors.
ARRAY_VENDOR_FIELDS: dict[str, list[tuple[str, str]]] = {
    "devices": [("extra_macs", "extra_macs_vendor")],
}


def enrich(parsed: dict, resolver: VendorResolver) -> dict:
    """Add ``*_vendor`` fields to every MAC-bearing parsed record, in place.

    ``parsed`` is the dict of section -> ``list[dict]`` produced by ``parse``;
    the same dict is returned for convenience.
    """
    for section, pairs in VENDOR_FIELDS.items():
        for record in parsed.get(section, []):
            for mac_field, vendor_field in pairs:
                record[vendor_field] = resolver.vendor_for(record.get(mac_field))
    for section, pairs in ARRAY_VENDOR_FIELDS.items():
        for record in parsed.get(section, []):
            for mac_field, vendor_field in pairs:
                macs = record.get(mac_field) or []
                record[vendor_field] = [resolver.vendor_for(m) for m in macs]
    return parsed


def update_main(argv: Optional[list[str]] = None) -> int:
    """``velop-oui-update`` entry point: refresh the local ``manuf`` file."""
    import sys

    from .config import Config

    cfg = Config.from_env()
    print(f"Downloading manuf from {cfg.oui_manuf_url} ...", file=sys.stderr)
    try:
        count = download_manuf(cfg.oui_manuf_path, cfg.oui_manuf_url)
    except (requests.RequestException, OSError) as exc:
        print(f"error: failed to update manuf: {exc}", file=sys.stderr)
        return 1
    print(f"Wrote {count} OUI entries to {cfg.oui_manuf_path}", file=sys.stderr)
    return 0
