"""Persist sysinfo snapshots into CrateDB via its HTTP endpoint (port 4200)."""

from __future__ import annotations

import uuid
from datetime import datetime

from crate import client

from .config import Config

# CrateDB requires an explicit primary key and has no autoincrement, so we
# generate a UUID per snapshot. raw_text keeps the unparsed page verbatim --
# parsing into structured columns is a later goal. The "velop" schema is
# created implicitly by the first CREATE TABLE.
DDL = """
CREATE TABLE IF NOT EXISTS velop.sysinfo (
    id TEXT PRIMARY KEY,
    router_host TEXT,
    fetched_at TIMESTAMP WITH TIME ZONE,
    generated_at TIMESTAMP WITH TIME ZONE,
    -- The dump is one ~250 KB blob. CrateDB's default full-text index AND its
    -- columnstore (doc values) both reject values over 32766 bytes, so disable
    -- both -- we store the page verbatim and don't query it by content.
    raw_text TEXT INDEX OFF STORAGE WITH (columnstore = false)
)
"""

# The crate client uses the qmark paramstyle.
INSERT = """
INSERT INTO velop.sysinfo (id, router_host, fetched_at, generated_at, raw_text)
VALUES (?, ?, ?, ?, ?)
"""


def connect(cfg: Config):
    """Open a CrateDB connection over HTTP."""
    return client.connect(
        cfg.crate_url,
        username=cfg.crate_user,
        password=cfg.crate_password or None,
    )


def ensure_schema(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(DDL)
    finally:
        cur.close()


def store_sysinfo(
    conn,
    raw_text: str,
    fetched_at: datetime,
    generated_at: datetime | None,
    router_host: str,
) -> str:
    row_id = str(uuid.uuid4())
    cur = conn.cursor()
    try:
        cur.execute(INSERT, (row_id, router_host, fetched_at, generated_at, raw_text))
    finally:
        cur.close()
    return row_id
