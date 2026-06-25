"""Store-and-forward outbox for the Kafka producer.

The watcher produces Avro to Kafka, but the broker (and the Schema Registry it
shares a host with) may be down -- and on the Pi every run is a fresh oneshot
process, so there is no in-memory retry. This module buffers a snapshot's
messages to disk when Kafka is unreachable and replays them on a later run.

Why buffer the *logical* message and not the Avro bytes: serializing needs the
Schema Registry on first use, so if Kafka/registry are down we cannot serialize
at all. We therefore store each message as ``(key, value_dict)`` JSON and defer
serialization to drain time, when Kafka is back.

On-disk format:
- One file per topic per buffered run, named ``<topic>.<yyyymmdd_HHMMSS>`` (UTC).
  A ``-N`` suffix is appended only to avoid a same-second collision.
- Content is JSON Lines: one ``{"key": ..., "value": {...}}`` object per line.
  ``datetime`` values (``fetched_at``) survive via a ``{"__dt_ms__": <epoch-ms>}``
  marker, restored to a UTC ``datetime`` on read.
- After a file is successfully replayed it is gzipped in place (``...gz``) so it
  is kept as an archive but never re-sent. ``.gz`` files are ignored on scan.

The drain/buffer helpers take a ``sink`` (a ``KafkaSink``) but this module never
imports ``confluent_kafka`` itself, so it (and its file logic) import cleanly.
"""

from __future__ import annotations

import gzip
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

_DT_KEY = "__dt_ms__"


def _json_default(obj):
    if isinstance(obj, datetime):
        return {_DT_KEY: int(obj.timestamp() * 1000)}
    raise TypeError(f"not JSON-serializable: {type(obj).__name__}")


def _json_object_hook(d: dict):
    if len(d) == 1 and _DT_KEY in d:
        return datetime.fromtimestamp(d[_DT_KEY] / 1000, tz=timezone.utc)
    return d


class Outbox:
    """File-backed buffer of pending Kafka messages (pure file I/O, no Kafka)."""

    def __init__(self, buffer_dir: str | Path):
        self.dir = Path(buffer_dir)

    def write(self, topic: str, messages, *, now: datetime | None = None) -> Path:
        """Buffer ``messages`` (iterable of ``(key, value)``) for ``topic``.

        Returns the path written. The file name encodes the topic and a
        second-resolution UTC timestamp; a ``-N`` suffix avoids a collision if
        the same topic is buffered twice within one second.
        """
        self.dir.mkdir(parents=True, exist_ok=True)
        ts = (now or datetime.now(timezone.utc)).strftime("%Y%m%d_%H%M%S")
        path = self.dir / f"{topic}.{ts}"
        n = 1
        while path.exists() or path.with_name(path.name + ".gz").exists():
            path = self.dir / f"{topic}.{ts}-{n}"
            n += 1
        # Write to a temp file then rename so a crash never leaves a half file
        # that drain would try to replay.
        tmp = path.with_name(path.name + ".part")
        with tmp.open("w", encoding="utf-8") as f:
            for key, value in messages:
                f.write(json.dumps({"key": key, "value": value}, default=_json_default))
                f.write("\n")
        tmp.replace(path)
        return path

    def pending_files(self) -> list[Path]:
        """Buffered, not-yet-replayed files (``.gz`` and ``.part`` excluded), oldest first."""
        if not self.dir.exists():
            return []
        files = [
            p for p in self.dir.iterdir()
            if p.is_file() and p.suffix not in (".gz", ".part")
        ]
        return sorted(files, key=lambda p: p.name)

    @staticmethod
    def topic_of(path: Path) -> str:
        """Recover the topic from a buffer filename ``<topic>.<ts>``.

        The topic itself contains dots (``velop.device``); the timestamp does
        not, so the topic is everything before the final dot.
        """
        return path.name.rsplit(".", 1)[0]

    @staticmethod
    def read(path: Path) -> list[tuple[str, dict]]:
        """Load a buffer file back into ``[(key, value_dict), ...]``."""
        out: list[tuple[str, dict]] = []
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line, object_hook=_json_object_hook)
                out.append((obj["key"], obj["value"]))
        return out

    @staticmethod
    def gzip_file(path: Path) -> Path:
        """Gzip ``path`` in place to ``<name>.gz`` and remove the original."""
        gz = path.with_name(path.name + ".gz")
        with path.open("rb") as src, gzip.open(gz, "wb") as dst:
            shutil.copyfileobj(src, dst)
        path.unlink()
        return gz


def buffer_snapshot(sink, outbox: Outbox, parsed: dict, snapshot_id: str,
                    fetched_at: datetime) -> dict[str, int]:
    """Write a whole snapshot to the outbox (one file per non-empty topic).

    Returns ``{topic: message_count}``. Used when Kafka is unreachable, or as a
    fallback if a live produce fails partway.
    """
    counts: dict[str, int] = {}
    for topic, msgs in sink.messages_for(parsed, snapshot_id, fetched_at).items():
        outbox.write(topic, msgs)
        counts[topic] = len(msgs)
    return counts


def drain(sink, outbox: Outbox, *, flush_timeout: float = 30.0,
          time_limit: float = 120.0) -> dict[str, int]:
    """Replay pending buffer files, gzipping each only on success.

    For each file: re-produce its messages, flush, and gzip it iff nothing was
    left in the producer queue and no delivery report failed. A file that fails
    is left pending (and its gzip is skipped) for the next run, and draining
    stops -- a failure means Kafka went away mid-drain.

    Bounded by ``time_limit`` seconds (default 120): the loop stops *before*
    starting a new file once the deadline has passed, so a large backlog never
    blocks a oneshot run indefinitely -- the remaining files just wait for the
    next run. The check is between files, so one in-flight file may overrun the
    limit, but a partial/failed file is never gzipped. ``time_limit <= 0``
    disables the bound.

    Returns ``{filename: message_count}`` for the files successfully sent.
    """
    import time

    deadline = time.monotonic() + time_limit if time_limit > 0 else None
    sent: dict[str, int] = {}
    for path in outbox.pending_files():
        if deadline is not None and time.monotonic() >= deadline:
            break
        topic = Outbox.topic_of(path)
        messages = Outbox.read(path)
        sink.reset_delivery_errors()
        for key, value in messages:
            sink.produce_one(topic, key, value)
        remaining = sink.flush(flush_timeout)
        if remaining or sink.delivery_errors:
            break
        outbox.gzip_file(path)
        sent[path.name] = len(messages)
    return sent


def drain_main(argv: list[str] | None = None) -> int:
    """Standalone entry point: replay the outbox if Kafka is up, then exit."""
    import sys

    from .config import Config
    from .kafka_sink import KafkaSink

    cfg = Config.from_env()
    outbox = Outbox(cfg.buffer_dir)
    pending = outbox.pending_files()
    if not pending:
        print("Outbox empty; nothing to send.", file=sys.stderr)
        return 0
    try:
        sink = KafkaSink(cfg)
    except ImportError:
        print("error: confluent-kafka is not installed. pip install -e .",
              file=sys.stderr)
        return 2
    if not sink.kafka_up(cfg.kafka_probe_timeout):
        print(f"Kafka ({cfg.kafka_bootstrap}) unreachable; left {len(pending)} "
              f"buffered file(s) in {cfg.buffer_dir}.", file=sys.stderr)
        return 1
    sent = drain(sink, outbox, time_limit=cfg.drain_time_limit)
    total = sum(sent.values())
    print(f"Drained {len(sent)} file(s) ({total} messages) to Kafka "
          f"{cfg.kafka_bootstrap}.", file=sys.stderr)
    remaining = len(outbox.pending_files())
    if remaining:
        print(f"WARN {remaining} file(s) still pending (Kafka went away mid-drain "
              f"or the {cfg.drain_time_limit:g}s drain limit was hit)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(drain_main())
