import time
from datetime import datetime, timezone

import pytest

from velop_watcher.fetch import parse_generated_at, read_until_marker, router_host


def test_router_host():
    assert router_host("https://10.13.1.1/sysinfo.cgi") == "10.13.1.1"


def test_parse_generated_at():
    text = "page generated on Thu Jun 18 14:58:22 UTC 2026\n\nUpTime:\n"
    assert parse_generated_at(text) == datetime(2026, 6, 18, 14, 58, 22, tzinfo=timezone.utc)


def test_parse_generated_at_missing():
    assert parse_generated_at("no timestamp here\n") is None


def test_read_until_marker_found():
    chunks = ["abc ", "End of ", "Sysinfo Output ****", " trailing"]
    text = read_until_marker(chunks, "End of Sysinfo Output")
    assert "End of Sysinfo Output" in text
    # Stops as soon as the marker is complete; later chunks are not consumed.
    assert "trailing" not in text


def test_read_until_marker_stream_ends_early():
    with pytest.raises(ValueError):
        read_until_marker(["partial output"], "End of Sysinfo Output")


def test_read_until_marker_deadline():
    def slow_chunks():
        yield "start"
        time.sleep(0.01)
        yield "more"

    with pytest.raises(TimeoutError):
        read_until_marker(slow_chunks(), "never", deadline=time.monotonic() - 1)
