"""Unit tests for the pure helpers in scripts/concurrency_benchmark.py.

Only the deterministic computation helpers are covered here — no HTTP, no
threads, no services. Run with ``pytest scripts/test_concurrency_benchmark.py``.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from concurrency_benchmark import (  # noqa: E402
    compute_metrics,
    event_latency_ms,
    latency_stats,
    parse_timestamp,
)


def test_parse_timestamp_z_suffix():
    dt = parse_timestamp("2026-01-01T00:00:00Z")
    assert (dt.year, dt.month, dt.day) == (2026, 1, 1)
    assert dt.tzinfo is not None


def test_parse_timestamp_naive_assumed_utc():
    dt = parse_timestamp("2026-01-01T00:00:00")
    assert dt.tzinfo is not None


def test_event_latency_ms():
    assert event_latency_ms("2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z") == 1000.0


def test_latency_stats_empty():
    assert latency_stats([]) == {
        "count": 0,
        "average": 0.0,
        "minimum": 0.0,
        "maximum": 0.0,
    }


def test_latency_stats_values():
    stats = latency_stats([10.0, 20.0, 30.0])
    assert stats["count"] == 3
    assert stats["average"] == 20.0
    assert stats["minimum"] == 10.0
    assert stats["maximum"] == 30.0


def test_compute_metrics_happy_path():
    metrics = compute_metrics(
        attempts=500,
        successful=50,
        rejected=450,
        initial_stock=50,
        final_stock=0,
    )
    assert metrics == {"overbooking_incidents": 0, "accounting_ok": True}


def test_compute_metrics_overbooking():
    metrics = compute_metrics(
        attempts=500,
        successful=51,
        rejected=449,
        initial_stock=50,
        final_stock=0,
    )
    assert metrics["overbooking_incidents"] == 1
    assert metrics["accounting_ok"] is False


def test_compute_metrics_negative_stock():
    metrics = compute_metrics(
        attempts=500,
        successful=50,
        rejected=450,
        initial_stock=50,
        final_stock=-1,
    )
    assert metrics["accounting_ok"] is False
