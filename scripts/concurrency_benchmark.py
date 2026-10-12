#!/usr/bin/env python3
"""Concurrency benchmark for the BE-3 event-driven e-commerce system.

Fires a burst of concurrent checkouts (default: 500 attempts x quantity 1)
against a single SKU with limited stock (default: 50 units) and verifies that
the asynchronous ``OrderCreated`` -> ``StockReserved`` / ``StockRejected``
pipeline never overbooks.

The benchmark drives the **real running services over HTTP** - no mocks: the
inventory database really executes ``SELECT ... FOR UPDATE`` and RabbitMQ
really carries the events.

Measured metrics:

* total requests, successful reservations, rejected reservations
* final stock, overbooking incidents, elapsed wall-clock time
* event latency: order creation (immediately before ``OrderCreated`` is
  published) -> order status flip (when ``StockReserved``/``StockRejected`` is
  handled by the order service), using server-side timestamps. This includes
  publication, queueing, inventory processing and the response event - the
  full propagation path. The reported value is whatever the machine actually
  measures; nothing is tuned towards any target.

Waiting for the asynchronous pipeline uses polling with a deadline
(``--timeout``), not fixed sleeps.

Exit codes:

* 0 - every invariant holds
* 1 - a business invariant was violated (overbooking, negative stock,
      inconsistent accounting, failed HTTP requests, unprocessed orders)
* 2 - the benchmark could not run (service unreachable, stock reset failed)

Usage::

    python scripts/concurrency_benchmark.py [--attempts 500] [--stock 50]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import httpx

EXIT_OK = 0
EXIT_VIOLATION = 1
EXIT_OPERATIONAL = 2

TERMINAL_STATUSES = {"RESERVED", "REJECTED"}

DEFAULT_TENANT = "benchmark"


def _headers(tenant: str) -> dict[str, str]:
    return {"X-Tenant-ID": tenant}


class BenchmarkError(Exception):
    """Raised when the benchmark cannot run (preflight/setup failure)."""


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in scripts/test_concurrency_benchmark.py)
# ---------------------------------------------------------------------------


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp (with ``Z`` or numeric offset) to UTC."""
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        # Server-side timestamps are UTC when the offset is missing.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def event_latency_ms(created_at: str, updated_at: str) -> float:
    """Latency from order creation to stock-result handling, in milliseconds.

    ``created_at`` is written by the order service right before it publishes
    ``OrderCreated``; ``updated_at`` is written when the order status flips to
    ``RESERVED``/``REJECTED`` after the stock result event is consumed.
    """
    delta = parse_timestamp(updated_at) - parse_timestamp(created_at)
    return delta.total_seconds() * 1000.0


def latency_stats(samples_ms: list[float]) -> dict[str, float | int]:
    """Return ``count``/``average``/``minimum``/``maximum`` for latencies."""
    if not samples_ms:
        return {"count": 0, "average": 0.0, "minimum": 0.0, "maximum": 0.0}
    return {
        "count": len(samples_ms),
        "average": sum(samples_ms) / len(samples_ms),
        "minimum": min(samples_ms),
        "maximum": max(samples_ms),
    }


def compute_metrics(
    *,
    attempts: int,
    successful: int,
    rejected: int,
    initial_stock: int,
    final_stock: int,
) -> dict[str, int]:
    """Derive overbooking/accounting metrics from raw counters.

    ``overbooking_incidents`` is the number of successful reservations beyond
    the available stock; ``accounting_ok`` is False when reserved + final
    stock does not reconcile with the initial stock or when any counter is
    impossible (negative stock, success + rejection != attempts).
    """
    overbooking = max(0, successful - initial_stock)
    consistent = (
        final_stock >= 0
        and successful <= initial_stock
        and successful + rejected == attempts
        and successful + final_stock == initial_stock
    )
    return {"overbooking_incidents": overbooking, "accounting_ok": consistent}


# ---------------------------------------------------------------------------
# HTTP helpers (real services, no mocks)
# ---------------------------------------------------------------------------


def _base(name: str, default: str) -> str:
    return os.environ.get(name, default).rstrip("/")


def wait_for_services(
    order_base: str, inventory_base: str, timeout: float, tenant: str
) -> None:
    """Poll ``/health`` on both services until ready or the deadline passes."""
    deadline = time.monotonic() + timeout
    last_error = "unknown"
    with httpx.Client(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                order_ok = client.get(f"{order_base}/health").status_code == 200
                inv_ok = (
                    client.get(f"{inventory_base}/health").status_code == 200
                )
                if order_ok and inv_ok:
                    return
                last_error = "health check returned non-200"
            except Exception as exc:  # noqa: BLE001 - want the root cause
                last_error = str(exc)
            time.sleep(0.5)
    raise BenchmarkError(f"services not ready within {timeout:.0f}s: {last_error}")


def reset_stock(
    inventory_base: str, sku: str, quantity: int, tenant: str
) -> int:
    """Reset ``sku`` to ``quantity`` units; returns the confirmed stock level.

    The real inventory API exposes ``PUT /inventory/{sku}`` (absolute set,
    row-locked) for this; when the SKU does not exist yet it is created with
    ``POST /inventory``.
    """
    headers = _headers(tenant)
    with httpx.Client(timeout=10.0, headers=headers) as client:
        current = client.get(f"{inventory_base}/inventory/{sku}")
        if current.status_code == 200:
            response = client.put(
                f"{inventory_base}/inventory/{sku}",
                json={"available_stock": quantity},
            )
            if response.status_code != 200:
                raise BenchmarkError(
                    f"stock reset failed ({response.status_code}: "
                    f"{response.text[:200]})"
                )
        elif current.status_code == 404:
            create = client.post(
                f"{inventory_base}/inventory",
                json={
                    "sku": sku,
                    "product_name": sku,
                    "available_stock": quantity,
                },
            )
            if create.status_code not in (200, 201):
                raise BenchmarkError(
                    f"stock reset failed (create -> {create.status_code}: "
                    f"{create.text[:200]})"
                )
        else:
            raise BenchmarkError(
                f"cannot inspect stock for {sku!r} "
                f"({current.status_code}: {current.text[:200]})"
            )
        confirm = client.get(f"{inventory_base}/inventory/{sku}")
        if confirm.status_code != 200:
            raise BenchmarkError(f"cannot read back stock for {sku!r}")
        return int(confirm.json()["available_stock"])


def create_order(
    order_base: str, sku: str, quantity: int, tenant: str
) -> dict[str, Any]:
    """Create one order; raises on transport/validation failure."""
    with httpx.Client(timeout=15.0, headers=_headers(tenant)) as client:
        response = client.post(
            f"{order_base}/orders", json={"sku": sku, "quantity": quantity}
        )
        response.raise_for_status()
        return response.json()


def fetch_order(
    order_base: str, order_id: str, tenant: str
) -> dict[str, Any]:
    with httpx.Client(timeout=10.0, headers=_headers(tenant)) as client:
        response = client.get(f"{order_base}/orders/{order_id}")
        response.raise_for_status()
        return response.json()


def fetch_stock(inventory_base: str, sku: str, tenant: str) -> int:
    with httpx.Client(timeout=10.0, headers=_headers(tenant)) as client:
        response = client.get(f"{inventory_base}/inventory/{sku}")
        response.raise_for_status()
        return int(response.json()["available_stock"])


def wait_for_completion(
    order_base: str, order_ids: list[str], timeout: float, tenant: str
) -> dict[str, dict[str, Any]]:
    """Poll orders until all reach RESERVED/REJECTED or the deadline passes."""
    deadline = time.monotonic() + timeout
    pending: set[str] = set(order_ids)
    latest: dict[str, dict[str, Any]] = {}
    with httpx.Client(timeout=10.0, headers=_headers(tenant)) as client:
        while pending and time.monotonic() < deadline:
            for order_id in sorted(pending):
                try:
                    response = client.get(f"{order_base}/orders/{order_id}")
                    if response.status_code != 200:
                        continue
                    payload = response.json()
                except Exception:  # noqa: BLE001 - transient; retry next round
                    continue
                latest[order_id] = payload
                if payload.get("status") in TERMINAL_STATUSES:
                    pending.discard(order_id)
            if pending:
                time.sleep(0.5)
    return latest

# ---------------------------------------------------------------------------
# Benchmark driver
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Concurrent checkout benchmark (500x1 vs 50 units by default)."
    )
    parser.add_argument("--attempts", type=int, default=500)
    parser.add_argument("--stock", type=int, default=50)
    parser.add_argument("--sku", default="BENCH-001")
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--workers", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--tenant", default=None)
    parser.add_argument("--order-base", default=None)
    parser.add_argument("--inventory-base", default=None)
    parser.add_argument("--json", dest="json_path", default=None)
    return parser.parse_args(argv)


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    order_base = args.order_base or _base("ORDER_BASE_URL", "http://127.0.0.1:8001")
    inventory_base = args.inventory_base or _base(
        "INVENTORY_BASE_URL", "http://127.0.0.1:8002"
    )
    tenant = args.tenant or os.environ.get("BENCH_TENANT", DEFAULT_TENANT)
    started = time.monotonic()
    wait_for_services(order_base, inventory_base, timeout=30.0, tenant=tenant)
    initial_stock = reset_stock(inventory_base, args.sku, args.stock, tenant)

    order_ids: list[str] = []
    created_at: dict[str, str] = {}
    failures: list[str] = []

    def _attempt(_index: int) -> dict[str, Any] | None:
        try:
            return create_order(order_base, args.sku, args.quantity, tenant)
        except Exception as exc:  # noqa: BLE001 - collected as failure
            return {"__error__": str(exc)}

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_attempt, i) for i in range(args.attempts)]
        for future in as_completed(futures):
            payload = future.result()
            if payload is None or "__error__" in payload:
                err = str(payload.get("__error__", "?")) if payload else "?"
                failures.append(err)
                continue
            order_id = str(payload.get("order_id") or payload.get("id") or "")
            if not order_id:
                failures.append(f"order without id: {payload!r:.120}")
                continue
            order_ids.append(order_id)
            if payload.get("created_at"):
                created_at[order_id] = payload["created_at"]

    orders = wait_for_completion(order_base, order_ids, timeout=args.timeout,
                                 tenant=tenant)
    final_stock = fetch_stock(inventory_base, args.sku, tenant)
    latencies: list[float] = []
    successful = 0
    rejected = 0
    unprocessed = 0
    for order_id in order_ids:
        order = orders.get(order_id, {})
        status = order.get("status")
        if status == "RESERVED":
            successful += 1
        elif status == "REJECTED":
            rejected += 1
        else:
            unprocessed += 1
            continue
        born = created_at.get(order_id) or order.get("created_at")
        done = order.get("updated_at")
        if born and done:
            try:
                latencies.append(event_latency_ms(born, done))
            except ValueError:
                continue
    metrics = compute_metrics(
        attempts=args.attempts,
        successful=successful,
        rejected=rejected,
        initial_stock=initial_stock,
        final_stock=final_stock,
    )
    stats = latency_stats(latencies)
    elapsed = time.monotonic() - started
    return {
        "attempts": args.attempts,
        "quantity": args.quantity,
        "sku": args.sku,
        "initial_stock": initial_stock,
        "successful": successful,
        "rejected": rejected,
        "unprocessed": unprocessed,
        "transport_failures": len(failures),
        "final_stock": final_stock,
        "overbooking_incidents": metrics["overbooking_incidents"],
        "accounting_ok": metrics["accounting_ok"],
        "latency": stats,
        "elapsed_s": elapsed,
        "failure_samples": failures[:5],
    }


def print_report(result: dict[str, Any]) -> None:
    latency = result["latency"]
    print("Concurrent checkout benchmark")
    print("-----------------------------")
    print(f"Attempts: {result['attempts']}")
    print(f"Requested quantity: {result['quantity']}")
    print(f"Initial stock: {result['initial_stock']}")
    print(f"Successful reservations: {result['successful']}")
    print(f"Rejected reservations: {result['rejected']}")
    print(f"Final stock: {result['final_stock']}")
    print(f"Overbooking incidents: {result['overbooking_incidents']}")
    print(f"Average event latency: {latency['average']:.1f} ms")
    print(f"Minimum event latency: {latency['minimum']:.1f} ms")
    print(f"Maximum event latency: {latency['maximum']:.1f} ms")
    print(f"Completed events: {latency['count']}")
    print(f"Total duration: {result['elapsed_s']:.1f} s")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_benchmark(args)
    except BenchmarkError as exc:
        print(f"benchmark setup failed: {exc}", file=sys.stderr)
        return EXIT_OPERATIONAL
    print_report(result)
    if args.json_path:
        with open(args.json_path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2)
    violations: list[str] = []
    if result["transport_failures"]:
        violations.append(f"{result['transport_failures']} requests failed")
    if result["unprocessed"]:
        violations.append(f"{result['unprocessed']} orders never terminal")
    if result["final_stock"] < 0:
        violations.append("final stock is negative")
    if result["successful"] > result["initial_stock"]:
        violations.append("successful reservations exceed initial stock")
    if result["overbooking_incidents"]:
        violations.append("overbooking detected")
    if not result["accounting_ok"]:
        violations.append("stock accounting is inconsistent")
    if violations:
        print("FAIL: " + "; ".join(violations), file=sys.stderr)
        return EXIT_VIOLATION
    print("PASS: no overbooking; stock accounting is consistent")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
