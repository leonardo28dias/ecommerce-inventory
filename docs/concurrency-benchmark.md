# Concurrency benchmark — evidence (Phase 7)

Automated proof that the system never overbooks: 500 concurrent checkout
attempts against 50 units of one SKU, driven against the **real running
services** (real Postgres row locks, real RabbitMQ events — no mocks).

Script: `scripts/concurrency_benchmark.py`
Unit tests for the metric helpers: `scripts/test_concurrency_benchmark.py`

## How it works

1. Reset SKU stock to 50 via `PUT /inventory/{sku}` (row-locked absolute
   set; `POST /inventory` when the SKU does not exist yet).
2. Fire 500 × `POST /orders` (quantity 1 each) through a
   `ThreadPoolExecutor` (default 50 workers).
3. Poll `GET /orders/{order_id}` until every order reaches a terminal
   status (`RESERVED` / `REJECTED`) or `--timeout` (default 120 s) expires —
   polling with a deadline, no fixed sleeps.
4. Read back final stock, count statuses, compute metrics, print the report.
5. Exit non-zero on any violation: negative final stock, more successes than
   initial stock, `successful + final != initial`, success + rejection count
   mismatch, failed HTTP requests, or orders stuck in `PENDING`.

Event latency per order = `updated_at − created_at` (server-side timestamps:
creation just before `OrderCreated` is published → status flip when the
`StockReserved`/`StockRejected` result is consumed). The report shows the
real measured average / minimum / maximum over all completed events.

## Results (3 consecutive runs, 2026-10-08)

Environment: local machine (Linux, 16 cores), Postgres 16 + RabbitMQ 3.12.1
local; both services run directly via uvicorn (no Docker daemon on this
machine); tenant `benchmark`; SKU `BENCH-001` reset to 50 before each run;
500 attempts × quantity 1, 50 checkout workers, 120 s poll timeout.

| Run | Attempts | Initial | Reserved | Rejected | Final | Overbook | Avg latency | Min latency | Max latency | Duration |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 500 | 50 | 50 | 450 | 0 | 0 | 4895.8 ms | 280.0 ms | 7652.0 ms | 12.0 s |
| 2 | 500 | 50 | 50 | 450 | 0 | 0 | 5422.5 ms | 275.1 ms | 8483.5 ms | 13.9 s |
| 3 | 500 | 50 | 50 | 450 | 0 | 0 | 5660.9 ms | 288.9 ms | 8897.6 ms | 14.2 s |

Every run: exactly 50 reservations, 450 rejections, final stock 0,
zero overbooking, exit code 0. Raw JSON for each run is kept alongside this
report (`/tmp/bench1.json` … `/tmp/bench3.json` on the benchmark machine;
re-run to regenerate).

## Reading the latency numbers honestly

The average (~5 s) is **queueing delay, not pipeline speed**: each service
consumes its RabbitMQ queue with a single-threaded pika consumer, so a burst
of 500 events is processed largely serially and later orders wait seconds
behind earlier ones. The minimum (~280 ms) approximates the unloaded cost of
the full `OrderCreated` → reserve → result → status-flip path on this
machine. The original ~24 ms project target assumed a tuned/parallel
pipeline; correctness (zero overbooking across all runs) was prioritised over
chasing that number, and no value was manufactured — these are the real
measurements. Parallelising the consumers (prefetch + threaded dispatch) is
the obvious follow-up if lower burst latency is needed.

Note: `/health` returns HTTP 200 with body `"status": "degraded"` on this
machine because the health check queries the RabbitMQ management API without
credentials (cosmetic — the AMQP publish/consume path is verified working by
the benchmark itself). The benchmark's readiness wait only requires HTTP 200.

## Reproduce

```bash
python scripts/concurrency_benchmark.py \
  --attempts 500 --stock 50 --sku BENCH-001 --json /tmp/bench.json
echo "exit=$?"
```
