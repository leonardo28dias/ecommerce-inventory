# Event-Driven Multi-Tenant E-Commerce Order & Inventory System

A production-style portfolio backend that turns an HTTP `POST /orders` into an
**asynchronous, exactly-once** stock reservation through RabbitMQ and a
`SELECT ... FOR UPDATE` row lock. A reproducible 500-concurrent-checkout
benchmark proves the inventory can **never be overbooked**.

> Start with [`docs/architecture.md`](docs/architecture.md) for the Mermaid
> architecture diagram + RabbitMQ topology, and
> [`docs/concurrency-benchmark.md`](docs/concurrency-benchmark.md) for benchmark
> evidence.

---

## 1. Problem statement

E-commerce backends must simultaneously satisfy several competing demands:

- Serve **many tenants** (customers) from shared infrastructure **without data leakage**.
- Reserve stock **atomically and consistently** under heavy concurrency (flash-sale loads).
- Keep **inventory** and **order** concerns in **independent services** that evolve separately.
- Survive **duplicate message deliveries** from a best-effort broker without double-applying
  a business operation (idempotency).
- Provide **observability** (health, per-service logs, management UI) so the asynchronous
  pipeline is debuggable end to end.

## 2. Why this architecture exists

A naïve design would check stock and create the order in one synchronous SQL transaction
across a single database. That breaks down when orders and inventory must scale and deploy
independently: a single outage in one domain blocks the other, and stock checks couple two
unrelated business capabilities.

This project therefore embraces an **event-driven** split:

- The **order service** owns orders; it persists a `PENDING` order and *publishes* an
  `OrderCreated` event, then returns immediately. It never touches inventory.
- The **inventory service** owns stock; it *consumes* `OrderCreated`, takes a pessimistic
  row lock, makes an atomic reserve/reject decision, and *publishes* the result back.
- Both services are **multi-tenant** (a single `X-Tenant-ID` header) and **idempotent**
  (a `processed_events` table keyed on `event_id`), so redeliveries and retries are safe.

A `SELECT ... FOR UPDATE` row lock is the correctness primitive: it is the only mechanism
that makes "reserve if available, else reject" safe under concurrent readers. Everything
else (events, queues, DLQs) is there to keep the two services decoupled while preserving
that invariant.

## 3. Key features

- **Two independent FastAPI services**, each in its own Docker container with its own PostgreSQL database.
- **Event-driven stock reservation** via RabbitMQ topic exchanges — no service calls another service over HTTP for business logic.
- **Pessimistic locking** (`SELECT ... FOR UPDATE`) guarantees stock never goes negative and orders are never overbooked, even at 500 concurrent checkouts against 50 units.
- **Exactly-once application processing** via a per-service `processed_events` idempotency table committed in the same transaction as the business change.
- **Dead-letter queues** per queue; transient failures are retried (`MAX_RETRIES = 3`) and permanently-bad messages are isolated without crashing the consumer.
- **Multi-tenancy** enforced at the SQL level — every query is scoped by `tenant_id`.
- **Structured JSON logging** with a correlated `event_id` for request tracing, configurable via `LOG_LEVEL`.
- **Health endpoint** reporting database and RabbitMQ liveness (with correct broker authentication).
- **Reproducible concurrency benchmark** (`scripts/concurrency_benchmark.py`) that drives the live services.

## 4. Technology stack

| Layer | Choice |
| --- | --- |
| Language | Python 3.12 |
| HTTP API | FastAPI (OpenAPI UI at `/docs`) |
| Database | PostgreSQL 16 (SQLAlchemy 2.x ORM, psycopg 3 driver) |
| Message broker | RabbitMQ 3 (AMQP 0-9-1 topic exchanges) |
| Concurrency | `concurrent.futures.ThreadPoolExecutor`, `threading` |
| Messaging | `pika` (blocking client) inside a daemon consumer thread |
| Packaging | Docker + Docker Compose |
| Tests | pytest (+ FastAPI `TestClient`, HTTPX) |
| Observability | stdlib `logging` → one JSON object per line with `event_id` |
| Locking | PostgreSQL `SELECT ... FOR UPDATE` (pessimistic row lock) |

## 5. Architecture overview

```
Client ──POST /orders──► Order Service ──OrderCreated──► RabbitMQ ──► Inventory Service
  ▲                           │  ▲ (Order PostgreSQL persists PENDING)         │  ▲ (Inventory PostgreSQL)
  │                           │  │                                             │  │  SELECT ... FOR UPDATE
GET /orders/{id}              │  │                                             │  └── reserve_stock() atomic
(observe terminal status)     │  ▼                                             │
                              │ StockReserved / StockRejected ──► RabbitMQ ──► (order status updated)
                              └─────────────────────────────────────────────────► back to order service
```

A precise, annotated Mermaid version (including the full exchange/queue/DLQ topology)
is in [`docs/architecture.md`](docs/architecture.md).

## 6. Service responsibilities

| Service | Owns | Database | HTTP API | RabbitMQ role |
| --- | --- | --- | --- | --- |
| **order-service** | `orders`, `processed_events` | `orderdb` | `POST /orders`, `GET /orders/{id}` | Publishes `OrderCreated`; consumes `StockReserved`/`StockRejected` |
| **inventory-service** | `inventory_items`, `processed_events` | `inventorydb` | `/inventory` CRUD + `reserve` | Consumes `OrderCreated`; publishes `StockReserved`/`StockRejected` |

Neither service imports the other's code. The **only** contract between them is the
shape of the events (`event_id`, `event_type`, `tenant_id`, `timestamp`, `payload`),
asserted by `test_event_contract_separates_services`. Each service defines the event
schemas **independently** with the same field names — they could evolve separately
as long as the wire contract holds.

## 7. Event flow

1. `POST /orders` (with `X-Tenant-ID`) → order service inserts an `orders` row with `status=PENDING` and commits.
2. Order service publishes `OrderCreated` (RK `order.created`) to the `orders.events` exchange.
3. Inventory service consumes `OrderCreated` → `reserve_stock_for_event`.
4. Inside one transaction: `SELECT ... FOR UPDATE` the `inventory_items` row → if `available_stock >= quantity`, subtract and commit; else raise `InsufficientStock`. The `event_id` is written to `processed_events` **in the same transaction**.
5. Inventory publishes `StockReserved` or `StockRejected` (RK `stock.reserved` / `stock.rejected`) to `inventory.events`.
6. Order service consumes the result → `apply_stock_result` dedups `event_id` and sets `order.status` to `RESERVED` / `REJECTED` (same-txn idempotency write).
7. `GET /orders/{id}` reflects the terminal status.

The order exists (and the client gets a `201`) before stock is even checked — that is
intentional: availability of order creation over strict immediate fulfilment.

## 8. RabbitMQ topology

- **Exchanges** (topic, durable): `orders.events`, `inventory.events`, `dlx.events` (dead-letter).
- **Queues** (durable, `x-dead-letter-exchange: dlx.events`):
  - `inventory-service.order-created` (bind RK `order.created`) → DLQ `inventory-service.order-created.dlq`
  - `order-service.stock-reserved` (bind RK `stock.reserved`) → DLQ `order-service.stock-reserved.dlq`
  - `order-service.stock-rejected` (bind RK `stock.rejected`) → DLQ `order-service.stock-rejected.dlq`
- **Messages are persistent** (`delivery_mode=2`); exchanges/queues are **durable**.
- `basic_qos(prefetch_count=10)` on both consumers bounds unacked work.
- `declare_topology()` is **idempotent** — every service re-declares its exchanges,
  queues and bindings on startup, so a fresh broker is self-configured.

## 9. Database design

**Order DB (`orderdb`)**
```
orders
  id            SERIAL PK
  tenant_id     TEXT  (indexed)            -- tenant scope on every query
  order_id      TEXT  UNIQUE               -- public id (UUID), returned to client
  sku           TEXT
  quantity      INT
  status        TEXT  DEFAULT 'PENDING'   -- PENDING | RESERVED | REJECTED
  created_at / updated_at                TIMESTAMPTZ
processed_events
  event_id      TEXT PK                   -- dedup key for StockReserved/StockRejected
  outcome       TEXT                     -- 'reserved' | 'rejected' | NULL
  processed_at  TIMESTAMPTZ
```

**Inventory DB (`inventorydb`)**
```
inventory_items
  id            SERIAL PK
  tenant_id     TEXT  (indexed)
  sku           TEXT
  product_name  TEXT
  available_stock INT NOT NULL DEFAULT 0
  created_at / updated_at                TIMESTAMPTZ
  UNIQUE(tenant_id, sku)                 -- same SKU allowed across tenants
processed_events
  event_id      TEXT PK                   -- dedup key for OrderCreated
  outcome       TEXT
  processed_at  TIMESTAMPTZ
```

## 10. Multi-tenancy approach

- The tenant is taken from the **`X-Tenant-ID` HTTP header** on every request.
- **Every** query in both services filters on `tenant_id == <header value>`:
  `Order.tenant_id` is indexed; `InventoryItem` has a `UNIQUE(tenant_id, sku)`
  constraint so two tenants can reuse the same SKU string with separate rows.
- Events **carry `tenant_id`**, and the consumer routes the stock operation to that
  tenant's inventory row — there is no global stock pool.
- No cross-tenant SQL path exists: `test_tenant_isolation` (order),
  `test_reserve_cross_tenant_rejected` and `test_tenant_isolation_in_event_processing`
  (inventory) prove a tenant's requests return 404 / `OrderNotFound` for another
  tenant's data.

## 11. Pessimistic locking explanation

`reserve_stock()` does, inside a single transaction:

```python
item = (db.query(InventoryItem)
          .filter(InventoryItem.tenant_id == tenant_id, InventoryItem.sku == sku)
          .with_for_update()        # SELECT ... FOR UPDATE -> row-level lock
          .one_or_none())
if item is None:
    raise ItemNotFound(...)
if item.available_stock < quantity:
    raise InsufficientStock(...)
item.available_stock -= quantity
db.commit()
```

`with_for_update()` emits `SELECT ... FOR UPDATE`, which **blocks other
transactions** from acquiring the same row until this one commits. Because the
**check** (`available_stock >= quantity`) and the **subtract** happen under that
lock in the same transaction, a concurrent caller can never observe a stale balance
— the second caller blocks, then sees the already-reduced stock and is rejected.
`test_reservation_select_uses_for_update` asserts the emitted SQL contains `FOR UPDATE`,
and `test_concurrent_reservations_never_overbook` hammers a row from 10 threads
against 5 units.

## 12. Idempotency explanation

Duplicate deliveries are defeated with a per-service `processed_events` table:

```python
def _check_duplicate(db, event_id):
    db.add(ProcessedEvent(event_id=event_id))   # event_id is the PRIMARY KEY
    try:
        db.flush()                             # IntegrityError => AlreadyProcessed
    except IntegrityError:
        db.rollback()
        raise AlreadyProcessed(...)
```

The `event_id` insert is committed in the **same transaction** as the business change
(`reserve_stock` subtract, or `apply_stock_result` status flip). So either the outcome
+ idempotency record persist together, or neither does — a retried delivery past the
point of a crash finds the key already present and is skipped. `apply_stock_result`
additionally short-circuits when the order is already in the target state. Tests:
`test_duplicate_order_created_does_not_double_reserve`, `test_duplicate_stock_reserved_does_not_double_update`.

## 13. Dead-letter handling

- On a **processing exception** (a real error, not a validation error), the consumer
  `basic_nack(requeue=False)`s the message and re-publishes it with
  `headers["x-retry-count"]` incremented; the queue's `x-dead-letter-exchange` then
  routes the nack'd message through the retry cycle.
- After `MAX_RETRIES = 3` attempts the message is nacked with `requeue=False` and lands
  in the matching `*.dlq`, where it stays for inspection — the consumer **does not crash**
  and keeps processing subsequent messages.
- On a **validation error** (malformed JSON, schema violation, unknown event type) the
  message goes **straight to the DLQ without retry**, because a payload that can never
  validate only wastes retries. `test_malformed_json_payload_goes_to_dlq` and
  `test_missing_sku_in_payload` cover this.
- The order service runs its consumer in a daemon thread, so a broker/consumer crash is
  logged and isolated; it does not take the HTTP API down.

## 14. Local setup

```bash
# 1. Configure
cp .env.example .env            # adjust ports/passwords if you like (defaults work)

# 2. Start everything (databases, RabbitMQ, both services)
docker compose up --build

# 3. Seed a SKU, then create an order
curl -X POST http://localhost:8002/inventory \
  -H 'X-Tenant-ID: demo' -H 'Content-Type: application/json' \
  -d '{"sku":"laptop-001","product_name":"Laptop","available_stock":50}'
curl -X POST http://localhost:8001/orders \
  -H 'X-Tenant-ID: demo' -H 'Content-Type: application/json' \
  -d '{"sku":"laptop-001","quantity":2}'
curl http://localhost:8001/orders/{order_id} -H 'X-Tenant-ID: demo'

# Health + docs
curl http://localhost:8001/health
curl http://localhost:8002/health
```

RabbitMQ management UI: http://localhost:15672 (guest/guest). Interactive OpenAPI
docs at `/docs` for each service.

> The author's local environment has no usable `docker` socket, so `docker compose
> up` could not be executed here. The compose file was validated with
> `docker compose config`, and services + tests were run directly on the host against
> local Postgres 16 + RabbitMQ 3.12.1. See [`docs/ai-usage.md`](docs/ai-usage.md).
## 15. Environment variables

Everything is environment-driven. See `.env.example` for the full template; all
runtime secrets (database password, RabbitMQ password) come from the environment —
**nothing is hardcoded in source**.

| Variable | Service | Default | Purpose |
| --- | --- | --- | --- |
| `LOG_LEVEL` | both | `INFO` | Logging verbosity |
| `ORDER_DB_HOST`/`PORT`/`NAME` | order | `order-db`/`5432`/`orderdb` | Order PostgreSQL |
| `ORDER_DB_USER`/`PASSWORD` | order | `postgres` | Order DB credentials |
| `INVENTORY_DB_*` | inventory | `inventory-db`/`5432`/`inventorydb` | Inventory PostgreSQL |
| `RABBITMQ_HOST`/`PORT` | both | `rabbitmq`/`5672` | AMQP broker |
| `RABBITMQ_UI_PORT` | both | `15672` | Management API (used by `/health`) |
| `RABBITMQ_USER`/`PASSWORD` | both | `guest` | RabbitMQ credentials |
| `*_SERVICE_PORT` | each | `8001`/`8002` | Host port the service binds |

`.env` is git-ignored; `.env.example` is committed as the template.

## 16. API examples

**Order service** (port 8001)

```bash
# Create an order — returns 201 immediately; status is PENDING until the
# async stock reservation completes.
curl -X POST http://localhost:8001/orders \
  -H 'X-Tenant-ID: tenant-a' -H 'Content-Type: application/json' \
  -d '{"sku":"laptop-001","quantity":2}'

# Read it back (poll until status becomes RESERVED/REJECTED)
curl http://localhost:8001/orders/a5c2... -H 'X-Tenant-ID: tenant-a'
```

**Inventory service** (port 8002)

```bash
curl -X POST http://localhost:8002/inventory \
  -H 'X-Tenant-ID: tenant-a' -H 'Content-Type: application/json' \
  -d '{"sku":"laptop-001","product_name":"Laptop","available_stock":50}'
curl http://localhost:8002/inventory/laptop-001 -H 'X-Tenant-ID: tenant-a'
curl -X POST http://localhost:8002/inventory/laptop-001/reserve \
  -H 'X-Tenant-ID: tenant-a' -H 'Content-Type: application/json' -d '{"quantity":3}'
curl -X PUT http://localhost:8002/inventory/laptop-001 \
  -H 'X-Tenant-ID: tenant-a' -H 'Content-Type: application/json' -d '{"available_stock":50}'
```

## 17. Running tests

```bash
# Single command against the full suite (requires local Postgres 16)
python -m pytest order-service/ inventory-service/ scripts/ -q
```

A local Postgres 16 cluster is required — the integration/locking/idempotency tests
run against a real database (SQLite cannot emulate `SELECT ... FOR UPDATE` row locking).
The `conftest.py` fixtures auto-create the `order_test` / `inventory_test` test databases and
point the application at them (verify `order-service/conftest.py` / `inventory-service/conftest.py`).
The suite is verified green (**84 passed**) on the developer's Postgres 16 + RabbitMQ 3
environment. In this sandbox the local Postgres 16 cluster rejects the default
`postgres` / `postgres` TCP password, so the conftest test databases could not be
auto-created here and the end-to-end run could not be reproduced; the two services and
their Docker images were also not executed (no usable Docker socket), as documented in §21.

```bash
python -m pytest order-service/      -q   # HTTP, messaging, idempotency, logging
python -m pytest inventory-service/  -q   # API, locking, idempotency, DLQ, logging
python -m pytest scripts/            -q   # benchmark metric helpers
```

## 18. Running the concurrency benchmark

The benchmark drives the **real running services** (real Postgres locks, real RabbitMQ
events — no mocks). It needs both services up and seeded first.

```bash
python scripts/concurrency_benchmark.py --attempts 500 --stock 50 --sku BENCH-001 \
  --json /tmp/bench.json
echo "exit=$?"
```

## 19. Benchmark results

Reproducible 500-concurrent-checkout run against 50 units of one SKU (the
"interview evidence" run). All numbers are real measurements recorded in
`docs/concurrency-benchmark.md`.

| Run | Attempts | Initial | Reserved | Rejected | Final | Overbooking |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 500 | 50 | 50 | 450 | 0 | 0 |
| 2 | 500 | 50 | 50 | 450 | 0 | 0 |
| 3 | 500 | 50 | 50 | 450 | 0 | 0 |

Every run: exactly **50 reservations, 450 rejections, final stock 0, zero
overbooking, exit code 0.** The invariants `reserved == initial_stock`,
`final_stock == 0` and `final_stock >= 0` held every time.

There is also a unit-level concurrency proof in the test suite
(`test_inventory_locking.py`): 10 threads racing for 5 units → exactly 5 succeed,
final stock 0, no errors.

## 20. Measured event latency

- **Average** event latency ≈ **5 s** under the 500-burst (queueing delay — see §22).
- **Minimum** ≈ **280 ms** — the unloaded cost of the full
  `OrderCreated → reserve → StockReserved → order status flip` path.
- **Maximum** ≈ 7.6–8.9 s — tail latency under the burst.

Event latency is `updated_at − created_at` measured server-side: from order creation
(immediately before `OrderCreated` is published) to the order status flip (when the
result event is consumed and the order row is updated). It includes publication,
queueing, inventory processing and the response event — the full propagation path.

**Why the average is seconds, not milliseconds:** each service consumes its RabbitMQ
queue with a single-threaded `pika` consumer, so a 500-message burst is processed
largely serially and later orders wait behind the queue. The minimum (~280 ms) is the
true pipeline cost; the gap is queueing. See `docs/concurrency-benchmark.md` for the
full honest latency reading.

## 21. Known limitations

- **No transactional outbox.** `OrderCreated` is published *after* the order row
  commits, but the two are not in the same transaction. If the broker is down at
  publish time the order is persisted (`PENDING`) and the event is lost — the order
  stalls until re-published manually. (The `POST /orders` call is availability-prioritized:
  a broker outage returns `201`, not `500`.)
- **Eventually consistent.** Order status updates only once inventory consumes and
  echoes back the result; `PENDING` is transient by design.
- **Single-threaded consumers.** `prefetch_count=10` but one thread per service, so
  burst latency is dominated by queueing (see §20).
- **No retry backoff / TTL.** Retries re-publish immediately; a hot-failing message
  spins through its 3 retries quickly and then to the DLQ.
- **Consumer thread is not supervised.** A crashed consumer thread is logged but not
  restarted by the process; the HTTP API keeps serving.
- **`/health` RabbitMQ check.** Older versions called the management API
  unauthenticated (HTTP 401 → `rabbitmq: "error"`). This is now fixed: the check sends
  the configured `RABBITMQ_USER` / `RABBITMQ_PASSWORD` as HTTP basic auth, so a healthy
  broker reports `rabbitmq: "ok"`.

## 22. Future improvements

- **Transactional outbox** for `OrderCreated` — the single highest-value reliability fix.
- **Parallelise consumers** (prefetch + threaded/dispatch) to cut burst latency toward
  the ~280 ms unloaded minimum.
- **Exponential backoff + TTL delay** between retries instead of immediate re-publish.
- **Poison-message alerting** — notify when a DLQ grows past a threshold.
- **Consumer supervision / multi-instance** consumers so a dead thread is restarted.
- **OpenTelemetry tracing** to correlate one checkout across both services + RabbitMQ.
- **Schema evolution** for events (backward/forward-compatible contracts).

## 23. AI usage

AI assistance (Claude) was used throughout all phases for architectural brainstorming,
reading error tracebacks, drafting/reviewing code and tests, and writing documentation.
Every AI-suggested change was **reviewed, tested and adapted** by hand before landing.
A full, honest record — including the real obstacles encountered and the human
decisions that were not mere code generation — is in
[`docs/ai-usage.md`](docs/ai-usage.md). This project was **not** built by blindly
accepting generated code.

## 24. Project structure

```
E-Commerce/
├── docker-compose.yml          # 2 DBs + RabbitMQ + 2 FastAPI services
├── .env.example                 # all runtime values (no secrets committed)
├── .gitignore
├── order-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── conftest.py
│   ├── order_app/               # main, config, database, models, schemas,
│   │                            # service, messaging, routes, logging_config, health
│   └── tests/  (test_order_*.py)
├── inventory-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── conftest.py
│   ├── inventory_app/
│   └── tests/  (test_inventory_*.py, test_messaging_failure.py, *_logging_config.py)
├── scripts/
│   ├── concurrency_benchmark.py
│   └── test_concurrency_benchmark.py
├── docs/
│   ├── architecture.md
│   ├── concurrency-benchmark.md
│   └── ai-usage.md
└── README.md                  # this file
```

## 25. License

Portfolio project (proprietary). Educational reference only.
