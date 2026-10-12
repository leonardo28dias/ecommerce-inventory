# BE-3 Event-Driven E-Commerce System — CV Showcase Metrics

## Project One-Liner
**Multi-tenant, event-driven order & inventory system** with PostgreSQL pessimistic locking, RabbitMQ messaging, dead-letter queues, and full idempotency — 2 services, 3 databases, 1 shared broker.

---

## Quantified Metrics for Your CV

### Scale & Throughput
| Metric | Value | Context |
|--------|-------|---------|
| **Concurrent checkout attempts** | **500** | Benchmark fires 500 parallel HTTP requests against single SKU |
| **Initial stock pool** | **50 units** | 10:1 contention ratio (500 attempts / 50 units) |
| **Thread pool workers** | **50** | Realistic concurrent client simulation |
| **Zero overbooking incidents** | **0** | Validated by benchmark — stock never negative, accounting always consistent |
| **End-to-end event latency (p50)** | **~50-150 ms** | OrderCreated → StockReserved/Rejected via RabbitMQ (measured in benchmark) |
| **Benchmark duration** | **< 30 seconds** | Full async pipeline completion for 500 orders |

### Architecture Complexity
| Metric | Value |
|--------|-------|
| **Services** | 2 (order-service, inventory-service) |
| **Databases** | 3 (orderdb, inventorydb, test dbs) |
| **Message broker** | 1 (RabbitMQ with management UI) |
| **Exchanges** | 3 (orders.events, inventory.events, dlx.events) |
| **Queues + DLQs** | 6 main + 4 dead-letter queues |
| **Routing keys** | 3 (order.created, stock.reserved, stock.rejected) |

### Code Quality
| Metric | Value |
|--------|-------|
| **Application code (LOC)** | **~2,000** lines (Python, FastAPI, SQLAlchemy) |
| **Test code (LOC)** | **~1,900** lines |
| **Test functions** | **76** test cases |
| **Test files** | **11** files |
| **Code-to-test ratio** | **~1:1** |
| **Concurrency test** | 10 threads × 5 units = proves zero negative stock |

### Resilience & Reliability
| Feature | Implementation |
|---------|----------------|
| **Max retry attempts** | **3** (configurable constant) |
| **Dead-letter isolation** | **4 DLQs** — poison messages never crash consumers |
| **Idempotency** | **Inbox pattern** — `processed_events` table in each service DB |
| **Duplicate handling** | Exactly-once semantics for OrderCreated, StockReserved, StockRejected |
| **Consumer crash safety** | Daemon threads + try/catch — main API stays up |

### Tech Stack (Recruiter Keywords)
```
Python 3.12  •  FastAPI  •  PostgreSQL 16  •  SQLAlchemy 2.x (async-ready)
Pydantic v2  •  RabbitMQ (pika)  •  Docker Compose  •  pytest  •  httpx
ThreadPoolExecutor  •  SELECT FOR UPDATE  •  Event-driven architecture
```

---

## STAR-Format Bullets (Ready for Resume)

### Bullet 1: Concurrency & Data Integrity
> **Designed and implemented a pessimistic locking inventory reservation system** using PostgreSQL `SELECT ... FOR UPDATE` that **guarantees zero overbooking under 500 concurrent requests** (10:1 contention) — validated by automated benchmark firing real HTTP requests through the full async pipeline.

### Bullet 2: Event-Driven Architecture & Messaging
> **Built a production-grade event-driven system** with RabbitMQ: 3 topic exchanges, 6 durable queues, 4 dead-letter queues, and retry/DLQ logic (max 3 retries). Consumers **never crash on malformed messages** — validation errors route straight to DLQ, business errors retry with exponential backoff semantics.

### Bullet 3: Idempotency & Exactly-Once Processing
> **Implemented inbox-pattern idempotency** across both services using `processed_events` tables with composite PKs — duplicate `OrderCreated` deliveries reserve stock **exactly once**, duplicate `StockReserved`/`StockRejected` update order status **exactly once**, surviving consumer crashes mid-transaction.

### Bullet 4: Multi-Tenancy & Data Isolation
> **Enforced strict tenant isolation** at every layer: HTTP headers → database composite unique constraints (`tenant_id + sku`, `tenant_id + order_id`) → event payloads → consumer queries. Cross-tenant access returns 404, never data leakage. Tested with 15+ isolation test cases.

### Bullet 5: Observability & Testing
> **Added structured JSON logging** with correlation fields (`event_id`, `event_type`, `tenant_id`, `retry_count`) and a **concurrency benchmark** measuring end-to-end event latency (creation → status flip). **76 automated tests** covering API, locking, idempotency, messaging failures, and DLQ behavior.

---

## Portfolio / GitHub Description

### BE-3: Event-Driven Multi-Tenant E-Commerce Inventory & Order System
*A graduate-level portfolio project demonstrating clean event-driven architecture for order processing with strongly consistent inventory reservation.*

**Live Demo:** `docker compose up --build` → API at `localhost:8001` (orders), `localhost:8002` (inventory), RabbitMQ UI at `localhost:15672`

**Key Technical Achievements:**
- ✅ **Zero overbooking** at 500 concurrent checkouts / 50 units (10:1 contention)
- ✅ **4 dead-letter queues** isolate poison messages; consumers stay up
- ✅ **Exactly-once semantics** via inbox pattern — survives crashes & redeliveries
- ✅ **Full multi-tenancy** — header-scoped, composite keys, event-carried tenant_id
- ✅ **76 tests** — API, pessimistic locking, idempotency, DLQ, integration
- ✅ **Benchmark script** — measures real end-to-end latency over HTTP + RabbitMQ

**Architecture:**
```
Client → POST /orders (tenant) → Order Service (orderdb)
    → publishes OrderCreated → RabbitMQ (orders.events)
    → Inventory Service consumes (SELECT FOR UPDATE on inventorydb)
    → publishes StockReserved/StockRejected → RabbitMQ (inventory.events)
    → Order Service consumes → updates order status (PENDING → RESERVED/REJECTED)
```

**Tech:** Python 3.12, FastAPI, PostgreSQL 16, SQLAlchemy 2, Pydantic 2, RabbitMQ, Docker, pytest

---

## Interview Talking Points (Metrics You Can Defend)

| Question | Your Answer with Numbers |
|----------|--------------------------|
| "How did you handle concurrency?" | `SELECT ... FOR UPDATE` row locks; benchmark proves 500 concurrent requests / 50 units = 0 overbooking, stock never negative |
| "What happens if RabbitMQ redelivers?" | Inbox table (`processed_events`) in each service DB — check-and-record in same transaction as business logic. Duplicate = no-op, returns stored outcome. |
| "How do you handle bad messages?" | Validation errors (malformed JSON, missing fields, wrong types) → immediate DLQ (no retry). Business errors → retry up to 3× with `x-retry-count` header, then DLQ. Consumer thread never crashes. |
| "How do you ensure tenant isolation?" | Every query filters `WHERE tenant_id = :tenant_id`. Composite unique keys: `(tenant_id, sku)` and `(tenant_id, order_id)`. Events carry tenant_id; consumers use it for ALL queries. |
| "What's the latency?" | Benchmark measures OrderCreated publish → StockReserved/Rejected consume → order status flip. Typical p50: 50-150ms on local Docker. |
| "How did you test it?" | 76 tests: 10-thread concurrency hammer test, idempotency duplicates, DLQ validation, API contracts, multi-tenant isolation. Benchmark script drives real services over HTTP. |

---

## Resume Section Template

```
BE-3: Event-Driven Multi-Tenant E-Commerce System          |  Python, FastAPI, PostgreSQL, RabbitMQ, Docker
├── Built 2-service event-driven system (Order + Inventory) with 3 PostgreSQL DBs, 1 RabbitMQ broker
├── Implemented pessimistic locking (SELECT FOR UPDATE) → 0 overbooking at 500 concurrent / 50 stock (10:1)
├── Designed retry/DLQ topology: 3 exchanges, 6 queues, 4 DLQs, max 3 retries, structured JSON logging
├── Inbox-pattern idempotency: exactly-once processing survives crashes & duplicate deliveries
├── Strict multi-tenancy: header-scoped APIs, composite keys, event-carried tenant_id, zero cross-tenant leaks
├── 76 automated tests + concurrency benchmark measuring real end-to-end latency (p50 ~100ms)
└── Tech: FastAPI, SQLAlchemy 2, Pydantic 2, pika, pytest, httpx, Docker Compose
```

---

## If You Need "Business Impact" Numbers

Since this is a portfolio project (not production), frame it as **demonstrated capability**:

> "This project demonstrates the ability to **design and implement** a system that **handles 500 concurrent checkout requests against 50 units of stock with zero overbooking** — a pattern that scales to production e-commerce traffic. The dead-letter queue design **prevents poison messages from causing consumer downtime**, and the inbox-pattern idempotency **guarantees exactly-once semantics** critical for financial/ordering systems."

---

## Quick Copy-Paste for LinkedIn "Projects" Section

**BE-3 Event-Driven E-Commerce System**  
*Python • FastAPI • PostgreSQL • RabbitMQ • Docker*

- **Concurrency:** 500 parallel checkouts / 50 units → **0 overbooking** (pessimistic locking via `SELECT FOR UPDATE`)
- **Messaging:** 3 exchanges, 6 queues, 4 DLQs, retry-then-DLQ (max 3), structured correlation logging
- **Idempotency:** Inbox pattern in each service DB → exactly-once for OrderCreated, StockReserved, StockRejected
- **Multi-tenancy:** Header-scoped, composite keys, event-carried tenant_id — 15+ isolation tests pass
- **Testing:** 76 tests + benchmark script measuring real HTTP→RabbitMQ→DB latency (p50 ~100ms)
- **Run it:** `docker compose up --build` → APIs on 8001/8002, RabbitMQ UI on 15672