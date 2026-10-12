# Architecture

BE-3 is an **event-driven**, **multi-tenant**, **two-service** e-commerce backend.
The diagram below is the core interview story — an `POST /orders` request that
ends up changing an order's status only *after* the inventory service has
atomically reserved stock through RabbitMQ and a `SELECT ... FOR UPDATE` row
lock.

```mermaid
flowchart TD
    subgraph Client["Client"]
        C["HTTP client"]
    end

    subgraph OrderSvc["Order Service<br/><small>(FastAPI / SQLAlchemy)</small>"]
        OA["POST /orders<br/>persist order PENDING"]
        PUB["publish_order_created()"]
        CS["consume_stock_events()<br/>(background thread)"]
        ASR["apply_stock_result()<br/>→ order.status = RESERVED/REJECTED"]
    end

    subgraph OrderDB["Order PostgreSQL<br/><small>orderdb</small>"]
        O["orders<br/>tenant_id, order_id, sku, quantity, status<br/>processed_events (idempotency)"]
    end

    subgraph InvSvc["Inventory Service<br/><small>(FastAPI / SQLAlchemy)</small>"]
        CO["consume_order_events()<br/>(background thread)"]
        RS["reserve_stock_for_event()<br/>SELECT ... FOR UPDATE"]
        PU["publish_stock_reserved / _rejected()"]
    end

    subgraph InvDB["Inventory PostgreSQL<br/><small>inventorydb</small>"]
        I["inventory_items<br/>tenant_id + sku UNIQUE<br/>available_stock<br/>processed_events (idempotency)"]
    end

    subgraph RMQ["RabbitMQ topology<br/><small>(shared broker)</small>"]
        EO["orders.events<br/>(topic, durable)"]
        EI["inventory.events<br/>(topic, durable)"]
        EL["dlx.events<br/>(topic, dead-letter)"]
        QOC["inventory-service.order-created<br/>(x-dead-letter-exchange: dlx.events)"]
        DLQOC["inventory-service.order-created.dlq"]
        QS_R["order-service.stock-reserved<br/>(x-dead-letter-exchange: dlx.events)"]
        QS_RJ["order-service.stock-rejected<br/>(x-dead-letter-exchange: dlx.events)"]
        DLQSR["order-service.stock-reserved.dlq"]
        DLQSRJ["order-service.stock-rejected.dlq"]
    end

    %% Data-store persistence (synchronous, within a DB transaction)
    OA -->|INSERT order| O
    O -->|order row| PUB
    %% Publish OrderCreated (async, durable, persistent)
    PUB -->|OrderCreated RK=order.created| EO
    EO -->|routing key order.created| QOC
    %% Inventory consumes, locks, decides, publishes result
    QOC -->|deliver OrderCreated| CO
    CO -->|FOR UPDATE + check + subtract| I
    I -->|row locked: available_stock never negative| RS
    RS -->|INSERT event_id + outcome (same txn)| I
    RS -->|StockReserved / StockRejected| PU
    PU -->|RK=stock.reserved / stock.rejected| EI
    %% Order service consumes the result
    EI -->|routing key| QS_R
    EI -->|routing key| QS_RJ
    QS_R -->|deliver StockReserved| CS
    QS_RJ -->|deliver StockRejected| CS
    CS -->|dedup event_id + UPDATE status (same txn)| ASR
    ASR -->|UPDATE order.status| O
    %% Dead-letter paths (max retries or malformed -> nack requeue=False)
    QOC -- max retries / malformed -->|basic_nack requeue=False| DLQOC
    QS_R -- max retries / malformed -->|basic_nack requeue=False| DLQSR
    QS_RJ -- max retries / malformed -->|basic_nack requeue=False| DLQSRJ
    DLQOC -->|bound by RK order.created| EL
    DLQSR -->|bound by RK stock.reserved| EL
    DLQSRJ -->|bound by RK stock.rejected| EL
    %% HTTP flow
    C -->|"1. POST /orders (X-Tenant-ID)"| OA
    classDef store fill:#e8f0fe,stroke:#1a73e8,stroke-width:1px,color:#0b2
    class O,I store
    class EO,EI,EL fill:#fef3e2,stroke:#ef7215,stroke-width:1px
## RabbitMQ topology

| Exchange | Type | Purpose |
| --- | --- | --- |
| `orders.events` | topic, durable | order service publishes `OrderCreated` (RK `order.created`) |
| `inventory.events` | topic, durable | inventory service publishes `StockReserved`/`StockRejected` (RK `stock.reserved` / `stock.rejected`) |
| `dlx.events` | topic, durable | dead-letter exchange; re-routes failed messages to per-queue DLQs |

| Queue | Consumer | Binds to | Dead-letters to |
| --- | --- | --- | --- |
| `inventory-service.order-created` | inventory service | `orders.events` (RK `order.created`) | `dlx.events` (RK `order.created`) |
| `inventory-service.order-created.dlq` | (none — inspection only) | `dlx.events` (RK `order.created`) | — |
| `order-service.stock-reserved` | order service | `inventory.events` (RK `stock.reserved`) | `dlx.events` (RK `stock.reserved`) |
| `order-service.stock-reserved.dlq` | (none — inspection only) | `dlx.events` (RK `stock.reserved`) | — |
| `order-service.stock-rejected` | order service | `inventory.events` (RK `stock.rejected`) | `dlx.events` (RK `stock.rejected`) |
| `order-service.stock-rejected.dlq` | (none — inspection only) | `dlx.events` (RK `stock.rejected`) | — |

* Messages are **persistent** (`delivery_mode=2`) and queues/exchanges are **durable**,
  so a broker restart does not lose in-flight orders.
* `basic_qos(prefetch_count=10)` is set on both consumers, so a fast publisher cannot
  overwhelm a backlogged consumer (each consumer drains only 10 unacked messages).
* Malformed/un-parseable messages are **not** retried — they are `basic_nack(requeue=False)`
  straight to the DLQ, because retrying a payload that can never validate wastes all retries.

## Event flow (the full path)

1. **HTTP request** — `POST /orders`, tenant from `X-Tenant-ID`, body `{sku, quantity}`.
2. **Order persisted** — the order row is written with `status = PENDING` and committed to
   `orderdb`. *This happens before any publishing, so a broker failure never loses the order.*
3. **`OrderCreated` published** — to `orders.events` (RK `order.created`), durable + persistent,
   carrying `event_id`, `tenant_id`, `event_type`, `timestamp`, and
   `payload {order_id, sku, quantity}`.
4. **Inventory consumes** — `OrderCreated` → `reserve_stock_for_event`.
5. **Pessimistic lock** — `SELECT ... FOR UPDATE` on the `inventory_items` row inside a single
   transaction, so concurrent reservations on the same SKU are serialised.
6. **Atomic decision** — if `available_stock >= quantity`, subtract and commit; otherwise raise
   `InsufficientStock`. Stock can never go negative. The `event_id` is inserted into
   `processed_events` in the **same** transaction, guaranteeing exactly-once processing even on
   retries (a duplicate `event_id` hits its PRIMARY KEY → `AlreadyProcessed` → skip).
7. **Response event** — `StockReserved` (RK `stock.reserved`) or `StockRejected`
   (RK `stock.rejected`), published to `inventory.events`.
8. **Order consumes** — `apply_stock_result` dedups the `event_id` (same idempotency table) and
   flips `order.status` to `RESERVED` / `REJECTED`, committed in the same transaction.
9. **Observer polls** — `GET /orders/{id}` shows the terminal status once step 8 completes.

## Retry & dead-letter behaviour

* On a *processing* exception (not a validation error), the consumer `basic_nack(requeue=False)`s
  the message (so it does **not** blindly redeliver to the same consumer) and re-publishes it to
  the originating exchange with `headers["x-retry-count"]` incremented. RabbitMQ's
  `x-dead-letter-exchange` on the queue then routes it through the retry path.
* After `MAX_RETRIES` (3) failed processing attempts, the message is `basic_nack(requeue=False)`
  and lands in the matching `*.dlq` for inspection — the consumer never crashes.
* The order service runs its consumer in a daemon thread; if it dies it is logged but does not take
  the HTTP API down (`main.py`). The same guard applies on the inventory side.

## Multi-tenancy

* **Tenant isolation is enforced at the query level** in every service: every read/write filters on
  `tenant_id == <X-Tenant-ID>` (orders: `tenant_id` index; inventory: `(tenant_id, sku)` UNIQUE
  constraint). Two tenants may use the *same* SKU string — they simply get separate rows.
* There is **no cross-tenant SQL path**; an order written by tenant B returns 404 to tenant A
  (`test_tenant_isolation`, `test_cross_tenant_event_rejected`).
* Each tenant's data lives in its own domain database (`orderdb` vs `inventorydb`); a compromise
  of one service cannot read the other's datastore.

## Consistency model

| Boundary | Guarantee | Mechanism |
| --- | --- | --- |
| Within a service, per tenant | Strong / ACID | PostgreSQL transactions |
| Stock reservation (inventory) | Atomic, never negative | `SELECT ... FOR UPDATE` + single-txn check/subtract |
| Duplicate delivery (RDBMS side) | At-most-once application | `processed_events.event_id` PRIMARY KEY in the same txn |
| Duplicate delivery (broker side) | At-least-once transport; app idempotent | idempotency key + `basic_ack` only after commit |
| Order ↔ inventory | **Eventual** | async events; order is PENDING until the result event is consumed |
| Order creation durability vs. broker | Order never lost; event can be lost | persist-then-publish, availability over completeness |

## Data flow

```
Order DB:     orders(id, tenant_id, order_id, sku, quantity, status, created_at, updated_at)
             processed_events(event_id PK, outcome, processed_at)

Inventory DB: inventory_items(id, tenant_id, sku, product_name, available_stock, created_at, updated_at)
              processed_events(event_id PK, outcome, processed_at)
```

Both services expose `GET /health` (liveness + dependency status) and the management-friendly
`/docs` OpenAPI UI. Observability uses structured **JSON** log lines (one per line) with a stable
`event_id` correlation field — see `order_app/logging_config.py` / `inventory_app/logging_config.py`.
```