# AI Usage

This file honestly records where AI assistance was used while building BE-3, what
was difficult, and which decisions still required human reasoning. It is written
as the project progresses; entries below cover **Phase 1 (repository, Docker and
service skeleton)**, **Phase 2 (inventory service and pessimistic locking)** and
**Phase 3 (order service)**.

## Where AI assistance was useful

- **Scaffolding the monorepo**: generating the initial Dockerfile,
  `docker-compose.yml`, `.env.example`, `.gitignore`, and the FastAPI service
  skeleton (config, database, health modules) from a short specification.
- **Reading error tracebacks**: when a dependency or import failed, AI helped
  interpret the traceback and pinpoint the fix faster than manual lookup.
- **Drafting the README structure** and the architecture diagram.

## Difficult moments and bugs actually encountered

1. **Docker is not usable in this development environment.** The `dockerd`
   daemon runs, but its control socket (`/var/run/docker.sock`) is owned by
   `root:root` instead of `root:docker`, and there is no `docker` group. The
   current user has no passwordless `sudo`/`su` access, so the socket
   permissions cannot be corrected, and rootless Docker is unavailable because
   `newuidmap`/`newgidmap` and the rootless scripts are missing. AI helped
   diagnose this (checking socket ownership, groups, sudoers, and rootless
   prerequisites), but **it could not be solved without administrator rights**.
   As a result `docker compose up --build` could not be executed here; the
   compose file was validated with `docker compose config`, and the services,
   health endpoints and tests were verified by running them directly on the host.

2. **`BaseSettings` import error.** `from pydantic import BaseSettings` raised
   `PydanticImportError` because, in Pydantic v2, `BaseSettings` moved to the
   separate `pydantic-settings` package. AI identified the migration and the
   correct import (`from pydantic_settings import BaseSettings`); the package was
   added to `requirements.txt`.

3. **Missing PostgreSQL driver.** `create_engine(...)` failed with
   `ModuleNotFoundError: No module named 'psycopg'`. AI read the SQLAlchemy
   traceback and noted that SQLAlchemy 2.x resolves the `postgresql://` URL to
   the **psycopg 3** dialect, so `psycopg[binary]` (not `psycopg2-binary`) was
   installed.

4. **`/health` returned HTTP 500.** The first live test showed a 500 because
   `check_db_health()` raised `OperationalError` when the database was
   unreachable, while `check_rabbitmq_health()` returned `False` instead. Found
   by actually running the service, then fixed so both checks return a boolean
   and never raise.

5. **Combined `pytest` run failed.** Two problems appeared only when running the
   whole suite at once: both test files were named `test_health.py` (pytest
   "import file mismatch"), and both services used a package literally named
   `app`, so the second service imported the *first* service's package. AI
   explained the collision and suggested unique package/test names. The packages
   were renamed to `order_app` and `inventory_app`, and the tests to
   `test_order_health.py` / `test_inventory_health.py`.

6. **Docker Compose credential mismatch.** On review, the app services set
   literal passwords in `environment:` while the database services read
   `${ORDER_DB_PASSWORD}` from `.env`. Customising `.env` would have desynced
   them. Fixed by reading the same variables everywhere, dropping the redundant
   `env_file`, and separating the host-port variable (`ORDER_DB_HOST_PORT`) from
   the container connection port.

## Decisions that still required human reasoning

- **Liveness vs. readiness for `/health`.** The requirement asks for "a simple
  JSON response indicating that the service is alive", so the endpoint returns
  HTTP 200 whenever the service can respond, and reports dependency health in
  the body (`status`, `services`). A stricter readiness semantics (503 when a
  dependency is down) was considered and documented as a possible future change.
- **Unique package names in a monorepo** — trading a tiny bit of boilerplate for
  the ability to run a single `pytest` across both services.
- **Keeping each service's database truly separate** and only sharing RabbitMQ.
- **Not adding abstraction layers** (no generic repositories or event frameworks)
  to keep the code interview-friendly, per the project standard.

## What was changed after testing AI-generated suggestions

- The health endpoint was first written with readiness semantics (503 when
  degraded); after running it live and re-reading the requirement, it was changed
  to liveness semantics (200 + status in body).
- The initial `psycopg2-binary` choice was replaced with `psycopg[binary]` after
  the runtime traceback proved SQLAlchemy 2.x expects psycopg 3.
- The first `docker-compose.yml` used `env_file` plus literal `environment:`
  overrides; this was refactored after spotting the credential desync risk.

---

# Phase 2 — Inventory service and pessimistic locking

## Where AI assistance was useful

- **Scaffolding the inventory layer**: the SQLAlchemy `InventoryItem` model (with
  the `UniqueConstraint(tenant_id, sku)`), the Pydantic schemas, the
  `X-Tenant-ID` header dependency and the three endpoints were generated from the
  specification and then adjusted by review.
- **SQLAlchemy pessimistic locking**: AI confirmed the ORM equivalent of
  `SELECT ... FOR UPDATE` is `.with_for_update()` and that the read-check-write
  sequence must stay inside one transaction for the lock to be meaningful.
- **Test-fixture design**: figuring out how to point the app at a dedicated
  `inventory_test` database by setting `INVENTORY_DB_*` environment variables
  *before* importing the app (settings/engine are constructed at import time),
  plus creating that database from a maintenance connection.

## Difficult moments and bugs actually encountered

1. **Getting a real PostgreSQL running without Docker.** Docker is blocked (see
   Phase 1) and the system cluster on port 5434 needs an unavailable password, so
   a private instance was created with `initdb`. The first start failed with
   `could not create lock file "/var/run/postgresql/...": permission denied`
   because the compiled-in unix-socket directory is not writable by a normal user.
   AI identified the fix — start with `-c unix_socket_directories=/tmp/...`. The
   instance now runs on port 5432 with trust auth.

2. **`_IncludedRouter` looked like missing routes.** After wiring the routers,
   inspecting `app.routes` showed only `/docs` and `/openapi.json`, which looked
   like the `/inventory` routes had not been registered. AI explained that FastAPI
   0.142 wraps included routers in an `_IncludedRouter` object that has no
   `.path`. The routes were confirmed present via `app.openapi()["paths"]`
   (`POST /inventory`, `GET /inventory/{sku}`, `POST /inventory/{sku}/reserve`).

3. **A misleading "tenant_id" check in a demonstration script.** A quick script
   printed `[NO tenant_id]` for `UPDATE ... WHERE id = ...`, which looked like a
   tenant-isolation bug. On reasoning it through, it is not: that row was already
   located *and locked* by the tenant-scoped
   `SELECT ... WHERE tenant_id = ? AND sku = ? FOR UPDATE` in the same
   transaction, so the primary-key `UPDATE` cannot touch another tenant's row. The
   demonstration was rewritten to separate tenant-scoped *lookups* (all filter by
   `tenant_id`) from the row mutation that follows the lock.

## Decisions that still required human reasoning

- **Where to put `reserve_stock()`.** Extracting it into `service.py` rather than
  inlining it in the route was deliberate: it lets the concurrency test call the
  *real* reservation logic from several threads (each with its own DB session)
  without going through HTTP.
- **A separate `inventory_test` database.** Tests must never touch the service's
  `inventorydb`, so the fixture bootstraps its own database.
- **Interpreting "all tenant-scoped queries include tenant_id".** Concluded it
  refers to the queries that *locate* a row for a tenant; mutating by primary key
  after that lock is safe, and this is documented explicitly in the README.
- **Status codes**: `201` on create, `409` for both duplicate-SKU and
  insufficient-stock (conflicts with current state), `422` for schema-level
  validation (pydantic/FastAPI default), `404` for tenant-scoped misses.

## What was changed after testing AI-generated suggestions

- The first concurrency-test draft fired threads without a start barrier; a
  `threading.Barrier` was added so all threads contend on the row simultaneously,
  making the overbooking test meaningful.
- The demonstration script's naive "does the SQL contain `tenant_id`?" check was
  replaced with the lookup-vs-mutation categorisation above, because the original
  output was misleading (it flagged the safe primary-key `UPDATE` as a violation).

---

# Phase 3 — Order service

## Where AI assistance was useful

- **Scaffolding from the Phase 2 pattern**: the order model, schemas, routes and
  fixtures were drafted by following the proven inventory-service structure
  (dedicated test database bootstrapped from `*_TEST_DB_*` variables *before* the
  app import). Where inventory used `INVENTORY_TEST_DB_*` / `inventory_test`, the
  order side now uses `ORDER_TEST_DB_*` / `order_test`.
- **Designing the persistence-boundary test**: asserting, via SQLAlchemy
  `inspect`, that the order database contains `orders` and no inventory table.

## Difficult moments and bugs actually encountered

1. **A relative-path slip in the demo script.** While chaining curls in one
   shell command, the `order_id` extraction ran as `../.venv/bin/python` from a
   subshell that was already inside `order-service/`, so it resolved to
   `order-service/../.venv/...` — wrong. Both `GET` calls then hit an empty ID
   (307). Not an app bug: re-running the two `GET`s with the absolute venv path
   returned the expected `200` + `404`.

2. **No real bug in this phase.** All 9 order API tests passed on the first run;
   the deliberate simplification (separate `service.py` looked unnecessary here —
   reservation has no row-lock interplay to share) held up.

## Decisions that still required human reasoning

- **Keeping the separation honest.** The order schema declares `RESERVED` and
  `REJECTED` as valid states so the docs and API reflect the real lifecycle, but
  the route can only ever persist `PENDING` — reservation still belongs to the
  inventory service and there is deliberately *no* code path in `order_app` that
  touches stock.
- **Testing the boundary at the source level, not just by behaviour.** Beyond
  "creating an order does not change stock", a dedicated test inspects the order
  database and fails if any inventory table ever appears there.
- **SKU is stored verbatim.** Phase 3 intentionally does no product-code format
  validation; it only requires a non-empty string (≤ 64 chars), since stock
  checking moves to the inventory service in a later phase.

## What was changed after testing AI-generated suggestions

- Nothing functional: the first draft passed every test. The only edits after
  the green run were the README (order API section, service-separation rules)
  and this log.

---

## Phase 8 — hardening, tests, and observability

Phase 8 added no new features. It was a review pass over reliability, test
coverage, readability, and lightweight observability.

### Where AI assistance was useful

- **Auditing test coverage against a checklist.** AI enumerated every
  `def test_` in both services and the benchmark and mapped them one-to-one to
  the Phase 8 required cases (inventory, orders, events, messaging,
  concurrency). This confirmed coverage was already complete, so the honest
  outcome was *not* to add redundant tests.
- **Spotting half-wired code.** AI grepped for `configure_logging` call sites and
  found the existing `logging_config.py` was never invoked and that the log
  calls used printf `%s` formatting instead of the `extra={"fields": {...}}`
  the JSON formatter expected — i.e. the "logs carry event identifiers" goal was
  not actually met. This was the main substantive finding of the phase.
- **Finding dead code.** A grep showed the inventory service still contained an
  unused `publish_order_created` (the inventory service consumes `OrderCreated`,
  it never publishes it), plus a now-orphaned `OrderCreatedPayload` import.

### Difficult moments and bugs actually encountered

1. **Structured logging vs. stdlib `extra`.** Stdlib `logging` silently drops
   unknown kwargs, so `logger.info("...", event_id=...)` would not reach the
   formatter. The existing design already anticipated this by reading a single
   `record.fields` dict, but no call site used it. The fix was to convert the
   event-flow calls to `extra={"fields": {...}}`. Verified by emitting real log
   lines and parsing them back as JSON in a shell check and in the new tests.

2. **README tree-editing friction.** The box-drawing characters (`│ ├ └`) in the
   project-structure block did not match reliably through the text editor, and
   two `sed` insertions dropped their leading indentation. This was corrected
   with a small Python rewrite of the exact substrings and re-verified by
   printing the block. Purely a tooling annoyance, not a code bug.

3. **PostgreSQL was not on the default port.** As in earlier phases, the test
   databases live on a host cluster that was not listening on `127.0.0.1:5432`
   at the start of the phase; a local instance had to be (re)started before the
   suites could run. No application impact.

### Decisions that still required human reasoning

- **Not chasing coverage.** The Phase 8 brief explicitly warns against arbitrary
  100% coverage. After mapping the checklist to existing tests, the judgement
  call was to add only the *meaningful* new tests (structured-logging output)
  and leave the rest, rather than padding numbers.
- **Keeping observability lightweight.** OpenTelemetry would mean a collector,
  exporters, and span-context propagation across RabbitMQ. For a
  two-service portfolio system the decision was to keep standard-library JSON
  logging (with a greppable `event_id`) and document tracing as future work —
  consistent with the project's "do not over-engineer" standard.
- **Deleting rather than commenting out.** The dead `publish_order_created` was
  removed outright (with its import) because leaving it invites a reader to
  think the inventory service can originate orders, which contradicts the
  service-separation guarantee the README documents.

### What was changed after testing AI-generated suggestions

- The JSON-log formatter and `configure_logging()` were verified by emitting
  real records and asserting the parsed JSON top-level keys, not by trusting the
  code. The three logging tests per service encode that as a regression guard.
- After converting the log calls, the full suites were re-run
  (order 38, inventory 38, benchmark 8) to confirm the `extra=` refactor and the
  dead-code removal did not break the messaging/DLQ tests.
