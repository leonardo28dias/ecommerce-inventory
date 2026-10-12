"""
Tests that demonstrate pessimistic row locking really protects the stock.

These use the real database: one test asserts the reservation SELECT is emitted
with ``FOR UPDATE``, and another hammers a single row from several threads at
once to prove the stock never goes negative.
"""
import threading
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import event

from inventory_app import service
from inventory_app.database import SessionLocal, engine
from inventory_app.models import InventoryItem


def _seed(tenant_id: str, sku: str, stock: int) -> None:
    session = SessionLocal()
    session.add(
        InventoryItem(
            tenant_id=tenant_id,
            sku=sku,
            product_name="Laptop",
            available_stock=stock,
        )
    )
    session.commit()
    session.close()


def _final_stock(tenant_id: str, sku: str) -> int:
    session = SessionLocal()
    item = (
        session.query(InventoryItem)
        .filter_by(tenant_id=tenant_id, sku=sku)
        .one()
    )
    stock = item.available_stock
    session.close()
    return stock


def test_reservation_select_uses_for_update():
    """The reservation SELECT is emitted with FOR UPDATE (row-level lock)."""
    _seed("tenant-a", "laptop-001", 5)

    captured: list[str] = []

    def before_cursor(conn, cursor, statement, parameters, context, executemany):
        captured.append(statement)

    event.listen(engine, "before_cursor_execute", before_cursor)
    try:
        session = SessionLocal()
        service.reserve_stock(session, "tenant-a", "laptop-001", 1)
        session.close()
    finally:
        event.remove(engine, "before_cursor_execute", before_cursor)

    selects = [s for s in captured if s.lstrip().upper().startswith("SELECT")]
    assert selects, f"no SELECT captured: {captured}"
    assert any("FOR UPDATE" in s.upper() for s in selects), (
        "reservation did not use SELECT ... FOR UPDATE; "
        f"captured statements: {selects}"
    )


def test_concurrent_reservations_never_overbook():
    """10 threads racing for 5 units -> exactly 5 succeed, stock ends at 0."""
    initial_stock = 5
    threads = 10

    _seed("tenant-a", "laptop-001", initial_stock)

    barrier = threading.Barrier(threads)
    outcomes: list[str] = []
    outcomes_lock = threading.Lock()

    def worker() -> None:
        session = SessionLocal()
        try:
            barrier.wait(timeout=10)
            service.reserve_stock(session, "tenant-a", "laptop-001", 1)
            with outcomes_lock:
                outcomes.append("reserved")
        except service.InsufficientStock:
            with outcomes_lock:
                outcomes.append("rejected")
        except Exception as exc:  # pragma: no cover - surfaced via assertions
            with outcomes_lock:
                outcomes.append(f"error:{type(exc).__name__}: {exc}")
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker) for _ in range(threads)]
        for future in futures:
            future.result()

    reserved = outcomes.count("reserved")
    rejected = outcomes.count("rejected")
    errors = [o for o in outcomes if o.startswith("error")]
    final_stock = _final_stock("tenant-a", "laptop-001")

    assert not errors, f"workers raised unexpected errors: {errors}"
    assert len(outcomes) == threads, f"not every worker reported: {outcomes}"
    # Without the row lock the stale reads would let every thread through.
    assert reserved == initial_stock, (
        f"expected exactly {initial_stock} reservations, got {reserved}: {outcomes}"
    )
    assert rejected == threads - initial_stock, outcomes
    assert final_stock == 0, f"expected 0 remaining, got {final_stock}"
    assert final_stock >= 0, "available_stock must never go negative"
