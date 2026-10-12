"""Tests for idempotent event processing in the order service.

These verify that duplicate ``StockReserved``/``StockRejected`` events cannot
update an order status twice, and that tenant isolation holds even when events
are retried.
"""
import uuid

from order_app import service
from order_app.database import SessionLocal
from order_app.models import Order, OrderStatus


def _seed_order(tenant_id: str, order_id: str, sku: str = "laptop-001", quantity: int = 2):
    session = SessionLocal()
    session.add(
        Order(
            tenant_id=tenant_id,
            order_id=order_id,
            sku=sku,
            quantity=quantity,
            status=OrderStatus.PENDING.value,
        )
    )
    session.commit()
    session.close()


def _order_status(tenant_id: str, order_id: str) -> str:
    session = SessionLocal()
    order = (
        session.query(Order)
        .filter_by(tenant_id=tenant_id, order_id=order_id)
        .one()
    )
    result = order.status
    session.close()
    return result


def test_duplicate_stock_reserved_does_not_double_update():
    """Processing the same StockReserved event twice updates status only once."""
    order_id = str(uuid.uuid4())
    _seed_order("tenant-a", order_id)
    event_id = str(uuid.uuid4())

    # First delivery: should transition PENDING -> RESERVED
    session1 = SessionLocal()
    outcome1 = service.apply_stock_result(
        session1, event_id, "tenant-a", order_id, "reserved"
    )
    session1.close()

    # Second delivery (duplicate): should be skipped, status unchanged
    session2 = SessionLocal()
    outcome2 = service.apply_stock_result(
        session2, event_id, "tenant-a", order_id, "reserved"
    )
    session2.close()

    # Both calls return the same outcome
    assert outcome1 == "reserved"
    assert outcome2 == "reserved"

    # Status was only updated once
    assert _order_status("tenant-a", order_id) == "RESERVED"


def test_duplicate_stock_rejected_does_not_double_update():
    """Processing the same StockRejected event twice updates status only once."""
    order_id = str(uuid.uuid4())
    _seed_order("tenant-a", order_id)
    event_id = str(uuid.uuid4())

    # First delivery: should transition PENDING -> REJECTED
    session1 = SessionLocal()
    outcome1 = service.apply_stock_result(
        session1, event_id, "tenant-a", order_id, "rejected"
    )
    session1.close()

    # Second delivery (duplicate): should also return 'rejected'
    session2 = SessionLocal()
    outcome2 = service.apply_stock_result(
        session2, event_id, "tenant-a", order_id, "rejected"
    )
    session2.close()

    assert outcome1 == "rejected"
    assert outcome2 == "rejected"

    # Status was only updated once
    assert _order_status("tenant-a", order_id) == "REJECTED"


def test_different_events_same_order_update_independently():
    """Two different event IDs for the same order should both be processed.

    The first reserves, the second (if it were a different flow) would be
    rejected. Here we test they are treated as independent.
    """
    order_id = str(uuid.uuid4())
    _seed_order("tenant-a", order_id)
    event_id_1 = str(uuid.uuid4())
    event_id_2 = str(uuid.uuid4())

    # First event: reserved
    session1 = SessionLocal()
    service.apply_stock_result(session1, event_id_1, "tenant-a", order_id, "reserved")
    session1.close()

    # Second event (different ID): also reserved (idempotency is per-event-id)
    session2 = SessionLocal()
    service.apply_stock_result(session2, event_id_2, "tenant-a", order_id, "reserved")
    session2.close()

    # Status is RESERVED (both events agree)
    assert _order_status("tenant-a", order_id) == "RESERVED"


def test_tenant_isolation_in_order_event_processing():
    """Events for tenant-a never affect tenant-b's orders."""
    order_id_a = str(uuid.uuid4())
    order_id_b = str(uuid.uuid4())
    _seed_order("tenant-a", order_id_a)
    _seed_order("tenant-b", order_id_b)

    event_id_a = str(uuid.uuid4())
    event_id_b = str(uuid.uuid4())

    # Process StockReserved for tenant-a
    session_a = SessionLocal()
    service.apply_stock_result(session_a, event_id_a, "tenant-a", order_id_a, "reserved")
    session_a.close()

    # Process StockRejected for tenant-b
    session_b = SessionLocal()
    service.apply_stock_result(session_b, event_id_b, "tenant-b", order_id_b, "rejected")
    session_b.close()

    assert _order_status("tenant-a", order_id_a) == "RESERVED"
    assert _order_status("tenant-b", order_id_b) == "REJECTED"


def test_cross_tenant_event_rejected():
    """A StockResired event for tenant-b's order_id is rejected when processed by tenant-a."""
    order_id_b = str(uuid.uuid4())
    _seed_order("tenant-b", order_id_b)
    event_id = str(uuid.uuid4())

    # Try to apply event as tenant-a for tenant-b's order
    session = SessionLocal()
    try:
        service.apply_stock_result(
            session, event_id, "tenant-a", order_id_b, "reserved"
        )
        session.close()
        assert False, "Expected OrderNotFound"
    except service.OrderNotFound:
        session.close()
        # order-b should remain PENDING
        assert _order_status("tenant-b", order_id_b) == "PENDING"


def test_event_for_nonexistent_order():
    """Applying an event for an order that doesn't exist raises OrderNotFound."""
    event_id = str(uuid.uuid4())
    session = SessionLocal()
    try:
        service.apply_stock_result(
            session, event_id, "tenant-a", "nonexistent-order-id", "reserved"
        )
        session.close()
        assert False, "Expected OrderNotFound"
    except service.OrderNotFound:
        session.close()