"""Tests for idempotent event processing in the inventory service.

These verify that duplicate ``OrderCreated`` events cannot reserve stock twice,
and that tenant isolation holds even when events are retried.
"""
import uuid

from inventory_app import service
from inventory_app.database import SessionLocal
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


def _stock(tenant_id: str, sku: str) -> int:
    session = SessionLocal()
    item = (
        session.query(InventoryItem)
        .filter_by(tenant_id=tenant_id, sku=sku)
        .one()
    )
    result = item.available_stock
    session.close()
    return result


def test_duplicate_order_created_does_not_double_reserve():
    """Processing the same OrderCreated event twice reserves stock only once."""
    _seed("tenant-a", "laptop-001", 10)
    event_id = str(uuid.uuid4())

    # First delivery: should reserve 3 units
    session1 = SessionLocal()
    outcome1 = service.reserve_stock_for_event(
        session1, event_id, "tenant-a", "laptop-001", 3
    )
    session1.close()

    # Second delivery (duplicate): should be skipped, stock unchanged
    session2 = SessionLocal()
    outcome2 = service.reserve_stock_for_event(
        session2, event_id, "tenant-a", "laptop-001", 3
    )
    session2.close()

    # Both calls return the same outcome
    assert outcome1 == "reserved"
    assert outcome2 == "reserved"

    # Stock was only reduced once
    assert _stock("tenant-a", "laptop-001") == 7


def test_duplicate_order_created_rejected_then_reserved():
    """If the first delivery was rejected, a duplicate returns 'rejected'."""
    _seed("tenant-a", "laptop-001", 2)  # only 2 available
    event_id = str(uuid.uuid4())

    # First delivery: insufficient stock -> rejected
    session1 = SessionLocal()
    outcome1 = service.reserve_stock_for_event(
        session1, event_id, "tenant-a", "laptop-001", 5
    )
    session1.close()

    # Second delivery (duplicate): should also return 'rejected'
    session2 = SessionLocal()
    outcome2 = service.reserve_stock_for_event(
        session2, event_id, "tenant-a", "laptop-001", 5
    )
    session2.close()

    assert outcome1 == "rejected"
    assert outcome2 == "rejected"

    # Stock is unchanged
    assert _stock("tenant-a", "laptop-001") == 2


def test_different_events_same_sku_reserve_independently():
    """Two different event IDs for the same SKU should both be processed."""
    _seed("tenant-a", "laptop-001", 10)
    event_id_1 = str(uuid.uuid4())
    event_id_2 = str(uuid.uuid4())

    session1 = SessionLocal()
    service.reserve_stock_for_event(session1, event_id_1, "tenant-a", "laptop-001", 3)
    session1.close()

    session2 = SessionLocal()
    service.reserve_stock_for_event(session2, event_id_2, "tenant-a", "laptop-001", 4)
    session2.close()

    assert _stock("tenant-a", "laptop-001") == 3


def test_tenant_isolation_in_event_processing():
    """Events for tenant-a never affect tenant-b's stock."""
    _seed("tenant-a", "laptop-001", 10)
    _seed("tenant-b", "laptop-001", 20)

    event_id_a = str(uuid.uuid4())
    event_id_b = str(uuid.uuid4())

    # Process event for tenant-a (reserve 4)
    session_a = SessionLocal()
    service.reserve_stock_for_event(session_a, event_id_a, "tenant-a", "laptop-001", 4)
    session_a.close()

    # Process event for tenant-b (reserve 5)
    session_b = SessionLocal()
    service.reserve_stock_for_event(session_b, event_id_b, "tenant-b", "laptop-001", 5)
    session_b.close()

    assert _stock("tenant-a", "laptop-001") == 6
    assert _stock("tenant-b", "laptop-001") == 15