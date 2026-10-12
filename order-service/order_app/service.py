import logging
from datetime import datetime, timezone

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import Order, OrderStatus, ProcessedEvent

logger = logging.getLogger(__name__)


class OrderError(Exception):
    """Base class for domain errors raised while processing order events."""

    pass


class OrderNotFound(OrderError):
    """The tenant does not have an order with this order_id."""

    pass


class InvalidOrderState(OrderError):
    """The order is not in a state that allows this transition."""

    pass


class AlreadyProcessed(OrderError):
    """The event was already processed; the caller must not re-apply it."""

    pass


def _check_duplicate(db: Session, event_id: str) -> None:
    """Record the event id, or raise AlreadyProcessed when it was seen before."""
    db.add(ProcessedEvent(event_id=event_id))
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        raise AlreadyProcessed(f"event {event_id} was already processed")


def apply_stock_result(
    db: Session, event_id: str, tenant_id: str, order_id: str, outcome: str
) -> str:
    """Apply a ``StockReserved`` or ``StockRejected`` event exactly once.

    The event id is stored in the same database transaction as the order
    status change, so a crashed/retried delivery can never double-apply the
    update. Returns ``"reserved"`` or ``"rejected"``.
    """
    try:
        _check_duplicate(db, event_id)
    except AlreadyProcessed:
        logger.info(
            "Skipping already-processed event",
            extra={"fields": {"event_id": event_id, "order_id": order_id, "tenant_id": tenant_id}},
        )
        db.rollback()
        stored = (
            db.query(ProcessedEvent).filter_by(event_id=event_id).one().outcome
        )
        if stored is None:
            # A previous attempt crashed between recording the id and storing
            # the outcome; without a stored answer the safest choice is to say
            # the reservation failed so the order service can retry the event.
            return "rejected"
        return stored

    if outcome not in ("reserved", "rejected"):
        raise ValueError(f"unexpected outcome: {outcome}")

    order = (
        db.query(Order)
        .filter(Order.tenant_id == tenant_id, Order.order_id == order_id)
        .one_or_none()
    )

    if order is None:
        raise OrderNotFound(
            f"Order {order_id} not found for tenant {tenant_id}"
        )

    # Idempotency: if the order already has the expected status, we can
    # consider this a success without re-applying.
    expected_status = (
        OrderStatus.RESERVED.value if outcome == "reserved" else OrderStatus.REJECTED.value
    )
    if order.status == expected_status:
        logger.info(
            "Order already in expected state; skipping duplicate event",
            extra={"fields": {
                "event_id": event_id,
                "order_id": order_id,
                "tenant_id": tenant_id,
                "event_type": "StockResult",
            }},
        )
        db.query(ProcessedEvent).filter_by(event_id=event_id).update(
            {"outcome": outcome}
        )
        db.commit()
        return outcome

    if order.status != OrderStatus.PENDING.value:
        raise InvalidOrderState(
            f"Order {order_id} is in {order.status} state, cannot transition to {expected_status}"
        )

    order.status = expected_status
    order.updated_at = datetime.now(timezone.utc)

    db.query(ProcessedEvent).filter_by(event_id=event_id).update(
        {"outcome": outcome}
    )
    db.commit()
    return outcome