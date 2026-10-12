import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import InventoryItem, ProcessedEvent

logger = logging.getLogger(__name__)


class InventoryError(Exception):
    """Base class for domain errors raised while reserving stock."""


class ItemNotFound(InventoryError):
    """The tenant does not have an inventory record for this SKU."""


class InsufficientStock(InventoryError):
    """The requested quantity exceeds what is currently available."""

    def __init__(self, requested: int, available: int) -> None:
        self.requested = requested
        self.available = available
        super().__init__(
            f"Insufficient stock: requested {requested}, available {available}"
        )


class AlreadyProcessed(InventoryError):
    """The event was already processed; the caller must not re-apply it."""


def _check_duplicate(db: Session, event_id: str) -> None:
    """Record the event id, or raise AlreadyProcessed when it was seen before."""
    db.add(ProcessedEvent(event_id=event_id))
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        raise AlreadyProcessed(f"event {event_id} was already processed")


def reserve_stock(db: Session, tenant_id: str, sku: str, quantity: int) -> InventoryItem:
    """Atomically reserve ``quantity`` units of ``sku`` for ``tenant_id``.

    The row is read with ``SELECT ... FOR UPDATE`` inside the current
    transaction, which takes a pessimistic row-level lock. Concurrent callers
    are therefore serialised on this row, the stock check and the subtraction
    happen under the same lock, and ``available_stock`` can never go negative.

    Raises:
        ItemNotFound: no inventory row exists for this tenant + SKU.
        InsufficientStock: the row exists but does not have enough stock.
    """
    item = (
        db.query(InventoryItem)
        .filter(InventoryItem.tenant_id == tenant_id, InventoryItem.sku == sku)
        .with_for_update()
        .one_or_none()
    )

    if item is None:
        raise ItemNotFound(f"SKU {sku} not found for tenant {tenant_id}")

    if item.available_stock < quantity:
        raise InsufficientStock(requested=quantity, available=item.available_stock)

    item.available_stock -= quantity
    db.commit()
    db.refresh(item)
    return item


def reserve_stock_for_event(
    db: Session, event_id: str, tenant_id: str, sku: str, quantity: int
) -> str:
    """Apply an ``OrderCreated`` reservation exactly once.

    The event id is stored in the same database transaction as the stock
    change, so a crashed/retried delivery can never double-apply the
    reservation. Returns ``"reserved"`` or ``"rejected"``.
    """
    try:
        _check_duplicate(db, event_id)
    except AlreadyProcessed:
        logger.info(
            "Skipping already-processed event",
            extra={"fields": {"event_id": event_id, "tenant_id": tenant_id, "sku": sku}},
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

    outcome = "reserved"
    try:
        reserve_stock(db, tenant_id, sku, quantity)
    except (ItemNotFound, InsufficientStock):
        outcome = "rejected"

    db.query(ProcessedEvent).filter_by(event_id=event_id).update(
        {"outcome": outcome}
    )
    db.commit()
    return outcome

