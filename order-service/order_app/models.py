import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class OrderStatus(str, Enum):
    """Lifecycle states for an order.

    Phase 3 only ever produces PENDING. RESERVED and REJECTED belong to the
    asynchronous inventory flow (a later phase) and already exist here so the
    schema and the API docs reflect the full lifecycle from the start.
    """

    PENDING = "PENDING"
    RESERVED = "RESERVED"
    REJECTED = "REJECTED"


class Order(Base):
    """A customer order, scoped to a single tenant.

    Boundary: the order service owns order records only. It never reads or
    writes the inventory database — stock is checked and reserved by the
    inventory service, not here.
    """

    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    order_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        unique=True,
        default=lambda: str(uuid.uuid4()),
    )
    sku: Mapped[str] = mapped_column(String(64), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default=OrderStatus.PENDING.value,
        server_default="PENDING",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ProcessedEvent(Base):
    """Record of an event id that was already applied (idempotency).

    A redelivered ``StockReserved`` or ``StockRejected`` must never update the
    order status twice, so the event id is stored in the same database as the
    order. ``outcome`` remembers whether this delivery was ``"reserved"`` or
    ``"rejected"`` so a duplicate can re-emit the same result without touching
    the order again.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    outcome: Mapped[str | None] = mapped_column(String(16), nullable=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
