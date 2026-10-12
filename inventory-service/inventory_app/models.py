from datetime import datetime

from sqlalchemy import DateTime, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class InventoryItem(Base):
    """Stock record for a single SKU, scoped to a single tenant."""

    __tablename__ = "inventory_items"
    __table_args__ = (
        # One tenant may not hold two rows with the same SKU. Two different
        # tenants are free to use the same SKU because tenant_id is part of the
        # key (e.g. tenant-a/laptop-001 and tenant-b/laptop-001 coexist).
        UniqueConstraint("tenant_id", "sku", name="uq_inventory_tenant_sku"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    sku: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    available_stock: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ProcessedEvent(Base):
    """Record of an event id that was already applied (idempotency).

    A redelivered ``OrderCreated`` must never reserve the stock twice, so the
    event id is stored in the same database as the stock. ``outcome`` remembers
    whether this delivery was ``"reserved"`` or ``"rejected"`` so a duplicate
    can republish the same response without touching the stock again.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    outcome: Mapped[str | None] = mapped_column(String(16), nullable=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
