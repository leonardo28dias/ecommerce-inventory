from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class OrderCreate(BaseModel):
    """Payload for creating an order.

    The SKU is the same product code the inventory service tracks; it is not
    validated against inventory here — checking stock is the inventory
    service's job (a later phase), not the order service's.
    """

    sku: str = Field(..., min_length=1, max_length=64)
    quantity: int = Field(..., gt=0)


class OrderResponse(BaseModel):
    """Serialized order returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    tenant_id: str
    order_id: str
    sku: str
    quantity: int
    status: str
    created_at: datetime
    updated_at: datetime


# --- Event structures (Phase 6) ---


class EventBase(BaseModel):
    """Base fields shared by all domain events."""

    event_id: str = Field(..., description="Globally unique event identifier")
    event_type: str = Field(..., description="Event type, e.g. OrderCreated")
    tenant_id: str = Field(..., description="Tenant that owns the event")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="UTC timestamp"
    )


class OrderCreatedPayload(BaseModel):
    """Payload for OrderCreated events."""

    order_id: str
    sku: str
    quantity: int = Field(..., gt=0)


class OrderCreated(EventBase):
    """Event published when a new order is created."""

    event_type: str = "OrderCreated"
    payload: OrderCreatedPayload


class StockReservedPayload(BaseModel):
    """Payload for StockReserved events."""

    order_id: str
    sku: str
    quantity: int = Field(..., gt=0)


class StockReserved(EventBase):
    """Event published when stock is successfully reserved."""

    event_type: str = "StockReserved"
    payload: StockReservedPayload


class StockRejectedPayload(BaseModel):
    """Payload for StockRejected events."""

    order_id: str
    sku: str
    quantity: int = Field(..., gt=0)
    reason: str


class StockRejected(EventBase):
    """Event published when stock reservation fails."""

    event_type: str = "StockRejected"
    payload: StockRejectedPayload
