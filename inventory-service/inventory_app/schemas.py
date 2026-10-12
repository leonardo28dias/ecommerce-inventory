from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


# --- Event structures ---


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


class InventoryCreate(BaseModel):
    """Payload for creating an inventory record."""

    sku: str = Field(..., min_length=1, max_length=64)
    product_name: str = Field(..., min_length=1, max_length=255)
    available_stock: int = Field(..., ge=0)


class ReserveRequest(BaseModel):
    """Payload for reserving stock. The quantity must be a positive integer."""

    quantity: int = Field(..., gt=0)


class InventoryAdjust(BaseModel):
    """Payload for setting the absolute stock level of an existing SKU.

    Used by operators and the concurrency benchmark to reset stock without
    deleting and re-creating the record.
    """

    available_stock: int = Field(..., ge=0)


class InventoryResponse(BaseModel):
    """Serialized inventory record returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    tenant_id: str
    sku: str
    product_name: str
    available_stock: int
    created_at: datetime
    updated_at: datetime
