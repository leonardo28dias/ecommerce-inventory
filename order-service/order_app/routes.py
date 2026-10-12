import logging

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from .database import get_db
from .messaging import publish_order_created
from .models import Order, OrderStatus
from .schemas import OrderCreate, OrderResponse

logger = logging.getLogger(__name__)

router = APIRouter()


def get_tenant_id(x_tenant_id: str = Header(...)) -> str:
    """Resolve the current tenant from the ``X-Tenant-ID`` header."""
    tenant = x_tenant_id.strip()
    if not tenant:
        raise HTTPException(
            status_code=400, detail="X-Tenant-ID header must not be empty"
        )
    return tenant


@router.post("/orders", response_model=OrderResponse, status_code=201)
def create_order(
    payload: OrderCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db),
):
    # Persist first: the order starts PENDING and the stock is deliberately
    # left untouched — reservation happens in the inventory service.
    order = Order(
        tenant_id=tenant_id,
        sku=payload.sku,
        quantity=payload.quantity,
        status=OrderStatus.PENDING.value,
    )
    db.add(order)
    db.commit()
    db.refresh(order)

    # Phase 6: announce the new order so the inventory service can reserve
    # stock. A broker outage must not fail the API call — the order is already
    # persisted and stays PENDING (see README "Known limitations").
    try:
        publish_order_created(
            tenant_id, order.order_id, order.sku, order.quantity
        )
    except Exception as exc:  # noqa: BLE001 - availability over completeness
        logger.warning(
            "Could not publish OrderCreated; order stays PENDING",
            extra={"fields": {
                "order_id": order.order_id,
                "tenant_id": tenant_id,
                "sku": order.sku,
                "event_type": "OrderCreated",
                "error": str(exc),
            }},
        )

    return order


@router.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(
    order_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db),
):
    order = (
        db.query(Order)
        .filter(Order.tenant_id == tenant_id, Order.order_id == order_id)
        .first()
    )
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
