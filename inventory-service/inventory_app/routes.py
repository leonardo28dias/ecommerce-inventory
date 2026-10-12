from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from . import service
from .database import get_db
from .models import InventoryItem
from .schemas import (
    InventoryAdjust,
    InventoryCreate,
    InventoryResponse,
    ReserveRequest,
)

router = APIRouter()


def get_tenant_id(x_tenant_id: str = Header(...)) -> str:
    """Resolve the current tenant from the ``X-Tenant-ID`` header."""
    tenant = x_tenant_id.strip()
    if not tenant:
        raise HTTPException(
            status_code=400, detail="X-Tenant-ID header must not be empty"
        )
    return tenant


@router.post("/inventory", response_model=InventoryResponse, status_code=201)
def create_inventory(
    payload: InventoryCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db),
):
    existing = (
        db.query(InventoryItem)
        .filter(InventoryItem.tenant_id == tenant_id, InventoryItem.sku == payload.sku)
        .first()
    )
    if existing is not None:
        raise HTTPException(
            status_code=409, detail="SKU already exists for this tenant"
        )

    item = InventoryItem(
        tenant_id=tenant_id,
        sku=payload.sku,
        product_name=payload.product_name,
        available_stock=payload.available_stock,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.get("/inventory/{sku}", response_model=InventoryResponse)
def get_inventory(
    sku: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db),
):
    item = (
        db.query(InventoryItem)
        .filter(InventoryItem.tenant_id == tenant_id, InventoryItem.sku == sku)
        .first()
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Inventory item not found")
    return item


@router.put("/inventory/{sku}", response_model=InventoryResponse)
def adjust_inventory(
    sku: str,
    payload: InventoryAdjust,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db),
):
    """Set ``available_stock`` to an absolute value for an existing SKU.

    The row is locked with ``SELECT ... FOR UPDATE`` first, so an adjustment
    can never interleave with a concurrent reservation and produce a
    torn read. This is an operational endpoint (restock, benchmark reset) —
    it replaces the value, it is not a delta.
    """
    item = (
        db.query(InventoryItem)
        .filter(InventoryItem.tenant_id == tenant_id, InventoryItem.sku == sku)
        .with_for_update()
        .one_or_none()
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Inventory item not found")

    item.available_stock = payload.available_stock
    db.commit()
    db.refresh(item)
    return item


@router.post("/inventory/{sku}/reserve", response_model=InventoryResponse)
def reserve_inventory(
    sku: str,
    payload: ReserveRequest,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db),
):
    try:
        item = service.reserve_stock(db, tenant_id, sku, payload.quantity)
    except service.ItemNotFound:
        db.rollback()
        raise HTTPException(status_code=404, detail="Inventory item not found")
    except service.InsufficientStock as exc:
        # Release the row lock taken by SELECT ... FOR UPDATE before replying.
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=f"Insufficient stock: requested {exc.requested}, available {exc.available}",
        )
    return item
