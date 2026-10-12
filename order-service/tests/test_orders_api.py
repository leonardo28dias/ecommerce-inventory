"""API-level tests for the order endpoints (HTTP behaviour + tenant scoping)."""

TENANT_A = {"X-Tenant-ID": "tenant-a"}
TENANT_B = {"X-Tenant-ID": "tenant-b"}


def _create(client, headers, sku="laptop-001", quantity=2):
    return client.post(
        "/orders", headers=headers, json={"sku": sku, "quantity": quantity}
    )


def test_create_order(client):
    resp = _create(client, TENANT_A, sku="laptop-001", quantity=2)
    assert resp.status_code == 201
    data = resp.json()
    assert data["tenant_id"] == "tenant-a"
    assert data["sku"] == "laptop-001"
    assert data["quantity"] == 2
    assert data["status"] == "PENDING"
    assert data["order_id"]  # public identifier is returned
    assert data["id"] > 0
    assert "created_at" in data and "updated_at" in data


def test_create_order_requires_tenant_header(client):
    resp = client.post("/orders", json={"sku": "laptop-001", "quantity": 2})
    # FastAPI treats a missing required header as a request validation error.
    assert resp.status_code == 422


def test_get_order(client):
    created = _create(client, TENANT_A, sku="laptop-001", quantity=2).json()
    resp = client.get(f"/orders/{created['order_id']}", headers=TENANT_A)
    assert resp.status_code == 200
    data = resp.json()
    assert data["order_id"] == created["order_id"]
    assert data["tenant_id"] == "tenant-a"
    assert data["sku"] == "laptop-001"
    assert data["quantity"] == 2
    assert data["status"] == "PENDING"


def test_get_nonexistent_order(client):
    resp = client.get(
        "/orders/00000000-0000-0000-0000-000000000000", headers=TENANT_A
    )
    assert resp.status_code == 404


def test_create_order_invalid_quantity(client):
    zero = _create(client, TENANT_A, quantity=0)
    assert zero.status_code == 422

    negative = _create(client, TENANT_A, quantity=-1)
    assert negative.status_code == 422


def test_create_order_invalid_sku(client):
    # The SKU must be a non-empty string of at most 64 characters; no stricter
    # product-code format is enforced in this phase.
    empty = _create(client, TENANT_A, sku="")
    assert empty.status_code == 422

    too_long = _create(client, TENANT_A, sku="x" * 65)
    assert too_long.status_code == 422


def test_tenant_isolation(client):
    # An order created under tenant-b...
    created = _create(client, TENANT_B, sku="laptop-001", quantity=1).json()

    # ...cannot be accessed using tenant-a.
    resp_a = client.get(f"/orders/{created['order_id']}", headers=TENANT_A)
    assert resp_a.status_code == 404

    # ...but is visible to its own tenant.
    resp_b = client.get(f"/orders/{created['order_id']}", headers=TENANT_B)
    assert resp_b.status_code == 200
    assert resp_b.json()["tenant_id"] == "tenant-b"


def test_get_cross_tenant_rejected(client):
    """tenant-a cannot read tenant-b's order via GET."""
    created = _create(client, TENANT_B, sku="laptop-001", quantity=1).json()
    resp = client.get(f"/orders/{created['order_id']}", headers=TENANT_A)
    assert resp.status_code == 404


def test_status_persists_as_pending(client):
    created = _create(client, TENANT_A, sku="laptop-001", quantity=2).json()
    assert created["status"] == "PENDING"

    retrieved = client.get(f"/orders/{created['order_id']}", headers=TENANT_A)
    assert retrieved.status_code == 200
    data = retrieved.json()
    assert data["status"] == "PENDING"
    assert data["order_id"] == created["order_id"]
    assert data["tenant_id"] == created["tenant_id"]
    assert data["sku"] == created["sku"]
    assert data["quantity"] == created["quantity"]


def test_order_database_has_no_inventory_schema(_database):
    """Prove the persistence boundary: the order DB holds orders — and nothing
    from the inventory domain. Order creation therefore cannot change stock."""
    from sqlalchemy import inspect

    from order_app.database import engine

    tables = inspect(engine).get_table_names()
    assert "orders" in tables
    assert "inventory_items" not in tables
    assert "inventory" not in engine.url.database
