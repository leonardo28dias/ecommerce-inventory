"""API-level tests for the inventory endpoints (HTTP behaviour + tenant scoping)."""

TENANT_A = {"X-Tenant-ID": "tenant-a"}
TENANT_B = {"X-Tenant-ID": "tenant-b"}


def _create(client, headers, sku="laptop-001", product="Laptop", stock=5):
    return client.post(
        "/inventory",
        headers=headers,
        json={"sku": sku, "product_name": product, "available_stock": stock},
    )


def test_create_inventory(client):
    resp = _create(client, TENANT_A, stock=5)
    assert resp.status_code == 201
    data = resp.json()
    assert data["tenant_id"] == "tenant-a"
    assert data["sku"] == "laptop-001"
    assert data["product_name"] == "Laptop"
    assert data["available_stock"] == 5
    assert data["id"] > 0
    assert "created_at" in data and "updated_at" in data


def test_create_inventory_requires_tenant_header(client):
    resp = client.post(
        "/inventory",
        json={"sku": "x", "product_name": "X", "available_stock": 1},
    )
    # FastAPI treats a missing required header as a request validation error.
    assert resp.status_code == 422


def test_get_inventory(client):
    _create(client, TENANT_A, stock=5)
    resp = client.get("/inventory/laptop-001", headers=TENANT_A)
    assert resp.status_code == 200
    assert resp.json()["available_stock"] == 5


def test_get_inventory_not_found(client):
    resp = client.get("/inventory/unknown-sku", headers=TENANT_A)
    assert resp.status_code == 404


def test_reserve_success(client):
    _create(client, TENANT_A, stock=5)
    resp = client.post(
        "/inventory/laptop-001/reserve", headers=TENANT_A, json={"quantity": 3}
    )
    assert resp.status_code == 200
    assert resp.json()["available_stock"] == 2


def test_reserve_insufficient_stock_keeps_stock(client):
    _create(client, TENANT_A, stock=5)

    # Reserve 3 -> stock becomes 2.
    first = client.post(
        "/inventory/laptop-001/reserve", headers=TENANT_A, json={"quantity": 3}
    )
    assert first.status_code == 200
    assert first.json()["available_stock"] == 2

    # Reserve 3 again -> rejected (only 2 left) and the stock is unchanged.
    second = client.post(
        "/inventory/laptop-001/reserve", headers=TENANT_A, json={"quantity": 3}
    )
    assert second.status_code == 409

    after = client.get("/inventory/laptop-001", headers=TENANT_A)
    assert after.json()["available_stock"] == 2


def test_reserve_invalid_quantity(client):
    _create(client, TENANT_A, stock=5)

    zero = client.post(
        "/inventory/laptop-001/reserve", headers=TENANT_A, json={"quantity": 0}
    )
    assert zero.status_code == 422

    negative = client.post(
        "/inventory/laptop-001/reserve", headers=TENANT_A, json={"quantity": -1}
    )
    assert negative.status_code == 422

    # Stock is untouched by the rejected requests.
    after = client.get("/inventory/laptop-001", headers=TENANT_A)
    assert after.json()["available_stock"] == 5


def test_duplicate_sku_same_tenant_rejected(client):
    assert _create(client, TENANT_A, stock=5).status_code == 201
    duplicate = _create(client, TENANT_A, stock=99)
    assert duplicate.status_code == 409


def test_same_sku_allowed_for_different_tenants(client):
    a = _create(client, TENANT_A, stock=5)
    b = _create(client, TENANT_B, stock=7)
    assert a.status_code == 201
    assert b.status_code == 201

    # Each tenant keeps its own independent record and stock level.
    assert (
        client.get("/inventory/laptop-001", headers=TENANT_A).json()[
            "available_stock"
        ]
        == 5
    )
    assert (
        client.get("/inventory/laptop-001", headers=TENANT_B).json()[
            "available_stock"
        ]
        == 7
    )


def test_tenant_isolation(client):
    # tenant-a creates a record.
    assert _create(client, TENANT_A, stock=5).status_code == 201

    # tenant-b must NOT see tenant-a's record.
    resp_b = client.get("/inventory/laptop-001", headers=TENANT_B)
    assert resp_b.status_code == 404

    # tenant-a still sees it.
    resp_a = client.get("/inventory/laptop-001", headers=TENANT_A)
    assert resp_a.status_code == 200
    assert resp_a.json()["available_stock"] == 5


def test_reserve_cross_tenant_rejected(client):
    """tenant-b cannot reserve stock from tenant-a's inventory."""
    # tenant-a creates inventory with 5 units
    assert _create(client, TENANT_A, stock=5).status_code == 201

    # tenant-b tries to reserve from tenant-a's SKU -> 404 (not found for tenant-b)
    resp = client.post(
        "/inventory/laptop-001/reserve", headers=TENANT_B, json={"quantity": 1}
    )
    assert resp.status_code == 404

    # tenant-a's stock is untouched
    assert client.get("/inventory/laptop-001", headers=TENANT_A).json()["available_stock"] == 5


def test_get_cross_tenant_rejected(client):
    """tenant-b cannot read tenant-a's inventory via GET."""
    assert _create(client, TENANT_A, stock=5).status_code == 201
    resp = client.get("/inventory/laptop-001", headers=TENANT_B)
    assert resp.status_code == 404


def test_adjust_stock_resets_absolute_value(client):
    """PUT sets the absolute stock level (used to reset the benchmark)."""
    _create(client, TENANT_A, stock=5)

    resp = client.put(
        "/inventory/laptop-001", headers=TENANT_A, json={"available_stock": 50}
    )
    assert resp.status_code == 200
    assert resp.json()["available_stock"] == 50

    # The change is persisted and visible on a subsequent read.
    assert (
        client.get("/inventory/laptop-001", headers=TENANT_A).json()[
            "available_stock"
        ]
        == 50
    )


def test_adjust_stock_not_found(client):
    resp = client.put(
        "/inventory/unknown-sku", headers=TENANT_A, json={"available_stock": 10}
    )
    assert resp.status_code == 404


def test_adjust_stock_rejects_negative(client):
    _create(client, TENANT_A, stock=5)
    resp = client.put(
        "/inventory/laptop-001", headers=TENANT_A, json={"available_stock": -1}
    )
    assert resp.status_code == 422
    # Stock unchanged after the rejected adjustment.
    assert (
        client.get("/inventory/laptop-001", headers=TENANT_A).json()[
            "available_stock"
        ]
        == 5
    )


def test_adjust_stock_cross_tenant_rejected(client):
    """tenant-b cannot adjust tenant-a's stock."""
    assert _create(client, TENANT_A, stock=5).status_code == 201
    resp = client.put(
        "/inventory/laptop-001", headers=TENANT_B, json={"available_stock": 999}
    )
    assert resp.status_code == 404
    assert (
        client.get("/inventory/laptop-001", headers=TENANT_A).json()[
            "available_stock"
        ]
        == 5
    )
