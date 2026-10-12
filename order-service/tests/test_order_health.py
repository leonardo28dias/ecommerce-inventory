from fastapi.testclient import TestClient

from order_app.main import app


def test_health_reports_healthy_when_dependencies_are_ok(monkeypatch):
    monkeypatch.setattr("order_app.health.check_db_health", lambda: True)
    monkeypatch.setattr("order_app.health.check_rabbitmq_health", lambda: True)

    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["services"]["database"] == "ok"
    assert body["services"]["rabbitmq"] == "ok"


def test_health_reports_degraded_when_dependencies_are_down(monkeypatch):
    monkeypatch.setattr("order_app.health.check_db_health", lambda: False)
    monkeypatch.setattr("order_app.health.check_rabbitmq_health", lambda: False)

    client = TestClient(app)
    response = client.get("/health")

    # The service is still alive (HTTP 200); the body reports degradation.
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "degraded"
    assert body["services"]["database"] == "error"
    assert body["services"]["rabbitmq"] == "error"
