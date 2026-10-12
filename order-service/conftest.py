"""
Pytest setup for the order-service tests.

The application is pointed at a dedicated ``order_test`` database (created
automatically) so the tests never touch the service's own ``orderdb``.
Connection settings come from ``ORDER_TEST_DB_*`` environment variables,
defaulting to a local server.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# --- Read test database settings BEFORE the application is imported --------
_TEST_HOST = os.environ.get("ORDER_TEST_DB_HOST", "127.0.0.1")
_TEST_PORT = os.environ.get("ORDER_TEST_DB_PORT", "5432")
_TEST_USER = os.environ.get("ORDER_TEST_DB_USER", "postgres")
_TEST_PASSWORD = os.environ.get("ORDER_TEST_DB_PASSWORD", "postgres")
_TEST_NAME = os.environ.get("ORDER_TEST_DB_NAME", "order_test")

# Point the application (settings + engine are built at import time) at the
# test database. This must happen before `order_app` is imported.
os.environ["ORDER_DB_HOST"] = _TEST_HOST
os.environ["ORDER_DB_PORT"] = _TEST_PORT
os.environ["ORDER_DB_USER"] = _TEST_USER
os.environ["ORDER_DB_PASSWORD"] = _TEST_PASSWORD
os.environ["ORDER_DB_NAME"] = _TEST_NAME

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from order_app.database import Base, engine
from order_app.main import app


@pytest.fixture(scope="session")
def _database():
    """Create the test database (if missing) and the schema."""
    admin_url = (
        f"postgresql://{_TEST_USER}:{_TEST_PASSWORD}"
        f"@{_TEST_HOST}:{_TEST_PORT}/postgres"
    )
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": _TEST_NAME},
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{_TEST_NAME}"'))
    admin_engine.dispose()

    Base.metadata.create_all(bind=engine)
    yield


@pytest.fixture(autouse=True)
def _clean_database(_database):
    """Empty the orders and processed_events tables before every test."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM orders"))
        conn.execute(text("DELETE FROM processed_events"))
    yield


@pytest.fixture()
def client(_database, _clean_database):
    """HTTP client wired to the app (backed by the test database)."""
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def mock_publish_order_created(monkeypatch):
    """Keep tests hermetic: never open a real broker connection.

    ``POST /orders`` publishes ``OrderCreated`` after persisting the order.
    Tests replace the publisher with a mock (except the ones that explicitly
    re-patch it to assert on the published event or to simulate an outage).
    """
    from unittest.mock import MagicMock

    mock = MagicMock(name="publish_order_created")
    monkeypatch.setattr("order_app.routes.publish_order_created", mock)
    return mock

