"""
Pytest setup for the inventory-service tests.

``SELECT ... FOR UPDATE`` is a database feature, so these tests run against a
real PostgreSQL server. Connection settings come from ``INVENTORY_TEST_DB_*``
environment variables (defaulting to a local server) and the application is
pointed at a dedicated ``inventory_test`` database so the tests never touch the
service's own ``inventorydb``.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# --- Read test database settings BEFORE the application is imported --------
_TEST_HOST = os.environ.get("INVENTORY_TEST_DB_HOST", "127.0.0.1")
_TEST_PORT = os.environ.get("INVENTORY_TEST_DB_PORT", "5432")
_TEST_USER = os.environ.get("INVENTORY_TEST_DB_USER", "postgres")
_TEST_PASSWORD = os.environ.get("INVENTORY_TEST_DB_PASSWORD", "postgres")
_TEST_NAME = os.environ.get("INVENTORY_TEST_DB_NAME", "inventory_test")

# Point the application (settings + engine are built at import time) at the
# test database. This must happen before `inventory_app` is imported.
os.environ["INVENTORY_DB_HOST"] = _TEST_HOST
os.environ["INVENTORY_DB_PORT"] = _TEST_PORT
os.environ["INVENTORY_DB_USER"] = _TEST_USER
os.environ["INVENTORY_DB_PASSWORD"] = _TEST_PASSWORD
os.environ["INVENTORY_DB_NAME"] = _TEST_NAME

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from inventory_app.database import Base, engine
from inventory_app.main import app


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
    """Empty the inventory and processed_events tables before every test."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM inventory_items"))
        conn.execute(text("DELETE FROM processed_events"))
    yield


@pytest.fixture()
def client(_database, _clean_database):
    """HTTP client wired to the app (backed by the test database)."""
    with TestClient(app) as c:
        yield c

