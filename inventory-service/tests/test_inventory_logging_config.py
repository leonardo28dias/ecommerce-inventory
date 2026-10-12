"""Tests for the structured JSON logging setup.

These verify that the event-flow logs carry the identifiers (service,
event_id, tenant_id, order_id, ...) that make a request traceable across
services. The observability is intentionally lightweight: one JSON object per
line, no external tracing dependency.
"""
import json
import logging

from inventory_app.logging_config import SERVICE_NAME, JsonFormatter


def _emit(fields, message="msg", level=logging.INFO):
    """Format a single log record the way the service would and parse it back."""
    record = logging.LogRecord(
        name="inventory_app.messaging",
        level=level,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    record.fields = fields
    return json.loads(JsonFormatter().format(record))


def test_log_line_is_valid_json_with_service_context():
    line = _emit({"event_id": "e-1", "tenant_id": "tenant-a"})
    assert line["service"] == SERVICE_NAME
    assert line["level"] == "INFO"
    assert line["message"] == "msg"
    assert "time" in line


def test_event_identifiers_are_flattened_to_top_level():
    line = _emit(
        {
            "event_id": "e-1",
            "event_type": "StockReserved",
            "tenant_id": "tenant-a",
            "order_id": "o-1",
            "sku": "laptop-001",
            "quantity": 3,
        }
    )
    # Every identifier is a top-level JSON key (not buried in a nested dict),
    # so log aggregation tools can filter on them directly.
    assert line["event_id"] == "e-1"
    assert line["event_type"] == "StockReserved"
    assert line["tenant_id"] == "tenant-a"
    assert line["order_id"] == "o-1"
    assert line["sku"] == "laptop-001"
    assert line["quantity"] == 3


def test_none_values_are_omitted():
    line = _emit({"event_id": "e-1", "order_id": None})
    assert line["event_id"] == "e-1"
    assert "order_id" not in line
