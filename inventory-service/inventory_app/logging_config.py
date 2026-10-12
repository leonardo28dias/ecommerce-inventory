"""Shared structured logging setup for the inventory service.

Emits one JSON object per log line so the event-driven logs stay machine
readable and carry the fields that make a request traceable across services:

* ``service``       - which service produced the line (static context)
* ``event_id``      - the correlation id carried by every event
* ``tenant_id``     - tenant the line belongs to
* ``order_id``      - the order involved, when known
* ``event_type``    - the domain event being published / consumed
* ``sku``           - the SKU involved, when known
* ``message``       - the log message
* ``level``/``logger``/``time`` - standard context

The extra fields are passed as structured context: ``log.info("...",
event_id=..., order_id=...)``. Stdlib ``logging`` drops unknown kwargs, so we
pass a single ``extra`` dict instead (``extra={"fields": {...}}``). The
formatter is installed on the root logger, so ``getLogger(__name__)`` in any
module automatically picks it up - each service only imports this once, at
startup in ``main.py``.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

SERVICE_NAME = "inventory-service"

# Context keys flattened onto every line (the stable, always-useful fields).
_KNOWN_FIELDS = ("event_id", "tenant_id", "order_id", "event_type", "sku")


class JsonFormatter(logging.Formatter):
    """Format a log record as a single JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": SERVICE_NAME,
            "message": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            for key in _KNOWN_FIELDS:
                value = fields.get(key)
                if value is not None:
                    payload[key] = value
            # Preserve any additional ad-hoc context keys.
            for key, value in fields.items():
                if key not in _KNOWN_FIELDS and value is not None:
                    payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Install the JSON formatter on the root logger (idempotent)."""
    root = logging.getLogger()
    root.setLevel(level.upper())
    # Replace any existing handlers so repeated startup calls stay clean.
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
