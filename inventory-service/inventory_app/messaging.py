"""RabbitMQ connection and topology management for the inventory service."""

import json
import logging
from contextlib import contextmanager

import pika
from pika.spec import Basic, BasicProperties
from pydantic import ValidationError

from .config import settings
from .schemas import (
    OrderCreated,
    StockReserved,
    StockReservedPayload,
    StockRejected,
    StockRejectedPayload,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Topology constants
# ---------------------------------------------------------------------------
EXCHANGE_ORDERS = "orders.events"
EXCHANGE_INVENTORY = "inventory.events"
EXCHANGE_DLX = "dlx.events"

QUEUE_ORDER_CREATED = "inventory-service.order-created"

DLQ_ORDER_CREATED = "inventory-service.order-created.dlq"

RK_ORDER_CREATED = "order.created"
RK_STOCK_RESERVED = "stock.reserved"
RK_STOCK_REJECTED = "stock.rejected"

MAX_RETRIES = 3


def _get_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(settings.rabbitmq_user, settings.rabbitmq_password)
    parameters = pika.ConnectionParameters(
        host=settings.rabbitmq_host,
        port=settings.rabbitmq_port,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    return pika.BlockingConnection(parameters)


def declare_topology(channel: pika.channel.Channel) -> None:
    """Declare all exchanges, queues, and bindings for the inventory service."""
    # Exchanges
    channel.exchange_declare(exchange=EXCHANGE_ORDERS, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_INVENTORY, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_DLX, exchange_type="topic", durable=True)

    # Dead-letter queue (no consumers, just for inspection)
    channel.queue_declare(queue=DLQ_ORDER_CREATED, durable=True)

    # Bind DLQ to DLX with original routing key
    channel.queue_bind(queue=DLQ_ORDER_CREATED, exchange=EXCHANGE_DLX, routing_key=RK_ORDER_CREATED)

    # Main queue with dead-letter configuration
    args = {
        "x-dead-letter-exchange": EXCHANGE_DLX,
        "x-dead-letter-routing-key": RK_ORDER_CREATED,
    }
    channel.queue_declare(queue=QUEUE_ORDER_CREATED, durable=True, arguments=args)

    # Bind main queue to orders exchange (where order service publishes)
    channel.queue_bind(queue=QUEUE_ORDER_CREATED, exchange=EXCHANGE_ORDERS, routing_key=RK_ORDER_CREATED)


@contextmanager
def connection_context():
    """Context manager for a RabbitMQ connection with automatic cleanup."""
    conn = _get_connection()
    try:
        yield conn
    finally:
        conn.close()


def publish_stock_reserved(
    tenant_id: str, order_id: str, sku: str, quantity: int, event_id: str
) -> None:
    """Publish a StockReserved event to the inventory exchange."""
    event = StockReserved(
        event_id=event_id,
        tenant_id=tenant_id,
        payload=StockReservedPayload(order_id=order_id, sku=sku, quantity=quantity),
    )

    with connection_context() as conn:
        channel = conn.channel()
        declare_topology(channel)

        channel.basic_publish(
            exchange=EXCHANGE_INVENTORY,
            routing_key=RK_STOCK_RESERVED,
            body=event.model_dump_json(),
            properties=BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=event_id,
                headers={"x-retry-count": 0},
            ),
        )
        logger.info(
            "Published StockReserved",
            extra={"fields": {
                "event_id": event_id,
                "event_type": "StockReserved",
                "tenant_id": tenant_id,
                "order_id": order_id,
                "sku": sku,
                "quantity": quantity,
            }},
        )


def publish_stock_rejected(
    tenant_id: str, order_id: str, sku: str, quantity: int, reason: str, event_id: str
) -> None:
    """Publish a StockRejected event to the inventory exchange."""
    event = StockRejected(
        event_id=event_id,
        tenant_id=tenant_id,
        payload=StockRejectedPayload(order_id=order_id, sku=sku, quantity=quantity, reason=reason),
    )

    with connection_context() as conn:
        channel = conn.channel()
        declare_topology(channel)

        channel.basic_publish(
            exchange=EXCHANGE_INVENTORY,
            routing_key=RK_STOCK_REJECTED,
            body=event.model_dump_json(),
            properties=BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=event_id,
                headers={"x-retry-count": 0},
            ),
        )
        logger.info(
            "Published StockRejected",
            extra={"fields": {
                "event_id": event_id,
                "event_type": "StockRejected",
                "tenant_id": tenant_id,
                "order_id": order_id,
                "sku": sku,
                "quantity": quantity,
                "reason": reason,
            }},
        )


def _handle_order_created(
    db, event: OrderCreated, event_id: str
) -> tuple[str, str | None]:
    """Process an OrderCreated event using the service layer.

    Returns (outcome, error_message) where outcome is "reserved" or "rejected"
    and error_message is None on success or contains the rejection reason.
    """
    from . import service

    try:
        outcome = service.reserve_stock_for_event(
            db,
            event_id=event_id,
            tenant_id=event.tenant_id,
            sku=event.payload.sku,
            quantity=event.payload.quantity,
        )
        return outcome, None
    except service.ItemNotFound as exc:
        return "rejected", str(exc)
    except service.InsufficientStock as exc:
        return "rejected", str(exc)
    except service.AlreadyProcessed:
        # Already processed - get the stored outcome
        stored = (
            db.query(service.ProcessedEvent)
            .filter_by(event_id=event_id)
            .one()
            .outcome
        )
        return stored or "rejected", None


def _process_message(
    db_factory,
    channel: pika.channel.Channel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: bytes,
    expected_types: tuple,
    processor,
) -> None:
    """Generic message processing with retry/DLQ logic.

    - Parses and validates the event
    - Calls the processor function
    - On success: ACK
    - On validation error: reject (no requeue) -> DLQ
    - On business error: retry up to MAX_RETRIES, then DLQ
    """
    retry_count = 0
    if properties.headers and "x-retry-count" in properties.headers:
        retry_count = properties.headers["x-retry-count"]

    event_id = properties.message_id or "unknown"
    event_type = "unknown"
    tenant_id = "unknown"

    # --- Validation phase: malformed or invalid events go straight to the DLQ.
    # Retrying a message that can never be parsed would only waste retries.
    try:
        # Parse event
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("Event must be a JSON object")

        event_type = payload.get("event_type", "unknown")
        tenant_id = payload.get("tenant_id", "unknown")

        # Validate against expected types
        if event_type not in [t.__name__ for t in expected_types]:
            raise ValueError(f"Unexpected event type: {event_type}")

        event_model = None
        for t in expected_types:
            if t.__name__ == event_type:
                event_model = t.model_validate(payload)
                break

        if event_model is None:
            raise ValueError(f"Failed to parse event type {event_type}")
    except (json.JSONDecodeError, ValidationError, ValueError) as exc:
        logger.error(
            "Invalid event rejected without retry (dead-letter)",
            extra={"fields": {
                "event_id": event_id,
                "event_type": event_type,
                "tenant_id": tenant_id,
                "error": str(exc),
            }},
        )
        # Reject without requeue -> goes to DLQ via x-dead-letter-exchange
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    logger.info(
        "Processing event",
        extra={"fields": {
            "event_id": event_id,
            "event_type": event_type,
            "tenant_id": tenant_id,
            "attempt": retry_count + 1,
            "max_attempts": MAX_RETRIES + 1,
        }},
    )

    # --- Business phase: retry up to MAX_RETRIES, then DLQ ---
    try:
        # Process the event
        db = db_factory()
        try:
            outcome, error = processor(db, event_model, event_id)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

        # Publish response event
        if outcome == "reserved":
            publish_stock_reserved(
                tenant_id=tenant_id,
                order_id=event_model.payload.order_id,
                sku=event_model.payload.sku,
                quantity=event_model.payload.quantity,
                event_id=event_id,
            )
        else:
            publish_stock_rejected(
                tenant_id=tenant_id,
                order_id=event_model.payload.order_id,
                sku=event_model.payload.sku,
                quantity=event_model.payload.quantity,
                reason=error or "Unknown error",
                event_id=event_id,
            )

        # Success - ACK
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(
            "Processed event",
            extra={"fields": {
                "event_id": event_id,
                "event_type": event_type,
                "tenant_id": tenant_id,
                "outcome": outcome,
            }},
        )

    except Exception as exc:
        # Determine if we should retry or dead-letter
        if retry_count < MAX_RETRIES:
            # Retry with incremented count
            logger.warning(
                "Event processing failed; scheduling retry",
                extra={"fields": {
                    "event_id": event_id,
                    "event_type": event_type,
                    "tenant_id": tenant_id,
                    "retry": retry_count + 1,
                    "max_retries": MAX_RETRIES,
                    "error": str(exc),
                }},
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            # Re-publish with incremented retry count
            with connection_context() as conn:
                retry_channel = conn.channel()
                retry_channel.basic_publish(
                    exchange=EXCHANGE_ORDERS,
                    routing_key=method.routing_key,
                    body=body,
                    properties=BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                        message_id=event_id,
                        headers={"x-retry-count": retry_count + 1},
                    ),
                )
        else:
            # Max retries exceeded - dead-letter
            logger.error(
                "Event processing failed permanently (dead-letter)",
                extra={"fields": {
                    "event_id": event_id,
                    "event_type": event_type,
                    "tenant_id": tenant_id,
                    "error": str(exc),
                }},
            )
            # Reject without requeue -> goes to DLQ via x-dead-letter-exchange
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def consume_order_events(db_factory) -> None:
    """Start consuming OrderCreated events.

    This blocks indefinitely. Run in a separate thread/process.
    """
    with connection_context() as conn:
        channel = conn.channel()
        declare_topology(channel)

        channel.basic_qos(prefetch_count=10)

        def on_order_created(ch, method, props, body):
            _process_message(
                db_factory,
                ch,
                method,
                props,
                body,
                expected_types=(OrderCreated,),
                processor=lambda db, ev, eid: _handle_order_created(db, ev, eid),
            )

        channel.basic_consume(queue=QUEUE_ORDER_CREATED, on_message_callback=on_order_created)

        logger.info("Starting to consume OrderCreated events")
        channel.start_consuming()
