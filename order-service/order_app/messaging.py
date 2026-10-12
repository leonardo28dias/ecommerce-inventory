"""RabbitMQ connection and topology management for the order service."""

import json
import logging
import uuid
from contextlib import contextmanager
from typing import Optional

import pika
from pika.spec import Basic, BasicProperties
from pydantic import ValidationError

from .config import settings
from .schemas import OrderCreated, OrderCreatedPayload, StockRejected, StockReserved

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Topology constants
# ---------------------------------------------------------------------------
EXCHANGE_ORDERS = "orders.events"
EXCHANGE_INVENTORY = "inventory.events"
EXCHANGE_DLX = "dlx.events"

QUEUE_STOCK_RESERVED = "order-service.stock-reserved"
QUEUE_STOCK_REJECTED = "order-service.stock-rejected"

DLQ_STOCK_RESERVED = "order-service.stock-reserved.dlq"
DLQ_STOCK_REJECTED = "order-service.stock-rejected.dlq"

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
    """Declare all exchanges, queues, and bindings for the order service.

    This is idempotent and safe to call on every startup.
    """
    # Exchanges
    channel.exchange_declare(exchange=EXCHANGE_ORDERS, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_INVENTORY, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_DLX, exchange_type="topic", durable=True)

    # Dead-letter queues (no consumers, just for inspection)
    channel.queue_declare(queue=DLQ_STOCK_RESERVED, durable=True)
    channel.queue_declare(queue=DLQ_STOCK_REJECTED, durable=True)

    # Bind DLQs to DLX with original routing keys
    channel.queue_bind(queue=DLQ_STOCK_RESERVED, exchange=EXCHANGE_DLX, routing_key=RK_STOCK_RESERVED)
    channel.queue_bind(queue=DLQ_STOCK_REJECTED, exchange=EXCHANGE_DLX, routing_key=RK_STOCK_REJECTED)

    # Main queues with dead-letter configuration
    args = {
        "x-dead-letter-exchange": EXCHANGE_DLX,
        "x-dead-letter-routing-key": RK_STOCK_RESERVED,
    }
    channel.queue_declare(queue=QUEUE_STOCK_RESERVED, durable=True, arguments=args)

    args = {
        "x-dead-letter-exchange": EXCHANGE_DLX,
        "x-dead-letter-routing-key": RK_STOCK_REJECTED,
    }
    channel.queue_declare(queue=QUEUE_STOCK_REJECTED, durable=True, arguments=args)

    # Bind main queues to inventory exchange (where inventory service publishes)
    channel.queue_bind(queue=QUEUE_STOCK_RESERVED, exchange=EXCHANGE_INVENTORY, routing_key=RK_STOCK_RESERVED)
    channel.queue_bind(queue=QUEUE_STOCK_REJECTED, exchange=EXCHANGE_INVENTORY, routing_key=RK_STOCK_REJECTED)


@contextmanager
def connection_context():
    """Context manager for a RabbitMQ connection with automatic cleanup."""
    conn = _get_connection()
    try:
        yield conn
    finally:
        conn.close()


def publish_order_created(
    tenant_id: str, order_id: str, sku: str, quantity: int
) -> str:
    """Publish an OrderCreated event to the orders exchange.

    Returns the generated event_id.
    """
    event_id = str(uuid.uuid4())
    event = OrderCreated(
        event_id=event_id,
        tenant_id=tenant_id,
        payload=OrderCreatedPayload(order_id=order_id, sku=sku, quantity=quantity),
    )

    with connection_context() as conn:
        channel = conn.channel()
        declare_topology(channel)

        channel.basic_publish(
            exchange=EXCHANGE_ORDERS,
            routing_key=RK_ORDER_CREATED,
            body=event.model_dump_json(),
            properties=BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistent
                message_id=event_id,
                headers={"x-retry-count": 0},
            ),
        )
        logger.info(
            "Published OrderCreated",
            extra={"fields": {
                "event_id": event_id,
                "event_type": "OrderCreated",
                "tenant_id": tenant_id,
                "order_id": order_id,
                "sku": sku,
                "quantity": quantity,
            }},
        )

    return event_id


def _handle_stock_result(
    db, event: StockReserved | StockRejected, event_id: str
) -> str:
    """Process a stock result event using the service layer."""
    from . import service

    return service.apply_stock_result(
        db,
        event_id=event_id,
        tenant_id=event.tenant_id,
        order_id=event.payload.order_id,
        outcome="reserved" if isinstance(event, StockReserved) else "rejected",
    )


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
            processor(db, event_model, event_id)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

        # Success - ACK
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(
            "Processed event",
            extra={"fields": {"event_id": event_id, "event_type": event_type}},
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
                    exchange=EXCHANGE_INVENTORY,
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


def consume_stock_events(db_factory) -> None:
    """Start consuming StockReserved and StockRejected events.

    This blocks indefinitely. Run in a separate thread/process.
    """
    with connection_context() as conn:
        channel = conn.channel()
        declare_topology(channel)

        channel.basic_qos(prefetch_count=10)

        def on_stock_reserved(ch, method, props, body):
            _process_message(
                db_factory,
                ch,
                method,
                props,
                body,
                expected_types=(StockReserved,),
                processor=lambda db, ev, eid: _handle_stock_result(db, ev, eid),
            )

        def on_stock_rejected(ch, method, props, body):
            _process_message(
                db_factory,
                ch,
                method,
                props,
                body,
                expected_types=(StockRejected,),
                processor=lambda db, ev, eid: _handle_stock_result(db, ev, eid),
            )

        channel.basic_consume(queue=QUEUE_STOCK_RESERVED, on_message_callback=on_stock_reserved)
        channel.basic_consume(queue=QUEUE_STOCK_REJECTED, on_message_callback=on_stock_rejected)

        logger.info("Starting to consume stock events")
        channel.start_consuming()