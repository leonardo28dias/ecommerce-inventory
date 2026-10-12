"""Tests for inventory service messaging failure handling and DLQ behavior."""

import json
import uuid
from unittest.mock import MagicMock, patch

import pika
import pytest
from pika.spec import Basic, BasicProperties

from inventory_app.messaging import (
    MAX_RETRIES,
    _process_message,
    declare_topology,
    publish_stock_rejected,
    publish_stock_reserved,
)
from inventory_app.schemas import OrderCreated, OrderCreatedPayload, StockRejected, StockReserved
from inventory_app.database import SessionLocal
from inventory_app import models
from inventory_app import service


@pytest.fixture
def db_session():
    """Provide a test database session."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def mock_channel():
    """Create a mock RabbitMQ channel."""
    channel = MagicMock(spec=pika.channel.Channel)
    channel.basic_ack = MagicMock()
    channel.basic_nack = MagicMock()
    channel.basic_publish = MagicMock()
    return channel


@pytest.fixture
def mock_method():
    """Create a mock delivery method."""
    method = MagicMock(spec=Basic.Deliver)
    method.delivery_tag = 1
    method.routing_key = "order.created"
    return method


@pytest.fixture
def mock_properties():
    """Create mock message properties."""
    props = MagicMock(spec=BasicProperties)
    props.message_id = str(uuid.uuid4())
    props.headers = {"x-retry-count": 0}
    return props


def test_declare_topology_creates_dlq():
    """Test that declare_topology creates the dead-letter queue and exchange."""
    channel = MagicMock(spec=pika.channel.Channel)

    declare_topology(channel)

    # Verify exchanges declared
    exchange_calls = [call[1]["exchange"] for call in channel.exchange_declare.call_args_list]
    assert "orders.events" in exchange_calls
    assert "inventory.events" in exchange_calls
    assert "dlx.events" in exchange_calls

    # Verify DLQ declared
    queue_calls = [call[1]["queue"] for call in channel.queue_declare.call_args_list]
    assert "inventory-service.order-created.dlq" in queue_calls
    assert "inventory-service.order-created" in queue_calls

    # Verify DLQ binding
    bind_calls = channel.queue_bind.call_args_list
    dlq_binds = [call for call in bind_calls if call[1]["queue"] == "inventory-service.order-created.dlq"]
    assert len(dlq_binds) > 0


def test_process_message_success(db_session, mock_channel, mock_method, mock_properties):
    """Test successful message processing results in ACK."""
    event_id = str(uuid.uuid4())
    event = OrderCreated(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=OrderCreatedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=1),
    )

    body = event.model_dump_json().encode()

    def processor(db, ev, eid):
        return "reserved", None

    with patch("inventory_app.messaging.publish_stock_reserved") as mock_publish:
        _process_message(
            lambda: db_session,
            mock_channel,
            mock_method,
            mock_properties,
            body,
            expected_types=(OrderCreated,),
            processor=processor,
        )

    mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
    mock_publish.assert_called_once()


def test_process_message_validation_error_goes_to_dlq(db_session, mock_channel, mock_method, mock_properties):
    """Test that validation errors (malformed payload) go directly to DLQ without retry."""
    # Malformed payload - missing required fields
    body = json.dumps({"event_type": "OrderCreated", "tenant_id": "tenant-a"}).encode()

    _process_message(
        lambda: db_session,
        mock_channel,
        mock_method,
        mock_properties,
        body,
        expected_types=(OrderCreated,),
        processor=lambda db, ev, eid: ("reserved", None),
    )

    # Should reject without requeue (goes to DLQ)
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    # Should NOT retry (no basic_publish for retry)
    assert mock_channel.basic_publish.call_count == 0


def test_process_message_retry_on_business_error(db_session, mock_channel, mock_method, mock_properties):
    """Test that business errors trigger retry up to MAX_RETRIES."""
    event_id = str(uuid.uuid4())
    event = OrderCreated(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=OrderCreatedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=1),
    )

    body = event.model_dump_json().encode()

    def failing_processor(db, ev, eid):
        raise service.ItemNotFound("SKU not found")

    with patch("inventory_app.messaging.connection_context") as mock_conn_ctx:
        mock_retry_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn.channel.return_value = mock_retry_channel
        mock_conn_ctx.return_value.__enter__.return_value = mock_conn

        _process_message(
            lambda: db_session,
            mock_channel,
            mock_method,
            mock_properties,
            body,
            expected_types=(OrderCreated,),
            processor=failing_processor,
        )

    # Should nack and re-publish with incremented retry count
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    mock_retry_channel.basic_publish.assert_called_once()

    # Verify retry count was incremented
    publish_call = mock_retry_channel.basic_publish.call_args
    assert publish_call[1]["properties"].headers["x-retry-count"] == 1


def test_process_message_max_retries_exceeded_goes_to_dlq(db_session, mock_channel, mock_method, mock_properties):
    """Test that after MAX_RETRIES, message goes to DLQ."""
    event_id = str(uuid.uuid4())
    event = OrderCreated(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=OrderCreatedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=1),
    )

    body = event.model_dump_json().encode()

    # Set retry count to MAX_RETRIES
    mock_properties.headers = {"x-retry-count": MAX_RETRIES}

    def failing_processor(db, ev, eid):
        raise service.ItemNotFound("SKU not found")

    _process_message(
        lambda: db_session,
        mock_channel,
        mock_method,
        mock_properties,
        body,
        expected_types=(OrderCreated,),
        processor=failing_processor,
    )

    # Should nack without requeue (goes to DLQ)
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    # Should NOT re-publish for retry
    mock_channel.basic_publish.assert_not_called()


def test_process_message_unexpected_event_type_goes_to_dlq(db_session, mock_channel, mock_method, mock_properties):
    """Test that unexpected event types go directly to DLQ."""
    body = json.dumps({"event_type": "UnknownEvent", "tenant_id": "tenant-a"}).encode()

    _process_message(
        lambda: db_session,
        mock_channel,
        mock_method,
        mock_properties,
        body,
        expected_types=(OrderCreated,),
        processor=lambda db, ev, eid: ("reserved", None),
    )

    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_consumer_does_not_crash_on_exception(db_session, mock_channel, mock_method, mock_properties):
    """Test that the consumer doesn't crash permanently on any exception."""
    event_id = str(uuid.uuid4())
    event = OrderCreated(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=OrderCreatedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=1),
    )

    body = event.model_dump_json().encode()

    def crashing_processor(db, ev, eid):
        raise RuntimeError("Simulated crash")

    # This should not raise an exception - it should handle it gracefully
    with patch("inventory_app.messaging.connection_context") as mock_conn_ctx:
        mock_retry_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn.channel.return_value = mock_retry_channel
        mock_conn_ctx.return_value.__enter__.return_value = mock_conn

        _process_message(
            lambda: db_session,
            mock_channel,
            mock_method,
            mock_properties,
            body,
            expected_types=(OrderCreated,),
            processor=crashing_processor,
        )

    # Should handle gracefully with nack and retry
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_valid_event_after_failure_still_works(db_session, mock_channel, mock_method, mock_properties):
    """Test that valid events continue to process after failures."""
    event_id = str(uuid.uuid4())
    event = OrderCreated(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=OrderCreatedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=1),
    )

    body = event.model_dump_json().encode()

    def processor(db, ev, eid):
        return "reserved", None

    with patch("inventory_app.messaging.publish_stock_reserved") as mock_publish:
        _process_message(
            lambda: db_session,
            mock_channel,
            mock_method,
            mock_properties,
            body,
            expected_types=(OrderCreated,),
            processor=processor,
        )

    mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
    mock_publish.assert_called_once()


def test_malformed_json_payload_goes_to_dlq(db_session, mock_channel, mock_method, mock_properties):
    """Test that completely malformed JSON goes to DLQ."""
    body = b"not valid json at all"

    _process_message(
        lambda: db_session,
        mock_channel,
        mock_method,
        mock_properties,
        body,
        expected_types=(OrderCreated,),
        processor=lambda db, ev, eid: ("reserved", None),
    )

    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_missing_sku_in_payload(db_session, mock_channel, mock_method, mock_properties):
    """Test OrderCreated with missing SKU field."""
    body = json.dumps({
        "event_id": str(uuid.uuid4()),
        "event_type": "OrderCreated",
        "tenant_id": "tenant-a",
        "payload": {"order_id": str(uuid.uuid4()), "quantity": 1}  # missing sku
    }).encode()

    _process_message(
        lambda: db_session,
        mock_channel,
        mock_method,
        mock_properties,
        body,
        expected_types=(OrderCreated,),
        processor=lambda db, ev, eid: ("reserved", None),
    )

    # Pydantic validation should fail, message goes to DLQ
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_invalid_quantity_in_payload(db_session, mock_channel, mock_method, mock_properties):
    """Test OrderCreated with invalid quantity (negative)."""
    body = json.dumps({
        "event_id": str(uuid.uuid4()),
        "event_type": "OrderCreated",
        "tenant_id": "tenant-a",
        "payload": {"order_id": str(uuid.uuid4()), "sku": "laptop-001", "quantity": -5}
    }).encode()

    _process_message(
        lambda: db_session,
        mock_channel,
        mock_method,
        mock_properties,
        body,
        expected_types=(OrderCreated,),
        processor=lambda db, ev, eid: ("reserved", None),
    )

    # Pydantic validation should fail (quantity must be > 0), message goes to DLQ
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)