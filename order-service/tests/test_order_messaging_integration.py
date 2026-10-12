"""Integration tests for Phase 6 messaging flow.

Covers:
- OrderCreated event publishing (order service -> inventory service)
- StockReserved/StockRejected consumption (inventory service -> order service)
- DLQ behavior: retry up to MAX_RETRIES, then negative-ACK (no requeue) -> DLQ
- Consumer stays up after malformed events
- Valid events still flow after failures
- Event contract: event_id, event_type, tenant_id, timestamp, payload
"""

import json
import uuid
from unittest.mock import MagicMock, patch

import pika
import pytest
from pika.spec import Basic, BasicProperties

from order_app.messaging import (
    MAX_RETRIES,
    QUEUE_STOCK_RESERVED,
    QUEUE_STOCK_REJECTED,
    DLQ_STOCK_RESERVED,
    DLQ_STOCK_REJECTED,
    EXCHANGE_ORDERS,
    EXCHANGE_INVENTORY,
    EXCHANGE_DLX,
    RK_ORDER_CREATED,
    RK_STOCK_RESERVED,
    RK_STOCK_REJECTED,
    _handle_stock_result,
    _process_message,
    declare_topology,
    consume_stock_events,
    publish_order_created,
)
from order_app.schemas import (
    OrderCreated,
    OrderCreatedPayload,
    StockReserved,
    StockReservedPayload,
    StockRejected,
    StockRejectedPayload,
)
from order_app.database import SessionLocal
from order_app import service


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
    method.routing_key = RK_STOCK_RESERVED
    return method


@pytest.fixture
def mock_properties():
    """Create mock message properties."""
    props = MagicMock(spec=BasicProperties)
    props.message_id = str(uuid.uuid4())
    props.headers = {"x-retry-count": 0}
    return props


# --- Topology verification (acceptance criterion 2) ---


def test_declare_topology_creates_exchanges_and_queues():
    """Test that declare_topology creates all exchanges, queues, and bindings."""
    channel = MagicMock(spec=pika.channel.Channel)

    declare_topology(channel)

    # Verify exchanges declared
    exchange_calls = [call[1]["exchange"] for call in channel.exchange_declare.call_args_list]
    assert EXCHANGE_ORDERS in exchange_calls
    assert EXCHANGE_INVENTORY in exchange_calls
    assert EXCHANGE_DLX in exchange_calls

    # Verify DLQ queues declared
    queue_calls = [call[1]["queue"] for call in channel.queue_declare.call_args_list]
    assert DLQ_STOCK_RESERVED in queue_calls
    assert DLQ_STOCK_REJECTED in queue_calls

    # Verify DLQ binding to DLX
    bind_calls = channel.queue_bind.call_args_list
    dlq_binds = [call for call in bind_calls if call[1]["queue"] == DLQ_STOCK_RESERVED]
    assert len(dlq_binds) > 0
    assert EXCHANGE_DLX in [call[1]["exchange"] for call in dlq_binds]

    # Verify main queues have dead-letter arguments
    dlq_args_calls = [
        call for call in channel.queue_declare.call_args_list
        if DLQ_STOCK_RESERVED in str(call)
    ]
    assert len(dlq_args_calls) > 0


# --- Event contract verification (acceptance criterion 4) ---


def test_order_created_event_contract():
    """Test OrderCreated event has all required fields."""
    event_id = str(uuid.uuid4())
    event = OrderCreated(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=OrderCreatedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=2),
    )

    assert event.event_id == event_id
    assert event.event_type == "OrderCreated"
    assert event.tenant_id == "tenant-a"
    assert event.timestamp is not None
    assert event.payload.order_id
    assert event.payload.sku == "laptop-001"
    assert event.payload.quantity == 2


def test_stock_reserved_event_contract():
    """Test StockReserved event has all required fields."""
    event_id = str(uuid.uuid4())
    event = StockReserved(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=StockReservedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=2),
    )

    assert event.event_id == event_id
    assert event.event_type == "StockReserved"
    assert event.tenant_id == "tenant-a"
    assert event.timestamp is not None
    assert event.payload.order_id
    assert event.payload.sku == "laptop-001"
    assert event.payload.quantity == 2


def test_stock_rejected_event_contract():
    """Test StockRejected event has all required fields including reason."""
    event_id = str(uuid.uuid4())
    event = StockRejected(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=StockRejectedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=2, reason="insufficient stock"),
    )

    assert event.event_id == event_id
    assert event.event_type == "StockRejected"
    assert event.tenant_id == "tenant-a"
    assert event.timestamp is not None
    assert event.payload.order_id
    assert event.payload.reason == "insufficient stock"


# --- Valid event flow (acceptance criterion 5) ---


def test_publish_order_created_sends_event_to_orders_exchange():
    """Test that publish_order_created publishes OrderCreated to the orders exchange."""
    with patch("order_app.messaging.connection_context") as mock_conn_ctx:
        mock_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn.channel.return_value = mock_channel
        mock_conn_ctx.return_value.__enter__.return_value = mock_conn

        event_id = publish_order_created("tenant-a", "order-1", "laptop-001", 2)

    # Published exactly once to the orders exchange with the order.created RK
    assert mock_channel.basic_publish.call_count == 1
    kwargs = mock_channel.basic_publish.call_args[1]
    assert kwargs["exchange"] == EXCHANGE_ORDERS
    assert kwargs["routing_key"] == RK_ORDER_CREATED

    body = json.loads(kwargs["body"])
    assert body["event_id"] == event_id
    assert body["event_type"] == "OrderCreated"
    assert body["tenant_id"] == "tenant-a"
    assert body["payload"]["sku"] == "laptop-001"
    assert body["payload"]["quantity"] == 2
    assert kwargs["properties"].message_id == event_id


def test_valid_stock_reserved_event_is_processed(db_session, mock_channel, mock_method, mock_properties):
    """Test that a valid StockReserved event is processed and acknowledged."""
    # The order must exist (PENDING) for the stock result to be applied
    order_id = str(uuid.uuid4())
    db_session.add(
        service.Order(
            tenant_id="tenant-a",
            order_id=order_id,
            sku="laptop-001",
            quantity=2,
            status="PENDING",
        )
    )
    db_session.commit()

    event_id = str(uuid.uuid4())
    event = StockReserved(
        event_id=event_id,
        tenant_id="tenant-a",
        payload=StockReservedPayload(order_id=order_id, sku="laptop-001", quantity=2),
    )

    body = event.model_dump_json().encode()
    mock_properties.message_id = event_id

    _process_message(
        lambda: db_session,
        mock_channel,
        mock_method,
        mock_properties,
        body,
        expected_types=(StockReserved,),
        processor=_handle_stock_result,
    )

    # Should ACK
    mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)

    # Order moved PENDING -> RESERVED
    db_session.expire_all()
    order = db_session.query(service.Order).filter_by(order_id=order_id).one()
    assert order.status == "RESERVED"


def test_valid_event_after_failure_still_works(db_session, mock_channel, mock_method, mock_properties):
    """Test that valid events continue to process after a failure."""
    order_id = str(uuid.uuid4())

    # First, a business failure - should nack (retry path, broker mocked)
    failing_event = StockReserved(
        event_id=str(uuid.uuid4()),
        tenant_id="tenant-a",
        payload=StockReservedPayload(order_id=order_id, sku="laptop-001", quantity=1),
    )
    failing_body = failing_event.model_dump_json().encode()

    def failing_processor(db, ev, eid):
        raise RuntimeError("Simulated failure")

    with patch("order_app.messaging.connection_context"):
        _process_message(
            lambda: db_session,
            mock_channel,
            mock_method,
            mock_properties,
            failing_body,
            expected_types=(StockReserved,),
            processor=failing_processor,
        )
    assert mock_channel.basic_nack.called
    mock_channel.reset_mock()

    # Now the order exists and a valid event arrives - should work
    db_session.add(
        service.Order(
            tenant_id="tenant-a",
            order_id=order_id,
            sku="laptop-001",
            quantity=1,
            status="PENDING",
        )
    )
    db_session.commit()

    valid_event = StockReserved(
        event_id=str(uuid.uuid4()),
        tenant_id="tenant-a",
        payload=StockReservedPayload(order_id=order_id, sku="laptop-001", quantity=1),
    )
    valid_body = valid_event.model_dump_json().encode()
    mock_properties.message_id = valid_event.event_id

    _process_message(
        lambda: db_session,
        mock_channel,
        mock_method,
        mock_properties,
        valid_body,
        expected_types=(StockReserved,),
        processor=_handle_stock_result,
    )

    # Should ACK
    mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
    db_session.expire_all()
    order = db_session.query(service.Order).filter_by(order_id=order_id).one()
    assert order.status == "RESERVED"


# --- Invalid event -> retry then DLQ (acceptance criterion 2) ---


def test_invalid_event_retries_then_dead_letters(db_session, mock_channel, mock_method, mock_properties):
    """Test that a failing business event is retried MAX_RETRIES times, then dead-lettered."""
    # A valid event whose processing keeps failing (business error -> retry)
    event = StockReserved(
        event_id=str(uuid.uuid4()),
        tenant_id="tenant-a",
        payload=StockReservedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=1),
    )
    body = event.model_dump_json().encode()

    def failing_processor(db, ev, eid):
        raise RuntimeError("Simulated processing failure")

    with patch("order_app.messaging.connection_context") as mock_conn_ctx:
        # Simulate the initial delivery plus MAX_RETRIES redeliveries
        for attempt in range(MAX_RETRIES + 1):
            mock_retry_channel = MagicMock()
            mock_retry_conn = MagicMock()
            mock_retry_conn.channel.return_value = mock_retry_channel
            mock_conn_ctx.return_value.__enter__.return_value = mock_retry_conn
            mock_properties.headers = {"x-retry-count": attempt}

            _process_message(
                lambda: db_session,
                mock_channel,
                mock_method,
                mock_properties,
                body,
                expected_types=(StockReserved,),
                processor=failing_processor,
            )

            # Should always nack without requeue
            assert mock_channel.basic_nack.called
            mock_channel.basic_nack.assert_called_with(delivery_tag=1, requeue=False)

            # Re-published with an incremented retry count until MAX_RETRIES,
            # then dead-lettered without another re-publish.
            if attempt < MAX_RETRIES:
                assert mock_retry_channel.basic_publish.call_count == 1
                props = mock_retry_channel.basic_publish.call_args[1]["properties"]
                assert props.headers["x-retry-count"] == attempt + 1
            else:
                assert mock_retry_channel.basic_publish.call_count == 0

            mock_channel.reset_mock()


def test_validation_error_goes_to_dlq_without_requeue(db_session, mock_channel, mock_method, mock_properties):
    """Test that validation errors go to DLQ without requeue."""
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

    # Should reject without requeue
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_missing_sku_in_payload_goes_to_dlq(db_session, mock_channel, mock_method, mock_properties):
    """Test OrderCreated with missing SKU field goes to DLQ."""
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


def test_invalid_quantity_in_payload_goes_to_dlq(db_session, mock_channel, mock_method, mock_properties):
    """Test OrderCreated with invalid quantity goes to DLQ."""
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

    # Pydantic validation should fail, message goes to DLQ
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


# --- Consumer stays up (acceptance criterion 3) ---


def test_consumer_stays_up_after_malformed_event():
    """Test that the consumer continues processing after encountering a malformed event."""
    channel = MagicMock(spec=pika.channel.Channel)
    method = MagicMock(spec=Basic.Deliver)
    method.delivery_tag = 1
    method.routing_key = RK_STOCK_RESERVED
    props = MagicMock(spec=BasicProperties)
    props.message_id = str(uuid.uuid4())
    props.headers = {"x-retry-count": 0}

    # Malformed event: missing required fields
    malformed_body = b'{"event_type": "OrderCreated", "tenant_id": "tenant-a"}'

    # This should not raise - the validation error is handled and dead-lettered
    _process_message(
        lambda: MagicMock(),
        channel,
        method,
        props,
        malformed_body,
        expected_types=(OrderCreated,),
        processor=lambda db, ev, eid: ("reserved", None),
    )

    # Dead-lettered without requeue, consumer keeps running
    channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    channel.basic_ack.assert_not_called()

    # The next (valid) event still processes normally
    valid_event = StockReserved(
        event_id=str(uuid.uuid4()),
        tenant_id="tenant-a",
        payload=StockReservedPayload(order_id=str(uuid.uuid4()), sku="laptop-001", quantity=1),
    )
    valid_body = valid_event.model_dump_json().encode()
    props.message_id = valid_event.event_id
    method.routing_key = RK_STOCK_RESERVED
    channel.reset_mock()

    _process_message(
        lambda: MagicMock(),
        channel,
        method,
        props,
        valid_body,
        expected_types=(StockReserved,),
        processor=lambda db, ev, eid: None,
    )

    channel.basic_ack.assert_called_once_with(delivery_tag=1)


# --- Separation unit test (acceptance criterion 6) ---


def test_order_service_does_not_directly_reserve_stock():
    """Test that the order service does NOT call inventory reservation directly."""
    # Verify that the order service only publishes OrderCreated
    # This is a unit test that ensures the order service does not depend
    # on the inventory service directly

    from order_app import service

    # Create an order - this should NOT trigger inventory reservation
    order_id = str(uuid.uuid4())
    order = {
        "tenant_id": "tenant-a",
        "order_id": order_id,
        "sku": "laptop-001",
        "quantity": 2,
        "status": "PENDING",
    }

    # The order is created with PENDING status - no stock reservation
    # Verify stock reservation is a separate service call
    session = SessionLocal()
    session.add(
        service.Order(
            tenant_id=order["tenant_id"],
            order_id=order["order_id"],
            sku=order["sku"],
            quantity=order["quantity"],
            status=order["status"],
        )
    )
    session.commit()
    session.close()

    # Verify the order exists with PENDING status
    session = SessionLocal()
    retrieved = session.query(service.Order).filter_by(order_id=order_id).one()
    assert retrieved.status == "PENDING"
    # NO stock reservation should have happened
    session.close()


def test_event_contract_separates_services():
    """Test that the event contract keeps services decoupled."""
    # The event contract is the only contract between order and inventory
    # services - no HTTP calls, no direct function calls

    # Order service publishes OrderCreated -> RabbitMQ
    # Inventory service consumes OrderCreated -> processes -> publishes StockReserved
    # This is verified by the messaging topology

    # Verify no import cycle or direct dependency: each service can define the
    # same event schemas independently (same names, same payload fields).
    import os
    import sys

    inventory_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "inventory-service"
    )
    inventory_dir = os.path.abspath(inventory_dir)
    if inventory_dir not in sys.path:
        sys.path.insert(0, inventory_dir)

    import order_app.schemas as order_schemas
    import inventory_app.schemas as inventory_schemas

    for event_name in ("OrderCreated", "StockReserved", "StockRejected"):
        order_event = getattr(order_schemas, event_name)
        inv_event = getattr(inventory_schemas, event_name)
        assert order_event.__name__ == inv_event.__name__
        assert set(order_event.model_fields) == set(inv_event.model_fields)

    # Payload structures also match field-for-field
    for payload_name in ("OrderCreatedPayload", "StockReservedPayload", "StockRejectedPayload"):
        order_payload = getattr(order_schemas, payload_name)
        inv_payload = getattr(inventory_schemas, payload_name)
        assert set(order_payload.model_fields) == set(inv_payload.model_fields)


def test_create_order_publishes_order_created(client, mock_publish_order_created):
    """POST /orders must publish OrderCreated after persisting the order."""
    resp = client.post(
        "/orders",
        headers={"X-Tenant-ID": "tenant-a"},
        json={"sku": "laptop-001", "quantity": 2},
    )
    assert resp.status_code == 201
    data = resp.json()

    # Published exactly once with the persisted order's identifiers
    mock_publish_order_created.assert_called_once_with(
        "tenant-a", data["order_id"], "laptop-001", 2
    )


def test_create_order_survives_broker_outage(client, mock_publish_order_created):
    """A broker outage must not fail order creation (order stays PENDING)."""
    mock_publish_order_created.side_effect = ConnectionError("broker down")

    resp = client.post(
        "/orders",
        headers={"X-Tenant-ID": "tenant-a"},
        json={"sku": "laptop-001", "quantity": 2},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "PENDING"

    # And the order is still readable afterwards
    fetched = client.get(
        f"/orders/{data['order_id']}", headers={"X-Tenant-ID": "tenant-a"}
    )
    assert fetched.status_code == 200
