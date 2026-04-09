from auto_execution_engine.domain.events.models import EventType
from auto_execution_engine.domain.orders.models import (
    OrderAggregate,
    OrderError,
    OrderSide,
    OrderStatus,
    OrderType,
)



def make_order() -> OrderAggregate:
    return OrderAggregate(
        account_id="acct-1",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=2.0,
        order_type=OrderType.MARKET,
    )



def test_order_starts_created_with_creation_event():
    order = make_order()

    event = order.create_event()

    assert order.status is OrderStatus.CREATED
    assert event.event_type is EventType.ORDER_CREATED
    assert event.payload["status"] == "created"



def test_valid_transition_sequence_to_reconciled():
    order = make_order()

    order, event = order.transition(OrderStatus.RISK_APPROVED)
    assert order.status is OrderStatus.RISK_APPROVED
    assert event.event_type is EventType.RISK_APPROVED

    order, event = order.transition(OrderStatus.SUBMITTED)
    assert order.status is OrderStatus.SUBMITTED
    assert event.event_type is EventType.ORDER_SUBMITTED

    order, event = order.transition(OrderStatus.ACKNOWLEDGED)
    assert order.status is OrderStatus.ACKNOWLEDGED
    assert event.event_type is EventType.ORDER_ACKNOWLEDGED

    order, event = order.transition(
        OrderStatus.PARTIALLY_FILLED,
        fill_quantity_delta=1.0,
        fill_price=50000.0,
    )
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.filled_quantity == 1.0
    assert order.average_fill_price == 50000.0
    assert event.event_type is EventType.ORDER_PARTIALLY_FILLED

    order, event = order.transition(
        OrderStatus.FILLED,
        fill_quantity_delta=1.0,
        fill_price=50100.0,
    )
    assert order.status is OrderStatus.FILLED
    assert order.filled_quantity == 2.0
    assert order.average_fill_price == 50100.0
    assert event.event_type is EventType.ORDER_FILLED

    order, event = order.transition(OrderStatus.RECONCILED)
    assert order.status is OrderStatus.RECONCILED
    assert event.event_type is EventType.ORDER_RECONCILED



def test_invalid_transition_is_rejected():
    order = make_order()

    try:
        order.transition(OrderStatus.SUBMITTED)
        assert False, "expected OrderError"
    except OrderError as exc:
        assert "invalid transition" in str(exc)



def test_fill_quantity_cannot_exceed_order_quantity():
    order = make_order()
    order, _ = order.transition(OrderStatus.RISK_APPROVED)
    order, _ = order.transition(OrderStatus.SUBMITTED)

    try:
        order.transition(OrderStatus.FILLED, fill_quantity_delta=3.0)
        assert False, "expected OrderError"
    except OrderError as exc:
        assert "cannot exceed" in str(exc)
