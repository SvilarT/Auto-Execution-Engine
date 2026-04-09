import pytest

from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequestBuilder,
    BrokerSubmissionService,
    DuplicateSubmissionError,
    IdempotentSubmissionBook,
)
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderSide, OrderType



def make_order() -> OrderAggregate:
    return OrderAggregate(
        account_id="acct-1",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=1.5,
        order_type=OrderType.MARKET,
    )



def test_broker_request_builder_is_deterministic():
    order = make_order()
    builder = BrokerOrderRequestBuilder()

    request = builder.build(order=order)

    assert request.account_id == "acct-1"
    assert request.client_order_id == order.client_order_id
    assert request.symbol == "BTC-USD"
    assert request.side == "buy"
    assert request.quantity == 1.5
    assert request.order_type == "market"



def test_duplicate_submission_is_rejected():
    order = make_order()
    builder = BrokerOrderRequestBuilder()
    request = builder.build(order=order)
    service = BrokerSubmissionService(submission_book=IdempotentSubmissionBook())

    ack = service.register_submission(request=request)
    assert ack.accepted is True

    with pytest.raises(DuplicateSubmissionError):
        service.register_submission(request=request)
