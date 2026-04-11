import pytest

from auto_execution_engine.adapters.broker.models import (
    BrokerRetryDisposition,
    BrokerSubmissionOutcome,
)
from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequestBuilder,
    BrokerSubmissionService,
    DuplicateSubmissionError,
    IdempotentSubmissionBook,
    RegisteredSubmission,
    RetryableBrokerSubmissionError,
    TerminalBrokerSubmissionError,
    UnknownBrokerSubmissionError,
)
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderSide, OrderType


class StaticSubmitter:
    def __init__(self, submission: RegisteredSubmission) -> None:
        self._submission = submission

    def submit(self, *, request):
        del request
        return self._submission


def make_order() -> OrderAggregate:
    return OrderAggregate(
        account_id="acct-1",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=1.5,
        order_type=OrderType.MARKET,
    )


def make_submission(
    *,
    order: OrderAggregate,
    outcome: BrokerSubmissionOutcome,
    retry_disposition: BrokerRetryDisposition = BrokerRetryDisposition.DO_NOT_RETRY,
    broker_order_id: str | None = None,
    message: str = "submission registered",
) -> RegisteredSubmission:
    return RegisteredSubmission(
        account_id=order.account_id,
        client_order_id=order.client_order_id,
        broker_order_id=broker_order_id,
        outcome=outcome,
        retry_disposition=retry_disposition,
        message=message,
    )


def test_broker_request_builder_is_deterministic() -> None:
    order = make_order()
    builder = BrokerOrderRequestBuilder()

    request = builder.build(order=order)

    assert request.account_id == "acct-1"
    assert request.client_order_id == order.client_order_id
    assert request.symbol == "BTC-USD"
    assert request.side == "buy"
    assert request.quantity == 1.5
    assert request.order_type == "market"


def test_duplicate_submission_is_rejected() -> None:
    order = make_order()
    builder = BrokerOrderRequestBuilder()
    request = builder.build(order=order)
    service = BrokerSubmissionService(submission_book=IdempotentSubmissionBook())

    ack = service.register_submission(request=request)
    assert ack.accepted is True
    assert ack.outcome is BrokerSubmissionOutcome.ACCEPTED
    assert ack.retry_disposition is BrokerRetryDisposition.DO_NOT_RETRY

    with pytest.raises(DuplicateSubmissionError):
        service.register_submission(request=request)


def test_broker_submission_service_returns_explicit_accepted_ack() -> None:
    order = make_order()
    request = BrokerOrderRequestBuilder().build(order=order)
    submission = make_submission(
        order=order,
        outcome=BrokerSubmissionOutcome.ACCEPTED,
        broker_order_id=f"broker::{order.client_order_id}",
    )
    service = BrokerSubmissionService(
        submission_book=IdempotentSubmissionBook(),
        submitter=StaticSubmitter(submission),
    )

    ack = service.register_submission(request=request)

    assert ack.accepted is True
    assert ack.broker_order_id == f"broker::{order.client_order_id}"
    assert ack.outcome is BrokerSubmissionOutcome.ACCEPTED
    assert ack.retry_disposition is BrokerRetryDisposition.DO_NOT_RETRY


def test_broker_submission_service_raises_terminal_error_for_rejection() -> None:
    order = make_order()
    request = BrokerOrderRequestBuilder().build(order=order)
    submission = make_submission(
        order=order,
        outcome=BrokerSubmissionOutcome.REJECTED,
        message="price bands rejected order",
    )
    service = BrokerSubmissionService(
        submission_book=IdempotentSubmissionBook(),
        submitter=StaticSubmitter(submission),
    )

    with pytest.raises(TerminalBrokerSubmissionError, match="price bands rejected order"):
        service.register_submission(request=request)

    persisted = service.load_submission(client_order_id=order.client_order_id)
    assert persisted is not None
    assert persisted.accepted is False
    assert persisted.outcome is BrokerSubmissionOutcome.REJECTED
    assert persisted.retry_disposition is BrokerRetryDisposition.DO_NOT_RETRY


def test_broker_submission_service_raises_unknown_error_for_non_retryable_unknown_outcome() -> None:
    order = make_order()
    request = BrokerOrderRequestBuilder().build(order=order)
    submission = make_submission(
        order=order,
        outcome=BrokerSubmissionOutcome.UNKNOWN,
        message="broker timed out after accepting transport connection",
    )
    service = BrokerSubmissionService(
        submission_book=IdempotentSubmissionBook(),
        submitter=StaticSubmitter(submission),
    )

    with pytest.raises(
        UnknownBrokerSubmissionError,
        match="broker timed out after accepting transport connection",
    ):
        service.register_submission(request=request)

    persisted = service.load_submission(client_order_id=order.client_order_id)
    assert persisted is not None
    assert persisted.outcome is BrokerSubmissionOutcome.UNKNOWN
    assert persisted.retry_disposition is BrokerRetryDisposition.DO_NOT_RETRY


def test_broker_submission_service_raises_retryable_error_without_persisting_failed_attempt() -> None:
    order = make_order()
    request = BrokerOrderRequestBuilder().build(order=order)
    submission = make_submission(
        order=order,
        outcome=BrokerSubmissionOutcome.FAILED,
        retry_disposition=BrokerRetryDisposition.SAFE_TO_RETRY,
        message="transport failed before broker receipt was confirmed",
    )
    service = BrokerSubmissionService(
        submission_book=IdempotentSubmissionBook(),
        submitter=StaticSubmitter(submission),
    )

    with pytest.raises(
        RetryableBrokerSubmissionError,
        match="transport failed before broker receipt was confirmed",
    ):
        service.register_submission(request=request)

    assert service.load_submission(client_order_id=order.client_order_id) is None
