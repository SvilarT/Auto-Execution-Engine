from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from auto_execution_engine.adapters.broker.models import (
    BrokerOrderAck,
    BrokerOrderRequest,
    BrokerOrderSide,
    BrokerOrderType,
)
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderSide, OrderType


class DuplicateSubmissionError(ValueError):
    """Raised when an already-submitted client order is submitted again."""


@dataclass(frozen=True)
class RegisteredSubmission:
    account_id: str
    client_order_id: str
    broker_order_id: str
    accepted: bool
    message: str

    def to_ack(self) -> BrokerOrderAck:
        return BrokerOrderAck(
            account_id=self.account_id,
            client_order_id=self.client_order_id,
            broker_order_id=self.broker_order_id,
            accepted=self.accepted,
            message=self.message,
        )


class SubmissionBook(Protocol):
    def mark_submitted(
        self, *, request: BrokerOrderRequest, submission: RegisteredSubmission
    ) -> None: ...

    def load_submission(self, *, client_order_id: str) -> RegisteredSubmission | None: ...


@dataclass
class IdempotentSubmissionBook:
    submissions_by_client_order_id: dict[str, RegisteredSubmission] = field(
        default_factory=dict
    )

    @property
    def submitted_client_order_ids(self) -> set[str]:
        return set(self.submissions_by_client_order_id)

    def mark_submitted(
        self, *, request: BrokerOrderRequest, submission: RegisteredSubmission
    ) -> None:
        if request.client_order_id in self.submissions_by_client_order_id:
            raise DuplicateSubmissionError(
                f"client order {request.client_order_id} has already been submitted"
            )
        self.submissions_by_client_order_id[request.client_order_id] = submission

    def load_submission(self, *, client_order_id: str) -> RegisteredSubmission | None:
        return self.submissions_by_client_order_id.get(client_order_id)


class BrokerOrderRequestBuilder:
    def build(self, *, order: OrderAggregate) -> BrokerOrderRequest:
        side = {
            OrderSide.BUY: BrokerOrderSide.BUY,
            OrderSide.SELL: BrokerOrderSide.SELL,
        }[order.side]
        order_type = {
            OrderType.MARKET: BrokerOrderType.MARKET,
            OrderType.LIMIT: BrokerOrderType.LIMIT,
        }[order.order_type]
        return BrokerOrderRequest(
            account_id=order.account_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=side,
            quantity=order.quantity,
            order_type=order_type,
        )


class BrokerSubmissionService:
    def __init__(self, *, submission_book: SubmissionBook) -> None:
        self._submission_book = submission_book

    def register_submission(self, *, request: BrokerOrderRequest) -> BrokerOrderAck:
        submission = RegisteredSubmission(
            account_id=request.account_id,
            client_order_id=request.client_order_id,
            broker_order_id=f"pending::{request.client_order_id}",
            accepted=True,
            message="submission registered",
        )
        self._submission_book.mark_submitted(request=request, submission=submission)
        return submission.to_ack()

    def load_submission(self, *, client_order_id: str) -> BrokerOrderAck | None:
        submission = self._submission_book.load_submission(
            client_order_id=client_order_id
        )
        if submission is None:
            return None
        return submission.to_ack()
