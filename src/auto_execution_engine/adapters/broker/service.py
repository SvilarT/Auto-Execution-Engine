from __future__ import annotations

from dataclasses import dataclass, field

from auto_execution_engine.adapters.broker.models import (
    BrokerOrderAck,
    BrokerOrderRequest,
    BrokerOrderSide,
    BrokerOrderType,
)
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderSide, OrderType


class DuplicateSubmissionError(ValueError):
    """Raised when an already-submitted client order is submitted again."""


@dataclass
class IdempotentSubmissionBook:
    submitted_client_order_ids: set[str] = field(default_factory=set)

    def mark_submitted(self, *, client_order_id: str) -> None:
        if client_order_id in self.submitted_client_order_ids:
            raise DuplicateSubmissionError(
                f"client order {client_order_id} has already been submitted"
            )
        self.submitted_client_order_ids.add(client_order_id)


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
    def __init__(self, *, submission_book: IdempotentSubmissionBook) -> None:
        self._submission_book = submission_book

    def register_submission(self, *, request: BrokerOrderRequest) -> BrokerOrderAck:
        self._submission_book.mark_submitted(client_order_id=request.client_order_id)
        return BrokerOrderAck(
            account_id=request.account_id,
            client_order_id=request.client_order_id,
            broker_order_id=f"pending::{request.client_order_id}",
            accepted=True,
            message="submission registered",
        )
