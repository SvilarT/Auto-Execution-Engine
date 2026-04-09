from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4

from auto_execution_engine.domain.events.models import DomainEvent, EventType


class OrderError(ValueError):
    """Raised when an invalid order lifecycle transition is attempted."""


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(str, Enum):
    CREATED = "created"
    RISK_APPROVED = "risk_approved"
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCEL_PENDING = "cancel_pending"
    CANCELLED = "cancelled"
    RECONCILED = "reconciled"


_ALLOWED_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.CREATED: {OrderStatus.RISK_APPROVED, OrderStatus.REJECTED},
    OrderStatus.RISK_APPROVED: {OrderStatus.SUBMITTED, OrderStatus.REJECTED},
    OrderStatus.SUBMITTED: {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.REJECTED,
        OrderStatus.CANCEL_PENDING,
    },
    OrderStatus.ACKNOWLEDGED: {
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.REJECTED,
        OrderStatus.CANCEL_PENDING,
    },
    OrderStatus.PARTIALLY_FILLED: {
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCEL_PENDING,
    },
    OrderStatus.CANCEL_PENDING: {OrderStatus.CANCELLED, OrderStatus.PARTIALLY_FILLED},
    OrderStatus.FILLED: {OrderStatus.RECONCILED},
    OrderStatus.CANCELLED: {OrderStatus.RECONCILED},
    OrderStatus.REJECTED: {OrderStatus.RECONCILED},
    OrderStatus.RECONCILED: set(),
}


_EVENT_BY_STATUS: dict[OrderStatus, EventType] = {
    OrderStatus.CREATED: EventType.ORDER_CREATED,
    OrderStatus.RISK_APPROVED: EventType.RISK_APPROVED,
    OrderStatus.SUBMITTED: EventType.ORDER_SUBMITTED,
    OrderStatus.ACKNOWLEDGED: EventType.ORDER_ACKNOWLEDGED,
    OrderStatus.PARTIALLY_FILLED: EventType.ORDER_PARTIALLY_FILLED,
    OrderStatus.FILLED: EventType.ORDER_FILLED,
    OrderStatus.REJECTED: EventType.ORDER_REJECTED,
    OrderStatus.CANCEL_PENDING: EventType.ORDER_CANCEL_REQUESTED,
    OrderStatus.CANCELLED: EventType.ORDER_CANCELLED,
    OrderStatus.RECONCILED: EventType.ORDER_RECONCILED,
}


@dataclass(frozen=True)
class OrderAggregate:
    """Deterministic internal representation of a trade order."""

    account_id: str
    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    client_order_id: str = field(default_factory=lambda: str(uuid4()))
    status: OrderStatus = OrderStatus.CREATED
    filled_quantity: float = 0.0
    average_fill_price: float | None = None

    def create_event(self) -> DomainEvent:
        return self._event_for_status(self.status)

    def transition(
        self,
        next_status: OrderStatus,
        *,
        fill_quantity_delta: float = 0.0,
        fill_price: float | None = None,
    ) -> tuple["OrderAggregate", DomainEvent]:
        allowed = _ALLOWED_TRANSITIONS[self.status]
        if next_status not in allowed:
            raise OrderError(
                f"invalid transition from {self.status.value} to {next_status.value}"
            )

        new_filled_quantity = self.filled_quantity + fill_quantity_delta
        if new_filled_quantity < 0:
            raise OrderError("filled quantity cannot become negative")
        if new_filled_quantity > self.quantity:
            raise OrderError("filled quantity cannot exceed total quantity")

        average_fill_price = self.average_fill_price
        if fill_price is not None:
            average_fill_price = fill_price

        updated = OrderAggregate(
            account_id=self.account_id,
            symbol=self.symbol,
            side=self.side,
            quantity=self.quantity,
            order_type=self.order_type,
            client_order_id=self.client_order_id,
            status=next_status,
            filled_quantity=new_filled_quantity,
            average_fill_price=average_fill_price,
        )
        return updated, updated._event_for_status(next_status)

    def _event_for_status(self, status: OrderStatus) -> DomainEvent:
        return DomainEvent(
            event_type=_EVENT_BY_STATUS[status],
            aggregate_id=self.client_order_id,
            account_id=self.account_id,
            payload={
                "status": status.value,
                "symbol": self.symbol,
                "side": self.side.value,
                "quantity": self.quantity,
                "filled_quantity": self.filled_quantity,
                "order_type": self.order_type.value,
                "average_fill_price": self.average_fill_price,
            },
        )
