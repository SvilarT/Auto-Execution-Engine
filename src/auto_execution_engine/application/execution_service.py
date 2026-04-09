from __future__ import annotations

from dataclasses import dataclass

from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequestBuilder,
    BrokerSubmissionService,
)
from auto_execution_engine.domain.events.models import DomainEvent
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderStatus
from auto_execution_engine.domain.risk.models import KillSwitch, OrderIntent, RiskDecision
from auto_execution_engine.domain.risk.service import RiskService
from auto_execution_engine.trading_plane.leases import AccountLease, AccountLeaseService, LeaseError


class ExecutionRejectedError(ValueError):
    """Raised when order execution is blocked by a safety boundary."""


@dataclass(frozen=True)
class ExecutionResult:
    order: OrderAggregate
    events: list[DomainEvent]
    broker_order_id: str
    lease: AccountLease


class ExecutionApplicationService:
    def __init__(
        self,
        *,
        risk_service: RiskService,
        lease_service: AccountLeaseService,
        broker_request_builder: BrokerOrderRequestBuilder,
        broker_submission_service: BrokerSubmissionService,
    ) -> None:
        self._risk_service = risk_service
        self._lease_service = lease_service
        self._broker_request_builder = broker_request_builder
        self._broker_submission_service = broker_submission_service

    def execute_order(
        self,
        *,
        order: OrderAggregate,
        strategy_id: str,
        reference_price: float,
        kill_switch: KillSwitch,
        owner_id: str,
        existing_lease: AccountLease | None,
    ) -> ExecutionResult:
        intent = OrderIntent(
            account_id=order.account_id,
            symbol=order.symbol,
            side=order.side.value,
            quantity=order.quantity,
            notional=order.quantity * reference_price,
            strategy_id=strategy_id,
        )
        decision = self._risk_service.evaluate(intent=intent, kill_switch=kill_switch)
        if decision.decision is not RiskDecision.APPROVE:
            raise ExecutionRejectedError(
                f"risk rejected order {order.client_order_id}: {decision.reason_code}"
            )

        try:
            lease = self._lease_service.acquire(
                existing_lease=existing_lease,
                account_id=order.account_id,
                owner_id=owner_id,
            )
        except LeaseError as exc:
            raise ExecutionRejectedError(str(exc)) from exc

        risk_approved_order, risk_event = order.transition(OrderStatus.RISK_APPROVED)
        broker_request = self._broker_request_builder.build(order=risk_approved_order)
        broker_ack = self._broker_submission_service.register_submission(request=broker_request)
        submitted_order, submitted_event = risk_approved_order.transition(OrderStatus.SUBMITTED)

        return ExecutionResult(
            order=submitted_order,
            events=[risk_event, submitted_event],
            broker_order_id=broker_ack.broker_order_id,
            lease=lease,
        )
