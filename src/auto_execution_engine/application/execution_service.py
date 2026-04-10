from dataclasses import dataclass
from typing import Protocol

from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequestBuilder,
    BrokerSubmissionService,
)
from auto_execution_engine.domain.events.models import DomainEvent
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderStatus
from auto_execution_engine.domain.risk.models import KillSwitch, OrderIntent, RiskDecision
from auto_execution_engine.domain.risk.service import RiskService
from auto_execution_engine.reconciliation.service import ReconciliationQuarantineError
from auto_execution_engine.trading_plane.leases import AccountLease, AccountLeaseService, LeaseError


class DurableOrderStore(Protocol):
    def record_events(self, *, events: list[DomainEvent]) -> None: ...

    def load_latest_order(self, *, client_order_id: str) -> OrderAggregate | None: ...

    def load_order(self, *, client_order_id: str) -> OrderAggregate: ...


class AccountExecutionGate(Protocol):
    def ensure_account_clear(self, *, account_id: str) -> None: ...


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
        order_store: DurableOrderStore | None = None,
        account_execution_gate: AccountExecutionGate | None = None,
    ) -> None:
        self._risk_service = risk_service
        self._lease_service = lease_service
        self._broker_request_builder = broker_request_builder
        self._broker_submission_service = broker_submission_service
        self._order_store = order_store
        self._account_execution_gate = account_execution_gate

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
        self._ensure_account_execution_allowed(account_id=order.account_id)
        order = self._restore_or_record_order(order=order)
        if order.status is not OrderStatus.CREATED:
            raise ExecutionRejectedError(
                "order "
                f"{order.client_order_id} is already in durable state {order.status.value} "
                "and cannot be executed again"
            )

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

        if self._order_store is not None:
            self._order_store.record_events(events=[risk_event, submitted_event])

        return ExecutionResult(
            order=submitted_order,
            events=[risk_event, submitted_event],
            broker_order_id=broker_ack.broker_order_id,
            lease=lease,
        )

    def recover_order(self, *, client_order_id: str) -> OrderAggregate:
        if self._order_store is None:
            raise ExecutionRejectedError("durable order store is not configured")
        return self._order_store.load_order(client_order_id=client_order_id)

    def _ensure_account_execution_allowed(self, *, account_id: str) -> None:
        if self._account_execution_gate is None:
            return

        try:
            self._account_execution_gate.ensure_account_clear(account_id=account_id)
        except ReconciliationQuarantineError as exc:
            raise ExecutionRejectedError(str(exc)) from exc

    def _restore_or_record_order(self, *, order: OrderAggregate) -> OrderAggregate:
        if self._order_store is None:
            return order

        recovered_order = self._order_store.load_latest_order(
            client_order_id=order.client_order_id
        )
        if recovered_order is not None:
            return recovered_order

        self._order_store.record_events(events=[order.create_event()])
        return order
