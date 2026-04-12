from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from auto_execution_engine.adapters.broker.models import (
    BrokerOrderAck,
    BrokerSubmissionOutcome,
)
from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequestBuilder,
    BrokerSubmissionService,
    RetryableBrokerSubmissionError,
    TerminalBrokerSubmissionError,
    UnknownBrokerSubmissionError,
)
from auto_execution_engine.domain.events.models import DomainEvent
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderFill, OrderStatus
from auto_execution_engine.domain.risk.models import KillSwitch, OrderIntent, RiskDecision
from auto_execution_engine.domain.risk.service import RiskService
from auto_execution_engine.reconciliation.service import ReconciliationQuarantineError
from auto_execution_engine.trading_plane.leases import AccountLease, AccountLeaseService, LeaseError


class DurableOrderStore(Protocol):
    def record_events(self, *, events: list[DomainEvent]) -> None: ...

    def load_latest_order(self, *, client_order_id: str) -> OrderAggregate | None: ...

    def load_order(self, *, client_order_id: str) -> OrderAggregate: ...

    def ingest_fill(
        self,
        *,
        client_order_id: str,
        fill: OrderFill,
    ) -> tuple[OrderAggregate, DomainEvent]: ...


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


@dataclass(frozen=True)
class FillIngestionResult:
    order: OrderAggregate
    event: DomainEvent


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
        recovered_submission = self._broker_submission_service.load_submission(
            client_order_id=order.client_order_id
        )
        if recovered_submission is not None:
            lease = self._acquire_execution_lease(
                existing_lease=existing_lease,
                account_id=order.account_id,
                owner_id=owner_id,
            )
            repaired_order, recovery_events = self._recover_order_from_submission(
                order=order,
                submission=recovered_submission,
            )
            if recovery_events and self._order_store is not None:
                self._order_store.record_events(events=recovery_events)
            if recovered_submission.outcome is BrokerSubmissionOutcome.ACCEPTED:
                return ExecutionResult(
                    order=repaired_order,
                    events=recovery_events,
                    broker_order_id=recovered_submission.broker_order_id or "",
                    lease=lease,
                )
            raise ExecutionRejectedError(
                self._message_for_nonaccepted_submission(
                    client_order_id=order.client_order_id,
                    submission=recovered_submission,
                )
            )

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

        lease = self._acquire_execution_lease(
            existing_lease=existing_lease,
            account_id=order.account_id,
            owner_id=owner_id,
        )

        risk_approved_order, risk_event = order.transition(OrderStatus.RISK_APPROVED)
        broker_request = self._broker_request_builder.build(order=risk_approved_order)

        try:
            broker_ack = self._broker_submission_service.register_submission(
                request=broker_request
            )
        except RetryableBrokerSubmissionError as exc:
            raise ExecutionRejectedError(
                f"broker submission for order {order.client_order_id} failed safely and may be retried: {exc}"
            ) from exc
        except TerminalBrokerSubmissionError as exc:
            recovered_order, recovery_events = self._recover_order_from_submission(
                order=order,
                submission=exc.submission.to_ack(),
            )
            if recovery_events and self._order_store is not None:
                self._order_store.record_events(events=recovery_events)
            raise ExecutionRejectedError(
                f"broker rejected order {order.client_order_id}: {exc}"
            ) from exc
        except UnknownBrokerSubmissionError as exc:
            recovered_order, recovery_events = self._recover_order_from_submission(
                order=order,
                submission=exc.submission.to_ack(),
            )
            if recovery_events and self._order_store is not None:
                self._order_store.record_events(events=recovery_events)
            raise ExecutionRejectedError(
                f"broker submission outcome for order {order.client_order_id} is unknown and cannot be retried safely: {exc}"
            ) from exc

        submitted_order, submitted_event = risk_approved_order.transition(OrderStatus.SUBMITTED)

        if self._order_store is not None:
            self._order_store.record_events(events=[risk_event, submitted_event])

        return ExecutionResult(
            order=submitted_order,
            events=[risk_event, submitted_event],
            broker_order_id=broker_ack.broker_order_id or "",
            lease=lease,
        )

    def recover_order(self, *, client_order_id: str) -> OrderAggregate:
        if self._order_store is None:
            raise ExecutionRejectedError("durable order store is not configured")
        return self._order_store.load_order(client_order_id=client_order_id)

    def ingest_fill(
        self,
        *,
        client_order_id: str,
        fill_id: str,
        fill_quantity: float,
        fill_price: float,
        occurred_at: datetime,
        broker_order_id: str | None = None,
        source: str = "broker",
    ) -> FillIngestionResult:
        if self._order_store is None:
            raise ExecutionRejectedError("durable order store is not configured")

        try:
            order, event = self._order_store.ingest_fill(
                client_order_id=client_order_id,
                fill=OrderFill(
                    fill_id=fill_id,
                    quantity=fill_quantity,
                    price=fill_price,
                    occurred_at=occurred_at,
                    broker_order_id=broker_order_id,
                    source=source,
                ),
            )
        except ValueError as exc:
            raise ExecutionRejectedError(str(exc)) from exc

        return FillIngestionResult(order=order, event=event)

    def _acquire_execution_lease(
        self,
        *,
        existing_lease: AccountLease | None,
        account_id: str,
        owner_id: str,
    ) -> AccountLease:
        try:
            return self._lease_service.acquire(
                existing_lease=existing_lease,
                account_id=account_id,
                owner_id=owner_id,
            )
        except LeaseError as exc:
            raise ExecutionRejectedError(str(exc)) from exc

    def _recover_order_from_submission(
        self, *, order: OrderAggregate, submission: BrokerOrderAck
    ) -> tuple[OrderAggregate, list[DomainEvent]]:
        recovery_events: list[DomainEvent] = []

        if order.status is OrderStatus.CREATED and submission.outcome in {
            BrokerSubmissionOutcome.ACCEPTED,
            BrokerSubmissionOutcome.REJECTED,
            BrokerSubmissionOutcome.UNKNOWN,
        }:
            order, risk_event = order.transition(OrderStatus.RISK_APPROVED)
            recovery_events.append(risk_event)

        if (
            submission.outcome is BrokerSubmissionOutcome.ACCEPTED
            and order.status is OrderStatus.RISK_APPROVED
        ):
            order, submitted_event = order.transition(OrderStatus.SUBMITTED)
            recovery_events.append(submitted_event)
        elif (
            submission.outcome is BrokerSubmissionOutcome.REJECTED
            and order.status is OrderStatus.RISK_APPROVED
        ):
            order, rejected_event = order.transition(OrderStatus.REJECTED)
            recovery_events.append(rejected_event)

        return order, recovery_events

    def _message_for_nonaccepted_submission(
        self, *, client_order_id: str, submission: BrokerOrderAck
    ) -> str:
        if submission.outcome is BrokerSubmissionOutcome.REJECTED:
            return f"broker rejected order {client_order_id}: {submission.message}"
        return (
            f"broker submission outcome for order {client_order_id} is unknown and "
            f"cannot be retried safely: {submission.message}"
        )

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
