from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
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
from auto_execution_engine.domain.events.models import DomainEvent, EventType
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderFill, OrderStatus
from auto_execution_engine.domain.risk.models import (
    AccountExposureSnapshot,
    KillSwitch,
    KillSwitchState,
    OrderIntent,
    RiskDecision,
)
from auto_execution_engine.domain.risk.service import RiskService
from auto_execution_engine.observability_models import OperatorActionRecord, RuntimeHealthSummary
from auto_execution_engine.observability_service import AccountRuntimeSnapshot, RuntimeHealthService
from auto_execution_engine.reconciliation.models import CashSnapshot, PositionSnapshot
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

    def list_internal_order_snapshots(self, *, account_id: str | None = None) -> list: ...

    def project_internal_positions(self, *, account_id: str) -> list[PositionSnapshot]: ...

    def project_internal_cash(
        self,
        *,
        account_id: str,
        opening_balance: float = 0.0,
    ) -> CashSnapshot: ...

    def project_internal_exposure(self, *, account_id: str) -> AccountExposureSnapshot: ...

    def load_latest_reconciliation_report(self, *, account_id: str): ...

    def load_latest_reconciliation_run(self, *, account_id: str): ...

    def record_operator_action(self, *, record: OperatorActionRecord) -> None: ...

    def list_operator_actions(self, *, account_id: str, limit: int = 50) -> list[OperatorActionRecord]: ...

    def record_runtime_health_summary(self, *, summary: RuntimeHealthSummary) -> None: ...

    def load_latest_runtime_health_summary(
        self, *, account_id: str
    ) -> RuntimeHealthSummary | None: ...

    def list_runtime_health_summaries(
        self, *, account_id: str, limit: int = 50
    ) -> list[RuntimeHealthSummary]: ...

    def list_accounts_with_runtime_health(self) -> list[str]: ...


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
        decision = self._risk_service.evaluate(
            intent=intent,
            kill_switch=kill_switch,
            current_exposure=self._load_current_exposure(account_id=order.account_id),
        )
        if decision.decision is not RiskDecision.APPROVE:
            rejection_event = self._build_risk_event(
                order=order,
                strategy_id=strategy_id,
                decision=decision,
                approved=False,
            )
            if self._order_store is not None:
                self._order_store.record_events(events=[rejection_event])
            raise ExecutionRejectedError(
                f"risk rejected order {order.client_order_id}: {decision.reason_code}"
            )

        lease = self._acquire_execution_lease(
            existing_lease=existing_lease,
            account_id=order.account_id,
            owner_id=owner_id,
        )

        risk_approved_order, _ = order.transition(OrderStatus.RISK_APPROVED)
        risk_event = self._build_risk_event(
            order=order,
            strategy_id=strategy_id,
            decision=decision,
            approved=True,
        )
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

    def _load_current_exposure(self, *, account_id: str) -> AccountExposureSnapshot | None:
        if self._order_store is None:
            return None
        return self._order_store.project_internal_exposure(account_id=account_id)

    def _build_risk_event(
        self,
        *,
        order: OrderAggregate,
        strategy_id: str,
        decision,
        approved: bool,
    ) -> DomainEvent:
        payload: dict[str, str | int | float | bool | None] = {
            "client_order_id": order.client_order_id,
            "account_id": order.account_id,
            "symbol": order.symbol,
            "side": order.side.value,
            "quantity": order.quantity,
            "order_type": order.order_type.value,
            "status": (
                OrderStatus.RISK_APPROVED.value if approved else OrderStatus.REJECTED.value
            ),
            "filled_quantity": order.filled_quantity,
            "average_fill_price": order.average_fill_price,
            "policy_name": decision.policy_name,
            "policy_version": decision.policy_version,
            "reason_code": decision.reason_code,
        }
        payload.update(decision.audit_details)
        return DomainEvent(
            event_type=EventType.RISK_APPROVED if approved else EventType.RISK_REJECTED,
            aggregate_id=order.client_order_id,
            account_id=order.account_id,
            strategy_id=strategy_id,
            occurred_at=datetime.now(UTC),
            payload=payload,
        )

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


class OperatorControlService:
    def __init__(self, *, order_store: DurableOrderStore) -> None:
        self._order_store = order_store

    def activate_kill_switch(
        self,
        *,
        account_id: str,
        reason: str,
        operator_id: str | None = None,
        correlation_id: str | None = None,
    ) -> KillSwitch:
        kill_switch = KillSwitch(
            account_id=account_id,
            state=KillSwitchState.ACTIVE,
            activated_reason=reason,
            updated_at=datetime.now(UTC),
        )
        record = OperatorActionRecord(
            account_id=account_id,
            action_type="kill_switch_activated",
            detail=reason,
            operator_id=operator_id,
            correlation_id=correlation_id,
            recorded_at=kill_switch.updated_at,
        )
        event = DomainEvent(
            event_type=EventType.KILL_SWITCH_ACTIVATED,
            aggregate_id=account_id,
            account_id=account_id,
            occurred_at=kill_switch.updated_at,
            correlation_id=correlation_id,
            payload={
                "account_id": account_id,
                "action_type": record.action_type,
                "detail": reason,
                "operator_id": operator_id,
                "kill_switch_state": kill_switch.state.value,
            },
        )
        self._order_store.record_events(events=[event])
        self._order_store.record_operator_action(record=record)
        return kill_switch

    def release_kill_switch(
        self,
        *,
        account_id: str,
        detail: str,
        operator_id: str | None = None,
        correlation_id: str | None = None,
    ) -> KillSwitch:
        released_at = datetime.now(UTC)
        kill_switch = KillSwitch(
            account_id=account_id,
            state=KillSwitchState.INACTIVE,
            activated_reason=None,
            updated_at=released_at,
        )
        record = OperatorActionRecord(
            account_id=account_id,
            action_type="kill_switch_released",
            detail=detail,
            operator_id=operator_id,
            correlation_id=correlation_id,
            recorded_at=released_at,
        )
        event = DomainEvent(
            event_type=EventType.OPERATOR_OVERRIDE_RECORDED,
            aggregate_id=account_id,
            account_id=account_id,
            occurred_at=released_at,
            correlation_id=correlation_id,
            payload={
                "account_id": account_id,
                "action_type": record.action_type,
                "detail": detail,
                "operator_id": operator_id,
                "kill_switch_state": kill_switch.state.value,
            },
        )
        self._order_store.record_events(events=[event])
        self._order_store.record_operator_action(record=record)
        return kill_switch

    def record_override(
        self,
        *,
        account_id: str,
        detail: str,
        operator_id: str | None = None,
        correlation_id: str | None = None,
    ) -> OperatorActionRecord:
        recorded_at = datetime.now(UTC)
        record = OperatorActionRecord(
            account_id=account_id,
            action_type="operator_override_recorded",
            detail=detail,
            operator_id=operator_id,
            correlation_id=correlation_id,
            recorded_at=recorded_at,
        )
        event = DomainEvent(
            event_type=EventType.OPERATOR_OVERRIDE_RECORDED,
            aggregate_id=account_id,
            account_id=account_id,
            occurred_at=recorded_at,
            correlation_id=correlation_id,
            payload={
                "account_id": account_id,
                "action_type": record.action_type,
                "detail": detail,
                "operator_id": operator_id,
            },
        )
        self._order_store.record_events(events=[event])
        self._order_store.record_operator_action(record=record)
        return record

    def list_history(self, *, account_id: str, limit: int = 50) -> list[OperatorActionRecord]:
        return self._order_store.list_operator_actions(account_id=account_id, limit=limit)


class RuntimeDiagnosticsService:
    def __init__(
        self,
        *,
        order_store: DurableOrderStore,
        runtime_health_service: RuntimeHealthService | None = None,
    ) -> None:
        self._order_store = order_store
        self._runtime_health_service = runtime_health_service or RuntimeHealthService()

    def capture_account_health(
        self,
        *,
        account_id: str,
        kill_switch: KillSwitch,
        is_quarantined: bool = False,
        opening_balance: float = 0.0,
    ) -> RuntimeHealthSummary:
        internal_orders = self._order_store.list_internal_order_snapshots(account_id=account_id)
        positions = self._order_store.project_internal_positions(account_id=account_id)
        cash = self._order_store.project_internal_cash(
            account_id=account_id,
            opening_balance=opening_balance,
        )
        exposure = self._order_store.project_internal_exposure(account_id=account_id)
        latest_report = self._order_store.load_latest_reconciliation_report(account_id=account_id)
        latest_run = self._order_store.load_latest_reconciliation_run(account_id=account_id)
        operator_actions = self._order_store.list_operator_actions(account_id=account_id, limit=1)
        summary = self._runtime_health_service.summarize(
            snapshot=AccountRuntimeSnapshot(
                account_id=account_id,
                active_order_count=len(
                    [
                        order
                        for order in internal_orders
                        if order.status not in {OrderStatus.FILLED.value, OrderStatus.CANCELLED.value, OrderStatus.REJECTED.value}
                    ]
                ),
                open_position_count=len([position for position in positions if position.quantity != 0]),
                gross_notional=exposure.gross_notional,
                cash_balance=cash.balance,
                is_quarantined=is_quarantined,
                kill_switch=kill_switch,
                latest_reconciliation_report=latest_report,
                latest_reconciliation_run=latest_run,
                latest_operator_action=operator_actions[0] if operator_actions else None,
            )
        )
        self._order_store.record_runtime_health_summary(summary=summary)
        return summary

    def list_history(self, *, account_id: str, limit: int = 50) -> list[RuntimeHealthSummary]:
        return self._order_store.list_runtime_health_summaries(account_id=account_id, limit=limit)

    def load_latest(self, *, account_id: str) -> RuntimeHealthSummary | None:
        return self._order_store.load_latest_runtime_health_summary(account_id=account_id)
