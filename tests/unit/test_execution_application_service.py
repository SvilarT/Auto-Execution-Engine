from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequestBuilder,
    BrokerSubmissionService,
    IdempotentSubmissionBook,
)
from auto_execution_engine.adapters.persistence.sqlite_order_store import (
    SQLiteAccountLeaseBackend,
    SQLiteOrderStore,
    SQLiteSubmissionBook,
)
from auto_execution_engine.application.execution_service import (
    ExecutionApplicationService,
    ExecutionRejectedError,
)
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderSide, OrderStatus, OrderType
from auto_execution_engine.domain.risk.models import KillSwitch, KillSwitchState
from auto_execution_engine.domain.risk.service import RiskLimits, RiskService
from auto_execution_engine.reconciliation.models import (
    DriftCategory,
    ReconciliationAction,
    ReconciliationDrift,
    ReconciliationReport,
)
from auto_execution_engine.reconciliation.service import AccountQuarantineRegistry
from auto_execution_engine.trading_plane.leases import AccountLease, AccountLeaseService


def make_order() -> OrderAggregate:
    return OrderAggregate(
        account_id="acct-1",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=1.0,
        order_type=OrderType.MARKET,
    )


def make_service(
    *,
    order_store: SQLiteOrderStore | None = None,
    account_execution_gate: AccountQuarantineRegistry | None = None,
    lease_service: AccountLeaseService | None = None,
    broker_submission_service: BrokerSubmissionService | None = None,
) -> ExecutionApplicationService:
    return ExecutionApplicationService(
        risk_service=RiskService(
            limits=RiskLimits(max_order_notional=100_000, max_order_quantity=5)
        ),
        lease_service=lease_service or AccountLeaseService(),
        broker_request_builder=BrokerOrderRequestBuilder(),
        broker_submission_service=broker_submission_service
        or BrokerSubmissionService(submission_book=IdempotentSubmissionBook()),
        order_store=order_store,
        account_execution_gate=account_execution_gate,
    )


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "execution_service.db"


def test_execution_service_submits_order_after_risk_and_lease_checks() -> None:
    service = make_service()
    order = make_order()

    result = service.execute_order(
        order=order,
        strategy_id="strat-1",
        reference_price=30_000,
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
        owner_id="worker-a",
        existing_lease=None,
    )

    assert result.order.status is OrderStatus.SUBMITTED
    assert result.broker_order_id == f"pending::{order.client_order_id}"
    assert len(result.events) == 2
    assert result.lease.owner_id == "worker-a"


def test_execution_service_rejects_when_risk_blocks_order() -> None:
    service = make_service()
    order = make_order()

    with pytest.raises(ExecutionRejectedError):
        service.execute_order(
            order=order,
            strategy_id="strat-1",
            reference_price=200_000,
            kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
            owner_id="worker-a",
            existing_lease=None,
        )


def test_execution_service_rejects_when_another_worker_owns_account() -> None:
    service = make_service()
    order = make_order()
    now = datetime.now(UTC)
    active_lease = AccountLease(
        account_id="acct-1",
        owner_id="worker-b",
        acquired_at=now,
        expires_at=now + timedelta(seconds=30),
    )

    with pytest.raises(ExecutionRejectedError):
        service.execute_order(
            order=order,
            strategy_id="strat-1",
            reference_price=30_000,
            kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
            owner_id="worker-a",
            existing_lease=active_lease,
        )


def test_execution_service_rejects_when_account_is_quarantined_from_restart_drift() -> None:
    registry = AccountQuarantineRegistry()
    registry.record(
        report=ReconciliationReport(
            account_id="acct-1",
            drifts=(
                ReconciliationDrift(
                    category=DriftCategory.MISSING_BROKER_ORDER,
                    account_id="acct-1",
                    client_order_id="ord-1",
                    detail="persisted order missing from broker snapshot",
                ),
            ),
            action=ReconciliationAction.QUARANTINE_ACCOUNT,
        )
    )
    service = make_service(account_execution_gate=registry)

    with pytest.raises(ExecutionRejectedError, match="account acct-1 is quarantined"):
        service.execute_order(
            order=make_order(),
            strategy_id="strat-1",
            reference_price=30_000,
            kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
            owner_id="worker-a",
            existing_lease=None,
        )


def test_execution_service_persists_created_and_submission_events_for_restart_recovery(
    db_path: Path,
) -> None:
    order_store = SQLiteOrderStore(db_path=db_path)
    order_store.initialize()
    service = make_service(order_store=order_store)
    order = make_order()

    result = service.execute_order(
        order=order,
        strategy_id="strat-1",
        reference_price=30_000,
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
        owner_id="worker-a",
        existing_lease=None,
    )

    restarted_service = make_service(order_store=SQLiteOrderStore(db_path=db_path))
    restored_order = restarted_service.recover_order(client_order_id=order.client_order_id)
    persisted_events = order_store.list_events(aggregate_id=order.client_order_id)

    assert result.order.status is OrderStatus.SUBMITTED
    assert restored_order == result.order
    assert [event.event_type.value for event in persisted_events] == [
        "order_created",
        "risk_approved",
        "order_submitted",
    ]


def test_execution_service_rejects_duplicate_execution_when_order_is_already_recovered(
    db_path: Path,
) -> None:
    order_store = SQLiteOrderStore(db_path=db_path)
    order_store.initialize()
    service = make_service(order_store=order_store)
    order = make_order()

    service.execute_order(
        order=order,
        strategy_id="strat-1",
        reference_price=30_000,
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
        owner_id="worker-a",
        existing_lease=None,
    )

    restarted_service = make_service(order_store=SQLiteOrderStore(db_path=db_path))
    with pytest.raises(ExecutionRejectedError, match="already in durable state submitted"):
        restarted_service.execute_order(
            order=order,
            strategy_id="strat-1",
            reference_price=30_000,
            kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
            owner_id="worker-a",
            existing_lease=None,
        )


def test_execution_service_uses_durable_submission_book_across_restart(db_path: Path) -> None:
    order_store = SQLiteOrderStore(db_path=db_path)
    order_store.initialize()
    submission_service = BrokerSubmissionService(
        submission_book=SQLiteSubmissionBook(db_path=db_path)
    )
    service = make_service(
        order_store=order_store,
        broker_submission_service=submission_service,
    )
    order = make_order()

    result = service.execute_order(
        order=order,
        strategy_id="strat-1",
        reference_price=30_000,
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
        owner_id="worker-a",
        existing_lease=None,
    )

    restarted_submission_service = BrokerSubmissionService(
        submission_book=SQLiteSubmissionBook(db_path=db_path)
    )
    restarted_ack = restarted_submission_service.load_submission(
        client_order_id=order.client_order_id
    )

    assert result.broker_order_id == f"pending::{order.client_order_id}"
    assert restarted_ack is not None
    assert restarted_ack.broker_order_id == result.broker_order_id


def test_execution_service_uses_durable_account_lease_across_restart(db_path: Path) -> None:
    lease_service = AccountLeaseService(
        backend=SQLiteAccountLeaseBackend(db_path=db_path)
    )
    now = datetime.now(UTC)
    first_lease = lease_service.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=now,
        ttl_seconds=30,
    )

    restarted_lease_service = AccountLeaseService(
        backend=SQLiteAccountLeaseBackend(db_path=db_path)
    )
    with pytest.raises(ExecutionRejectedError, match="already controlled by worker-a"):
        make_service(lease_service=restarted_lease_service).execute_order(
            order=make_order(),
            strategy_id="strat-1",
            reference_price=30_000,
            kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
            owner_id="worker-b",
            existing_lease=first_lease,
        )
