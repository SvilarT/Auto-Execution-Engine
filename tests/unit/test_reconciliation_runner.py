from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from auto_execution_engine.adapters.persistence.sqlite_order_store import SQLiteOrderStore
from auto_execution_engine.bootstrap.startup import StartupContext, build_account_quarantine_registry
from auto_execution_engine.config.execution_mode import (
    ExecutionMode,
    SafetyGateConfig,
    get_runtime_profile,
)
from auto_execution_engine.domain.orders.models import (
    OrderAggregate,
    OrderSide,
    OrderStatus,
    OrderType,
)
from auto_execution_engine.reconciliation.models import (
    BrokerOrderSnapshot,
    DriftCategory,
    ReconciliationAction,
    ReconciliationRunStatus,
)
from auto_execution_engine.reconciliation.runner import ReconciliationRunner
from auto_execution_engine.trading_plane.leases import AccountLeaseService


@pytest.fixture
def startup_context(tmp_path: Path) -> StartupContext:
    return StartupContext(
        profile=get_runtime_profile(ExecutionMode.PAPER),
        safety=SafetyGateConfig(
            allow_paper=True,
            allow_live=False,
            broker_credentials_present=True,
            risk_engine_configured=True,
            reconciliation_enabled=True,
            durable_state_enabled=True,
            operator_approval_present=False,
        ),
        durable_state_root=tmp_path,
    )


@pytest.fixture
def order_store(startup_context: StartupContext) -> SQLiteOrderStore:
    store = SQLiteOrderStore(
        db_path=startup_context.durable_state_root / "paper" / "orders.sqlite3"
    )
    store.initialize()
    return store


@pytest.fixture
def lease_service(order_store: SQLiteOrderStore) -> AccountLeaseService:
    return AccountLeaseService(backend=order_store.build_account_lease_backend())


def _record_submitted_order(store: SQLiteOrderStore, *, account_id: str, client_order_id: str) -> None:
    order = OrderAggregate(
        account_id=account_id,
        client_order_id=client_order_id,
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        order_type=OrderType.MARKET,
    )
    created_event = order.create_event()
    risk_approved_order, risk_approved_event = order.transition(OrderStatus.RISK_APPROVED)
    submitted_order, submitted_event = risk_approved_order.transition(OrderStatus.SUBMITTED)
    assert submitted_order.status is OrderStatus.SUBMITTED
    store.record_events(events=[created_event, risk_approved_event, submitted_event])


def test_reconciliation_runner_records_completed_run(
    startup_context: StartupContext,
    order_store: SQLiteOrderStore,
    lease_service: AccountLeaseService,
) -> None:
    _record_submitted_order(order_store, account_id="acct-1", client_order_id="order-1")
    registry = build_account_quarantine_registry(order_store=order_store)
    runner = ReconciliationRunner(
        order_store=order_store,
        lease_service=lease_service,
        quarantine_registry=registry,
        owner_id="runner-a",
    )

    records = runner.run_once(
        account_ids=["acct-1"],
        broker_snapshots_by_account={
            "acct-1": [
                BrokerOrderSnapshot(
                    account_id="acct-1",
                    client_order_id="order-1",
                    status=OrderStatus.SUBMITTED.value,
                    filled_quantity=0.0,
                    symbol="AAPL",
                )
            ]
        },
        now=datetime(2026, 1, 4, tzinfo=UTC),
    )

    assert [record.status for record in records] == [ReconciliationRunStatus.COMPLETED]
    latest_run = order_store.load_latest_reconciliation_run(account_id="acct-1")
    assert latest_run is not None
    assert latest_run.status is ReconciliationRunStatus.COMPLETED
    assert latest_run.report is not None
    assert latest_run.report.action is ReconciliationAction.NO_ACTION
    assert not registry.is_quarantined(account_id="acct-1")


def test_reconciliation_runner_records_skipped_run_when_account_is_already_leased(
    order_store: SQLiteOrderStore,
    lease_service: AccountLeaseService,
) -> None:
    now = datetime(2026, 1, 4, tzinfo=UTC)
    lease = lease_service.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="other-worker",
        now=now,
        ttl_seconds=60,
    )
    runner = ReconciliationRunner(
        order_store=order_store,
        lease_service=lease_service,
        owner_id="runner-b",
    )

    record = runner.run_once(account_ids=["acct-1"], now=now)[0]

    assert record.status is ReconciliationRunStatus.SKIPPED
    assert "already controlled by other-worker" in record.detail
    assert order_store.load_latest_reconciliation_run(account_id="acct-1") == record

    lease_service.release(
        existing_lease=lease,
        account_id="acct-1",
        owner_id="other-worker",
        now=now,
    )


def test_reconciliation_runner_fails_closed_when_snapshot_loading_errors(
    order_store: SQLiteOrderStore,
    lease_service: AccountLeaseService,
) -> None:
    _record_submitted_order(order_store, account_id="acct-9", client_order_id="order-9")
    registry = build_account_quarantine_registry(order_store=order_store)
    runner = ReconciliationRunner(
        order_store=order_store,
        lease_service=lease_service,
        quarantine_registry=registry,
        owner_id="runner-c",
    )

    record = runner.run_once(
        account_ids=["acct-9"],
        snapshot_loader=lambda _account_id: (_ for _ in ()).throw(RuntimeError("broker timeout")),
        now=datetime(2026, 1, 4, 0, 5, 0, tzinfo=UTC),
    )[0]

    assert record.status is ReconciliationRunStatus.FAILED
    assert record.report is not None
    assert record.report.action is ReconciliationAction.QUARANTINE_ACCOUNT
    assert record.report.drifts[0].category is DriftCategory.RECONCILIATION_RUN_FAILURE
    assert record.report.drifts[0].detail == "broker timeout"
    assert registry.is_quarantined(account_id="acct-9")
    assert order_store.load_latest_reconciliation_report(account_id="acct-9") == record.report
