from pathlib import Path

from auto_execution_engine.bootstrap.startup import (
    build_account_lease_service,
    build_account_quarantine_registry,
    build_order_store,
    build_submission_service,
    load_startup_context,
    reconcile_account_startup_state,
    reconcile_all_startup_accounts,
    resolve_order_store_path,
)
from datetime import UTC, datetime

import pytest

from auto_execution_engine.adapters.broker.models import (
    BrokerOrderRequest,
    BrokerOrderSide,
    BrokerOrderType,
)
from auto_execution_engine.config.execution_mode import ConfigurationError, ExecutionMode
from auto_execution_engine.domain.orders.models import (
    OrderAggregate,
    OrderSide,
    OrderStatus,
    OrderType,
)
from auto_execution_engine.reconciliation.models import (
    BrokerOrderSnapshot,
    ReconciliationAction,
)


def test_simulation_mode_starts_with_defaults(monkeypatch) -> None:
    monkeypatch.delenv("AEE_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("AEE_ALLOW_PAPER", raising=False)
    monkeypatch.delenv("AEE_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("AEE_DURABLE_STATE_ROOT", raising=False)

    context = load_startup_context()

    assert context.profile.mode is ExecutionMode.SIMULATION
    assert context.profile.synthetic_data_allowed is True
    assert context.profile.external_broker_effects_allowed is False
    assert context.durable_state_root == Path("/home/ubuntu/.auto_execution_engine/state")


def test_paper_mode_requires_explicit_enablement(monkeypatch) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")

    try:
        load_startup_context()
        assert False, "expected ConfigurationError"
    except ConfigurationError as exc:
        assert "paper mode is disabled" in str(exc)


def test_live_mode_requires_operator_approval(monkeypatch) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "live")
    monkeypatch.setenv("AEE_ALLOW_LIVE", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.delenv("AEE_OPERATOR_APPROVAL_PRESENT", raising=False)

    try:
        load_startup_context()
        assert False, "expected ConfigurationError"
    except ConfigurationError as exc:
        assert "operator approval" in str(exc)


def test_live_mode_starts_only_when_all_safety_gates_are_present(monkeypatch) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "live")
    monkeypatch.setenv("AEE_ALLOW_LIVE", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_OPERATOR_APPROVAL_PRESENT", "true")

    context = load_startup_context()

    assert context.profile.mode is ExecutionMode.LIVE
    assert context.profile.synthetic_data_allowed is False
    assert context.profile.persistence_namespace == "live"


def test_runtime_namespace_resolves_durable_store_path(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()

    assert resolve_order_store_path(context=context) == tmp_path / "paper" / "orders.sqlite3"


def test_build_order_store_initializes_namespace_backed_sqlite_file(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()
    store = build_order_store(context=context)

    assert store.db_path == tmp_path / "paper" / "orders.sqlite3"
    assert store.db_path.exists() is True


def test_startup_builds_durable_submission_service_from_runtime_namespace(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()
    submission_service = build_submission_service(context=context)
    first_ack = submission_service.register_submission(
        request=BrokerOrderRequest(
            account_id="acct-1",
            client_order_id="startup-ord-1",
            symbol="BTC-USD",
            side=BrokerOrderSide.BUY,
            quantity=1.0,
            order_type=BrokerOrderType.MARKET,
        )
    )
    restarted_submission_service = build_submission_service(context=context)
    restarted_ack = restarted_submission_service.load_submission(
        client_order_id="startup-ord-1"
    )

    assert first_ack.broker_order_id == "pending::startup-ord-1"
    assert restarted_ack is not None
    assert restarted_ack.broker_order_id == first_ack.broker_order_id


def test_startup_builds_durable_account_lease_service_from_runtime_namespace(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()
    first_service = build_account_lease_service(context=context)
    first_lease = first_service.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=datetime(2026, 1, 1, tzinfo=UTC),
        ttl_seconds=30,
    )
    restarted_service = build_account_lease_service(context=context)

    with pytest.raises(ValueError, match="already controlled by worker-a"):
        restarted_service.acquire(
            existing_lease=first_lease,
            account_id="acct-1",
            owner_id="worker-b",
            now=datetime(2026, 1, 1, 0, 0, 10, tzinfo=UTC),
            ttl_seconds=30,
        )


def test_startup_reconciliation_repairs_created_order_from_durable_submission(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()
    store = build_order_store(context=context)
    order = OrderAggregate(
        account_id="acct-1",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=1.0,
        order_type=OrderType.MARKET,
        client_order_id="startup-gap-1",
    )
    store.record_events(events=[order.create_event()])
    build_submission_service(context=context).register_submission(
        request=BrokerOrderRequest(
            account_id="acct-1",
            client_order_id="startup-gap-1",
            symbol="BTC-USD",
            side=BrokerOrderSide.BUY,
            quantity=1.0,
            order_type=BrokerOrderType.MARKET,
        )
    )

    report = reconcile_account_startup_state(
        context=context,
        account_id="acct-1",
        broker_orders=[
            BrokerOrderSnapshot(
                account_id="acct-1",
                client_order_id="startup-gap-1",
                status="submitted",
                filled_quantity=0.0,
                symbol="BTC-USD",
            )
        ],
        order_store=store,
    )

    assert report.has_drift is False
    assert store.load_order(client_order_id="startup-gap-1").status is OrderStatus.SUBMITTED


def test_startup_reconciliation_records_quarantine_from_persisted_internal_snapshots(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()
    store = build_order_store(context=context)
    registry = build_account_quarantine_registry()
    order = OrderAggregate(
        account_id="acct-1",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=1.0,
        order_type=OrderType.MARKET,
        client_order_id="restart-ord-1",
    )
    store.record_events(events=[order.create_event()])

    report = reconcile_account_startup_state(
        context=context,
        account_id="acct-1",
        broker_orders=[],
        order_store=store,
        quarantine_registry=registry,
    )

    assert report.action is ReconciliationAction.QUARANTINE_ACCOUNT
    assert registry.is_quarantined(account_id="acct-1") is True


def test_startup_reconciliation_allows_clear_account_when_snapshots_match(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()
    store = build_order_store(context=context)
    registry = build_account_quarantine_registry()
    order = OrderAggregate(
        account_id="acct-1",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=1.0,
        order_type=OrderType.MARKET,
        client_order_id="restart-ord-2",
    )
    risk_approved_order, risk_approved_event = order.transition(OrderStatus.RISK_APPROVED)
    submitted_order, submitted_event = risk_approved_order.transition(OrderStatus.SUBMITTED)
    store.record_events(
        events=[order.create_event(), risk_approved_event, submitted_event]
    )

    report = reconcile_account_startup_state(
        context=context,
        account_id="acct-1",
        broker_orders=[
            BrokerOrderSnapshot(
                account_id="acct-1",
                client_order_id=submitted_order.client_order_id,
                status="submitted",
                filled_quantity=0.0,
                symbol="BTC-USD",
            )
        ],
        order_store=store,
        quarantine_registry=registry,
    )

    assert report.action is ReconciliationAction.NO_ACTION
    assert registry.is_quarantined(account_id="acct-1") is False


def test_build_account_quarantine_registry_can_preload_latest_persisted_reports(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()
    store = build_order_store(context=context)
    drift_order = OrderAggregate(
        account_id="acct-drift",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=1.0,
        order_type=OrderType.MARKET,
        client_order_id="restart-ord-3",
    )
    clear_order = OrderAggregate(
        account_id="acct-clear",
        symbol="ETH-USD",
        side=OrderSide.SELL,
        quantity=2.0,
        order_type=OrderType.MARKET,
        client_order_id="restart-ord-4",
    )
    clear_risk_approved, clear_risk_event = clear_order.transition(OrderStatus.RISK_APPROVED)
    clear_submitted, clear_submitted_event = clear_risk_approved.transition(
        OrderStatus.SUBMITTED
    )
    store.record_events(
        events=[
            drift_order.create_event(),
            clear_order.create_event(),
            clear_risk_event,
            clear_submitted_event,
        ]
    )

    reconcile_account_startup_state(
        context=context,
        account_id="acct-drift",
        broker_orders=[],
        order_store=store,
        quarantine_registry=None,
    )
    reconcile_account_startup_state(
        context=context,
        account_id="acct-clear",
        broker_orders=[
            BrokerOrderSnapshot(
                account_id="acct-clear",
                client_order_id=clear_submitted.client_order_id,
                status="submitted",
                filled_quantity=0.0,
                symbol="ETH-USD",
            )
        ],
        order_store=store,
        quarantine_registry=None,
    )

    reloaded_registry = build_account_quarantine_registry(
        order_store=store,
        preload_persisted_reports=True,
    )

    assert reloaded_registry.is_quarantined(account_id="acct-drift") is True
    assert reloaded_registry.is_quarantined(account_id="acct-clear") is False


def test_reconcile_all_startup_accounts_sweeps_active_and_previously_reported_accounts(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")
    monkeypatch.setenv("AEE_ALLOW_PAPER", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ROOT", str(tmp_path))

    context = load_startup_context()
    store = build_order_store(context=context)
    registry = build_account_quarantine_registry()

    active_order = OrderAggregate(
        account_id="acct-active",
        symbol="SOL-USD",
        side=OrderSide.BUY,
        quantity=5.0,
        order_type=OrderType.MARKET,
        client_order_id="restart-ord-5",
    )
    reported_only_order = OrderAggregate(
        account_id="acct-reported",
        symbol="AVAX-USD",
        side=OrderSide.BUY,
        quantity=3.0,
        order_type=OrderType.MARKET,
        client_order_id="restart-ord-6",
    )
    reported_risk_approved, reported_risk_event = reported_only_order.transition(
        OrderStatus.RISK_APPROVED
    )
    reported_submitted, reported_submitted_event = reported_risk_approved.transition(
        OrderStatus.SUBMITTED
    )
    reported_filled, reported_filled_event = reported_submitted.transition(
        OrderStatus.FILLED,
        fill_quantity_delta=3.0,
        fill_price=42.0,
    )

    store.record_events(
        events=[
            active_order.create_event(),
            reported_only_order.create_event(),
            reported_risk_event,
            reported_submitted_event,
            reported_filled_event,
        ]
    )
    reconcile_account_startup_state(
        context=context,
        account_id="acct-reported",
        broker_orders=[],
        order_store=store,
        quarantine_registry=None,
    )

    reports = reconcile_all_startup_accounts(
        context=context,
        broker_snapshots_by_account={
            "acct-reported": [],
            "acct-active": [],
        },
        order_store=store,
        quarantine_registry=registry,
    )

    assert [report.account_id for report in reports] == ["acct-active", "acct-reported"]
    assert registry.is_quarantined(account_id="acct-active") is True
    assert registry.is_quarantined(account_id="acct-reported") is True
