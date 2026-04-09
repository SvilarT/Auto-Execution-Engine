from datetime import UTC, datetime, timedelta

import pytest

from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequestBuilder,
    BrokerSubmissionService,
    IdempotentSubmissionBook,
)
from auto_execution_engine.application.execution_service import (
    ExecutionApplicationService,
    ExecutionRejectedError,
)
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderSide, OrderType, OrderStatus
from auto_execution_engine.domain.risk.models import KillSwitch, KillSwitchState
from auto_execution_engine.domain.risk.service import RiskLimits, RiskService
from auto_execution_engine.trading_plane.leases import AccountLease, AccountLeaseService



def make_order() -> OrderAggregate:
    return OrderAggregate(
        account_id="acct-1",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        quantity=1.0,
        order_type=OrderType.MARKET,
    )



def make_service() -> ExecutionApplicationService:
    return ExecutionApplicationService(
        risk_service=RiskService(
            limits=RiskLimits(max_order_notional=100_000, max_order_quantity=5)
        ),
        lease_service=AccountLeaseService(),
        broker_request_builder=BrokerOrderRequestBuilder(),
        broker_submission_service=BrokerSubmissionService(
            submission_book=IdempotentSubmissionBook()
        ),
    )



def test_execution_service_submits_order_after_risk_and_lease_checks():
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



def test_execution_service_rejects_when_risk_blocks_order():
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



def test_execution_service_rejects_when_another_worker_owns_account():
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
