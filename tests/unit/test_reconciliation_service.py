from auto_execution_engine.reconciliation.models import (
    BrokerOrderSnapshot,
    DriftCategory,
    InternalOrderSnapshot,
    ReconciliationAction,
)
from auto_execution_engine.reconciliation.service import ReconciliationService



def test_reconciliation_returns_no_action_when_states_match():
    service = ReconciliationService()

    report = service.compare_orders(
        account_id="acct-1",
        internal_orders=[
            InternalOrderSnapshot(
                account_id="acct-1",
                client_order_id="ord-1",
                status="filled",
                filled_quantity=1.0,
                symbol="BTC-USD",
            )
        ],
        broker_orders=[
            BrokerOrderSnapshot(
                account_id="acct-1",
                client_order_id="ord-1",
                status="filled",
                filled_quantity=1.0,
                symbol="BTC-USD",
            )
        ],
    )

    assert report.has_drift is False
    assert report.action is ReconciliationAction.NO_ACTION



def test_reconciliation_quarantines_when_broker_order_is_missing():
    service = ReconciliationService()

    report = service.compare_orders(
        account_id="acct-1",
        internal_orders=[
            InternalOrderSnapshot(
                account_id="acct-1",
                client_order_id="ord-1",
                status="submitted",
                filled_quantity=0.0,
                symbol="BTC-USD",
            )
        ],
        broker_orders=[],
    )

    assert report.has_drift is True
    assert report.action is ReconciliationAction.QUARANTINE_ACCOUNT
    assert report.drifts[0].category is DriftCategory.MISSING_BROKER_ORDER



def test_reconciliation_quarantines_when_broker_has_unknown_order():
    service = ReconciliationService()

    report = service.compare_orders(
        account_id="acct-1",
        internal_orders=[],
        broker_orders=[
            BrokerOrderSnapshot(
                account_id="acct-1",
                client_order_id="ord-99",
                status="acknowledged",
                filled_quantity=0.0,
                symbol="BTC-USD",
            )
        ],
    )

    assert report.has_drift is True
    assert report.action is ReconciliationAction.QUARANTINE_ACCOUNT
    assert report.drifts[0].category is DriftCategory.UNKNOWN_BROKER_ORDER



def test_reconciliation_quarantines_on_status_mismatch():
    service = ReconciliationService()

    report = service.compare_orders(
        account_id="acct-1",
        internal_orders=[
            InternalOrderSnapshot(
                account_id="acct-1",
                client_order_id="ord-1",
                status="submitted",
                filled_quantity=0.0,
                symbol="BTC-USD",
            )
        ],
        broker_orders=[
            BrokerOrderSnapshot(
                account_id="acct-1",
                client_order_id="ord-1",
                status="filled",
                filled_quantity=1.0,
                symbol="BTC-USD",
            )
        ],
    )

    assert report.has_drift is True
    assert report.action is ReconciliationAction.QUARANTINE_ACCOUNT
    assert {drift.category for drift in report.drifts} == {
        DriftCategory.ORDER_STATUS_MISMATCH,
        DriftCategory.FILLED_QUANTITY_MISMATCH,
    }
