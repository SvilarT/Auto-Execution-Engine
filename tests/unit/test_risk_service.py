from auto_execution_engine.domain.risk.models import KillSwitch, KillSwitchState, OrderIntent, RiskDecision
from auto_execution_engine.domain.risk.service import KillSwitchService, RiskLimits, RiskService



def make_intent(quantity: float = 1.0, notional: float = 1000.0) -> OrderIntent:
    return OrderIntent(
        account_id="acct-1",
        symbol="BTC-USD",
        side="buy",
        quantity=quantity,
        notional=notional,
        strategy_id="strategy-1",
    )



def test_risk_service_approves_valid_order():
    service = RiskService(limits=RiskLimits(max_order_notional=5000.0, max_order_quantity=2.0))

    decision = service.evaluate(
        intent=make_intent(quantity=1.0, notional=1000.0),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
    )

    assert decision.decision is RiskDecision.APPROVE
    assert decision.reason_code == "approved"



def test_risk_service_rejects_quantity_limit_breach():
    service = RiskService(limits=RiskLimits(max_order_notional=5000.0, max_order_quantity=2.0))

    decision = service.evaluate(
        intent=make_intent(quantity=3.0, notional=1000.0),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
    )

    assert decision.decision is RiskDecision.REJECT
    assert decision.reason_code == "quantity_limit_exceeded"



def test_risk_service_rejects_active_kill_switch():
    service = RiskService(limits=RiskLimits(max_order_notional=5000.0, max_order_quantity=2.0))

    decision = service.evaluate(
        intent=make_intent(quantity=1.0, notional=1000.0),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.ACTIVE),
    )

    assert decision.decision is RiskDecision.REJECT
    assert decision.reason_code == "kill_switch_active"



def test_kill_switch_service_activates_and_deactivates_account():
    service = KillSwitchService()

    active = service.activate(account_id="acct-1", reason="manual_halt")
    assert active.state is KillSwitchState.ACTIVE
    assert active.activated_reason == "manual_halt"

    inactive = service.deactivate(account_id="acct-1")
    assert inactive.state is KillSwitchState.INACTIVE
