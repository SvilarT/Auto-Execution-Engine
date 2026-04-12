from auto_execution_engine.domain.risk.models import (
    AccountExposureSnapshot,
    KillSwitch,
    KillSwitchState,
    OrderIntent,
    RiskDecision,
    SymbolExposureSnapshot,
)
from auto_execution_engine.domain.risk.service import KillSwitchService, RiskLimits, RiskService


def make_intent(quantity: float = 1.0, notional: float = 1000.0, side: str = "buy") -> OrderIntent:
    return OrderIntent(
        account_id="acct-1",
        symbol="BTC-USD",
        side=side,
        quantity=quantity,
        notional=notional,
        strategy_id="strategy-1",
    )


def test_risk_service_approves_valid_order() -> None:
    service = RiskService(
        limits=RiskLimits(
            max_order_notional=5000.0,
            max_order_quantity=2.0,
        )
    )

    decision = service.evaluate(
        intent=make_intent(quantity=1.0, notional=1000.0),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
    )

    assert decision.decision is RiskDecision.APPROVE
    assert decision.reason_code == "approved"
    assert decision.policy_version == "2"
    assert decision.audit_details["projected_account_gross_notional"] == 1000.0


def test_risk_service_rejects_quantity_limit_breach() -> None:
    service = RiskService(
        limits=RiskLimits(
            max_order_notional=5000.0,
            max_order_quantity=2.0,
        )
    )

    decision = service.evaluate(
        intent=make_intent(quantity=3.0, notional=1000.0),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
    )

    assert decision.decision is RiskDecision.REJECT
    assert decision.reason_code == "quantity_limit_exceeded"


def test_risk_service_rejects_active_kill_switch() -> None:
    service = RiskService(
        limits=RiskLimits(
            max_order_notional=5000.0,
            max_order_quantity=2.0,
        )
    )

    decision = service.evaluate(
        intent=make_intent(quantity=1.0, notional=1000.0),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.ACTIVE),
    )

    assert decision.decision is RiskDecision.REJECT
    assert decision.reason_code == "kill_switch_active"


def test_risk_service_rejects_projected_account_gross_limit_breach() -> None:
    service = RiskService(
        limits=RiskLimits(
            max_order_notional=10000.0,
            max_order_quantity=5.0,
            max_account_gross_notional=12000.0,
        )
    )

    decision = service.evaluate(
        intent=make_intent(quantity=1.0, notional=4000.0),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
        current_exposure=AccountExposureSnapshot(account_id="acct-1", gross_notional=9000.0),
    )

    assert decision.decision is RiskDecision.REJECT
    assert decision.reason_code == "account_gross_notional_limit_exceeded"
    assert decision.audit_details["projected_account_gross_notional"] == 13000.0


def test_risk_service_rejects_projected_symbol_concentration_breach() -> None:
    service = RiskService(
        limits=RiskLimits(
            max_order_notional=20000.0,
            max_order_quantity=10.0,
            max_account_gross_notional=20000.0,
            max_symbol_concentration=0.60,
        )
    )

    current_exposure = AccountExposureSnapshot(
        account_id="acct-1",
        gross_notional=10000.0,
        symbol_exposures=(
            SymbolExposureSnapshot(
                account_id="acct-1",
                symbol="BTC-USD",
                quantity=1.0,
                mark_price=5000.0,
                gross_notional=5000.0,
            ),
        ),
    )
    decision = service.evaluate(
        intent=make_intent(quantity=1.0, notional=7000.0),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
        current_exposure=current_exposure,
    )

    assert decision.decision is RiskDecision.REJECT
    assert decision.reason_code == "symbol_concentration_limit_exceeded"
    assert decision.audit_details["projected_symbol_concentration"] == 14000.0 / 19000.0


def test_risk_service_rejects_sell_order_when_resulting_book_still_breaches_concentration_limit() -> None:
    service = RiskService(
        limits=RiskLimits(
            max_order_notional=10000.0,
            max_order_quantity=5.0,
            max_account_gross_notional=15000.0,
            max_symbol_gross_notional=10000.0,
            max_symbol_concentration=0.90,
        )
    )

    current_exposure = AccountExposureSnapshot(
        account_id="acct-1",
        gross_notional=10000.0,
        symbol_exposures=(
            SymbolExposureSnapshot(
                account_id="acct-1",
                symbol="BTC-USD",
                quantity=2.0,
                mark_price=5000.0,
                gross_notional=10000.0,
            ),
        ),
    )
    decision = service.evaluate(
        intent=make_intent(quantity=1.0, notional=4000.0, side="sell"),
        kill_switch=KillSwitch(account_id="acct-1", state=KillSwitchState.INACTIVE),
        current_exposure=current_exposure,
    )

    assert decision.decision is RiskDecision.REJECT
    assert decision.reason_code == "symbol_concentration_limit_exceeded"


def test_kill_switch_service_activates_and_deactivates_account() -> None:
    service = KillSwitchService()

    active = service.activate(account_id="acct-1", reason="manual_halt")
    assert active.state is KillSwitchState.ACTIVE
    assert active.activated_reason == "manual_halt"

    inactive = service.deactivate(account_id="acct-1")
    assert inactive.state is KillSwitchState.INACTIVE
