from __future__ import annotations

from dataclasses import dataclass

from auto_execution_engine.domain.risk.models import (
    KillSwitch,
    KillSwitchState,
    OrderIntent,
    RiskDecision,
    RiskPolicyDecision,
)


@dataclass(frozen=True)
class RiskLimits:
    max_order_notional: float
    max_order_quantity: float


class RiskService:
    """Independent policy authority for pre-trade approval."""

    def __init__(self, *, limits: RiskLimits) -> None:
        self._limits = limits

    def evaluate(
        self,
        *,
        intent: OrderIntent,
        kill_switch: KillSwitch,
    ) -> RiskPolicyDecision:
        if kill_switch.state is KillSwitchState.ACTIVE:
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="kill_switch_active",
                policy_name="global_pre_trade_policy",
                policy_version="1",
            )

        if intent.quantity <= 0:
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="invalid_quantity",
                policy_name="global_pre_trade_policy",
                policy_version="1",
            )

        if intent.quantity > self._limits.max_order_quantity:
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="quantity_limit_exceeded",
                policy_name="global_pre_trade_policy",
                policy_version="1",
            )

        if intent.notional > self._limits.max_order_notional:
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="notional_limit_exceeded",
                policy_name="global_pre_trade_policy",
                policy_version="1",
            )

        return RiskPolicyDecision(
            decision=RiskDecision.APPROVE,
            reason_code="approved",
            policy_name="global_pre_trade_policy",
            policy_version="1",
        )


class KillSwitchService:
    """Fail-closed kill-switch authority for an account."""

    def activate(self, *, account_id: str, reason: str) -> KillSwitch:
        return KillSwitch(
            account_id=account_id,
            state=KillSwitchState.ACTIVE,
            activated_reason=reason,
        )

    def deactivate(self, *, account_id: str) -> KillSwitch:
        return KillSwitch(account_id=account_id, state=KillSwitchState.INACTIVE)
