from __future__ import annotations

from dataclasses import dataclass

from auto_execution_engine.domain.risk.models import (
    AccountExposureSnapshot,
    KillSwitch,
    KillSwitchState,
    OrderIntent,
    RiskDecision,
    RiskPolicyDecision,
    SymbolExposureSnapshot,
)


@dataclass(frozen=True)
class RiskLimits:
    max_order_notional: float
    max_order_quantity: float
    max_account_gross_notional: float | None = None
    max_symbol_gross_notional: float | None = None
    max_symbol_concentration: float | None = None
    policy_name: str = "global_pre_trade_policy"
    policy_version: str = "2"


class RiskService:
    """Independent policy authority for pre-trade approval."""

    def __init__(self, *, limits: RiskLimits) -> None:
        self._limits = limits

    def evaluate(
        self,
        *,
        intent: OrderIntent,
        kill_switch: KillSwitch,
        current_exposure: AccountExposureSnapshot | None = None,
    ) -> RiskPolicyDecision:
        exposure = current_exposure or AccountExposureSnapshot(account_id=intent.account_id)
        policy_name = self._limits.policy_name
        policy_version = self._limits.policy_version

        if kill_switch.state is KillSwitchState.ACTIVE:
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="kill_switch_active",
                policy_name=policy_name,
                policy_version=policy_version,
                audit_details={
                    "account_id": intent.account_id,
                    "symbol": intent.symbol,
                },
            )

        if intent.quantity <= 0:
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="invalid_quantity",
                policy_name=policy_name,
                policy_version=policy_version,
                audit_details={
                    "account_id": intent.account_id,
                    "requested_quantity": intent.quantity,
                },
            )

        if intent.quantity > self._limits.max_order_quantity:
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="quantity_limit_exceeded",
                policy_name=policy_name,
                policy_version=policy_version,
                audit_details={
                    "account_id": intent.account_id,
                    "requested_quantity": intent.quantity,
                    "max_order_quantity": self._limits.max_order_quantity,
                },
            )

        if intent.notional > self._limits.max_order_notional:
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="notional_limit_exceeded",
                policy_name=policy_name,
                policy_version=policy_version,
                audit_details={
                    "account_id": intent.account_id,
                    "requested_notional": intent.notional,
                    "max_order_notional": self._limits.max_order_notional,
                },
            )

        projected_symbol_exposure = self._project_symbol_exposure(
            intent=intent,
            current_exposure=exposure,
        )
        projected_gross_notional = (
            exposure.gross_notional
            - self._current_symbol_gross_notional(intent=intent, current_exposure=exposure)
            + projected_symbol_exposure.gross_notional
        )

        if (
            self._limits.max_account_gross_notional is not None
            and projected_gross_notional > self._limits.max_account_gross_notional
        ):
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="account_gross_notional_limit_exceeded",
                policy_name=policy_name,
                policy_version=policy_version,
                audit_details={
                    "account_id": intent.account_id,
                    "projected_account_gross_notional": projected_gross_notional,
                    "max_account_gross_notional": self._limits.max_account_gross_notional,
                    "symbol": intent.symbol,
                },
            )

        if (
            self._limits.max_symbol_gross_notional is not None
            and projected_symbol_exposure.gross_notional > self._limits.max_symbol_gross_notional
        ):
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="symbol_gross_notional_limit_exceeded",
                policy_name=policy_name,
                policy_version=policy_version,
                audit_details={
                    "account_id": intent.account_id,
                    "symbol": intent.symbol,
                    "projected_symbol_gross_notional": projected_symbol_exposure.gross_notional,
                    "max_symbol_gross_notional": self._limits.max_symbol_gross_notional,
                },
            )

        projected_symbol_concentration = 0.0
        if projected_gross_notional > 1e-9:
            projected_symbol_concentration = (
                projected_symbol_exposure.gross_notional / projected_gross_notional
            )

        if (
            self._limits.max_symbol_concentration is not None
            and projected_symbol_concentration > self._limits.max_symbol_concentration
        ):
            return RiskPolicyDecision(
                decision=RiskDecision.REJECT,
                reason_code="symbol_concentration_limit_exceeded",
                policy_name=policy_name,
                policy_version=policy_version,
                audit_details={
                    "account_id": intent.account_id,
                    "symbol": intent.symbol,
                    "projected_symbol_concentration": projected_symbol_concentration,
                    "max_symbol_concentration": self._limits.max_symbol_concentration,
                    "projected_account_gross_notional": projected_gross_notional,
                },
            )

        return RiskPolicyDecision(
            decision=RiskDecision.APPROVE,
            reason_code="approved",
            policy_name=policy_name,
            policy_version=policy_version,
            audit_details={
                "account_id": intent.account_id,
                "symbol": intent.symbol,
                "projected_account_gross_notional": projected_gross_notional,
                "projected_symbol_gross_notional": projected_symbol_exposure.gross_notional,
                "projected_symbol_concentration": projected_symbol_concentration,
            },
        )

    def _project_symbol_exposure(
        self,
        *,
        intent: OrderIntent,
        current_exposure: AccountExposureSnapshot,
    ) -> SymbolExposureSnapshot:
        existing = current_exposure.exposure_for_symbol(symbol=intent.symbol)
        existing_quantity = existing.quantity if existing is not None else 0.0
        signed_order_quantity = intent.quantity if intent.side == "buy" else -intent.quantity
        projected_quantity = existing_quantity + signed_order_quantity
        mark_price = intent.notional / intent.quantity
        return SymbolExposureSnapshot(
            account_id=intent.account_id,
            symbol=intent.symbol,
            quantity=projected_quantity,
            mark_price=mark_price,
            gross_notional=abs(projected_quantity) * mark_price,
        )

    def _current_symbol_gross_notional(
        self,
        *,
        intent: OrderIntent,
        current_exposure: AccountExposureSnapshot,
    ) -> float:
        existing = current_exposure.exposure_for_symbol(symbol=intent.symbol)
        if existing is None:
            return 0.0
        return existing.gross_notional


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
