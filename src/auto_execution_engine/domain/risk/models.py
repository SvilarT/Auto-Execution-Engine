from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


class RiskDecision(str, Enum):
    APPROVE = "approve"
    REJECT = "reject"


class KillSwitchState(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclass(frozen=True)
class OrderIntent:
    account_id: str
    symbol: str
    side: str
    quantity: float
    notional: float
    strategy_id: str


@dataclass(frozen=True)
class SymbolExposureSnapshot:
    account_id: str
    symbol: str
    quantity: float
    mark_price: float
    gross_notional: float


@dataclass(frozen=True)
class AccountExposureSnapshot:
    account_id: str
    gross_notional: float = 0.0
    symbol_exposures: tuple[SymbolExposureSnapshot, ...] = ()

    def exposure_for_symbol(self, *, symbol: str) -> SymbolExposureSnapshot | None:
        for exposure in self.symbol_exposures:
            if exposure.symbol == symbol:
                return exposure
        return None


@dataclass(frozen=True)
class RiskPolicyDecision:
    decision: RiskDecision
    reason_code: str
    policy_name: str
    policy_version: str
    audit_details: dict[str, str | float | int | bool | None] = field(default_factory=dict)


@dataclass(frozen=True)
class KillSwitch:
    account_id: str
    state: KillSwitchState = KillSwitchState.INACTIVE
    activated_reason: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
