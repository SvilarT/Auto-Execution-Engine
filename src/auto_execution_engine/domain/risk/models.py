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
class RiskPolicyDecision:
    decision: RiskDecision
    reason_code: str
    policy_name: str
    policy_version: str


@dataclass(frozen=True)
class KillSwitch:
    account_id: str
    state: KillSwitchState = KillSwitchState.INACTIVE
    activated_reason: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
