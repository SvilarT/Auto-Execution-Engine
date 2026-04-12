from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class OperatorActionRecord:
    account_id: str
    action_type: str
    detail: str
    operator_id: str | None = None
    correlation_id: str | None = None
    recorded_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class RuntimeHealthSummary:
    account_id: str
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    status: str = "healthy"
    active_order_count: int = 0
    open_position_count: int = 0
    gross_notional: float = 0.0
    cash_balance: float = 0.0
    is_quarantined: bool = False
    kill_switch_active: bool = False
    latest_reconciliation_action: str = "no_action"
    latest_reconciliation_status: str | None = None
    latest_reconciliation_detail: str | None = None
    drift_count: int = 0
    last_operator_action_type: str | None = None
    detail: str = ""
