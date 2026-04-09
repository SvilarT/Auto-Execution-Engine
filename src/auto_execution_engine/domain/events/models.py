from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from uuid import uuid4


class EventType(str, Enum):
    """Economically meaningful event categories."""

    SIGNAL_GENERATED = "signal_generated"
    RISK_APPROVED = "risk_approved"
    RISK_REJECTED = "risk_rejected"
    ORDER_CREATED = "order_created"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_ACKNOWLEDGED = "order_acknowledged"
    ORDER_PARTIALLY_FILLED = "order_partially_filled"
    ORDER_FILLED = "order_filled"
    ORDER_REJECTED = "order_rejected"
    ORDER_CANCEL_REQUESTED = "order_cancel_requested"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_RECONCILED = "order_reconciled"
    RECONCILIATION_DRIFT_DETECTED = "reconciliation_drift_detected"
    ACCOUNT_QUARANTINED = "account_quarantined"
    KILL_SWITCH_ACTIVATED = "kill_switch_activated"
    OPERATOR_OVERRIDE_RECORDED = "operator_override_recorded"


@dataclass(frozen=True)
class DomainEvent:
    """Immutable event envelope for durable trading-system facts."""

    event_type: EventType
    aggregate_id: str
    payload: dict[str, str | int | float | bool | None]
    event_id: str = field(default_factory=lambda: str(uuid4()))
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    correlation_id: str | None = None
    causation_id: str | None = None
    account_id: str | None = None
    strategy_id: str | None = None
    mode: str | None = None
