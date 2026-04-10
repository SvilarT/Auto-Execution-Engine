from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


class DriftCategory(str, Enum):
    ORDER_STATUS_MISMATCH = "order_status_mismatch"
    FILLED_QUANTITY_MISMATCH = "filled_quantity_mismatch"
    POSITION_MISMATCH = "position_mismatch"
    CASH_MISMATCH = "cash_mismatch"
    UNKNOWN_BROKER_ORDER = "unknown_broker_order"
    MISSING_BROKER_ORDER = "missing_broker_order"
    RECONCILIATION_RUN_FAILURE = "reconciliation_run_failure"


class ReconciliationAction(str, Enum):
    NO_ACTION = "no_action"
    LOG_ONLY = "log_only"
    QUARANTINE_ACCOUNT = "quarantine_account"


class ReconciliationRunStatus(str, Enum):
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class InternalOrderSnapshot:
    account_id: str
    client_order_id: str
    status: str
    filled_quantity: float
    symbol: str


@dataclass(frozen=True)
class BrokerOrderSnapshot:
    account_id: str
    client_order_id: str
    status: str
    filled_quantity: float
    symbol: str


@dataclass(frozen=True)
class ReconciliationDrift:
    category: DriftCategory
    account_id: str
    client_order_id: str | None
    detail: str


@dataclass(frozen=True)
class ReconciliationReport:
    account_id: str
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    drifts: tuple[ReconciliationDrift, ...] = ()
    action: ReconciliationAction = ReconciliationAction.NO_ACTION

    @property
    def has_drift(self) -> bool:
        return bool(self.drifts)


@dataclass(frozen=True)
class ReconciliationCycleRecord:
    account_id: str
    generated_at: datetime
    internal_orders: tuple[InternalOrderSnapshot, ...]
    broker_orders: tuple[BrokerOrderSnapshot, ...]
    report: ReconciliationReport


@dataclass(frozen=True)
class ReconciliationRunRecord:
    run_id: str
    account_id: str
    owner_id: str
    started_at: datetime
    completed_at: datetime
    status: ReconciliationRunStatus
    detail: str
    report: ReconciliationReport | None = None
