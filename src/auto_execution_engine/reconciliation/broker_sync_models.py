from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


class BrokerSyncRunStatus(str, Enum):
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class BrokerSyncCursor:
    account_id: str
    activity_cursor: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class BrokerActivityIngestionResult:
    account_id: str
    activity_id: str
    duplicate: bool
    recorded_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class BrokerSyncBatchResult:
    account_id: str
    raw_order_count: int = 0
    raw_activity_count: int = 0
    duplicate_activity_count: int = 0
    next_activity_cursor: str | None = None


@dataclass(frozen=True)
class BrokerSyncRunRecord:
    run_id: str
    account_id: str
    owner_id: str
    started_at: datetime
    completed_at: datetime
    status: BrokerSyncRunStatus
    detail: str
    batch_result: BrokerSyncBatchResult | None = None
