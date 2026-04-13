from dataclasses import dataclass
from enum import Enum


class BrokerOrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class BrokerOrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class BrokerSubmissionOutcome(str, Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"
    FAILED = "failed"


class BrokerRetryDisposition(str, Enum):
    SAFE_TO_RETRY = "safe_to_retry"
    DO_NOT_RETRY = "do_not_retry"


class CanonicalBrokerOrderStatus(str, Enum):
    NEW = "new"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    PENDING_CANCEL = "pending_cancel"
    PENDING_REPLACE = "pending_replace"
    REPLACED = "replaced"
    SUSPENDED = "suspended"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class BrokerOrderRequest:
    account_id: str
    client_order_id: str
    symbol: str
    side: BrokerOrderSide
    quantity: float
    order_type: BrokerOrderType
    limit_price: float | None = None


@dataclass(frozen=True)
class BrokerOrderAck:
    account_id: str
    client_order_id: str
    broker_order_id: str | None
    accepted: bool
    outcome: BrokerSubmissionOutcome
    retry_disposition: BrokerRetryDisposition
    message: str | None = None


@dataclass(frozen=True)
class RawBrokerOrderSnapshot:
    account_id: str
    broker_order_id: str
    client_order_id: str
    symbol: str
    raw_status: str
    canonical_status: CanonicalBrokerOrderStatus
    filled_quantity: float
    raw_payload: dict[str, object]
    observed_at: str


@dataclass(frozen=True)
class RawBrokerActivity:
    account_id: str
    activity_id: str
    activity_type: str
    client_order_id: str | None
    broker_order_id: str | None
    raw_payload: dict[str, object]
    occurred_at: str


@dataclass(frozen=True)
class BrokerOrderActivityPage:
    account_id: str
    activities: tuple[RawBrokerActivity, ...]
    cursor: str | None = None
    has_more: bool = False
