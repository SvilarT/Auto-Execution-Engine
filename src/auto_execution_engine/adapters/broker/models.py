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
