from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class BrokerOrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class BrokerOrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


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
    broker_order_id: str
    accepted: bool
    message: str | None = None
