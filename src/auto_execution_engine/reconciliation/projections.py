from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from auto_execution_engine.domain.events.models import DomainEvent, EventType
from auto_execution_engine.domain.orders.models import OrderSide
from auto_execution_engine.domain.risk.models import AccountExposureSnapshot, SymbolExposureSnapshot
from auto_execution_engine.reconciliation.models import CashSnapshot, PositionSnapshot


_FILL_EVENT_TYPES = {
    EventType.ORDER_PARTIALLY_FILLED,
    EventType.ORDER_FILLED,
}


class ProjectionReplayError(ValueError):
    """Raised when the event log cannot be replayed into deterministic account projections."""


@dataclass(frozen=True)
class _FillDelta:
    account_id: str
    aggregate_id: str
    symbol: str
    side: OrderSide
    quantity_delta: float
    price: float


class EventLogProjectionService:
    """Rebuild deterministic account-level position, cash, and exposure state from recorded events."""

    def rebuild_positions(
        self,
        *,
        account_id: str,
        events: Iterable[DomainEvent],
    ) -> list[PositionSnapshot]:
        quantities_by_symbol: dict[str, float] = {}
        for fill in self._iter_fill_deltas(events=events, account_id=account_id):
            signed_quantity = fill.quantity_delta
            if fill.side is OrderSide.SELL:
                signed_quantity *= -1.0
            quantities_by_symbol[fill.symbol] = quantities_by_symbol.get(fill.symbol, 0.0) + signed_quantity

        return [
            PositionSnapshot(
                account_id=account_id,
                symbol=symbol,
                quantity=quantity,
            )
            for symbol, quantity in sorted(quantities_by_symbol.items())
            if abs(quantity) > 1e-9
        ]

    def rebuild_cash(
        self,
        *,
        account_id: str,
        events: Iterable[DomainEvent],
        opening_balance: float = 0.0,
    ) -> CashSnapshot:
        balance = opening_balance
        for fill in self._iter_fill_deltas(events=events, account_id=account_id):
            notional = fill.quantity_delta * fill.price
            if fill.side is OrderSide.BUY:
                balance -= notional
            else:
                balance += notional

        return CashSnapshot(account_id=account_id, balance=balance)

    def rebuild_exposure(
        self,
        *,
        account_id: str,
        events: Iterable[DomainEvent],
    ) -> AccountExposureSnapshot:
        quantities_by_symbol: dict[str, float] = {}
        last_price_by_symbol: dict[str, float] = {}

        for fill in self._iter_fill_deltas(events=events, account_id=account_id):
            signed_quantity = fill.quantity_delta
            if fill.side is OrderSide.SELL:
                signed_quantity *= -1.0
            quantities_by_symbol[fill.symbol] = quantities_by_symbol.get(fill.symbol, 0.0) + signed_quantity
            last_price_by_symbol[fill.symbol] = fill.price

        symbol_exposures: list[SymbolExposureSnapshot] = []
        for symbol, quantity in sorted(quantities_by_symbol.items()):
            if abs(quantity) <= 1e-9:
                continue
            mark_price = last_price_by_symbol[symbol]
            symbol_exposures.append(
                SymbolExposureSnapshot(
                    account_id=account_id,
                    symbol=symbol,
                    quantity=quantity,
                    mark_price=mark_price,
                    gross_notional=abs(quantity) * mark_price,
                )
            )

        return AccountExposureSnapshot(
            account_id=account_id,
            gross_notional=sum(exposure.gross_notional for exposure in symbol_exposures),
            symbol_exposures=tuple(symbol_exposures),
        )

    def _iter_fill_deltas(
        self,
        *,
        events: Iterable[DomainEvent],
        account_id: str,
    ) -> Iterable[_FillDelta]:
        prior_filled_by_order: dict[str, float] = {}
        prior_average_price_by_order: dict[str, float | None] = {}

        for event in events:
            if event.account_id != account_id or event.event_type not in _FILL_EVENT_TYPES:
                continue

            aggregate_id = event.aggregate_id
            payload = event.payload
            symbol = str(payload["symbol"])
            side = OrderSide(str(payload["side"]))
            current_total_filled = float(payload["filled_quantity"])
            prior_total_filled = prior_filled_by_order.get(aggregate_id, 0.0)
            quantity_delta = current_total_filled - prior_total_filled
            if quantity_delta <= 0:
                raise ProjectionReplayError(
                    "fill event replay produced a non-positive fill delta for "
                    f"aggregate {aggregate_id}"
                )

            if payload.get("last_fill_quantity") is not None:
                explicit_quantity = float(payload["last_fill_quantity"])
                if abs(explicit_quantity - quantity_delta) > 1e-9:
                    raise ProjectionReplayError(
                        "fill event replay detected mismatched cumulative and explicit fill quantities "
                        f"for aggregate {aggregate_id}"
                    )

            price = self._resolve_fill_price(
                aggregate_id=aggregate_id,
                payload=payload,
                prior_total_filled=prior_total_filled,
                current_total_filled=current_total_filled,
                quantity_delta=quantity_delta,
                prior_average_price=prior_average_price_by_order.get(aggregate_id),
            )

            prior_filled_by_order[aggregate_id] = current_total_filled
            prior_average_price_by_order[aggregate_id] = float(payload["average_fill_price"])
            yield _FillDelta(
                account_id=account_id,
                aggregate_id=aggregate_id,
                symbol=symbol,
                side=side,
                quantity_delta=quantity_delta,
                price=price,
            )

    def _resolve_fill_price(
        self,
        *,
        aggregate_id: str,
        payload: dict[str, str | int | float | bool | None],
        prior_total_filled: float,
        current_total_filled: float,
        quantity_delta: float,
        prior_average_price: float | None,
    ) -> float:
        if payload.get("last_fill_price") is not None:
            return float(payload["last_fill_price"])

        current_average_price = payload.get("average_fill_price")
        if current_average_price is None:
            raise ProjectionReplayError(
                f"fill event replay is missing pricing data for aggregate {aggregate_id}"
            )

        current_average_price = float(current_average_price)
        if prior_total_filled <= 1e-9:
            return current_average_price
        if prior_average_price is None:
            raise ProjectionReplayError(
                "fill event replay cannot infer incremental fill price without prior average price "
                f"for aggregate {aggregate_id}"
            )

        numerator = (current_average_price * current_total_filled) - (
            prior_average_price * prior_total_filled
        )
        return numerator / quantity_delta
