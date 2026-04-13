from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from auto_execution_engine.adapters.broker.models import (
    BrokerOrderActivityPage,
    CanonicalBrokerOrderStatus,
    RawBrokerActivity,
    RawBrokerOrderSnapshot,
)
from auto_execution_engine.adapters.broker.service import BrokerStateReader
from auto_execution_engine.domain.events.models import DomainEvent
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderFill, OrderStatus
from auto_execution_engine.reconciliation.broker_sync_models import (
    BrokerSyncBatchResult,
    BrokerSyncCursor,
    BrokerSyncRunRecord,
    BrokerSyncRunStatus,
)
from auto_execution_engine.trading_plane.leases import AccountLeaseService, LeaseError


class BrokerSyncStore(Protocol):
    def append_broker_sync_raw_orders(self, *, orders: Iterable[RawBrokerOrderSnapshot]) -> int: ...

    def record_broker_sync_activity(self, *, activity: RawBrokerActivity): ...

    def load_broker_sync_cursor(self, *, account_id: str) -> BrokerSyncCursor | None: ...

    def update_broker_sync_cursor(self, *, cursor: BrokerSyncCursor) -> None: ...

    def append_broker_sync_run(self, *, record: BrokerSyncRunRecord) -> None: ...

    def load_latest_order(self, *, client_order_id: str) -> OrderAggregate | None: ...

    def record_events(self, *, events: list[DomainEvent]) -> None: ...

    def ingest_fill(
        self,
        *,
        client_order_id: str,
        fill: OrderFill,
    ) -> tuple[OrderAggregate, DomainEvent]: ...

    def repair_orders_from_submissions(self, *, account_id: str | None = None) -> list[str]: ...

    def list_accounts_requiring_reconciliation(self) -> list[str]: ...


@dataclass(frozen=True)
class ParsedFillActivity:
    fill_id: str
    client_order_id: str
    quantity: float
    price: float
    occurred_at: datetime
    broker_order_id: str | None = None


class BrokerSyncService:
    """Poll broker truth, durably store it, and safely advance internal order state."""

    def __init__(self, *, order_store: BrokerSyncStore, activity_page_limit: int = 100) -> None:
        self._order_store = order_store
        self._activity_page_limit = activity_page_limit

    def synchronize_account(
        self,
        *,
        account_id: str,
        broker_state_reader: BrokerStateReader,
        now: datetime | None = None,
    ) -> BrokerSyncBatchResult:
        current_time = now or datetime.now(UTC)
        self._order_store.repair_orders_from_submissions(account_id=account_id)

        raw_orders = broker_state_reader.list_order_snapshots(account_id=account_id)
        raw_order_count = self._order_store.append_broker_sync_raw_orders(orders=raw_orders)

        cursor_record = self._order_store.load_broker_sync_cursor(account_id=account_id)
        next_cursor = cursor_record.activity_cursor if cursor_record is not None else None
        raw_activity_count = 0
        duplicate_activity_count = 0

        while True:
            activity_page = broker_state_reader.list_order_activities(
                account_id=account_id,
                cursor=next_cursor,
                limit=self._activity_page_limit,
            )
            page_count = len(activity_page.activities)
            raw_activity_count += page_count

            for activity in activity_page.activities:
                result = self._order_store.record_broker_sync_activity(activity=activity)
                if result.duplicate:
                    duplicate_activity_count += 1
                    continue
                self._apply_activity(activity=activity)

            previous_cursor = next_cursor
            next_cursor = activity_page.cursor
            if page_count == 0 or not activity_page.has_more or next_cursor == previous_cursor:
                break

        for snapshot in raw_orders:
            self._apply_snapshot(snapshot=snapshot)

        self._order_store.update_broker_sync_cursor(
            cursor=BrokerSyncCursor(
                account_id=account_id,
                activity_cursor=next_cursor,
                updated_at=current_time,
            )
        )
        return BrokerSyncBatchResult(
            account_id=account_id,
            raw_order_count=raw_order_count,
            raw_activity_count=raw_activity_count,
            duplicate_activity_count=duplicate_activity_count,
            next_activity_cursor=next_cursor,
        )

    def _apply_activity(self, *, activity: RawBrokerActivity) -> None:
        parsed_fill = self._parse_fill_activity(activity=activity)
        if parsed_fill is None:
            return
        if self._order_store.load_latest_order(client_order_id=parsed_fill.client_order_id) is None:
            return
        self._order_store.ingest_fill(
            client_order_id=parsed_fill.client_order_id,
            fill=OrderFill(
                fill_id=parsed_fill.fill_id,
                quantity=parsed_fill.quantity,
                price=parsed_fill.price,
                occurred_at=parsed_fill.occurred_at,
                broker_order_id=parsed_fill.broker_order_id,
                source="broker_sync",
            ),
        )

    def _apply_snapshot(self, *, snapshot: RawBrokerOrderSnapshot) -> None:
        order = self._order_store.load_latest_order(client_order_id=snapshot.client_order_id)
        if order is None:
            return

        target_status = self._target_status_for_snapshot(order=order, snapshot=snapshot)
        if target_status is None or target_status is order.status:
            return

        updated_order, event = order.transition(target_status)
        self._order_store.record_events(events=[event])

        if (
            target_status is OrderStatus.CANCEL_PENDING
            and snapshot.canonical_status is CanonicalBrokerOrderStatus.CANCELED
        ):
            cancelled_order, cancelled_event = updated_order.transition(OrderStatus.CANCELLED)
            self._order_store.record_events(events=[cancelled_event])
            _ = cancelled_order

    def _target_status_for_snapshot(
        self,
        *,
        order: OrderAggregate,
        snapshot: RawBrokerOrderSnapshot,
    ) -> OrderStatus | None:
        status = snapshot.canonical_status
        if status in {
            CanonicalBrokerOrderStatus.NEW,
            CanonicalBrokerOrderStatus.ACKNOWLEDGED,
            CanonicalBrokerOrderStatus.UNKNOWN,
            CanonicalBrokerOrderStatus.REPLACED,
            CanonicalBrokerOrderStatus.PENDING_REPLACE,
            CanonicalBrokerOrderStatus.SUSPENDED,
        }:
            if order.status is OrderStatus.SUBMITTED:
                return OrderStatus.ACKNOWLEDGED
            return None
        if status is CanonicalBrokerOrderStatus.PARTIALLY_FILLED:
            if order.status in {OrderStatus.SUBMITTED, OrderStatus.ACKNOWLEDGED}:
                if abs(order.filled_quantity - snapshot.filled_quantity) <= 1e-9 and order.filled_quantity > 0:
                    return OrderStatus.PARTIALLY_FILLED
            return None
        if status is CanonicalBrokerOrderStatus.FILLED:
            if order.status in {OrderStatus.SUBMITTED, OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIALLY_FILLED}:
                if abs(order.filled_quantity - order.quantity) <= 1e-9:
                    return OrderStatus.FILLED
            return None
        if status in {CanonicalBrokerOrderStatus.REJECTED, CanonicalBrokerOrderStatus.EXPIRED}:
            if order.status is OrderStatus.SUBMITTED:
                return OrderStatus.REJECTED
            return None
        if status is CanonicalBrokerOrderStatus.PENDING_CANCEL:
            if order.status in {
                OrderStatus.SUBMITTED,
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.PARTIALLY_FILLED,
            }:
                return OrderStatus.CANCEL_PENDING
            return None
        if status is CanonicalBrokerOrderStatus.CANCELED:
            if order.status is OrderStatus.CANCEL_PENDING:
                return OrderStatus.CANCELLED
            if order.status in {
                OrderStatus.SUBMITTED,
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.PARTIALLY_FILLED,
            }:
                return OrderStatus.CANCEL_PENDING
            return None
        return None

    def _parse_fill_activity(self, *, activity: RawBrokerActivity) -> ParsedFillActivity | None:
        payload = activity.raw_payload
        client_order_id = activity.client_order_id or self._coerce_string(
            payload.get("client_order_id")
            or payload.get("client_order_uuid")
            or payload.get("order_id")
        )
        if client_order_id is None:
            return None

        quantity = self._coerce_float(
            payload.get("qty")
            or payload.get("quantity")
            or payload.get("last_fill_qty")
            or payload.get("filled_qty")
        )
        price = self._coerce_float(
            payload.get("price")
            or payload.get("last_fill_price")
            or payload.get("fill_price")
        )
        if quantity is None or quantity <= 0 or price is None or price <= 0:
            return None

        occurred_at = self._parse_datetime(activity.occurred_at)
        fill_id = activity.activity_id
        return ParsedFillActivity(
            fill_id=fill_id,
            client_order_id=client_order_id,
            quantity=quantity,
            price=price,
            occurred_at=occurred_at,
            broker_order_id=activity.broker_order_id,
        )

    def _coerce_string(self, value: object) -> str | None:
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    def _coerce_float(self, value: object) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip():
            try:
                return float(value)
            except ValueError:
                return None
        return None

    def _parse_datetime(self, value: str) -> datetime:
        normalized = value.strip() if value.strip() else datetime.now(UTC).isoformat()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed


class BrokerSyncRunner:
    """Run broker synchronization periodically with durable per-account run records."""

    def __init__(
        self,
        *,
        order_store: BrokerSyncStore,
        lease_service: AccountLeaseService,
        broker_state_reader: BrokerStateReader,
        sync_service: BrokerSyncService | None = None,
        owner_id: str = "broker-sync-runner",
        lease_ttl_seconds: int = 30,
    ) -> None:
        self._order_store = order_store
        self._lease_service = lease_service
        self._broker_state_reader = broker_state_reader
        self._sync_service = sync_service or BrokerSyncService(order_store=order_store)
        self._owner_id = owner_id
        self._lease_ttl_seconds = lease_ttl_seconds

    def run_once(
        self,
        *,
        account_ids: Iterable[str] | None = None,
        now: datetime | None = None,
    ) -> list[BrokerSyncRunRecord]:
        current_time = now or datetime.now(UTC)
        resolved_account_ids = sorted(set(account_ids or self._order_store.list_accounts_requiring_reconciliation()))
        return [
            self._run_account(account_id=account_id, now=current_time)
            for account_id in resolved_account_ids
        ]

    def _run_account(self, *, account_id: str, now: datetime) -> BrokerSyncRunRecord:
        run_id = str(uuid4())
        try:
            lease = self._lease_service.acquire(
                existing_lease=None,
                account_id=account_id,
                owner_id=self._owner_id,
                now=now,
                ttl_seconds=self._lease_ttl_seconds,
            )
        except LeaseError as exc:
            record = BrokerSyncRunRecord(
                run_id=run_id,
                account_id=account_id,
                owner_id=self._owner_id,
                started_at=now,
                completed_at=now,
                status=BrokerSyncRunStatus.SKIPPED,
                detail=str(exc),
            )
            self._order_store.append_broker_sync_run(record=record)
            return record

        try:
            batch_result = self._sync_service.synchronize_account(
                account_id=account_id,
                broker_state_reader=self._broker_state_reader,
                now=now,
            )
            record = BrokerSyncRunRecord(
                run_id=run_id,
                account_id=account_id,
                owner_id=self._owner_id,
                started_at=now,
                completed_at=now,
                status=BrokerSyncRunStatus.COMPLETED,
                detail=(
                    "broker sync completed without new activity"
                    if batch_result.raw_order_count == 0 and batch_result.raw_activity_count == 0
                    else "broker sync completed"
                ),
                batch_result=batch_result,
            )
            self._order_store.append_broker_sync_run(record=record)
            return record
        except Exception as exc:
            record = BrokerSyncRunRecord(
                run_id=run_id,
                account_id=account_id,
                owner_id=self._owner_id,
                started_at=now,
                completed_at=now,
                status=BrokerSyncRunStatus.FAILED,
                detail=str(exc),
            )
            self._order_store.append_broker_sync_run(record=record)
            return record
        finally:
            self._lease_service.release(
                existing_lease=lease,
                account_id=account_id,
                owner_id=self._owner_id,
                now=now,
            )
