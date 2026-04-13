from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from auto_execution_engine.adapters.broker.models import (
    BrokerOrderActivityPage,
    CanonicalBrokerOrderStatus,
    RawBrokerActivity,
    RawBrokerOrderSnapshot,
)
from auto_execution_engine.adapters.persistence.sqlite_order_store import SQLiteOrderStore
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderSide, OrderStatus, OrderType
from auto_execution_engine.reconciliation.broker_sync_models import BrokerSyncRunStatus
from auto_execution_engine.reconciliation.broker_sync_service import BrokerSyncRunner, BrokerSyncService
from auto_execution_engine.trading_plane.leases import (
    AccountLease,
    AccountLeaseService,
    InMemoryAccountLeaseBackend,
    LeaseError,
)


@dataclass
class StubBrokerStateReader:
    snapshots: tuple[RawBrokerOrderSnapshot, ...]
    pages: list[BrokerOrderActivityPage]

    def __post_init__(self) -> None:
        self.calls: list[tuple[str, str | None, int]] = []
        self._page_index = 0

    def list_order_snapshots(self, *, account_id: str) -> tuple[RawBrokerOrderSnapshot, ...]:
        return tuple(snapshot for snapshot in self.snapshots if snapshot.account_id == account_id)

    def list_order_activities(
        self,
        *,
        account_id: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> BrokerOrderActivityPage:
        self.calls.append((account_id, cursor, limit))
        if self._page_index >= len(self.pages):
            return BrokerOrderActivityPage(account_id=account_id, activities=(), cursor=cursor, has_more=False)
        page = self.pages[self._page_index]
        self._page_index += 1
        return page

    def get_order_by_client_order_id(
        self, *, account_id: str, client_order_id: str
    ) -> RawBrokerOrderSnapshot | None:
        for snapshot in self.snapshots:
            if snapshot.account_id == account_id and snapshot.client_order_id == client_order_id:
                return snapshot
        return None


def make_store(tmp_path) -> SQLiteOrderStore:
    store = SQLiteOrderStore(db_path=tmp_path / "broker-sync.db")
    store.initialize()
    return store


def record_submitted_order(
    store: SQLiteOrderStore,
    *,
    account_id: str = "acct-1",
    client_order_id: str = "ord-1",
    quantity: float = 1.0,
) -> OrderAggregate:
    order = OrderAggregate(
        account_id=account_id,
        client_order_id=client_order_id,
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=quantity,
        order_type=OrderType.MARKET,
    )
    created_event = order.create_event()
    risk_approved_order, risk_event = order.transition(OrderStatus.RISK_APPROVED)
    submitted_order, submitted_event = risk_approved_order.transition(OrderStatus.SUBMITTED)
    store.record_events(events=[created_event, risk_event, submitted_event])
    return submitted_order


def make_snapshot(
    *,
    account_id: str = "acct-1",
    client_order_id: str = "ord-1",
    broker_order_id: str = "broker-1",
    status: CanonicalBrokerOrderStatus,
    filled_quantity: float,
) -> RawBrokerOrderSnapshot:
    return RawBrokerOrderSnapshot(
        account_id=account_id,
        broker_order_id=broker_order_id,
        client_order_id=client_order_id,
        symbol="AAPL",
        raw_status=status.value,
        canonical_status=status,
        filled_quantity=filled_quantity,
        raw_payload={"status": status.value, "filled_qty": filled_quantity},
        observed_at="2026-04-12T12:00:00Z",
    )


def make_fill_activity(
    *,
    account_id: str = "acct-1",
    client_order_id: str = "ord-1",
    activity_id: str = "fill-1",
    quantity: float = 0.25,
    price: float = 101.5,
) -> RawBrokerActivity:
    return RawBrokerActivity(
        account_id=account_id,
        activity_id=activity_id,
        activity_type="fill",
        client_order_id=client_order_id,
        broker_order_id="broker-1",
        raw_payload={
            "client_order_id": client_order_id,
            "qty": quantity,
            "price": price,
        },
        occurred_at="2026-04-12T12:01:00Z",
    )


def test_broker_sync_service_ingests_fill_advances_status_and_persists_raw_truth(tmp_path) -> None:
    store = make_store(tmp_path)
    record_submitted_order(store)
    service = BrokerSyncService(order_store=store, activity_page_limit=2)
    reader = StubBrokerStateReader(
        snapshots=(
            make_snapshot(
                status=CanonicalBrokerOrderStatus.PARTIALLY_FILLED,
                filled_quantity=0.25,
            ),
        ),
        pages=[
            BrokerOrderActivityPage(
                account_id="acct-1",
                activities=(make_fill_activity(),),
                cursor="cursor-1",
                has_more=False,
            )
        ],
    )

    result = service.synchronize_account(
        account_id="acct-1",
        broker_state_reader=reader,
        now=datetime(2026, 4, 12, 12, 2, tzinfo=UTC),
    )

    recovered = store.load_order(client_order_id="ord-1")
    raw_orders = store.list_broker_sync_raw_orders(account_id="acct-1")
    raw_activities = store.list_broker_sync_raw_activities(account_id="acct-1")
    cursor = store.load_broker_sync_cursor(account_id="acct-1")

    assert recovered.status is OrderStatus.PARTIALLY_FILLED
    assert recovered.filled_quantity == 0.25
    assert result.raw_order_count == 1
    assert result.raw_activity_count == 1
    assert result.duplicate_activity_count == 0
    assert result.next_activity_cursor == "cursor-1"
    assert len(raw_orders) == 1
    assert len(raw_activities) == 1
    assert cursor is not None
    assert cursor.activity_cursor == "cursor-1"


def test_broker_sync_service_suppresses_duplicate_activity_replay_across_restarts(tmp_path) -> None:
    store = make_store(tmp_path)
    record_submitted_order(store)
    first_service = BrokerSyncService(order_store=store)
    first_reader = StubBrokerStateReader(
        snapshots=(
            make_snapshot(
                status=CanonicalBrokerOrderStatus.PARTIALLY_FILLED,
                filled_quantity=0.5,
            ),
        ),
        pages=[
            BrokerOrderActivityPage(
                account_id="acct-1",
                activities=(make_fill_activity(activity_id="fill-dup", quantity=0.5),),
                cursor="cursor-dup",
                has_more=False,
            )
        ],
    )

    first_result = first_service.synchronize_account(
        account_id="acct-1",
        broker_state_reader=first_reader,
        now=datetime(2026, 4, 12, 12, 5, tzinfo=UTC),
    )

    second_service = BrokerSyncService(order_store=store)
    second_reader = StubBrokerStateReader(
        snapshots=(
            make_snapshot(
                status=CanonicalBrokerOrderStatus.PARTIALLY_FILLED,
                filled_quantity=0.5,
            ),
        ),
        pages=[
            BrokerOrderActivityPage(
                account_id="acct-1",
                activities=(make_fill_activity(activity_id="fill-dup", quantity=0.5),),
                cursor="cursor-dup",
                has_more=False,
            )
        ],
    )

    second_result = second_service.synchronize_account(
        account_id="acct-1",
        broker_state_reader=second_reader,
        now=datetime(2026, 4, 12, 12, 6, tzinfo=UTC),
    )

    recovered = store.load_order(client_order_id="ord-1")

    assert first_result.duplicate_activity_count == 0
    assert second_result.duplicate_activity_count == 1
    assert recovered.filled_quantity == 0.5
    assert recovered.status is OrderStatus.PARTIALLY_FILLED
    assert len(store.list_broker_sync_raw_activities(account_id="acct-1")) == 1


def test_broker_sync_service_promotes_acknowledged_and_rejected_terminal_states(tmp_path) -> None:
    store = make_store(tmp_path)
    record_submitted_order(store, client_order_id="ack-1")
    record_submitted_order(store, client_order_id="rej-1")
    service = BrokerSyncService(order_store=store)
    reader = StubBrokerStateReader(
        snapshots=(
            make_snapshot(
                client_order_id="ack-1",
                broker_order_id="broker-ack",
                status=CanonicalBrokerOrderStatus.ACKNOWLEDGED,
                filled_quantity=0.0,
            ),
            make_snapshot(
                client_order_id="rej-1",
                broker_order_id="broker-rej",
                status=CanonicalBrokerOrderStatus.REJECTED,
                filled_quantity=0.0,
            ),
        ),
        pages=[BrokerOrderActivityPage(account_id="acct-1", activities=(), cursor=None, has_more=False)],
    )

    service.synchronize_account(account_id="acct-1", broker_state_reader=reader)

    assert store.load_order(client_order_id="ack-1").status is OrderStatus.ACKNOWLEDGED
    assert store.load_order(client_order_id="rej-1").status is OrderStatus.REJECTED


def test_broker_sync_runner_records_completed_and_skipped_runs(tmp_path) -> None:
    class BlockingLeaseBackend(InMemoryAccountLeaseBackend):
        def __init__(self) -> None:
            self.blocking_lease: AccountLease | None = None

        def acquire(
            self,
            *,
            existing_lease: AccountLease | None,
            account_id: str,
            owner_id: str,
            now: datetime,
            ttl_seconds: int,
        ) -> AccountLease:
            if (
                self.blocking_lease is not None
                and self.blocking_lease.account_id == account_id
                and self.blocking_lease.is_active(now=now)
                and self.blocking_lease.owner_id != owner_id
            ):
                raise LeaseError(
                    f"account {account_id} is already controlled by {self.blocking_lease.owner_id}"
                )
            lease = super().acquire(
                existing_lease=existing_lease,
                account_id=account_id,
                owner_id=owner_id,
                now=now,
                ttl_seconds=ttl_seconds,
            )
            if owner_id == "external-owner":
                self.blocking_lease = lease
            return lease

        def release(
            self,
            *,
            existing_lease: AccountLease | None,
            account_id: str,
            owner_id: str,
            now: datetime,
        ) -> None:
            super().release(
                existing_lease=existing_lease,
                account_id=account_id,
                owner_id=owner_id,
                now=now,
            )
            if (
                self.blocking_lease is not None
                and existing_lease is not None
                and self.blocking_lease == existing_lease
            ):
                self.blocking_lease = None

    store = make_store(tmp_path)
    record_submitted_order(store)
    now = datetime(2026, 4, 12, 12, 10, tzinfo=UTC)
    backend = BlockingLeaseBackend()
    lease_service = AccountLeaseService(backend=backend)
    reader = StubBrokerStateReader(
        snapshots=(make_snapshot(status=CanonicalBrokerOrderStatus.NEW, filled_quantity=0.0),),
        pages=[BrokerOrderActivityPage(account_id="acct-1", activities=(), cursor="cursor-0", has_more=False)],
    )
    runner = BrokerSyncRunner(
        order_store=store,
        lease_service=lease_service,
        broker_state_reader=reader,
    )

    completed = runner.run_once(account_ids=["acct-1"], now=now)[0]

    blocking_lease = lease_service.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="external-owner",
        now=now,
        ttl_seconds=30,
    )
    skipped = runner.run_once(account_ids=["acct-1"], now=now)[0]
    lease_service.release(
        existing_lease=blocking_lease,
        account_id="acct-1",
        owner_id="external-owner",
        now=now,
    )

    latest_run = store.load_latest_broker_sync_run(account_id="acct-1")
    all_runs = store.list_broker_sync_runs(account_id="acct-1")

    assert completed.status is BrokerSyncRunStatus.COMPLETED
    assert completed.batch_result is not None
    assert completed.batch_result.next_activity_cursor == "cursor-0"
    assert skipped.status is BrokerSyncRunStatus.SKIPPED
    assert latest_run is not None
    assert latest_run.status is BrokerSyncRunStatus.SKIPPED
    assert [record.status for record in all_runs] == [
        BrokerSyncRunStatus.COMPLETED,
        BrokerSyncRunStatus.SKIPPED,
    ]
