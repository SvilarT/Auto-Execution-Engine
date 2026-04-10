from datetime import UTC, datetime
from pathlib import Path

import pytest

from auto_execution_engine.adapters.broker.models import BrokerOrderRequest, BrokerOrderSide, BrokerOrderType
from auto_execution_engine.adapters.broker.service import RegisteredSubmission
from auto_execution_engine.adapters.persistence.sqlite_order_store import (
    DuplicateEventError,
    OrderAggregateRehydrator,
    SQLiteAccountLeaseBackend,
    SQLiteEventStore,
    SQLiteOrderJournal,
    SQLiteOrderStore,
    SQLiteSubmissionBook,
)
from auto_execution_engine.domain.events.models import DomainEvent, EventType
from auto_execution_engine.domain.orders.models import (
    OrderAggregate,
    OrderSide,
    OrderStatus,
    OrderType,
)
from auto_execution_engine.reconciliation.models import InternalOrderSnapshot


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "order_store.db"


def make_order(*, client_order_id: str = "client-123") -> OrderAggregate:
    return OrderAggregate(
        account_id="acct-1",
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=10,
        order_type=OrderType.MARKET,
        client_order_id=client_order_id,
    )


def test_event_store_round_trips_domain_events(db_path: Path) -> None:
    store = SQLiteEventStore(db_path=db_path)
    store.initialize()

    event = DomainEvent(
        event_type=EventType.ORDER_CREATED,
        aggregate_id="client-123",
        account_id="acct-1",
        payload={
            "status": "created",
            "symbol": "AAPL",
            "side": "buy",
            "quantity": 10,
            "filled_quantity": 0.0,
            "order_type": "market",
            "average_fill_price": None,
        },
    )

    store.append(event=event)

    reloaded = SQLiteEventStore(db_path=db_path)
    events = reloaded.list_events(aggregate_id="client-123")

    assert len(events) == 1
    assert events[0] == event


def test_event_store_rejects_duplicate_event_ids(db_path: Path) -> None:
    store = SQLiteEventStore(db_path=db_path)
    store.initialize()

    event = make_order().create_event()

    store.append(event=event)
    with pytest.raises(DuplicateEventError):
        store.append(event=event)


def test_order_journal_returns_history_and_latest_snapshot(db_path: Path) -> None:
    journal = SQLiteOrderJournal(db_path=db_path)
    journal.initialize()

    order = make_order()
    created_event = order.create_event()
    approved_order, approved_event = order.transition(OrderStatus.RISK_APPROVED)
    submitted_order, submitted_event = approved_order.transition(OrderStatus.SUBMITTED)

    journal.append_batch_from_events(
        events=[created_event, approved_event, submitted_event]
    )

    entries = journal.list_entries(client_order_id=order.client_order_id)
    latest = journal.load_latest(client_order_id=order.client_order_id)

    assert [entry.status for entry in entries] == [
        OrderStatus.CREATED,
        OrderStatus.RISK_APPROVED,
        OrderStatus.SUBMITTED,
    ]
    assert latest == submitted_order


def test_order_rehydrator_rebuilds_order_from_recorded_events() -> None:
    order = make_order()
    created_event = order.create_event()
    approved_order, approved_event = order.transition(OrderStatus.RISK_APPROVED)
    submitted_order, submitted_event = approved_order.transition(OrderStatus.SUBMITTED)
    filled_order, filled_event = submitted_order.transition(
        OrderStatus.PARTIALLY_FILLED,
        fill_quantity_delta=4,
        fill_price=189.25,
    )

    rehydrator = OrderAggregateRehydrator()
    restored = rehydrator.replay(
        events=[created_event, approved_event, submitted_event, filled_event]
    )

    assert restored == filled_order


def test_order_store_persists_events_and_rehydrates_after_restart(db_path: Path) -> None:
    store = SQLiteOrderStore(db_path=db_path)
    store.initialize()

    order = make_order()
    created_event = order.create_event()
    approved_order, approved_event = order.transition(OrderStatus.RISK_APPROVED)
    submitted_order, submitted_event = approved_order.transition(OrderStatus.SUBMITTED)

    store.record_events(events=[created_event, approved_event, submitted_event])

    restarted_store = SQLiteOrderStore(db_path=db_path)
    restored_order = restarted_store.load_order(client_order_id=order.client_order_id)
    restored_history = restarted_store.load_order_history(client_order_id=order.client_order_id)

    assert restored_order == submitted_order
    assert [entry.event_id for entry in restored_history] == [
        created_event.event_id,
        approved_event.event_id,
        submitted_event.event_id,
    ]


def test_order_store_projects_latest_internal_snapshots_for_reconciliation(
    db_path: Path,
) -> None:
    store = SQLiteOrderStore(db_path=db_path)
    store.initialize()

    first_order = make_order(client_order_id="client-123")
    second_order = make_order(client_order_id="client-456")

    first_created = first_order.create_event()
    first_approved_order, first_approved = first_order.transition(OrderStatus.RISK_APPROVED)
    first_submitted_order, first_submitted = first_approved_order.transition(
        OrderStatus.SUBMITTED
    )
    _, first_partial_fill = first_submitted_order.transition(
        OrderStatus.PARTIALLY_FILLED,
        fill_quantity_delta=3,
        fill_price=188.5,
    )

    second_created = second_order.create_event()
    _, second_approved = second_order.transition(OrderStatus.RISK_APPROVED)

    store.record_events(
        events=[
            first_created,
            first_approved,
            first_submitted,
            first_partial_fill,
            second_created,
            second_approved,
        ]
    )

    snapshots = store.list_internal_order_snapshots(account_id="acct-1")

    assert snapshots == [
        InternalOrderSnapshot(
            account_id="acct-1",
            client_order_id="client-123",
            status="partially_filled",
            filled_quantity=3.0,
            symbol="AAPL",
        ),
        InternalOrderSnapshot(
            account_id="acct-1",
            client_order_id="client-456",
            status="risk_approved",
            filled_quantity=0.0,
            symbol="AAPL",
        ),
    ]


def test_submission_book_persists_registered_submission_across_restart(db_path: Path) -> None:
    book = SQLiteSubmissionBook(db_path=db_path)
    book.initialize()
    request = BrokerOrderRequest(
        account_id="acct-1",
        client_order_id="client-123",
        symbol="AAPL",
        side=BrokerOrderSide.BUY,
        quantity=10,
        order_type=BrokerOrderType.MARKET,
    )
    submission = RegisteredSubmission(
        account_id="acct-1",
        client_order_id="client-123",
        broker_order_id="pending::client-123",
        accepted=True,
        message="submission registered",
    )

    book.mark_submitted(request=request, submission=submission)

    restarted_book = SQLiteSubmissionBook(db_path=db_path)
    assert restarted_book.load_submission(client_order_id="client-123") == submission


def test_submission_book_rejects_duplicate_registered_submission_across_restart(
    db_path: Path,
) -> None:
    book = SQLiteSubmissionBook(db_path=db_path)
    book.initialize()
    request = BrokerOrderRequest(
        account_id="acct-1",
        client_order_id="client-123",
        symbol="AAPL",
        side=BrokerOrderSide.BUY,
        quantity=10,
        order_type=BrokerOrderType.MARKET,
    )
    submission = RegisteredSubmission(
        account_id="acct-1",
        client_order_id="client-123",
        broker_order_id="pending::client-123",
        accepted=True,
        message="submission registered",
    )

    book.mark_submitted(request=request, submission=submission)

    restarted_book = SQLiteSubmissionBook(db_path=db_path)
    with pytest.raises(ValueError, match="already been submitted"):
        restarted_book.mark_submitted(request=request, submission=submission)


def test_account_lease_backend_persists_and_renews_same_owner_across_restart(
    db_path: Path,
) -> None:
    backend = SQLiteAccountLeaseBackend(db_path=db_path)
    backend.initialize()
    now = datetime(2026, 1, 1, tzinfo=UTC)

    lease = backend.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=now,
        ttl_seconds=30,
    )

    restarted_backend = SQLiteAccountLeaseBackend(db_path=db_path)
    renewed = restarted_backend.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=now.replace(second=20),
        ttl_seconds=30,
    )

    assert renewed.account_id == "acct-1"
    assert renewed.owner_id == "worker-a"
    assert renewed.acquired_at == lease.acquired_at
    assert renewed.expires_at == datetime(2026, 1, 1, 0, 0, 50, tzinfo=UTC)


def test_account_lease_backend_rejects_competing_owner_while_active(db_path: Path) -> None:
    backend = SQLiteAccountLeaseBackend(db_path=db_path)
    backend.initialize()
    now = datetime(2026, 1, 1, tzinfo=UTC)

    backend.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=now,
        ttl_seconds=30,
    )

    restarted_backend = SQLiteAccountLeaseBackend(db_path=db_path)
    with pytest.raises(ValueError, match="already controlled by worker-a"):
        restarted_backend.acquire(
            existing_lease=None,
            account_id="acct-1",
            owner_id="worker-b",
            now=now.replace(second=10),
            ttl_seconds=30,
        )
