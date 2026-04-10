import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequest,
    DuplicateSubmissionError,
    RegisteredSubmission,
)
from auto_execution_engine.domain.events.models import DomainEvent, EventType
from auto_execution_engine.domain.orders.models import (
    OrderAggregate,
    OrderSide,
    OrderStatus,
    OrderType,
)
from auto_execution_engine.reconciliation.models import (
    DriftCategory,
    InternalOrderSnapshot,
    ReconciliationAction,
    ReconciliationDrift,
    ReconciliationReport,
)
from auto_execution_engine.trading_plane.leases import (
    AccountLease,
    AccountLeaseBackend,
    LeaseError,
)


ACTIVE_ORDER_STATUSES = {
    OrderStatus.CREATED,
    OrderStatus.RISK_APPROVED,
    OrderStatus.SUBMITTED,
    OrderStatus.ACKNOWLEDGED,
    OrderStatus.PARTIALLY_FILLED,
    OrderStatus.CANCEL_PENDING,
}


class PersistenceError(ValueError):
    """Raised when durable financial state cannot be recorded or restored."""


class DuplicateEventError(PersistenceError):
    """Raised when an immutable event is appended more than once."""


@dataclass(frozen=True)
class OrderJournalEntry:
    """Append-only snapshot of an order aggregate at a specific event boundary."""

    sequence_number: int
    event_id: str
    client_order_id: str
    account_id: str
    status: OrderStatus
    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    filled_quantity: float
    average_fill_price: float | None
    occurred_at: datetime

    def to_order_aggregate(self) -> OrderAggregate:
        return OrderAggregate(
            account_id=self.account_id,
            symbol=self.symbol,
            side=self.side,
            quantity=self.quantity,
            order_type=self.order_type,
            client_order_id=self.client_order_id,
            status=self.status,
            filled_quantity=self.filled_quantity,
            average_fill_price=self.average_fill_price,
        )

    def to_internal_order_snapshot(self) -> InternalOrderSnapshot:
        return InternalOrderSnapshot(
            account_id=self.account_id,
            client_order_id=self.client_order_id,
            status=self.status.value,
            filled_quantity=self.filled_quantity,
            symbol=self.symbol,
        )


_ORDER_EVENT_TYPES = {
    EventType.ORDER_CREATED,
    EventType.RISK_APPROVED,
    EventType.ORDER_SUBMITTED,
    EventType.ORDER_ACKNOWLEDGED,
    EventType.ORDER_PARTIALLY_FILLED,
    EventType.ORDER_FILLED,
    EventType.ORDER_REJECTED,
    EventType.ORDER_CANCEL_REQUESTED,
    EventType.ORDER_CANCELLED,
    EventType.ORDER_RECONCILED,
}


class _SQLiteStore:
    def __init__(self, *, db_path: str | Path) -> None:
        self._db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._db_path)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _initialize_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS domain_events (
                storage_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                correlation_id TEXT,
                causation_id TEXT,
                account_id TEXT,
                strategy_id TEXT,
                mode TEXT
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_domain_events_aggregate ON domain_events(aggregate_id, storage_sequence)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_domain_events_account ON domain_events(account_id, storage_sequence)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS order_journal (
                sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                client_order_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                status TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                order_type TEXT NOT NULL,
                filled_quantity REAL NOT NULL,
                average_fill_price REAL,
                occurred_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_order_journal_client ON order_journal(client_order_id, sequence_number)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_order_journal_account ON order_journal(account_id, sequence_number)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS reconciliation_reports (
                report_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                action TEXT NOT NULL,
                drifts_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_reconciliation_reports_account ON reconciliation_reports(account_id, report_sequence)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS submission_records (
                client_order_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                broker_order_id TEXT NOT NULL,
                accepted INTEGER NOT NULL,
                message TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_submission_records_account ON submission_records(account_id, client_order_id)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS account_leases (
                account_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_account_leases_owner ON account_leases(owner_id, expires_at)"
        )


class SQLiteEventStore(_SQLiteStore):
    """Durable append-only event store for domain events."""

    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def append(self, *, event: DomainEvent) -> None:
        self.append_batch(events=[event])

    def append_batch(self, *, events: Iterable[DomainEvent]) -> None:
        events = list(events)
        if not events:
            return

        with self._connect() as connection:
            self._initialize_schema(connection)
            self._append_batch_in_connection(connection=connection, events=events)

    def _append_batch_in_connection(
        self, *, connection: sqlite3.Connection, events: Iterable[DomainEvent]
    ) -> None:
        try:
            for event in events:
                connection.execute(
                    """
                    INSERT INTO domain_events(
                        event_id,
                        event_type,
                        aggregate_id,
                        occurred_at,
                        payload_json,
                        correlation_id,
                        causation_id,
                        account_id,
                        strategy_id,
                        mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.event_type.value,
                        event.aggregate_id,
                        event.occurred_at.isoformat(),
                        json.dumps(event.payload, sort_keys=True),
                        event.correlation_id,
                        event.causation_id,
                        event.account_id,
                        event.strategy_id,
                        event.mode,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateEventError("domain event already recorded") from exc

    def list_events(
        self,
        *,
        aggregate_id: str | None = None,
        account_id: str | None = None,
    ) -> list[DomainEvent]:
        predicates: list[str] = []
        params: list[str] = []
        if aggregate_id is not None:
            predicates.append("aggregate_id = ?")
            params.append(aggregate_id)
        if account_id is not None:
            predicates.append("account_id = ?")
            params.append(account_id)

        query = (
            "SELECT event_id, event_type, aggregate_id, occurred_at, payload_json, "
            "correlation_id, causation_id, account_id, strategy_id, mode "
            "FROM domain_events"
        )
        if predicates:
            query += " WHERE " + " AND ".join(predicates)
        query += " ORDER BY storage_sequence ASC"

        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(query, params).fetchall()

        return [
            DomainEvent(
                event_type=EventType(row["event_type"]),
                aggregate_id=row["aggregate_id"],
                payload=json.loads(row["payload_json"]),
                event_id=row["event_id"],
                occurred_at=datetime.fromisoformat(row["occurred_at"]),
                correlation_id=row["correlation_id"],
                causation_id=row["causation_id"],
                account_id=row["account_id"],
                strategy_id=row["strategy_id"],
                mode=row["mode"],
            )
            for row in rows
        ]


class SQLiteOrderJournal(_SQLiteStore):
    """Durable append-only order journal derived from immutable order events."""

    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def append_batch_from_events(self, *, events: Iterable[DomainEvent]) -> None:
        events = list(events)
        if not events:
            return

        with self._connect() as connection:
            self._initialize_schema(connection)
            self._append_batch_in_connection(connection=connection, events=events)

    def _append_batch_in_connection(
        self, *, connection: sqlite3.Connection, events: Iterable[DomainEvent]
    ) -> None:
        try:
            for event in events:
                if event.event_type not in _ORDER_EVENT_TYPES:
                    continue
                snapshot = self._journal_snapshot_from_event(event=event)
                connection.execute(
                    """
                    INSERT INTO order_journal(
                        event_id,
                        client_order_id,
                        account_id,
                        status,
                        symbol,
                        side,
                        quantity,
                        order_type,
                        filled_quantity,
                        average_fill_price,
                        occurred_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.event_id,
                        snapshot.client_order_id,
                        snapshot.account_id,
                        snapshot.status.value,
                        snapshot.symbol,
                        snapshot.side.value,
                        snapshot.quantity,
                        snapshot.order_type.value,
                        snapshot.filled_quantity,
                        snapshot.average_fill_price,
                        snapshot.occurred_at.isoformat(),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateEventError("order journal entry already recorded") from exc

    def list_entries(self, *, client_order_id: str) -> list[OrderJournalEntry]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT sequence_number, event_id, client_order_id, account_id, status,
                       symbol, side, quantity, order_type, filled_quantity,
                       average_fill_price, occurred_at
                FROM order_journal
                WHERE client_order_id = ?
                ORDER BY sequence_number ASC
                """,
                (client_order_id,),
            ).fetchall()

        return [self._entry_from_row(row) for row in rows]

    def list_latest_entries(
        self, *, account_id: str | None = None
    ) -> list[OrderJournalEntry]:
        predicates: list[str] = []
        params: list[str] = []
        if account_id is not None:
            predicates.append("account_id = ?")
            params.append(account_id)

        query = (
            "SELECT sequence_number, event_id, client_order_id, account_id, status, "
            "symbol, side, quantity, order_type, filled_quantity, average_fill_price, occurred_at "
            "FROM order_journal"
        )
        if predicates:
            query += " WHERE " + " AND ".join(predicates)
        query += " ORDER BY client_order_id ASC, sequence_number DESC"

        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(query, params).fetchall()

        latest_by_order: dict[str, OrderJournalEntry] = {}
        for row in rows:
            client_order_id = row["client_order_id"]
            if client_order_id not in latest_by_order:
                latest_by_order[client_order_id] = self._entry_from_row(row)

        return [latest_by_order[client_order_id] for client_order_id in sorted(latest_by_order)]

    def load_latest(self, *, client_order_id: str) -> OrderAggregate | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT sequence_number, event_id, client_order_id, account_id, status,
                       symbol, side, quantity, order_type, filled_quantity,
                       average_fill_price, occurred_at
                FROM order_journal
                WHERE client_order_id = ?
                ORDER BY sequence_number DESC
                LIMIT 1
                """,
                (client_order_id,),
            ).fetchone()

        if row is None:
            return None
        return self._entry_from_row(row).to_order_aggregate()

    def list_active_account_ids(self) -> list[str]:
        active_statuses = tuple(status.value for status in ACTIVE_ORDER_STATUSES)
        placeholders = ", ".join("?" for _ in active_statuses)

        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                f"""
                SELECT DISTINCT latest.account_id AS account_id
                FROM (
                    SELECT account_id, client_order_id, status, MAX(sequence_number) AS latest_sequence
                    FROM order_journal
                    GROUP BY account_id, client_order_id
                ) AS latest
                WHERE latest.status IN ({placeholders})
                ORDER BY latest.account_id ASC
                """,
                active_statuses,
            ).fetchall()

        return [str(row["account_id"]) for row in rows]

    @staticmethod
    def _journal_snapshot_from_event(*, event: DomainEvent) -> OrderJournalEntry:
        payload = event.payload
        account_id = event.account_id
        if account_id is None:
            raise PersistenceError(
                f"order event {event.event_id} is missing account_id in the event envelope"
            )

        return OrderJournalEntry(
            sequence_number=0,
            event_id=event.event_id,
            client_order_id=event.aggregate_id,
            account_id=account_id,
            status=OrderStatus(str(payload["status"])),
            symbol=str(payload["symbol"]),
            side=OrderSide(str(payload["side"])),
            quantity=float(payload["quantity"]),
            order_type=OrderType(str(payload["order_type"])),
            filled_quantity=float(payload.get("filled_quantity", 0.0)),
            average_fill_price=(
                None
                if payload.get("average_fill_price") is None
                else float(payload["average_fill_price"])
            ),
            occurred_at=event.occurred_at,
        )

    @staticmethod
    def _entry_from_row(row: sqlite3.Row) -> OrderJournalEntry:
        return OrderJournalEntry(
            sequence_number=int(row["sequence_number"]),
            event_id=row["event_id"],
            client_order_id=row["client_order_id"],
            account_id=row["account_id"],
            status=OrderStatus(row["status"]),
            symbol=row["symbol"],
            side=OrderSide(row["side"]),
            quantity=float(row["quantity"]),
            order_type=OrderType(row["order_type"]),
            filled_quantity=float(row["filled_quantity"]),
            average_fill_price=(
                None if row["average_fill_price"] is None else float(row["average_fill_price"])
            ),
            occurred_at=datetime.fromisoformat(row["occurred_at"]),
        )


class OrderAggregateRehydrator:
    """Reconstructs an order aggregate deterministically from immutable order events."""

    def replay(self, *, events: Iterable[DomainEvent]) -> OrderAggregate:
        events = [event for event in events if event.event_type in _ORDER_EVENT_TYPES]
        if not events:
            raise PersistenceError("cannot rehydrate order without recorded order events")

        first_event = events[0]
        if first_event.event_type is not EventType.ORDER_CREATED:
            raise PersistenceError("order replay must begin with an order_created event")

        latest_event = events[-1]
        latest_snapshot = SQLiteOrderJournal._journal_snapshot_from_event(event=latest_event)
        return latest_snapshot.to_order_aggregate()


class SQLiteReconciliationReportStore(_SQLiteStore):
    """Durable persistence for reconciliation decisions and recorded drifts."""

    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def append(self, *, report: ReconciliationReport) -> None:
        serialized_drifts = [
            {
                "category": drift.category.value,
                "account_id": drift.account_id,
                "client_order_id": drift.client_order_id,
                "detail": drift.detail,
            }
            for drift in report.drifts
        ]
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute(
                """
                INSERT INTO reconciliation_reports(
                    account_id,
                    generated_at,
                    action,
                    drifts_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    report.account_id,
                    report.generated_at.isoformat(),
                    report.action.value,
                    json.dumps(serialized_drifts, sort_keys=True),
                ),
            )

    def load_latest(self, *, account_id: str) -> ReconciliationReport | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT account_id, generated_at, action, drifts_json
                FROM reconciliation_reports
                WHERE account_id = ?
                ORDER BY report_sequence DESC
                LIMIT 1
                """,
                (account_id,),
            ).fetchone()

        if row is None:
            return None

        drifts_payload = json.loads(row["drifts_json"])
        return ReconciliationReport(
            account_id=row["account_id"],
            generated_at=datetime.fromisoformat(row["generated_at"]),
            action=ReconciliationAction(row["action"]),
            drifts=tuple(
                ReconciliationDrift(
                    category=DriftCategory(drift["category"]),
                    account_id=str(drift["account_id"]),
                    client_order_id=(
                        None
                        if drift.get("client_order_id") is None
                        else str(drift["client_order_id"])
                    ),
                    detail=str(drift["detail"]),
                )
                for drift in drifts_payload
            ),
        )

    def list_account_ids(self) -> list[str]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT DISTINCT account_id
                FROM reconciliation_reports
                ORDER BY account_id ASC
                """
            ).fetchall()

        return [str(row["account_id"]) for row in rows]


class SQLiteSubmissionBook(_SQLiteStore):
    """Durable broker submission deduplication that survives process restarts."""

    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def mark_submitted(
        self, *, request: BrokerOrderRequest, submission: RegisteredSubmission
    ) -> None:
        try:
            with self._connect() as connection:
                self._initialize_schema(connection)
                connection.execute(
                    """
                    INSERT INTO submission_records(
                        client_order_id,
                        account_id,
                        broker_order_id,
                        accepted,
                        message,
                        recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request.client_order_id,
                        request.account_id,
                        submission.broker_order_id,
                        1 if submission.accepted else 0,
                        submission.message,
                        datetime.now(UTC).isoformat(),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateSubmissionError(
                f"client order {request.client_order_id} has already been submitted"
            ) from exc

    def load_submission(self, *, client_order_id: str) -> RegisteredSubmission | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT account_id, client_order_id, broker_order_id, accepted, message
                FROM submission_records
                WHERE client_order_id = ?
                """,
                (client_order_id,),
            ).fetchone()

        if row is None:
            return None

        return RegisteredSubmission(
            account_id=row["account_id"],
            client_order_id=row["client_order_id"],
            broker_order_id=row["broker_order_id"],
            accepted=bool(row["accepted"]),
            message=row["message"],
        )


class SQLiteAccountLeaseBackend(_SQLiteStore):
    """Durable account lease authority for restart-safe multi-worker ownership."""

    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def acquire(
        self,
        *,
        existing_lease: AccountLease | None,
        account_id: str,
        owner_id: str,
        now: datetime,
        ttl_seconds: int,
    ) -> AccountLease:
        del existing_lease
        expires_at = now + timedelta(seconds=ttl_seconds)

        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT account_id, owner_id, acquired_at, expires_at
                FROM account_leases
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()

            if row is not None:
                current_lease = AccountLease(
                    account_id=row["account_id"],
                    owner_id=row["owner_id"],
                    acquired_at=datetime.fromisoformat(row["acquired_at"]),
                    expires_at=datetime.fromisoformat(row["expires_at"]),
                )
                if current_lease.is_active(now=now):
                    if current_lease.owner_id != owner_id:
                        raise LeaseError(
                            f"account {account_id} is already controlled by {current_lease.owner_id}"
                        )
                    connection.execute(
                        """
                        UPDATE account_leases
                        SET expires_at = ?
                        WHERE account_id = ?
                        """,
                        (expires_at.isoformat(), account_id),
                    )
                    return AccountLease(
                        account_id=account_id,
                        owner_id=owner_id,
                        acquired_at=current_lease.acquired_at,
                        expires_at=expires_at,
                    )

            connection.execute(
                """
                INSERT INTO account_leases(account_id, owner_id, acquired_at, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at
                """,
                (
                    account_id,
                    owner_id,
                    now.isoformat(),
                    expires_at.isoformat(),
                ),
            )

        return AccountLease(
            account_id=account_id,
            owner_id=owner_id,
            acquired_at=now,
            expires_at=expires_at,
        )


class SQLiteOrderStore:
    """Coordinates durable event recording, append-only journaling, and order replay."""

    def __init__(self, *, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._event_store = SQLiteEventStore(db_path=self.db_path)
        self._order_journal = SQLiteOrderJournal(db_path=self.db_path)
        self._reconciliation_reports = SQLiteReconciliationReportStore(db_path=self.db_path)
        self._submission_book = SQLiteSubmissionBook(db_path=self.db_path)
        self._lease_backend = SQLiteAccountLeaseBackend(db_path=self.db_path)
        self._rehydrator = OrderAggregateRehydrator()

    def initialize(self) -> None:
        self._event_store.initialize()
        self._order_journal.initialize()
        self._reconciliation_reports.initialize()
        self._submission_book.initialize()
        self._lease_backend.initialize()

    def record_events(self, *, events: Iterable[DomainEvent]) -> None:
        events = list(events)
        if not events:
            return

        with self._event_store._connect() as connection:
            self._event_store._initialize_schema(connection)
            self._event_store._append_batch_in_connection(
                connection=connection,
                events=events,
            )
            self._order_journal._append_batch_in_connection(
                connection=connection,
                events=events,
            )

    def list_events(self, *, aggregate_id: str) -> list[DomainEvent]:
        return self._event_store.list_events(aggregate_id=aggregate_id)

    def load_order(self, *, client_order_id: str) -> OrderAggregate:
        events = self._event_store.list_events(aggregate_id=client_order_id)
        return self._rehydrator.replay(events=events)

    def load_order_history(self, *, client_order_id: str) -> list[OrderJournalEntry]:
        return self._order_journal.list_entries(client_order_id=client_order_id)

    def load_latest_order(self, *, client_order_id: str) -> OrderAggregate | None:
        return self._order_journal.load_latest(client_order_id=client_order_id)

    def list_internal_order_snapshots(
        self, *, account_id: str | None = None
    ) -> list[InternalOrderSnapshot]:
        entries = self._order_journal.list_latest_entries(account_id=account_id)
        return [entry.to_internal_order_snapshot() for entry in entries]

    def repair_orders_from_submissions(self, *, account_id: str | None = None) -> list[str]:
        repaired_client_order_ids: list[str] = []
        entries = self._order_journal.list_latest_entries(account_id=account_id)

        for entry in entries:
            submission = self._submission_book.load_submission(
                client_order_id=entry.client_order_id
            )
            if submission is None:
                continue

            order = self.load_order(client_order_id=entry.client_order_id)
            recovery_events: list[DomainEvent] = []

            if order.status is OrderStatus.CREATED:
                order, risk_event = order.transition(OrderStatus.RISK_APPROVED)
                recovery_events.append(risk_event)
            if order.status is OrderStatus.RISK_APPROVED:
                order, submitted_event = order.transition(OrderStatus.SUBMITTED)
                recovery_events.append(submitted_event)

            if not recovery_events:
                continue

            self.record_events(events=recovery_events)
            repaired_client_order_ids.append(entry.client_order_id)

        return repaired_client_order_ids

    def list_accounts_requiring_reconciliation(self) -> list[str]:
        return self._order_journal.list_active_account_ids()

    def record_reconciliation_report(self, *, report: ReconciliationReport) -> None:
        self._reconciliation_reports.append(report=report)

    def load_latest_reconciliation_report(
        self, *, account_id: str
    ) -> ReconciliationReport | None:
        return self._reconciliation_reports.load_latest(account_id=account_id)

    def list_accounts_with_reconciliation_reports(self) -> list[str]:
        return self._reconciliation_reports.list_account_ids()

    def build_submission_book(self) -> SQLiteSubmissionBook:
        self._submission_book.initialize()
        return self._submission_book

    def build_account_lease_backend(self) -> SQLiteAccountLeaseBackend:
        self._lease_backend.initialize()
        return self._lease_backend
