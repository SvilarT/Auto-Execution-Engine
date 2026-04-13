import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable, TypedDict, cast

from auto_execution_engine.adapters.broker.models import (
    BrokerRetryDisposition,
    BrokerSubmissionOutcome,
    CanonicalBrokerOrderStatus,
    RawBrokerActivity,
    RawBrokerOrderSnapshot,
)
from auto_execution_engine.adapters.broker.service import (
    BrokerOrderRequest,
    DuplicateSubmissionError,
    RegisteredSubmission,
)
from auto_execution_engine.config.execution_mode import ExecutionMode
from auto_execution_engine.domain.events.models import DomainEvent, EventType
from auto_execution_engine.domain.orders.models import (
    OrderAggregate,
    OrderFill,
    OrderSide,
    OrderStatus,
    OrderType,
)
from auto_execution_engine.domain.risk.models import AccountExposureSnapshot
from auto_execution_engine.observability_models import (
    OperatorActionRecord,
    RuntimeHealthSummary,
)
from auto_execution_engine.promotion_gates import (
    PromotionCriterionResult,
    PromotionGateDecisionRecord,
)
from auto_execution_engine.reconciliation.broker_sync_models import (
    BrokerActivityIngestionResult,
    BrokerSyncBatchResult,
    BrokerSyncCursor,
    BrokerSyncRunRecord,
    BrokerSyncRunStatus,
)
from auto_execution_engine.reconciliation.models import (
    BrokerOrderSnapshot,
    CashSnapshot,
    DriftCategory,
    InternalOrderSnapshot,
    PositionSnapshot,
    ReconciliationAction,
    ReconciliationCycleRecord,
    ReconciliationDrift,
    ReconciliationReport,
    ReconciliationRunRecord,
    ReconciliationRunStatus,
)
from auto_execution_engine.reconciliation.projections import EventLogProjectionService
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


class ReconciliationDriftPayload(TypedDict):
    category: str
    account_id: str
    client_order_id: str | None
    detail: str


class ReconciliationReportPayload(TypedDict):
    account_id: str
    generated_at: str
    action: str
    drifts: list[ReconciliationDriftPayload]


class RawBrokerOrderPayload(TypedDict):
    account_id: str
    broker_order_id: str
    client_order_id: str
    symbol: str
    raw_status: str
    canonical_status: str
    filled_quantity: float
    raw_payload: dict[str, object]
    observed_at: str


class RawBrokerActivityPayload(TypedDict):
    account_id: str
    activity_id: str
    activity_type: str
    client_order_id: str | None
    broker_order_id: str | None
    raw_payload: dict[str, object]
    occurred_at: str


class BrokerSyncBatchPayload(TypedDict):
    account_id: str
    raw_order_count: int
    raw_activity_count: int
    duplicate_activity_count: int
    next_activity_cursor: str | None


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
                drifts_json TEXT NOT NULL,
                internal_orders_json TEXT NOT NULL DEFAULT '[]',
                broker_orders_json TEXT NOT NULL DEFAULT '[]',
                internal_positions_json TEXT NOT NULL DEFAULT '[]',
                broker_positions_json TEXT NOT NULL DEFAULT '[]',
                internal_cash_json TEXT,
                broker_cash_json TEXT
            )
            """
        )
        existing_report_columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(reconciliation_reports)"
            ).fetchall()
        }
        if "internal_orders_json" not in existing_report_columns:
            connection.execute(
                "ALTER TABLE reconciliation_reports ADD COLUMN internal_orders_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "broker_orders_json" not in existing_report_columns:
            connection.execute(
                "ALTER TABLE reconciliation_reports ADD COLUMN broker_orders_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "internal_positions_json" not in existing_report_columns:
            connection.execute(
                "ALTER TABLE reconciliation_reports ADD COLUMN internal_positions_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "broker_positions_json" not in existing_report_columns:
            connection.execute(
                "ALTER TABLE reconciliation_reports ADD COLUMN broker_positions_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "internal_cash_json" not in existing_report_columns:
            connection.execute(
                "ALTER TABLE reconciliation_reports ADD COLUMN internal_cash_json TEXT"
            )
        if "broker_cash_json" not in existing_report_columns:
            connection.execute(
                "ALTER TABLE reconciliation_reports ADD COLUMN broker_cash_json TEXT"
            )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_reconciliation_reports_account ON reconciliation_reports(account_id, report_sequence)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS reconciliation_runs (
                run_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL UNIQUE,
                account_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT NOT NULL,
                report_json TEXT
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_account ON reconciliation_runs(account_id, run_sequence)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_sync_raw_orders (
                sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                broker_order_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                raw_status TEXT NOT NULL,
                canonical_status TEXT NOT NULL,
                filled_quantity REAL NOT NULL,
                observed_at TEXT NOT NULL,
                raw_payload_json TEXT NOT NULL,
                UNIQUE(account_id, broker_order_id, observed_at)
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_broker_sync_raw_orders_account ON broker_sync_raw_orders(account_id, sequence_number)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_broker_sync_raw_orders_client ON broker_sync_raw_orders(client_order_id, sequence_number)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_sync_activities (
                sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                activity_id TEXT NOT NULL,
                activity_type TEXT NOT NULL,
                client_order_id TEXT,
                broker_order_id TEXT,
                occurred_at TEXT NOT NULL,
                raw_payload_json TEXT NOT NULL,
                UNIQUE(account_id, activity_id)
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_broker_sync_activities_account ON broker_sync_activities(account_id, sequence_number)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_broker_sync_activities_order ON broker_sync_activities(account_id, client_order_id, sequence_number)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_sync_cursors (
                account_id TEXT PRIMARY KEY,
                activity_cursor TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_sync_runs (
                run_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL UNIQUE,
                account_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT NOT NULL,
                batch_result_json TEXT
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_broker_sync_runs_account ON broker_sync_runs(account_id, run_sequence)"
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
        existing_submission_columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(submission_records)"
            ).fetchall()
        }
        if "outcome" not in existing_submission_columns:
            connection.execute(
                "ALTER TABLE submission_records ADD COLUMN outcome TEXT NOT NULL DEFAULT 'accepted'"
            )
            connection.execute(
                "UPDATE submission_records SET outcome = CASE accepted WHEN 1 THEN 'accepted' ELSE 'unknown' END"
            )
        if "retry_disposition" not in existing_submission_columns:
            connection.execute(
                "ALTER TABLE submission_records ADD COLUMN retry_disposition TEXT NOT NULL DEFAULT 'do_not_retry'"
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

    def append(
        self,
        *,
        report: ReconciliationReport,
        internal_orders: Iterable[InternalOrderSnapshot] = (),
        broker_orders: Iterable[BrokerOrderSnapshot] = (),
        internal_positions: Iterable[PositionSnapshot] = (),
        broker_positions: Iterable[PositionSnapshot] = (),
        internal_cash: CashSnapshot | None = None,
        broker_cash: CashSnapshot | None = None,
    ) -> None:
        serialized_drifts = [
            {
                "category": drift.category.value,
                "account_id": drift.account_id,
                "client_order_id": drift.client_order_id,
                "detail": drift.detail,
            }
            for drift in report.drifts
        ]
        serialized_internal_orders = [
            {
                "account_id": order.account_id,
                "client_order_id": order.client_order_id,
                "status": order.status,
                "filled_quantity": order.filled_quantity,
                "symbol": order.symbol,
            }
            for order in internal_orders
        ]
        serialized_broker_orders = [
            {
                "account_id": order.account_id,
                "client_order_id": order.client_order_id,
                "status": order.status,
                "filled_quantity": order.filled_quantity,
                "symbol": order.symbol,
            }
            for order in broker_orders
        ]
        serialized_internal_positions = [
            {
                "account_id": position.account_id,
                "symbol": position.symbol,
                "quantity": position.quantity,
            }
            for position in internal_positions
        ]
        serialized_broker_positions = [
            {
                "account_id": position.account_id,
                "symbol": position.symbol,
                "quantity": position.quantity,
            }
            for position in broker_positions
        ]
        serialized_internal_cash = None
        if internal_cash is not None:
            serialized_internal_cash = {
                "account_id": internal_cash.account_id,
                "balance": internal_cash.balance,
            }
        serialized_broker_cash = None
        if broker_cash is not None:
            serialized_broker_cash = {
                "account_id": broker_cash.account_id,
                "balance": broker_cash.balance,
            }
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute(
                """
                INSERT INTO reconciliation_reports(
                    account_id,
                    generated_at,
                    action,
                    drifts_json,
                    internal_orders_json,
                    broker_orders_json,
                    internal_positions_json,
                    broker_positions_json,
                    internal_cash_json,
                    broker_cash_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report.account_id,
                    report.generated_at.isoformat(),
                    report.action.value,
                    json.dumps(serialized_drifts, sort_keys=True),
                    json.dumps(serialized_internal_orders, sort_keys=True),
                    json.dumps(serialized_broker_orders, sort_keys=True),
                    json.dumps(serialized_internal_positions, sort_keys=True),
                    json.dumps(serialized_broker_positions, sort_keys=True),
                    json.dumps(serialized_internal_cash, sort_keys=True)
                    if serialized_internal_cash is not None
                    else None,
                    json.dumps(serialized_broker_cash, sort_keys=True)
                    if serialized_broker_cash is not None
                    else None,
                ),
            )

    def load_latest(self, *, account_id: str) -> ReconciliationReport | None:
        cycle = self.load_latest_cycle(account_id=account_id)
        if cycle is None:
            return None
        return cycle.report

    def load_latest_cycle(self, *, account_id: str) -> ReconciliationCycleRecord | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT account_id, generated_at, action, drifts_json, internal_orders_json, broker_orders_json,
                       internal_positions_json, broker_positions_json, internal_cash_json, broker_cash_json
                FROM reconciliation_reports
                WHERE account_id = ?
                ORDER BY report_sequence DESC
                LIMIT 1
                """,
                (account_id,),
            ).fetchone()

        if row is None:
            return None
        return self._cycle_from_row(row)

    def list_cycles(self, *, account_id: str) -> list[ReconciliationCycleRecord]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT account_id, generated_at, action, drifts_json, internal_orders_json, broker_orders_json,
                       internal_positions_json, broker_positions_json, internal_cash_json, broker_cash_json
                FROM reconciliation_reports
                WHERE account_id = ?
                ORDER BY report_sequence ASC
                """,
                (account_id,),
            ).fetchall()

        return [self._cycle_from_row(row) for row in rows]

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

    def append_run(self, *, record: ReconciliationRunRecord) -> None:
        serialized_report = None
        if record.report is not None:
            serialized_report = json.dumps(
                self._serialize_report(record.report),
                sort_keys=True,
            )
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute(
                """
                INSERT INTO reconciliation_runs(
                    run_id,
                    account_id,
                    owner_id,
                    started_at,
                    completed_at,
                    status,
                    detail,
                    report_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.run_id,
                    record.account_id,
                    record.owner_id,
                    record.started_at.isoformat(),
                    record.completed_at.isoformat(),
                    record.status.value,
                    record.detail,
                    serialized_report,
                ),
            )

    def load_latest_run(self, *, account_id: str) -> ReconciliationRunRecord | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT run_id, account_id, owner_id, started_at, completed_at, status, detail, report_json
                FROM reconciliation_runs
                WHERE account_id = ?
                ORDER BY run_sequence DESC
                LIMIT 1
                """,
                (account_id,),
            ).fetchone()

        if row is None:
            return None
        return self._run_from_row(row)

    def list_runs(self, *, account_id: str) -> list[ReconciliationRunRecord]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT run_id, account_id, owner_id, started_at, completed_at, status, detail, report_json
                FROM reconciliation_runs
                WHERE account_id = ?
                ORDER BY run_sequence ASC
                """,
                (account_id,),
            ).fetchall()

        return [self._run_from_row(row) for row in rows]

    def _cycle_from_row(self, row: sqlite3.Row) -> ReconciliationCycleRecord:
        drifts_payload = json.loads(row["drifts_json"])
        internal_payload = json.loads(row["internal_orders_json"])
        broker_payload = json.loads(row["broker_orders_json"])
        internal_positions_payload = json.loads(row["internal_positions_json"])
        broker_positions_payload = json.loads(row["broker_positions_json"])
        internal_cash_payload = (
            json.loads(row["internal_cash_json"])
            if row["internal_cash_json"] is not None
            else None
        )
        broker_cash_payload = (
            json.loads(row["broker_cash_json"])
            if row["broker_cash_json"] is not None
            else None
        )
        generated_at = datetime.fromisoformat(row["generated_at"])
        report = self._report_from_payload(
            {
                "account_id": row["account_id"],
                "generated_at": row["generated_at"],
                "action": row["action"],
                "drifts": drifts_payload,
            }
        )
        return ReconciliationCycleRecord(
            account_id=row["account_id"],
            generated_at=generated_at,
            internal_orders=tuple(
                InternalOrderSnapshot(
                    account_id=str(order["account_id"]),
                    client_order_id=str(order["client_order_id"]),
                    status=str(order["status"]),
                    filled_quantity=float(order["filled_quantity"]),
                    symbol=str(order["symbol"]),
                )
                for order in internal_payload
            ),
            broker_orders=tuple(
                BrokerOrderSnapshot(
                    account_id=str(order["account_id"]),
                    client_order_id=str(order["client_order_id"]),
                    status=str(order["status"]),
                    filled_quantity=float(order["filled_quantity"]),
                    symbol=str(order["symbol"]),
                )
                for order in broker_payload
            ),
            report=report,
            internal_positions=tuple(
                PositionSnapshot(
                    account_id=str(position["account_id"]),
                    symbol=str(position["symbol"]),
                    quantity=float(position["quantity"]),
                )
                for position in internal_positions_payload
            ),
            broker_positions=tuple(
                PositionSnapshot(
                    account_id=str(position["account_id"]),
                    symbol=str(position["symbol"]),
                    quantity=float(position["quantity"]),
                )
                for position in broker_positions_payload
            ),
            internal_cash=(
                CashSnapshot(
                    account_id=str(internal_cash_payload["account_id"]),
                    balance=float(internal_cash_payload["balance"]),
                )
                if internal_cash_payload is not None
                else None
            ),
            broker_cash=(
                CashSnapshot(
                    account_id=str(broker_cash_payload["account_id"]),
                    balance=float(broker_cash_payload["balance"]),
                )
                if broker_cash_payload is not None
                else None
            ),
        )

    def _run_from_row(self, row: sqlite3.Row) -> ReconciliationRunRecord:
        report = None
        if row["report_json"] is not None:
            report = self._report_from_payload(
                cast(ReconciliationReportPayload, json.loads(row["report_json"]))
            )
        return ReconciliationRunRecord(
            run_id=str(row["run_id"]),
            account_id=str(row["account_id"]),
            owner_id=str(row["owner_id"]),
            started_at=datetime.fromisoformat(row["started_at"]),
            completed_at=datetime.fromisoformat(row["completed_at"]),
            status=ReconciliationRunStatus(row["status"]),
            detail=str(row["detail"]),
            report=report,
        )

    def _serialize_report(self, report: ReconciliationReport) -> ReconciliationReportPayload:
        return ReconciliationReportPayload(
            account_id=report.account_id,
            generated_at=report.generated_at.isoformat(),
            action=report.action.value,
            drifts=[
                ReconciliationDriftPayload(
                    category=drift.category.value,
                    account_id=drift.account_id,
                    client_order_id=drift.client_order_id,
                    detail=drift.detail,
                )
                for drift in report.drifts
            ],
        )

    def _report_from_payload(self, payload: ReconciliationReportPayload) -> ReconciliationReport:
        return ReconciliationReport(
            account_id=payload["account_id"],
            generated_at=datetime.fromisoformat(payload["generated_at"]),
            action=ReconciliationAction(payload["action"]),
            drifts=tuple(
                ReconciliationDrift(
                    category=DriftCategory(drift["category"]),
                    account_id=drift["account_id"],
                    client_order_id=drift["client_order_id"],
                    detail=drift["detail"],
                )
                for drift in payload["drifts"]
            ),
        )


class SQLiteBrokerSyncStore(_SQLiteStore):
    """Durable storage for raw broker truth, ingestion cursors, and sync runs."""

    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def append_raw_orders(self, *, orders: Iterable[RawBrokerOrderSnapshot]) -> int:
        serialized_orders = [self._serialize_raw_order(order) for order in orders]
        if not serialized_orders:
            return 0
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.executemany(
                """
                INSERT OR IGNORE INTO broker_sync_raw_orders(
                    account_id,
                    broker_order_id,
                    client_order_id,
                    symbol,
                    raw_status,
                    canonical_status,
                    filled_quantity,
                    observed_at,
                    raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        payload["account_id"],
                        payload["broker_order_id"],
                        payload["client_order_id"],
                        payload["symbol"],
                        payload["raw_status"],
                        payload["canonical_status"],
                        payload["filled_quantity"],
                        payload["observed_at"],
                        json.dumps(payload["raw_payload"], sort_keys=True),
                    )
                    for payload in serialized_orders
                ],
            )
        return len(serialized_orders)

    def record_activity(self, *, activity: RawBrokerActivity) -> BrokerActivityIngestionResult:
        payload = self._serialize_raw_activity(activity)
        with self._connect() as connection:
            self._initialize_schema(connection)
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO broker_sync_activities(
                    account_id,
                    activity_id,
                    activity_type,
                    client_order_id,
                    broker_order_id,
                    occurred_at,
                    raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["account_id"],
                    payload["activity_id"],
                    payload["activity_type"],
                    payload["client_order_id"],
                    payload["broker_order_id"],
                    payload["occurred_at"],
                    json.dumps(payload["raw_payload"], sort_keys=True),
                ),
            )
        return BrokerActivityIngestionResult(
            account_id=activity.account_id,
            activity_id=activity.activity_id,
            duplicate=cursor.rowcount == 0,
        )

    def update_cursor(self, *, cursor_record: BrokerSyncCursor) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute(
                """
                INSERT INTO broker_sync_cursors(account_id, activity_cursor, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    activity_cursor = excluded.activity_cursor,
                    updated_at = excluded.updated_at
                """,
                (
                    cursor_record.account_id,
                    cursor_record.activity_cursor,
                    cursor_record.updated_at.isoformat(),
                ),
            )

    def load_cursor(self, *, account_id: str) -> BrokerSyncCursor | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT account_id, activity_cursor, updated_at
                FROM broker_sync_cursors
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        return BrokerSyncCursor(
            account_id=str(row["account_id"]),
            activity_cursor=str(row["activity_cursor"]) if row["activity_cursor"] is not None else None,
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def append_run(self, *, record: BrokerSyncRunRecord) -> None:
        serialized_batch = None
        if record.batch_result is not None:
            serialized_batch = json.dumps(
                self._serialize_batch_result(record.batch_result),
                sort_keys=True,
            )
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute(
                """
                INSERT INTO broker_sync_runs(
                    run_id,
                    account_id,
                    owner_id,
                    started_at,
                    completed_at,
                    status,
                    detail,
                    batch_result_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.run_id,
                    record.account_id,
                    record.owner_id,
                    record.started_at.isoformat(),
                    record.completed_at.isoformat(),
                    record.status.value,
                    record.detail,
                    serialized_batch,
                ),
            )

    def load_latest_run(self, *, account_id: str) -> BrokerSyncRunRecord | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT run_id, account_id, owner_id, started_at, completed_at, status, detail, batch_result_json
                FROM broker_sync_runs
                WHERE account_id = ?
                ORDER BY run_sequence DESC
                LIMIT 1
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        return self._run_from_row(row)

    def list_runs(self, *, account_id: str) -> list[BrokerSyncRunRecord]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT run_id, account_id, owner_id, started_at, completed_at, status, detail, batch_result_json
                FROM broker_sync_runs
                WHERE account_id = ?
                ORDER BY run_sequence ASC
                """,
                (account_id,),
            ).fetchall()
        return [self._run_from_row(row) for row in rows]

    def list_raw_orders(self, *, account_id: str) -> list[RawBrokerOrderSnapshot]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT account_id, broker_order_id, client_order_id, symbol, raw_status,
                       canonical_status, filled_quantity, observed_at, raw_payload_json
                FROM broker_sync_raw_orders
                WHERE account_id = ?
                ORDER BY sequence_number ASC
                """,
                (account_id,),
            ).fetchall()
        return [self._raw_order_from_row(row) for row in rows]

    def list_raw_activities(self, *, account_id: str) -> list[RawBrokerActivity]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT account_id, activity_id, activity_type, client_order_id,
                       broker_order_id, occurred_at, raw_payload_json
                FROM broker_sync_activities
                WHERE account_id = ?
                ORDER BY sequence_number ASC
                """,
                (account_id,),
            ).fetchall()
        return [self._raw_activity_from_row(row) for row in rows]

    def _serialize_raw_order(self, order: RawBrokerOrderSnapshot) -> RawBrokerOrderPayload:
        return RawBrokerOrderPayload(
            account_id=order.account_id,
            broker_order_id=order.broker_order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            raw_status=order.raw_status,
            canonical_status=order.canonical_status.value,
            filled_quantity=order.filled_quantity,
            raw_payload=order.raw_payload,
            observed_at=order.observed_at,
        )

    def _serialize_raw_activity(self, activity: RawBrokerActivity) -> RawBrokerActivityPayload:
        return RawBrokerActivityPayload(
            account_id=activity.account_id,
            activity_id=activity.activity_id,
            activity_type=activity.activity_type,
            client_order_id=activity.client_order_id,
            broker_order_id=activity.broker_order_id,
            raw_payload=activity.raw_payload,
            occurred_at=activity.occurred_at,
        )

    def _serialize_batch_result(self, result: BrokerSyncBatchResult) -> BrokerSyncBatchPayload:
        return BrokerSyncBatchPayload(
            account_id=result.account_id,
            raw_order_count=result.raw_order_count,
            raw_activity_count=result.raw_activity_count,
            duplicate_activity_count=result.duplicate_activity_count,
            next_activity_cursor=result.next_activity_cursor,
        )

    def _batch_result_from_payload(self, payload: BrokerSyncBatchPayload) -> BrokerSyncBatchResult:
        return BrokerSyncBatchResult(
            account_id=payload["account_id"],
            raw_order_count=int(payload["raw_order_count"]),
            raw_activity_count=int(payload["raw_activity_count"]),
            duplicate_activity_count=int(payload["duplicate_activity_count"]),
            next_activity_cursor=payload["next_activity_cursor"],
        )

    def _run_from_row(self, row: sqlite3.Row) -> BrokerSyncRunRecord:
        batch_result = None
        if row["batch_result_json"] is not None:
            batch_result = self._batch_result_from_payload(
                cast(BrokerSyncBatchPayload, json.loads(row["batch_result_json"]))
            )
        return BrokerSyncRunRecord(
            run_id=str(row["run_id"]),
            account_id=str(row["account_id"]),
            owner_id=str(row["owner_id"]),
            started_at=datetime.fromisoformat(row["started_at"]),
            completed_at=datetime.fromisoformat(row["completed_at"]),
            status=BrokerSyncRunStatus(row["status"]),
            detail=str(row["detail"]),
            batch_result=batch_result,
        )

    def _raw_order_from_row(self, row: sqlite3.Row) -> RawBrokerOrderSnapshot:
        return RawBrokerOrderSnapshot(
            account_id=str(row["account_id"]),
            broker_order_id=str(row["broker_order_id"]),
            client_order_id=str(row["client_order_id"]),
            symbol=str(row["symbol"]),
            raw_status=str(row["raw_status"]),
            canonical_status=CanonicalBrokerOrderStatus(str(row["canonical_status"])),
            filled_quantity=float(row["filled_quantity"]),
            raw_payload=cast(dict[str, object], json.loads(row["raw_payload_json"])),
            observed_at=str(row["observed_at"]),
        )

    def _raw_activity_from_row(self, row: sqlite3.Row) -> RawBrokerActivity:
        return RawBrokerActivity(
            account_id=str(row["account_id"]),
            activity_id=str(row["activity_id"]),
            activity_type=str(row["activity_type"]),
            client_order_id=str(row["client_order_id"]) if row["client_order_id"] is not None else None,
            broker_order_id=str(row["broker_order_id"]) if row["broker_order_id"] is not None else None,
            raw_payload=cast(dict[str, object], json.loads(row["raw_payload_json"])),
            occurred_at=str(row["occurred_at"]),
        )


class SQLiteSubmissionBook(_SQLiteStore):
    """Durable broker submission deduplication that survives process restarts."""

    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def record_submission(
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
                        outcome,
                        retry_disposition,
                        message,
                        recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request.client_order_id,
                        request.account_id,
                        submission.broker_order_id or "",
                        1 if submission.accepted else 0,
                        submission.outcome.value,
                        submission.retry_disposition.value,
                        submission.message,
                        datetime.now(UTC).isoformat(),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateSubmissionError(
                f"client order {request.client_order_id} has already been submitted"
            ) from exc

    def mark_submitted(
        self, *, request: BrokerOrderRequest, submission: RegisteredSubmission
    ) -> None:
        self.record_submission(request=request, submission=submission)

    def load_submission(self, *, client_order_id: str) -> RegisteredSubmission | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT
                    account_id,
                    client_order_id,
                    broker_order_id,
                    accepted,
                    outcome,
                    retry_disposition,
                    message
                FROM submission_records
                WHERE client_order_id = ?
                """,
                (client_order_id,),
            ).fetchone()

        if row is None:
            return None

        outcome = row["outcome"]
        if outcome is None:
            outcome = "accepted" if bool(row["accepted"]) else "unknown"

        broker_order_id = row["broker_order_id"]
        if broker_order_id == "":
            broker_order_id = None

        return RegisteredSubmission(
            account_id=row["account_id"],
            client_order_id=row["client_order_id"],
            broker_order_id=broker_order_id,
            outcome=BrokerSubmissionOutcome(outcome),
            retry_disposition=BrokerRetryDisposition(row["retry_disposition"]),
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
            connection.commit()
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

    def release(
        self,
        *,
        existing_lease: AccountLease | None,
        account_id: str,
        owner_id: str,
        now: datetime,
    ) -> None:
        del existing_lease

        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT account_id, owner_id, acquired_at, expires_at
                FROM account_leases
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()
            if row is None:
                return

            current_lease = AccountLease(
                account_id=row["account_id"],
                owner_id=row["owner_id"],
                acquired_at=datetime.fromisoformat(row["acquired_at"]),
                expires_at=datetime.fromisoformat(row["expires_at"]),
            )
            if not current_lease.is_active(now=now):
                connection.execute(
                    "DELETE FROM account_leases WHERE account_id = ?",
                    (account_id,),
                )
                return

            current_lease.assert_owned_by(owner_id=owner_id, now=now)
            connection.execute(
                "DELETE FROM account_leases WHERE account_id = ? AND owner_id = ?",
                (account_id, owner_id),
            )


class SQLiteObservabilityStore(_SQLiteStore):
    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def _initialize_schema(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS operator_action_history (
                sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                detail TEXT NOT NULL,
                operator_id TEXT,
                correlation_id TEXT,
                recorded_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_health_summaries (
                sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                status TEXT NOT NULL,
                active_order_count INTEGER NOT NULL,
                open_position_count INTEGER NOT NULL,
                gross_notional REAL NOT NULL,
                cash_balance REAL NOT NULL,
                is_quarantined INTEGER NOT NULL,
                kill_switch_active INTEGER NOT NULL,
                latest_reconciliation_action TEXT,
                latest_reconciliation_status TEXT,
                latest_reconciliation_detail TEXT,
                drift_count INTEGER NOT NULL,
                last_operator_action_type TEXT,
                detail TEXT NOT NULL
            )
            """
        )

    def append_operator_action(self, *, record: OperatorActionRecord) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute(
                """
                INSERT INTO operator_action_history(
                    account_id, action_type, detail, operator_id, correlation_id, recorded_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.account_id,
                    record.action_type,
                    record.detail,
                    record.operator_id,
                    record.correlation_id,
                    record.recorded_at.isoformat(),
                ),
            )

    def list_operator_actions(
        self, *, account_id: str, limit: int = 50
    ) -> list[OperatorActionRecord]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT account_id, action_type, detail, operator_id, correlation_id, recorded_at
                FROM operator_action_history
                WHERE account_id = ?
                ORDER BY sequence_number DESC
                LIMIT ?
                """,
                (account_id, limit),
            ).fetchall()
        return [
            OperatorActionRecord(
                account_id=row["account_id"],
                action_type=row["action_type"],
                detail=row["detail"],
                operator_id=row["operator_id"],
                correlation_id=row["correlation_id"],
                recorded_at=datetime.fromisoformat(row["recorded_at"]),
            )
            for row in rows
        ]

    def append_runtime_health_summary(self, *, summary: RuntimeHealthSummary) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute(
                """
                INSERT INTO runtime_health_summaries(
                    account_id, generated_at, status, active_order_count, open_position_count,
                    gross_notional, cash_balance, is_quarantined, kill_switch_active,
                    latest_reconciliation_action, latest_reconciliation_status,
                    latest_reconciliation_detail, drift_count, last_operator_action_type, detail
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    summary.account_id,
                    summary.generated_at.isoformat(),
                    summary.status,
                    summary.active_order_count,
                    summary.open_position_count,
                    summary.gross_notional,
                    summary.cash_balance,
                    int(summary.is_quarantined),
                    int(summary.kill_switch_active),
                    summary.latest_reconciliation_action,
                    summary.latest_reconciliation_status,
                    summary.latest_reconciliation_detail,
                    summary.drift_count,
                    summary.last_operator_action_type,
                    summary.detail,
                ),
            )

    def load_latest_runtime_health_summary(
        self, *, account_id: str
    ) -> RuntimeHealthSummary | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT account_id, generated_at, status, active_order_count, open_position_count,
                       gross_notional, cash_balance, is_quarantined, kill_switch_active,
                       latest_reconciliation_action, latest_reconciliation_status,
                       latest_reconciliation_detail, drift_count, last_operator_action_type, detail
                FROM runtime_health_summaries
                WHERE account_id = ?
                ORDER BY sequence_number DESC
                LIMIT 1
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        return RuntimeHealthSummary(
            account_id=row["account_id"],
            generated_at=datetime.fromisoformat(row["generated_at"]),
            status=row["status"],
            active_order_count=row["active_order_count"],
            open_position_count=row["open_position_count"],
            gross_notional=row["gross_notional"],
            cash_balance=row["cash_balance"],
            is_quarantined=bool(row["is_quarantined"]),
            kill_switch_active=bool(row["kill_switch_active"]),
            latest_reconciliation_action=row["latest_reconciliation_action"],
            latest_reconciliation_status=row["latest_reconciliation_status"],
            latest_reconciliation_detail=row["latest_reconciliation_detail"],
            drift_count=row["drift_count"],
            last_operator_action_type=row["last_operator_action_type"],
            detail=row["detail"],
        )

    def list_runtime_health_summaries(
        self, *, account_id: str, limit: int = 50
    ) -> list[RuntimeHealthSummary]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT account_id, generated_at, status, active_order_count, open_position_count,
                       gross_notional, cash_balance, is_quarantined, kill_switch_active,
                       latest_reconciliation_action, latest_reconciliation_status,
                       latest_reconciliation_detail, drift_count, last_operator_action_type, detail
                FROM runtime_health_summaries
                WHERE account_id = ?
                ORDER BY sequence_number DESC
                LIMIT ?
                """,
                (account_id, limit),
            ).fetchall()
        return [
            RuntimeHealthSummary(
                account_id=row["account_id"],
                generated_at=datetime.fromisoformat(row["generated_at"]),
                status=row["status"],
                active_order_count=row["active_order_count"],
                open_position_count=row["open_position_count"],
                gross_notional=row["gross_notional"],
                cash_balance=row["cash_balance"],
                is_quarantined=bool(row["is_quarantined"]),
                kill_switch_active=bool(row["kill_switch_active"]),
                latest_reconciliation_action=row["latest_reconciliation_action"],
                latest_reconciliation_status=row["latest_reconciliation_status"],
                latest_reconciliation_detail=row["latest_reconciliation_detail"],
                drift_count=row["drift_count"],
                last_operator_action_type=row["last_operator_action_type"],
                detail=row["detail"],
            )
            for row in rows
        ]

    def list_accounts_with_runtime_health(self) -> list[str]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            rows = connection.execute(
                """
                SELECT DISTINCT account_id
                FROM runtime_health_summaries
                ORDER BY account_id ASC
                """
            ).fetchall()
        return [row["account_id"] for row in rows]


class SQLitePromotionGateStore(_SQLiteStore):
    def initialize(self) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)

    def _initialize_schema(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS promotion_gate_decisions (
                sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
                target_mode TEXT NOT NULL,
                source_mode TEXT,
                approved INTEGER NOT NULL,
                summary TEXT NOT NULL,
                evaluator TEXT NOT NULL,
                evaluated_at TEXT NOT NULL,
                required_drills_json TEXT NOT NULL,
                completed_drills_json TEXT NOT NULL,
                criteria_json TEXT NOT NULL
            )
            """
        )

    def append_decision(self, *, record: PromotionGateDecisionRecord) -> None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            connection.execute(
                """
                INSERT INTO promotion_gate_decisions(
                    target_mode,
                    source_mode,
                    approved,
                    summary,
                    evaluator,
                    evaluated_at,
                    required_drills_json,
                    completed_drills_json,
                    criteria_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.target_mode.value,
                    record.source_mode.value if record.source_mode is not None else None,
                    int(record.approved),
                    record.summary,
                    record.evaluator,
                    record.evaluated_at.isoformat(),
                    json.dumps(list(record.required_drills)),
                    json.dumps(list(record.completed_drills)),
                    json.dumps(
                        [
                            {
                                "criterion_name": criterion.criterion_name,
                                "passed": criterion.passed,
                                "detail": criterion.detail,
                            }
                            for criterion in record.criteria
                        ]
                    ),
                ),
            )

    def load_latest_decision(
        self, *, target_mode: str
    ) -> PromotionGateDecisionRecord | None:
        with self._connect() as connection:
            self._initialize_schema(connection)
            row = connection.execute(
                """
                SELECT target_mode, source_mode, approved, summary, evaluator, evaluated_at,
                       required_drills_json, completed_drills_json, criteria_json
                FROM promotion_gate_decisions
                WHERE target_mode = ?
                ORDER BY sequence_number DESC
                LIMIT 1
                """,
                (target_mode,),
            ).fetchone()
        if row is None:
            return None
        return self._decision_from_row(row)

    def list_decisions(
        self, *, target_mode: str | None = None, limit: int = 50
    ) -> list[PromotionGateDecisionRecord]:
        with self._connect() as connection:
            self._initialize_schema(connection)
            if target_mode is None:
                rows = connection.execute(
                    """
                    SELECT target_mode, source_mode, approved, summary, evaluator, evaluated_at,
                           required_drills_json, completed_drills_json, criteria_json
                    FROM promotion_gate_decisions
                    ORDER BY sequence_number DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT target_mode, source_mode, approved, summary, evaluator, evaluated_at,
                           required_drills_json, completed_drills_json, criteria_json
                    FROM promotion_gate_decisions
                    WHERE target_mode = ?
                    ORDER BY sequence_number DESC
                    LIMIT ?
                    """,
                    (target_mode, limit),
                ).fetchall()
        return [self._decision_from_row(row) for row in rows]

    def _decision_from_row(self, row: sqlite3.Row) -> PromotionGateDecisionRecord:
        criteria_payload = json.loads(row["criteria_json"])
        return PromotionGateDecisionRecord(
            target_mode=ExecutionMode(row["target_mode"]),
            source_mode=(
                ExecutionMode(row["source_mode"])
                if row["source_mode"] is not None
                else None
            ),
            approved=bool(row["approved"]),
            summary=row["summary"],
            evaluator=row["evaluator"],
            evaluated_at=datetime.fromisoformat(row["evaluated_at"]),
            required_drills=tuple(json.loads(row["required_drills_json"])),
            completed_drills=tuple(json.loads(row["completed_drills_json"])),
            criteria=tuple(
                PromotionCriterionResult(
                    criterion_name=item["criterion_name"],
                    passed=bool(item["passed"]),
                    detail=item["detail"],
                )
                for item in criteria_payload
            ),
        )


class SQLiteOrderStore:
    """Coordinates durable event recording, append-only journaling, and order replay."""

    def __init__(self, *, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._event_store = SQLiteEventStore(db_path=self.db_path)
        self._order_journal = SQLiteOrderJournal(db_path=self.db_path)
        self._reconciliation_reports = SQLiteReconciliationReportStore(db_path=self.db_path)
        self._broker_sync_store = SQLiteBrokerSyncStore(db_path=self.db_path)
        self._submission_book = SQLiteSubmissionBook(db_path=self.db_path)
        self._lease_backend = SQLiteAccountLeaseBackend(db_path=self.db_path)
        self._observability_store = SQLiteObservabilityStore(db_path=self.db_path)
        self._promotion_gate_store = SQLitePromotionGateStore(db_path=self.db_path)
        self._rehydrator = OrderAggregateRehydrator()
        self._projection_service = EventLogProjectionService()

    def initialize(self) -> None:
        self._event_store.initialize()
        self._order_journal.initialize()
        self._reconciliation_reports.initialize()
        self._broker_sync_store.initialize()
        self._submission_book.initialize()
        self._lease_backend.initialize()
        self._observability_store.initialize()
        self._promotion_gate_store.initialize()

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

    def list_events(
        self,
        *,
        aggregate_id: str | None = None,
        account_id: str | None = None,
    ) -> list[DomainEvent]:
        return self._event_store.list_events(
            aggregate_id=aggregate_id,
            account_id=account_id,
        )

    def load_order(self, *, client_order_id: str) -> OrderAggregate:
        events = self._event_store.list_events(aggregate_id=client_order_id)
        return self._rehydrator.replay(events=events)

    def load_order_history(self, *, client_order_id: str) -> list[OrderJournalEntry]:
        return self._order_journal.list_entries(client_order_id=client_order_id)

    def load_latest_order(self, *, client_order_id: str) -> OrderAggregate | None:
        return self._order_journal.load_latest(client_order_id=client_order_id)

    def ingest_fill(
        self,
        *,
        client_order_id: str,
        fill: OrderFill,
    ) -> tuple[OrderAggregate, DomainEvent]:
        with self._event_store._connect() as connection:
            self._event_store._initialize_schema(connection)
            order = self._load_latest_order_in_connection(
                connection=connection,
                client_order_id=client_order_id,
            )
            if order is None:
                raise PersistenceError(
                    f"cannot ingest fill for unknown order {client_order_id}"
                )

            fill_event_id = order.fill_event_id(fill_id=fill.fill_id)
            existing_event = self._load_domain_event_by_id_in_connection(
                connection=connection,
                event_id=fill_event_id,
            )
            if existing_event is not None:
                payload = existing_event.payload
                if (
                    existing_event.aggregate_id != client_order_id
                    or payload.get("fill_id") != fill.fill_id
                    or abs(float(payload.get("last_fill_quantity", 0.0)) - fill.quantity) > 1e-9
                    or abs(float(payload.get("last_fill_price", 0.0)) - fill.price) > 1e-9
                    or payload.get("broker_order_id") != fill.broker_order_id
                    or payload.get("fill_source") != fill.source
                    or existing_event.occurred_at != fill.occurred_at
                ):
                    raise PersistenceError(
                        "conflicting fill replay for order "
                        f"{client_order_id} and fill {fill.fill_id}"
                    )
                latest_order = self._load_latest_order_in_connection(
                    connection=connection,
                    client_order_id=client_order_id,
                )
                if latest_order is None:
                    raise PersistenceError(
                        f"order {client_order_id} disappeared during fill replay"
                    )
                return latest_order, existing_event

            updated_order, fill_event = order.apply_fill(fill=fill)
            self._event_store._append_batch_in_connection(
                connection=connection,
                events=[fill_event],
            )
            self._order_journal._append_batch_in_connection(
                connection=connection,
                events=[fill_event],
            )
            return updated_order, fill_event

    def list_internal_order_snapshots(
        self, *, account_id: str | None = None
    ) -> list[InternalOrderSnapshot]:
        entries = self._order_journal.list_latest_entries(account_id=account_id)
        return [entry.to_internal_order_snapshot() for entry in entries]

    def project_internal_positions(
        self, *, account_id: str, events: Iterable[DomainEvent] | None = None
    ) -> list[PositionSnapshot]:
        replay_events = list(events) if events is not None else self.list_events(account_id=account_id)
        return self._projection_service.rebuild_positions(
            account_id=account_id,
            events=replay_events,
        )

    def project_internal_cash(
        self,
        *,
        account_id: str,
        opening_balance: float = 0.0,
        events: Iterable[DomainEvent] | None = None,
    ) -> CashSnapshot:
        replay_events = list(events) if events is not None else self.list_events(account_id=account_id)
        return self._projection_service.rebuild_cash(
            account_id=account_id,
            events=replay_events,
            opening_balance=opening_balance,
        )

    def project_internal_exposure(
        self,
        *,
        account_id: str,
        events: Iterable[DomainEvent] | None = None,
    ) -> AccountExposureSnapshot:
        replay_events = list(events) if events is not None else self.list_events(account_id=account_id)
        return self._projection_service.rebuild_exposure(
            account_id=account_id,
            events=replay_events,
        )

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

            if order.status is OrderStatus.CREATED and submission.outcome in {
                BrokerSubmissionOutcome.ACCEPTED,
                BrokerSubmissionOutcome.REJECTED,
                BrokerSubmissionOutcome.UNKNOWN,
            }:
                order, risk_event = order.transition(OrderStatus.RISK_APPROVED)
                recovery_events.append(risk_event)
            if (
                order.status is OrderStatus.RISK_APPROVED
                and submission.outcome is BrokerSubmissionOutcome.ACCEPTED
            ):
                order, submitted_event = order.transition(OrderStatus.SUBMITTED)
                recovery_events.append(submitted_event)
            elif (
                order.status is OrderStatus.RISK_APPROVED
                and submission.outcome is BrokerSubmissionOutcome.REJECTED
            ):
                order, rejected_event = order.transition(OrderStatus.REJECTED)
                recovery_events.append(rejected_event)

            if not recovery_events:
                continue

            self.record_events(events=recovery_events)
            repaired_client_order_ids.append(entry.client_order_id)

        return repaired_client_order_ids

    def list_accounts_requiring_reconciliation(self) -> list[str]:
        return self._order_journal.list_active_account_ids()

    def record_operator_action(self, *, record: OperatorActionRecord) -> None:
        self._observability_store.append_operator_action(record=record)

    def list_operator_actions(
        self, *, account_id: str, limit: int = 50
    ) -> list[OperatorActionRecord]:
        return self._observability_store.list_operator_actions(
            account_id=account_id,
            limit=limit,
        )

    def record_runtime_health_summary(self, *, summary: RuntimeHealthSummary) -> None:
        self._observability_store.append_runtime_health_summary(summary=summary)

    def load_latest_runtime_health_summary(
        self, *, account_id: str
    ) -> RuntimeHealthSummary | None:
        return self._observability_store.load_latest_runtime_health_summary(
            account_id=account_id
        )

    def list_runtime_health_summaries(
        self, *, account_id: str, limit: int = 50
    ) -> list[RuntimeHealthSummary]:
        return self._observability_store.list_runtime_health_summaries(
            account_id=account_id,
            limit=limit,
        )

    def list_accounts_with_runtime_health(self) -> list[str]:
        return self._observability_store.list_accounts_with_runtime_health()

    def record_promotion_decision(self, *, record: PromotionGateDecisionRecord) -> None:
        self._promotion_gate_store.append_decision(record=record)

    def load_latest_promotion_decision(
        self, *, target_mode: ExecutionMode
    ) -> PromotionGateDecisionRecord | None:
        return self._promotion_gate_store.load_latest_decision(target_mode=target_mode.value)

    def list_promotion_decisions(
        self,
        *,
        target_mode: ExecutionMode | None = None,
        limit: int = 50,
    ) -> list[PromotionGateDecisionRecord]:
        return self._promotion_gate_store.list_decisions(
            target_mode=target_mode.value if target_mode is not None else None,
            limit=limit,
        )

    def _load_latest_order_in_connection(
        self,
        *,
        connection: sqlite3.Connection,
        client_order_id: str,
    ) -> OrderAggregate | None:
        row = connection.execute(
            """
            SELECT sequence_number, event_id, client_order_id, account_id, status, symbol, side,
                   quantity, order_type, filled_quantity, average_fill_price, occurred_at
            FROM order_journal
            WHERE client_order_id = ?
            ORDER BY sequence_number DESC
            LIMIT 1
            """,
            (client_order_id,),
        ).fetchone()
        if row is None:
            return None
        return self._order_journal._entry_from_row(row).to_order_aggregate()

    def _load_domain_event_by_id_in_connection(
        self,
        *,
        connection: sqlite3.Connection,
        event_id: str,
    ) -> DomainEvent | None:
        row = connection.execute(
            """
            SELECT event_id, event_type, aggregate_id, occurred_at, payload_json,
                   correlation_id, causation_id, account_id, strategy_id, mode
            FROM domain_events
            WHERE event_id = ?
            LIMIT 1
            """,
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        return DomainEvent(
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

    def record_reconciliation_report(
        self,
        *,
        report: ReconciliationReport,
        internal_orders: Iterable[InternalOrderSnapshot] = (),
        broker_orders: Iterable[BrokerOrderSnapshot] = (),
        internal_positions: Iterable[PositionSnapshot] = (),
        broker_positions: Iterable[PositionSnapshot] = (),
        internal_cash: CashSnapshot | None = None,
        broker_cash: CashSnapshot | None = None,
    ) -> None:
        self._reconciliation_reports.append(
            report=report,
            internal_orders=internal_orders,
            broker_orders=broker_orders,
            internal_positions=internal_positions,
            broker_positions=broker_positions,
            internal_cash=internal_cash,
            broker_cash=broker_cash,
        )

    def load_latest_reconciliation_report(
        self, *, account_id: str
    ) -> ReconciliationReport | None:
        return self._reconciliation_reports.load_latest(account_id=account_id)

    def load_latest_reconciliation_cycle(
        self, *, account_id: str
    ) -> ReconciliationCycleRecord | None:
        return self._reconciliation_reports.load_latest_cycle(account_id=account_id)

    def list_reconciliation_cycles(
        self, *, account_id: str
    ) -> list[ReconciliationCycleRecord]:
        return self._reconciliation_reports.list_cycles(account_id=account_id)

    def list_accounts_with_reconciliation_reports(self) -> list[str]:
        return self._reconciliation_reports.list_account_ids()

    def record_reconciliation_run(self, *, record: ReconciliationRunRecord) -> None:
        self._reconciliation_reports.append_run(record=record)

    def load_latest_reconciliation_run(
        self, *, account_id: str
    ) -> ReconciliationRunRecord | None:
        return self._reconciliation_reports.load_latest_run(account_id=account_id)

    def list_reconciliation_runs(self, *, account_id: str) -> list[ReconciliationRunRecord]:
        return self._reconciliation_reports.list_runs(account_id=account_id)

    def append_broker_sync_raw_orders(self, *, orders: Iterable[RawBrokerOrderSnapshot]) -> int:
        return self._broker_sync_store.append_raw_orders(orders=orders)

    def record_broker_sync_activity(
        self, *, activity: RawBrokerActivity
    ) -> BrokerActivityIngestionResult:
        return self._broker_sync_store.record_activity(activity=activity)

    def load_broker_sync_cursor(self, *, account_id: str) -> BrokerSyncCursor | None:
        return self._broker_sync_store.load_cursor(account_id=account_id)

    def update_broker_sync_cursor(self, *, cursor: BrokerSyncCursor) -> None:
        self._broker_sync_store.update_cursor(cursor_record=cursor)

    def append_broker_sync_run(self, *, record: BrokerSyncRunRecord) -> None:
        self._broker_sync_store.append_run(record=record)

    def load_latest_broker_sync_run(self, *, account_id: str) -> BrokerSyncRunRecord | None:
        return self._broker_sync_store.load_latest_run(account_id=account_id)

    def list_broker_sync_runs(self, *, account_id: str) -> list[BrokerSyncRunRecord]:
        return self._broker_sync_store.list_runs(account_id=account_id)

    def list_broker_sync_raw_orders(self, *, account_id: str) -> list[RawBrokerOrderSnapshot]:
        return self._broker_sync_store.list_raw_orders(account_id=account_id)

    def list_broker_sync_raw_activities(self, *, account_id: str) -> list[RawBrokerActivity]:
        return self._broker_sync_store.list_raw_activities(account_id=account_id)

    def build_submission_book(self) -> SQLiteSubmissionBook:
        self._submission_book.initialize()
        return self._submission_book

    def build_account_lease_backend(self) -> SQLiteAccountLeaseBackend:
        self._lease_backend.initialize()
        return self._lease_backend
