from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping

from auto_execution_engine.reconciliation.models import CashSnapshot, PositionSnapshot
from datetime import UTC, datetime
from uuid import uuid4

from auto_execution_engine.adapters.persistence.sqlite_order_store import SQLiteOrderStore
from auto_execution_engine.reconciliation.models import (
    BrokerOrderSnapshot,
    DriftCategory,
    ReconciliationAction,
    ReconciliationDrift,
    ReconciliationReport,
    ReconciliationRunRecord,
    ReconciliationRunStatus,
)
from auto_execution_engine.reconciliation.service import (
    AccountQuarantineRegistry,
    ReconciliationService,
)
from auto_execution_engine.trading_plane.leases import AccountLeaseService, LeaseError

BrokerSnapshotLoader = Callable[[str], list[BrokerOrderSnapshot]]
BrokerPositionLoader = Callable[[str], list[PositionSnapshot]]
BrokerCashLoader = Callable[[str], CashSnapshot | None]


class ReconciliationRunner:
    """Run reconciliation periodically with durable per-account run records."""

    def __init__(
        self,
        *,
        order_store: SQLiteOrderStore,
        lease_service: AccountLeaseService,
        reconciliation_service: ReconciliationService | None = None,
        quarantine_registry: AccountQuarantineRegistry | None = None,
        owner_id: str = "reconciliation-runner",
        lease_ttl_seconds: int = 30,
    ) -> None:
        self._order_store = order_store
        self._lease_service = lease_service
        self._reconciliation_service = reconciliation_service or ReconciliationService()
        self._quarantine_registry = quarantine_registry
        self._owner_id = owner_id
        self._lease_ttl_seconds = lease_ttl_seconds

    def run_once(
        self,
        *,
        account_ids: Iterable[str] | None = None,
        broker_snapshots_by_account: Mapping[str, list[BrokerOrderSnapshot]] | None = None,
        broker_positions_by_account: Mapping[str, list[PositionSnapshot]] | None = None,
        broker_cash_by_account: Mapping[str, CashSnapshot] | None = None,
        opening_cash_by_account: Mapping[str, float] | None = None,
        snapshot_loader: BrokerSnapshotLoader | None = None,
        broker_position_loader: BrokerPositionLoader | None = None,
        broker_cash_loader: BrokerCashLoader | None = None,
        now: datetime | None = None,
    ) -> list[ReconciliationRunRecord]:
        current_time = now or datetime.now(UTC)
        resolved_account_ids = sorted(set(account_ids or self._default_account_ids()))
        return [
            self._run_account(
                account_id=account_id,
                broker_snapshots_by_account=broker_snapshots_by_account,
                broker_positions_by_account=broker_positions_by_account,
                broker_cash_by_account=broker_cash_by_account,
                opening_cash_by_account=opening_cash_by_account,
                snapshot_loader=snapshot_loader,
                broker_position_loader=broker_position_loader,
                broker_cash_loader=broker_cash_loader,
                now=current_time,
            )
            for account_id in resolved_account_ids
        ]

    def _default_account_ids(self) -> list[str]:
        account_ids = set(self._order_store.list_accounts_requiring_reconciliation())
        account_ids.update(self._order_store.list_accounts_with_reconciliation_reports())
        return sorted(account_ids)

    def _run_account(
        self,
        *,
        account_id: str,
        broker_snapshots_by_account: Mapping[str, list[BrokerOrderSnapshot]] | None,
        broker_positions_by_account: Mapping[str, list[PositionSnapshot]] | None,
        broker_cash_by_account: Mapping[str, CashSnapshot] | None,
        opening_cash_by_account: Mapping[str, float] | None,
        snapshot_loader: BrokerSnapshotLoader | None,
        broker_position_loader: BrokerPositionLoader | None,
        broker_cash_loader: BrokerCashLoader | None,
        now: datetime,
    ) -> ReconciliationRunRecord:
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
            record = ReconciliationRunRecord(
                run_id=run_id,
                account_id=account_id,
                owner_id=self._owner_id,
                started_at=now,
                completed_at=now,
                status=ReconciliationRunStatus.SKIPPED,
                detail=str(exc),
            )
            self._order_store.record_reconciliation_run(record=record)
            return record

        try:
            self._order_store.repair_orders_from_submissions(account_id=account_id)
            internal_orders = self._order_store.list_internal_order_snapshots(
                account_id=account_id
            )
            opening_balance = 0.0
            if opening_cash_by_account is not None:
                opening_balance = float(opening_cash_by_account.get(account_id, 0.0))
            internal_positions = self._order_store.project_internal_positions(
                account_id=account_id
            )
            internal_cash = self._order_store.project_internal_cash(
                account_id=account_id,
                opening_balance=opening_balance,
            )
            broker_orders = self._resolve_broker_orders(
                account_id=account_id,
                broker_snapshots_by_account=broker_snapshots_by_account,
                snapshot_loader=snapshot_loader,
            )
            broker_positions = self._resolve_broker_positions(
                account_id=account_id,
                broker_positions_by_account=broker_positions_by_account,
                broker_position_loader=broker_position_loader,
            )
            broker_cash = self._resolve_broker_cash(
                account_id=account_id,
                broker_cash_by_account=broker_cash_by_account,
                broker_cash_loader=broker_cash_loader,
            )
            report = self._reconciliation_service.compare_orders(
                account_id=account_id,
                internal_orders=internal_orders,
                broker_orders=broker_orders,
                internal_positions=internal_positions,
                broker_positions=broker_positions,
                internal_cash=internal_cash,
                broker_cash=broker_cash,
            )
            self._order_store.record_reconciliation_report(
                report=report,
                internal_orders=internal_orders,
                broker_orders=broker_orders,
                internal_positions=internal_positions,
                broker_positions=broker_positions or (),
                internal_cash=internal_cash,
                broker_cash=broker_cash,
            )
            if self._quarantine_registry is not None:
                self._quarantine_registry.record(report=report)

            record = ReconciliationRunRecord(
                run_id=run_id,
                account_id=account_id,
                owner_id=self._owner_id,
                started_at=now,
                completed_at=now,
                status=ReconciliationRunStatus.COMPLETED,
                detail=(
                    "reconciliation completed without drift"
                    if not report.has_drift
                    else f"reconciliation completed with action {report.action.value}"
                ),
                report=report,
            )
            self._order_store.record_reconciliation_run(record=record)
            return record
        except Exception as exc:
            failure_report = ReconciliationReport(
                account_id=account_id,
                generated_at=now,
                action=ReconciliationAction.QUARANTINE_ACCOUNT,
                drifts=(
                    ReconciliationDrift(
                        category=DriftCategory.RECONCILIATION_RUN_FAILURE,
                        account_id=account_id,
                        client_order_id=None,
                        detail=str(exc),
                    ),
                ),
            )
            self._order_store.record_reconciliation_report(report=failure_report)
            if self._quarantine_registry is not None:
                self._quarantine_registry.record(report=failure_report)
            record = ReconciliationRunRecord(
                run_id=run_id,
                account_id=account_id,
                owner_id=self._owner_id,
                started_at=now,
                completed_at=now,
                status=ReconciliationRunStatus.FAILED,
                detail=str(exc),
                report=failure_report,
            )
            self._order_store.record_reconciliation_run(record=record)
            return record
        finally:
            self._lease_service.release(
                existing_lease=lease,
                account_id=account_id,
                owner_id=self._owner_id,
                now=now,
            )

    def _resolve_broker_orders(
        self,
        *,
        account_id: str,
        broker_snapshots_by_account: Mapping[str, list[BrokerOrderSnapshot]] | None,
        snapshot_loader: BrokerSnapshotLoader | None,
    ) -> list[BrokerOrderSnapshot]:
        if broker_snapshots_by_account is not None:
            return list(broker_snapshots_by_account.get(account_id, []))
        if snapshot_loader is not None:
            return list(snapshot_loader(account_id))
        return []

    def _resolve_broker_positions(
        self,
        *,
        account_id: str,
        broker_positions_by_account: Mapping[str, list[PositionSnapshot]] | None,
        broker_position_loader: BrokerPositionLoader | None,
    ) -> list[PositionSnapshot] | None:
        if broker_positions_by_account is not None:
            return list(broker_positions_by_account.get(account_id, []))
        if broker_position_loader is not None:
            return list(broker_position_loader(account_id))
        return None

    def _resolve_broker_cash(
        self,
        *,
        account_id: str,
        broker_cash_by_account: Mapping[str, CashSnapshot] | None,
        broker_cash_loader: BrokerCashLoader | None,
    ) -> CashSnapshot | None:
        if broker_cash_by_account is not None:
            return broker_cash_by_account.get(account_id)
        if broker_cash_loader is not None:
            return broker_cash_loader(account_id)
        return None
