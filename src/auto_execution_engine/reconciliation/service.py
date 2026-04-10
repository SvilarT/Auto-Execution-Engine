from __future__ import annotations

from dataclasses import dataclass, field

from auto_execution_engine.reconciliation.models import (
    BrokerOrderSnapshot,
    DriftCategory,
    InternalOrderSnapshot,
    ReconciliationAction,
    ReconciliationDrift,
    ReconciliationReport,
)


QUARANTINE_CATEGORIES = {
    DriftCategory.ORDER_STATUS_MISMATCH,
    DriftCategory.FILLED_QUANTITY_MISMATCH,
    DriftCategory.UNKNOWN_BROKER_ORDER,
    DriftCategory.MISSING_BROKER_ORDER,
    DriftCategory.RECONCILIATION_RUN_FAILURE,
}


class ReconciliationQuarantineError(ValueError):
    """Raised when an account remains quarantined after reconciliation drift."""


@dataclass
class AccountQuarantineRegistry:
    """Stores the latest reconciliation decision for each account."""

    _reports: dict[str, ReconciliationReport] = field(default_factory=dict)

    def record(self, *, report: ReconciliationReport) -> None:
        self._reports[report.account_id] = report

    def load_report(self, *, account_id: str) -> ReconciliationReport | None:
        return self._reports.get(account_id)

    def is_quarantined(self, *, account_id: str) -> bool:
        report = self.load_report(account_id=account_id)
        if report is None:
            return False
        return report.action is ReconciliationAction.QUARANTINE_ACCOUNT

    def ensure_account_clear(self, *, account_id: str) -> None:
        report = self.load_report(account_id=account_id)
        if report is None:
            return
        if report.action is not ReconciliationAction.QUARANTINE_ACCOUNT:
            return

        drift_summary = ", ".join(drift.category.value for drift in report.drifts)
        raise ReconciliationQuarantineError(
            f"account {account_id} is quarantined due to reconciliation drift: {drift_summary}"
        )


class ReconciliationService:
    """Compare internal truth to broker truth and produce fail-closed decisions."""

    def compare_orders(
        self,
        *,
        account_id: str,
        internal_orders: list[InternalOrderSnapshot],
        broker_orders: list[BrokerOrderSnapshot],
    ) -> ReconciliationReport:
        internal_by_id = {order.client_order_id: order for order in internal_orders}
        broker_by_id = {order.client_order_id: order for order in broker_orders}
        drifts: list[ReconciliationDrift] = []

        for client_order_id, internal in internal_by_id.items():
            broker = broker_by_id.get(client_order_id)
            if broker is None:
                drifts.append(
                    ReconciliationDrift(
                        category=DriftCategory.MISSING_BROKER_ORDER,
                        account_id=account_id,
                        client_order_id=client_order_id,
                        detail="internal order exists but broker snapshot has no matching order",
                    )
                )
                continue

            if internal.status != broker.status:
                drifts.append(
                    ReconciliationDrift(
                        category=DriftCategory.ORDER_STATUS_MISMATCH,
                        account_id=account_id,
                        client_order_id=client_order_id,
                        detail=(
                            f"internal status is {internal.status} while broker status is {broker.status}"
                        ),
                    )
                )

            if abs(internal.filled_quantity - broker.filled_quantity) > 1e-9:
                drifts.append(
                    ReconciliationDrift(
                        category=DriftCategory.FILLED_QUANTITY_MISMATCH,
                        account_id=account_id,
                        client_order_id=client_order_id,
                        detail=(
                            "internal filled quantity is "
                            f"{internal.filled_quantity} while broker filled quantity is {broker.filled_quantity}"
                        ),
                    )
                )

        for client_order_id, broker in broker_by_id.items():
            if client_order_id not in internal_by_id:
                drifts.append(
                    ReconciliationDrift(
                        category=DriftCategory.UNKNOWN_BROKER_ORDER,
                        account_id=account_id,
                        client_order_id=client_order_id,
                        detail=(
                            "broker snapshot contains an order not present in internal state"
                        ),
                    )
                )

        action = ReconciliationAction.NO_ACTION
        if drifts:
            action = ReconciliationAction.LOG_ONLY
        if any(drift.category in QUARANTINE_CATEGORIES for drift in drifts):
            action = ReconciliationAction.QUARANTINE_ACCOUNT

        return ReconciliationReport(
            account_id=account_id,
            drifts=tuple(drifts),
            action=action,
        )
