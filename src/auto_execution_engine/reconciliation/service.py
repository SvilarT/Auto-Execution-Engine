from __future__ import annotations

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
}


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
