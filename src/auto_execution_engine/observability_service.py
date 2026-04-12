from __future__ import annotations

from dataclasses import dataclass

from auto_execution_engine.domain.risk.models import KillSwitch, KillSwitchState
from auto_execution_engine.observability_models import OperatorActionRecord, RuntimeHealthSummary
from auto_execution_engine.reconciliation.models import ReconciliationAction, ReconciliationReport, ReconciliationRunRecord


@dataclass
class AccountRuntimeSnapshot:
    account_id: str
    active_order_count: int
    open_position_count: int
    gross_notional: float
    cash_balance: float
    is_quarantined: bool
    kill_switch: KillSwitch
    latest_reconciliation_report: ReconciliationReport | None = None
    latest_reconciliation_run: ReconciliationRunRecord | None = None
    latest_operator_action: OperatorActionRecord | None = None


class RuntimeHealthService:
    def summarize(self, *, snapshot: AccountRuntimeSnapshot) -> RuntimeHealthSummary:
        status = "healthy"
        detail_parts: list[str] = []

        if snapshot.kill_switch.state is KillSwitchState.ACTIVE:
            status = "kill_switch_active"
            if snapshot.kill_switch.activated_reason:
                detail_parts.append(snapshot.kill_switch.activated_reason)
        elif snapshot.is_quarantined:
            status = "quarantined"
            detail_parts.append("account is quarantined")
        elif (
            snapshot.latest_reconciliation_report is not None
            and snapshot.latest_reconciliation_report.action
            is ReconciliationAction.QUARANTINE_ACCOUNT
        ):
            status = "quarantined"
            detail_parts.append("latest reconciliation requested quarantine")

        latest_reconciliation_status = None
        latest_reconciliation_detail = None
        if snapshot.latest_reconciliation_run is not None:
            latest_reconciliation_status = snapshot.latest_reconciliation_run.status.value
            latest_reconciliation_detail = snapshot.latest_reconciliation_run.detail
            if snapshot.latest_reconciliation_run.status.value == "failed" and status == "healthy":
                status = "degraded"
                detail_parts.append("latest reconciliation run failed")

        drift_count = 0
        latest_reconciliation_action = ReconciliationAction.NO_ACTION.value
        if snapshot.latest_reconciliation_report is not None:
            drift_count = len(snapshot.latest_reconciliation_report.drifts)
            latest_reconciliation_action = snapshot.latest_reconciliation_report.action.value
            if snapshot.latest_reconciliation_report.has_drift and status == "healthy":
                status = "drift_detected"
                detail_parts.append("latest reconciliation found drift")

        return RuntimeHealthSummary(
            account_id=snapshot.account_id,
            status=status,
            active_order_count=snapshot.active_order_count,
            open_position_count=snapshot.open_position_count,
            gross_notional=snapshot.gross_notional,
            cash_balance=snapshot.cash_balance,
            is_quarantined=snapshot.is_quarantined,
            kill_switch_active=snapshot.kill_switch.state is KillSwitchState.ACTIVE,
            latest_reconciliation_action=latest_reconciliation_action,
            latest_reconciliation_status=latest_reconciliation_status,
            latest_reconciliation_detail=latest_reconciliation_detail,
            drift_count=drift_count,
            last_operator_action_type=(
                snapshot.latest_operator_action.action_type
                if snapshot.latest_operator_action is not None
                else None
            ),
            detail="; ".join(detail_parts),
        )
