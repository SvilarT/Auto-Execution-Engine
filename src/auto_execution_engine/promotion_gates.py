from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from auto_execution_engine.config.execution_mode import (
    ConfigurationError,
    ExecutionMode,
    SafetyGateConfig,
    validate_startup,
)
from auto_execution_engine.reconciliation.models import (
    ReconciliationAction,
    ReconciliationRunStatus,
)


DEFAULT_REQUIRED_DRILLS: dict[ExecutionMode, tuple[str, ...]] = {
    ExecutionMode.PAPER: (
        "simulation_stability",
        "order_lifecycle_recovery",
    ),
    ExecutionMode.LIVE: (
        "paper_reconciliation",
        "kill_switch_response",
        "operator_override",
    ),
}


@dataclass(frozen=True)
class PromotionCriterionResult:
    criterion_name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class PromotionGateDecisionRecord:
    target_mode: ExecutionMode
    source_mode: ExecutionMode | None
    approved: bool
    summary: str
    criteria: tuple[PromotionCriterionResult, ...]
    required_drills: tuple[str, ...] = ()
    completed_drills: tuple[str, ...] = ()
    evaluator: str = "startup"
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class PromotionGateInputs:
    safety: SafetyGateConfig
    required_drills: tuple[str, ...] = ()
    completed_drills: tuple[str, ...] = ()
    min_healthy_accounts: int = 1


class PromotionEvidenceStore(Protocol):
    def list_accounts_with_runtime_health(self) -> list[str]: ...

    def load_latest_runtime_health_summary(self, *, account_id: str): ...

    def list_accounts_with_reconciliation_reports(self) -> list[str]: ...

    def load_latest_reconciliation_report(self, *, account_id: str): ...

    def load_latest_reconciliation_run(self, *, account_id: str): ...


class PromotionDecisionStore(Protocol):
    def record_promotion_decision(self, *, record: PromotionGateDecisionRecord) -> None: ...

    def load_latest_promotion_decision(
        self, *, target_mode: ExecutionMode
    ) -> PromotionGateDecisionRecord | None: ...


class PromotionGateEvaluator:
    def evaluate(
        self,
        *,
        target_mode: ExecutionMode,
        evidence_store: PromotionEvidenceStore,
        decision_store: PromotionDecisionStore,
        inputs: PromotionGateInputs,
        source_mode: ExecutionMode | None = None,
    ) -> PromotionGateDecisionRecord:
        source = source_mode or promotion_source_mode(target_mode)
        criteria: list[PromotionCriterionResult] = []

        criteria.append(
            self._evaluate_startup_safety(target_mode=target_mode, safety=inputs.safety)
        )
        criteria.append(
            self._evaluate_source_health(
                source_mode=source,
                evidence_store=evidence_store,
                min_healthy_accounts=inputs.min_healthy_accounts,
            )
        )
        criteria.append(
            self._evaluate_drills(
                target_mode=target_mode,
                required_drills=inputs.required_drills,
                completed_drills=inputs.completed_drills,
            )
        )

        if target_mode is ExecutionMode.LIVE:
            criteria.append(
                self._evaluate_paper_reconciliation(
                    evidence_store=evidence_store,
                    min_healthy_accounts=inputs.min_healthy_accounts,
                )
            )
            criteria.append(
                self._evaluate_prior_promotion(decision_store=decision_store)
            )

        approved = all(result.passed for result in criteria)
        failed = [result.criterion_name for result in criteria if not result.passed]
        if approved:
            summary = (
                f"promotion to {target_mode.value} approved"
                if source is None
                else f"promotion from {source.value} to {target_mode.value} approved"
            )
        else:
            summary = (
                f"promotion to {target_mode.value} blocked; failed criteria: "
                + ", ".join(failed)
            )

        return PromotionGateDecisionRecord(
            target_mode=target_mode,
            source_mode=source,
            approved=approved,
            summary=summary,
            criteria=tuple(criteria),
            required_drills=normalized_required_drills(
                target_mode=target_mode,
                required_drills=inputs.required_drills,
            ),
            completed_drills=tuple(sorted(set(inputs.completed_drills))),
        )

    def persist(
        self,
        *,
        decision_store: PromotionDecisionStore,
        record: PromotionGateDecisionRecord,
    ) -> None:
        decision_store.record_promotion_decision(record=record)

    def _evaluate_startup_safety(
        self,
        *,
        target_mode: ExecutionMode,
        safety: SafetyGateConfig,
    ) -> PromotionCriterionResult:
        try:
            validate_startup(mode=target_mode, safety=safety)
        except ConfigurationError as exc:
            return PromotionCriterionResult(
                criterion_name="startup_safety",
                passed=False,
                detail=str(exc),
            )
        return PromotionCriterionResult(
            criterion_name="startup_safety",
            passed=True,
            detail=f"{target_mode.value} startup safety requirements are satisfied",
        )

    def _evaluate_source_health(
        self,
        *,
        source_mode: ExecutionMode | None,
        evidence_store: PromotionEvidenceStore,
        min_healthy_accounts: int,
    ) -> PromotionCriterionResult:
        accounts = sorted(set(evidence_store.list_accounts_with_runtime_health()))
        if len(accounts) < min_healthy_accounts:
            return PromotionCriterionResult(
                criterion_name="source_runtime_health",
                passed=False,
                detail=(
                    f"expected at least {min_healthy_accounts} healthy account summaries "
                    f"from {source_mode.value if source_mode is not None else 'source'} mode, "
                    f"observed {len(accounts)}"
                ),
            )

        failing_accounts: list[str] = []
        for account_id in accounts:
            summary = evidence_store.load_latest_runtime_health_summary(account_id=account_id)
            if summary is None:
                failing_accounts.append(f"{account_id}:missing")
                continue
            if summary.status != "healthy" or summary.is_quarantined or summary.kill_switch_active:
                failing_accounts.append(
                    f"{account_id}:{summary.status}/quarantined={summary.is_quarantined}/kill_switch={summary.kill_switch_active}"
                )

        if failing_accounts:
            return PromotionCriterionResult(
                criterion_name="source_runtime_health",
                passed=False,
                detail="unhealthy source accounts detected: " + ", ".join(failing_accounts),
            )

        return PromotionCriterionResult(
            criterion_name="source_runtime_health",
            passed=True,
            detail=(
                f"validated {len(accounts)} healthy account summaries from "
                f"{source_mode.value if source_mode is not None else 'source'} mode"
            ),
        )

    def _evaluate_drills(
        self,
        *,
        target_mode: ExecutionMode,
        required_drills: tuple[str, ...],
        completed_drills: tuple[str, ...],
    ) -> PromotionCriterionResult:
        normalized_required = normalized_required_drills(
            target_mode=target_mode,
            required_drills=required_drills,
        )
        normalized_completed = tuple(sorted(set(completed_drills)))
        missing = [drill for drill in normalized_required if drill not in normalized_completed]
        if missing:
            return PromotionCriterionResult(
                criterion_name="required_drills",
                passed=False,
                detail=(
                    "missing completed promotion drills: "
                    + ", ".join(missing)
                    + "; completed="
                    + ", ".join(normalized_completed)
                ),
            )
        return PromotionCriterionResult(
            criterion_name="required_drills",
            passed=True,
            detail=(
                "completed drills: "
                + ", ".join(normalized_completed or normalized_required)
            ),
        )

    def _evaluate_paper_reconciliation(
        self,
        *,
        evidence_store: PromotionEvidenceStore,
        min_healthy_accounts: int,
    ) -> PromotionCriterionResult:
        accounts = sorted(
            set(evidence_store.list_accounts_with_runtime_health())
            | set(evidence_store.list_accounts_with_reconciliation_reports())
        )
        if len(accounts) < min_healthy_accounts:
            return PromotionCriterionResult(
                criterion_name="paper_reconciliation_clear",
                passed=False,
                detail=(
                    f"expected at least {min_healthy_accounts} paper accounts with reconciliation evidence, observed {len(accounts)}"
                ),
            )

        failing_accounts: list[str] = []
        for account_id in accounts:
            report = evidence_store.load_latest_reconciliation_report(account_id=account_id)
            run = evidence_store.load_latest_reconciliation_run(account_id=account_id)
            if report is None or run is None:
                failing_accounts.append(f"{account_id}:missing")
                continue
            if report.has_drift or report.action is not ReconciliationAction.NO_ACTION:
                failing_accounts.append(
                    f"{account_id}:drift/action={report.action.value}"
                )
                continue
            if run.status is not ReconciliationRunStatus.COMPLETED:
                failing_accounts.append(f"{account_id}:run_status={run.status.value}")

        if failing_accounts:
            return PromotionCriterionResult(
                criterion_name="paper_reconciliation_clear",
                passed=False,
                detail="paper reconciliation not clear for accounts: " + ", ".join(failing_accounts),
            )

        return PromotionCriterionResult(
            criterion_name="paper_reconciliation_clear",
            passed=True,
            detail=f"validated clear reconciliation evidence for {len(accounts)} paper accounts",
        )

    def _evaluate_prior_promotion(
        self,
        *,
        decision_store: PromotionDecisionStore,
    ) -> PromotionCriterionResult:
        prior_paper_decision = decision_store.load_latest_promotion_decision(
            target_mode=ExecutionMode.PAPER
        )
        if prior_paper_decision is None or not prior_paper_decision.approved:
            return PromotionCriterionResult(
                criterion_name="prior_paper_promotion",
                passed=False,
                detail="live promotion requires a previously approved simulation-to-paper decision",
            )
        return PromotionCriterionResult(
            criterion_name="prior_paper_promotion",
            passed=True,
            detail=(
                "found previously approved simulation-to-paper promotion decision "
                f"from {prior_paper_decision.evaluated_at.isoformat()}"
            ),
        )


def promotion_source_mode(target_mode: ExecutionMode) -> ExecutionMode | None:
    if target_mode is ExecutionMode.PAPER:
        return ExecutionMode.SIMULATION
    if target_mode is ExecutionMode.LIVE:
        return ExecutionMode.PAPER
    return None


def normalized_required_drills(
    *,
    target_mode: ExecutionMode,
    required_drills: tuple[str, ...],
) -> tuple[str, ...]:
    if required_drills:
        return tuple(sorted(set(required_drills)))
    return DEFAULT_REQUIRED_DRILLS.get(target_mode, ())
