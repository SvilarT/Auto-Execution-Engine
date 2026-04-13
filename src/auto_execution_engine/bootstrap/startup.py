import os
import os
from dataclasses import dataclass
from pathlib import Path

from auto_execution_engine.adapters.broker.alpaca import (
    AlpacaBrokerSubmitter,
    AlpacaTradingConfig,
    load_alpaca_trading_config,
)
from auto_execution_engine.adapters.broker.service import (
    BrokerSubmissionService,
    BrokerSubmitter,
    SyntheticBrokerSubmitter,
)
from auto_execution_engine.adapters.persistence.sqlite_order_store import SQLiteOrderStore
from auto_execution_engine.application.execution_service import (
    OperatorControlService,
    RuntimeDiagnosticsService,
)
from auto_execution_engine.trading_plane.leases import AccountLeaseService
from auto_execution_engine.config.execution_mode import (
    ConfigurationError,
    ExecutionMode,
    ModeRuntimeProfile,
    SafetyGateConfig,
    get_runtime_profile,
    validate_startup,
)
from auto_execution_engine.promotion_gates import (
    PromotionGateDecisionRecord,
    PromotionGateEvaluator,
    PromotionGateInputs,
    promotion_source_mode,
)
from auto_execution_engine.reconciliation.models import (
    BrokerOrderSnapshot,
    CashSnapshot,
    PositionSnapshot,
    ReconciliationReport,
    ReconciliationRunRecord,
)
from auto_execution_engine.reconciliation.runner import ReconciliationRunner


BrokerSnapshotByAccount = dict[str, list[BrokerOrderSnapshot]]
BrokerPositionsByAccount = dict[str, list[PositionSnapshot]]
BrokerCashByAccount = dict[str, CashSnapshot]
from auto_execution_engine.reconciliation.service import (
    AccountQuarantineRegistry,
    ReconciliationService,
)


TRUE_VALUES = {"1", "true", "yes", "on"}
DEFAULT_DURABLE_STATE_ROOT = Path("/home/ubuntu/.auto_execution_engine/state")


@dataclass(frozen=True)
class StartupContext:
    """Validated startup configuration for process boot."""

    profile: ModeRuntimeProfile
    safety: SafetyGateConfig
    durable_state_root: Path
    promotion_gate_decision: PromotionGateDecisionRecord | None = None
    broker_adapter_config: AlpacaTradingConfig | None = None


@dataclass(frozen=True)
class StartupConfig:
    """Parsed startup configuration before runtime object wiring begins."""

    mode: ExecutionMode
    safety: SafetyGateConfig
    durable_state_root: Path
    required_promotion_drills: tuple[str, ...] = ()
    completed_promotion_drills: tuple[str, ...] = ()
    promotion_min_healthy_accounts: int = 1


def parse_startup_config() -> StartupConfig:
    mode = _read_mode_env()
    safety = SafetyGateConfig(
        allow_paper=_read_bool_env("AEE_ALLOW_PAPER"),
        allow_live=_read_bool_env("AEE_ALLOW_LIVE"),
        broker_credentials_present=_read_bool_env("AEE_BROKER_CREDENTIALS_PRESENT"),
        risk_engine_configured=_read_bool_env("AEE_RISK_ENGINE_CONFIGURED"),
        reconciliation_enabled=_read_bool_env("AEE_RECONCILIATION_ENABLED"),
        durable_state_enabled=_read_bool_env("AEE_DURABLE_STATE_ENABLED"),
        operator_approval_present=_read_bool_env("AEE_OPERATOR_APPROVAL_PRESENT"),
    )
    config = StartupConfig(
        mode=mode,
        safety=safety,
        durable_state_root=_read_durable_state_root(),
        required_promotion_drills=_read_csv_env("AEE_REQUIRED_PROMOTION_DRILLS"),
        completed_promotion_drills=_read_csv_env("AEE_COMPLETED_PROMOTION_DRILLS"),
        promotion_min_healthy_accounts=_read_int_env(
            "AEE_PROMOTION_MIN_HEALTHY_ACCOUNTS",
            default=1,
        ),
    )
    _validate_startup_config(config=config)
    return config


def _validate_startup_config(*, config: StartupConfig) -> ModeRuntimeProfile:
    profile = validate_startup(mode=config.mode, safety=config.safety)
    if config.mode is ExecutionMode.PAPER and config.safety.allow_live:
        raise ConfigurationError(
            "paper startup cannot enable live mode simultaneously; unset AEE_ALLOW_LIVE"
        )
    if config.mode is ExecutionMode.LIVE and config.safety.allow_paper:
        raise ConfigurationError(
            "live startup cannot enable paper mode simultaneously; unset AEE_ALLOW_PAPER"
        )
    return profile


def _load_broker_adapter_config(*, config: StartupConfig) -> AlpacaTradingConfig | None:
    if config.mode is ExecutionMode.SIMULATION:
        return None
    return load_alpaca_trading_config(mode=config.mode)


def _promotion_gate_inputs_from_config(*, config: StartupConfig) -> PromotionGateInputs:
    return PromotionGateInputs(
        safety=config.safety,
        required_drills=config.required_promotion_drills,
        completed_drills=config.completed_promotion_drills,
        min_healthy_accounts=config.promotion_min_healthy_accounts,
    )


def _evaluate_mode_promotion_gate(*, config: StartupConfig) -> PromotionGateDecisionRecord | None:
    if config.mode is ExecutionMode.SIMULATION:
        return None
    return _evaluate_promotion_gate(
        mode=config.mode,
        durable_state_root=config.durable_state_root,
        inputs=_promotion_gate_inputs_from_config(config=config),
    )


def _read_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in TRUE_VALUES


def _read_mode_env() -> ExecutionMode:
    raw_mode = os.getenv("AEE_EXECUTION_MODE", ExecutionMode.SIMULATION.value)
    normalized = raw_mode.strip().lower()

    try:
        return ExecutionMode(normalized)
    except ValueError as exc:
        supported = ", ".join(mode.value for mode in ExecutionMode)
        raise ConfigurationError(
            f"unsupported execution mode '{raw_mode}'; expected one of: {supported}"
        ) from exc


def _read_durable_state_root() -> Path:
    raw_root = os.getenv("AEE_DURABLE_STATE_ROOT")
    if raw_root is None or not raw_root.strip():
        return DEFAULT_DURABLE_STATE_ROOT
    return Path(raw_root).expanduser()


def resolve_order_store_path(*, context: StartupContext) -> Path:
    return (
        context.durable_state_root
        / context.profile.persistence_namespace
        / "orders.sqlite3"
    )


def resolve_mode_order_store_path(
    *, durable_state_root: Path, mode: ExecutionMode
) -> Path:
    profile = get_runtime_profile(mode)
    return durable_state_root / profile.persistence_namespace / "orders.sqlite3"


def resolve_promotion_store_path(*, durable_state_root: Path) -> Path:
    return durable_state_root / "control" / "promotion.sqlite3"


def build_order_store(*, context: StartupContext) -> SQLiteOrderStore:
    store = SQLiteOrderStore(db_path=resolve_order_store_path(context=context))
    store.initialize()
    return store


def build_broker_submitter(
    *,
    context: StartupContext,
) -> BrokerSubmitter:
    if context.profile.mode is ExecutionMode.SIMULATION:
        return SyntheticBrokerSubmitter()
    if context.broker_adapter_config is None:
        raise ConfigurationError(
            "paper and live modes require a validated broker adapter configuration"
        )
    return AlpacaBrokerSubmitter(config=context.broker_adapter_config)


def build_submission_service(
    *,
    context: StartupContext,
    submitter: BrokerSubmitter | None = None,
) -> BrokerSubmissionService:
    store = build_order_store(context=context)
    return BrokerSubmissionService(
        submission_book=store.build_submission_book(),
        submitter=submitter or build_broker_submitter(context=context),
    )


def build_account_lease_service(*, context: StartupContext) -> AccountLeaseService:
    store = build_order_store(context=context)
    return AccountLeaseService(backend=store.build_account_lease_backend())


def build_operator_control_service(
    *,
    context: StartupContext,
    order_store: SQLiteOrderStore | None = None,
) -> OperatorControlService:
    store = order_store or build_order_store(context=context)
    return OperatorControlService(order_store=store)


def build_runtime_diagnostics_service(
    *,
    context: StartupContext,
    order_store: SQLiteOrderStore | None = None,
) -> RuntimeDiagnosticsService:
    store = order_store or build_order_store(context=context)
    return RuntimeDiagnosticsService(order_store=store)


def build_reconciliation_runner(
    *,
    context: StartupContext,
    order_store: SQLiteOrderStore | None = None,
    lease_service: AccountLeaseService | None = None,
    quarantine_registry: AccountQuarantineRegistry | None = None,
    reconciliation_service: ReconciliationService | None = None,
    owner_id: str = "startup-reconciliation-runner",
    lease_ttl_seconds: int = 30,
) -> ReconciliationRunner:
    store = order_store or build_order_store(context=context)
    leases = lease_service or build_account_lease_service(context=context)
    registry = quarantine_registry or build_account_quarantine_registry(
        context=context,
        order_store=store,
        preload_persisted_reports=True,
    )
    service = reconciliation_service or ReconciliationService()
    return ReconciliationRunner(
        order_store=store,
        lease_service=leases,
        quarantine_registry=registry,
        reconciliation_service=service,
        owner_id=owner_id,
        lease_ttl_seconds=lease_ttl_seconds,
    )


def build_account_quarantine_registry(
    *,
    context: StartupContext | None = None,
    order_store: SQLiteOrderStore | None = None,
    preload_persisted_reports: bool = False,
) -> AccountQuarantineRegistry:
    registry = AccountQuarantineRegistry()
    if not preload_persisted_reports:
        return registry

    if order_store is None:
        if context is None:
            raise ValueError(
                "context or order_store is required when preload_persisted_reports is enabled"
            )
        order_store = build_order_store(context=context)

    for account_id in order_store.list_accounts_with_reconciliation_reports():
        report = order_store.load_latest_reconciliation_report(account_id=account_id)
        if report is not None:
            registry.record(report=report)

    return registry


def reconcile_account_startup_state(
    *,
    context: StartupContext,
    account_id: str,
    broker_orders: list[BrokerOrderSnapshot],
    broker_positions: list[PositionSnapshot] | None = None,
    broker_cash: CashSnapshot | None = None,
    opening_cash_balance: float = 0.0,
    order_store: SQLiteOrderStore | None = None,
    reconciliation_service: ReconciliationService | None = None,
    quarantine_registry: AccountQuarantineRegistry | None = None,
    persist_report: bool = True,
) -> ReconciliationReport:
    store = order_store or build_order_store(context=context)
    store.repair_orders_from_submissions(account_id=account_id)
    internal_orders = store.list_internal_order_snapshots(account_id=account_id)
    internal_positions = store.project_internal_positions(account_id=account_id)
    internal_cash = store.project_internal_cash(
        account_id=account_id,
        opening_balance=opening_cash_balance,
    )
    service = reconciliation_service or ReconciliationService()
    report = service.compare_orders(
        account_id=account_id,
        internal_orders=internal_orders,
        broker_orders=broker_orders,
        internal_positions=internal_positions,
        broker_positions=broker_positions,
        internal_cash=internal_cash,
        broker_cash=broker_cash,
    )
    if persist_report:
        store.record_reconciliation_report(
            report=report,
            internal_orders=internal_orders,
            broker_orders=broker_orders,
            internal_positions=internal_positions,
            broker_positions=broker_positions or (),
            internal_cash=internal_cash,
            broker_cash=broker_cash,
        )
    if quarantine_registry is not None:
        quarantine_registry.record(report=report)
    return report


def reconcile_all_startup_accounts(
    *,
    context: StartupContext,
    broker_snapshots_by_account: BrokerSnapshotByAccount,
    broker_positions_by_account: BrokerPositionsByAccount | None = None,
    broker_cash_by_account: BrokerCashByAccount | None = None,
    opening_cash_by_account: dict[str, float] | None = None,
    order_store: SQLiteOrderStore | None = None,
    reconciliation_service: ReconciliationService | None = None,
    quarantine_registry: AccountQuarantineRegistry | None = None,
) -> list[ReconciliationReport]:
    runner = build_reconciliation_runner(
        context=context,
        order_store=order_store,
        quarantine_registry=quarantine_registry,
        reconciliation_service=reconciliation_service,
        owner_id="startup-reconciliation-runner",
    )
    records = runner.run_once(
        broker_snapshots_by_account=broker_snapshots_by_account,
        broker_positions_by_account=broker_positions_by_account,
        broker_cash_by_account=broker_cash_by_account,
        opening_cash_by_account=opening_cash_by_account,
    )
    return [record.report for record in records if record.report is not None]


def _read_csv_env(name: str) -> tuple[str, ...]:
    raw_value = os.getenv(name, "")
    if not raw_value.strip():
        return ()
    return tuple(
        sorted(
            {
                item.strip()
                for item in raw_value.split(",")
                if item.strip()
            }
        )
    )


def _read_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        parsed = int(raw_value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer value") from exc
    if parsed < 1:
        raise ConfigurationError(f"{name} must be greater than or equal to 1")
    return parsed


def _evaluate_promotion_gate(
    *,
    mode: ExecutionMode,
    durable_state_root: Path,
    inputs: PromotionGateInputs,
) -> PromotionGateDecisionRecord:
    source_mode = promotion_source_mode(mode)
    if source_mode is None:
        raise ConfigurationError(
            "promotion gates are only defined for paper and live startup"
        )

    evaluator = PromotionGateEvaluator()
    evidence_store = SQLiteOrderStore(
        db_path=resolve_mode_order_store_path(
            durable_state_root=durable_state_root,
            mode=source_mode,
        )
    )
    evidence_store.initialize()
    decision_store = SQLiteOrderStore(
        db_path=resolve_promotion_store_path(durable_state_root=durable_state_root)
    )
    decision_store.initialize()
    decision = evaluator.evaluate(
        target_mode=mode,
        source_mode=source_mode,
        evidence_store=evidence_store,
        decision_store=decision_store,
        inputs=inputs,
    )
    evaluator.persist(decision_store=decision_store, record=decision)
    if not decision.approved:
        failed_detail = next(
            (
                criterion.detail
                for criterion in decision.criteria
                if not criterion.passed
            ),
            decision.summary,
        )
        raise ConfigurationError(failed_detail)
    return decision


def load_startup_context() -> StartupContext:
    config = parse_startup_config()
    profile = _validate_startup_config(config=config)
    broker_adapter_config = _load_broker_adapter_config(config=config)
    promotion_gate_decision = _evaluate_mode_promotion_gate(config=config)

    return StartupContext(
        profile=profile,
        safety=config.safety,
        durable_state_root=config.durable_state_root,
        promotion_gate_decision=promotion_gate_decision,
        broker_adapter_config=broker_adapter_config,
    )
