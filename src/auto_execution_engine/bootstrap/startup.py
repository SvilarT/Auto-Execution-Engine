import os
from dataclasses import dataclass
from pathlib import Path

from auto_execution_engine.adapters.broker.service import BrokerSubmissionService
from auto_execution_engine.adapters.persistence.sqlite_order_store import SQLiteOrderStore
from auto_execution_engine.trading_plane.leases import AccountLeaseService
from auto_execution_engine.config.execution_mode import (
    ConfigurationError,
    ExecutionMode,
    ModeRuntimeProfile,
    SafetyGateConfig,
    validate_startup,
)
from auto_execution_engine.reconciliation.models import BrokerOrderSnapshot, ReconciliationReport


BrokerSnapshotByAccount = dict[str, list[BrokerOrderSnapshot]]
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


def build_order_store(*, context: StartupContext) -> SQLiteOrderStore:
    store = SQLiteOrderStore(db_path=resolve_order_store_path(context=context))
    store.initialize()
    return store


def build_submission_service(*, context: StartupContext) -> BrokerSubmissionService:
    store = build_order_store(context=context)
    return BrokerSubmissionService(submission_book=store.build_submission_book())


def build_account_lease_service(*, context: StartupContext) -> AccountLeaseService:
    store = build_order_store(context=context)
    return AccountLeaseService(backend=store.build_account_lease_backend())


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
    order_store: SQLiteOrderStore | None = None,
    reconciliation_service: ReconciliationService | None = None,
    quarantine_registry: AccountQuarantineRegistry | None = None,
    persist_report: bool = True,
) -> ReconciliationReport:
    store = order_store or build_order_store(context=context)
    store.repair_orders_from_submissions(account_id=account_id)
    internal_orders = store.list_internal_order_snapshots(account_id=account_id)
    service = reconciliation_service or ReconciliationService()
    report = service.compare_orders(
        account_id=account_id,
        internal_orders=internal_orders,
        broker_orders=broker_orders,
    )
    if persist_report:
        store.record_reconciliation_report(
            report=report,
            internal_orders=internal_orders,
            broker_orders=broker_orders,
        )
    if quarantine_registry is not None:
        quarantine_registry.record(report=report)
    return report


def reconcile_all_startup_accounts(
    *,
    context: StartupContext,
    broker_snapshots_by_account: BrokerSnapshotByAccount,
    order_store: SQLiteOrderStore | None = None,
    reconciliation_service: ReconciliationService | None = None,
    quarantine_registry: AccountQuarantineRegistry | None = None,
) -> list[ReconciliationReport]:
    store = order_store or build_order_store(context=context)
    reports: list[ReconciliationReport] = []

    account_ids = set(store.list_accounts_requiring_reconciliation())
    account_ids.update(store.list_accounts_with_reconciliation_reports())

    for account_id in sorted(account_ids):
        store.repair_orders_from_submissions(account_id=account_id)

    account_ids = set(store.list_accounts_requiring_reconciliation())
    account_ids.update(store.list_accounts_with_reconciliation_reports())

    for account_id in sorted(account_ids):
        reports.append(
            reconcile_account_startup_state(
                context=context,
                account_id=account_id,
                broker_orders=broker_snapshots_by_account.get(account_id, []),
                order_store=store,
                reconciliation_service=reconciliation_service,
                quarantine_registry=quarantine_registry,
                persist_report=True,
            )
        )

    return reports


def load_startup_context() -> StartupContext:
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

    profile = validate_startup(mode=mode, safety=safety)
    durable_state_root = _read_durable_state_root()
    return StartupContext(
        profile=profile,
        safety=safety,
        durable_state_root=durable_state_root,
    )
