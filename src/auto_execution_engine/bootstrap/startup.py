from __future__ import annotations

import os
from dataclasses import dataclass

from auto_execution_engine.config.execution_mode import (
    ConfigurationError,
    ExecutionMode,
    ModeRuntimeProfile,
    SafetyGateConfig,
    validate_startup,
)


TRUE_VALUES = {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class StartupContext:
    """Validated startup configuration for process boot."""

    profile: ModeRuntimeProfile
    safety: SafetyGateConfig



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
    return StartupContext(profile=profile, safety=safety)
