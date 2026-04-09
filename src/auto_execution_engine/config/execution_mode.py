from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ExecutionMode(str, Enum):
    """Explicit execution domains for the trading platform."""

    SIMULATION = "simulation"
    PAPER = "paper"
    LIVE = "live"


class ConfigurationError(ValueError):
    """Raised when startup configuration is unsafe or incomplete."""


@dataclass(frozen=True)
class SafetyGateConfig:
    """Boot-time safety requirements that control whether a mode may start."""

    allow_paper: bool = False
    allow_live: bool = False
    broker_credentials_present: bool = False
    risk_engine_configured: bool = False
    reconciliation_enabled: bool = False
    durable_state_enabled: bool = False
    operator_approval_present: bool = False


@dataclass(frozen=True)
class ModeRuntimeProfile:
    """Resolved runtime profile for a given execution domain."""

    mode: ExecutionMode
    market_data_provider: str
    execution_adapter: str
    persistence_namespace: str
    synthetic_data_allowed: bool
    external_broker_effects_allowed: bool


_RUNTIME_PROFILES: dict[ExecutionMode, ModeRuntimeProfile] = {
    ExecutionMode.SIMULATION: ModeRuntimeProfile(
        mode=ExecutionMode.SIMULATION,
        market_data_provider="simulation_market_data",
        execution_adapter="simulation_execution_adapter",
        persistence_namespace="simulation",
        synthetic_data_allowed=True,
        external_broker_effects_allowed=False,
    ),
    ExecutionMode.PAPER: ModeRuntimeProfile(
        mode=ExecutionMode.PAPER,
        market_data_provider="paper_market_data",
        execution_adapter="paper_broker_adapter",
        persistence_namespace="paper",
        synthetic_data_allowed=False,
        external_broker_effects_allowed=True,
    ),
    ExecutionMode.LIVE: ModeRuntimeProfile(
        mode=ExecutionMode.LIVE,
        market_data_provider="live_market_data",
        execution_adapter="live_broker_adapter",
        persistence_namespace="live",
        synthetic_data_allowed=False,
        external_broker_effects_allowed=True,
    ),
}


def get_runtime_profile(mode: ExecutionMode) -> ModeRuntimeProfile:
    return _RUNTIME_PROFILES[mode]



def validate_startup(mode: ExecutionMode, safety: SafetyGateConfig) -> ModeRuntimeProfile:
    """Validate boot-time safety conditions before the platform starts."""

    profile = get_runtime_profile(mode)

    if mode is ExecutionMode.SIMULATION:
        return profile

    if mode is ExecutionMode.PAPER and not safety.allow_paper:
        raise ConfigurationError(
            "paper mode is disabled; explicit paper enablement is required"
        )

    if mode is ExecutionMode.LIVE and not safety.allow_live:
        raise ConfigurationError(
            "live mode is disabled; explicit live enablement is required"
        )

    if not safety.broker_credentials_present:
        raise ConfigurationError(
            f"{mode.value} mode requires broker credentials before startup"
        )

    if not safety.risk_engine_configured:
        raise ConfigurationError(
            f"{mode.value} mode requires an independent risk engine configuration"
        )

    if not safety.reconciliation_enabled:
        raise ConfigurationError(
            f"{mode.value} mode requires broker reconciliation before startup"
        )

    if not safety.durable_state_enabled:
        raise ConfigurationError(
            f"{mode.value} mode requires durable financial state before startup"
        )

    if mode is ExecutionMode.LIVE and not safety.operator_approval_present:
        raise ConfigurationError(
            "live mode requires explicit operator approval before startup"
        )

    return profile
