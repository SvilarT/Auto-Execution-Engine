from auto_execution_engine.bootstrap.startup import load_startup_context
from auto_execution_engine.config.execution_mode import ConfigurationError, ExecutionMode



def test_simulation_mode_starts_with_defaults(monkeypatch):
    monkeypatch.delenv("AEE_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("AEE_ALLOW_PAPER", raising=False)
    monkeypatch.delenv("AEE_ALLOW_LIVE", raising=False)

    context = load_startup_context()

    assert context.profile.mode is ExecutionMode.SIMULATION
    assert context.profile.synthetic_data_allowed is True
    assert context.profile.external_broker_effects_allowed is False



def test_paper_mode_requires_explicit_enablement(monkeypatch):
    monkeypatch.setenv("AEE_EXECUTION_MODE", "paper")

    try:
        load_startup_context()
        assert False, "expected ConfigurationError"
    except ConfigurationError as exc:
        assert "paper mode is disabled" in str(exc)



def test_live_mode_requires_operator_approval(monkeypatch):
    monkeypatch.setenv("AEE_EXECUTION_MODE", "live")
    monkeypatch.setenv("AEE_ALLOW_LIVE", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.delenv("AEE_OPERATOR_APPROVAL_PRESENT", raising=False)

    try:
        load_startup_context()
        assert False, "expected ConfigurationError"
    except ConfigurationError as exc:
        assert "operator approval" in str(exc)



def test_live_mode_starts_only_when_all_safety_gates_are_present(monkeypatch):
    monkeypatch.setenv("AEE_EXECUTION_MODE", "live")
    monkeypatch.setenv("AEE_ALLOW_LIVE", "true")
    monkeypatch.setenv("AEE_BROKER_CREDENTIALS_PRESENT", "true")
    monkeypatch.setenv("AEE_RISK_ENGINE_CONFIGURED", "true")
    monkeypatch.setenv("AEE_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("AEE_DURABLE_STATE_ENABLED", "true")
    monkeypatch.setenv("AEE_OPERATOR_APPROVAL_PRESENT", "true")

    context = load_startup_context()

    assert context.profile.mode is ExecutionMode.LIVE
    assert context.profile.synthetic_data_allowed is False
    assert context.profile.persistence_namespace == "live"
