from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Protocol, TypedDict
from urllib.parse import urlencode

import requests

from auto_execution_engine.adapters.broker.models import (
    BrokerRetryDisposition,
    BrokerSubmissionOutcome,
)
from auto_execution_engine.adapters.broker.service import RegisteredSubmission
from auto_execution_engine.config.execution_mode import ConfigurationError, ExecutionMode


_RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
_TERMINAL_STATUS_CODES = {400, 401, 403, 404, 422}


class AlpacaOrderPayload(TypedDict, total=False):
    id: str
    client_order_id: str
    status: str
    message: str
    detail: str
    error: str
    code: str


class HTTPResponse(Protocol):
    status_code: int
    text: str

    def json(self) -> object: ...


class HTTPSession(Protocol):
    def post(
        self,
        url: str,
        *,
        json: dict[str, str],
        headers: dict[str, str],
        timeout: float,
    ) -> HTTPResponse: ...

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
    ) -> HTTPResponse: ...


@dataclass(frozen=True)
class AlpacaTradingConfig:
    api_key_id: str
    api_secret_key: str
    trading_base_url: str
    timeout_seconds: float = 10.0
    default_time_in_force: str = "day"

    @property
    def orders_url(self) -> str:
        return f"{self.trading_base_url.rstrip('/')}/v2/orders"

    @property
    def order_lookup_base_url(self) -> str:
        return f"{self.trading_base_url.rstrip('/')}/v2/orders:by_client_order_id"

    def headers(self) -> dict[str, str]:
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "APCA-API-KEY-ID": self.api_key_id,
            "APCA-API-SECRET-KEY": self.api_secret_key,
        }


@dataclass(frozen=True)
class AlpacaBrokerSubmitter:
    config: AlpacaTradingConfig
    session: HTTPSession | None = None

    def submit(self, *, request) -> RegisteredSubmission:
        session = self.session or requests.Session()
        payload = _build_payload(request=request, config=self.config)
        try:
            response = session.post(
                self.config.orders_url,
                json=payload,
                headers=self.config.headers(),
                timeout=self.config.timeout_seconds,
            )
        except requests.RequestException as exc:
            recovered_submission = self._lookup_existing_submission(
                session=session,
                account_id=request.account_id,
                client_order_id=request.client_order_id,
            )
            if recovered_submission is not None:
                return recovered_submission
            return RegisteredSubmission(
                account_id=request.account_id,
                client_order_id=request.client_order_id,
                broker_order_id=None,
                outcome=BrokerSubmissionOutcome.FAILED,
                retry_disposition=BrokerRetryDisposition.SAFE_TO_RETRY,
                message=(
                    "transport failed before broker receipt was confirmed: "
                    f"{exc.__class__.__name__}"
                ),
            )

        body = _safe_json(response)
        status_code = int(response.status_code)
        if 200 <= status_code < 300:
            submission = _submission_from_order_payload(
                payload=body,
                account_id=request.account_id,
                client_order_id=request.client_order_id,
            )
            if submission is not None:
                return submission
            recovered_submission = self._lookup_existing_submission(
                session=session,
                account_id=request.account_id,
                client_order_id=request.client_order_id,
            )
            if recovered_submission is not None:
                return recovered_submission
            return RegisteredSubmission(
                account_id=request.account_id,
                client_order_id=request.client_order_id,
                broker_order_id=None,
                outcome=BrokerSubmissionOutcome.UNKNOWN,
                retry_disposition=BrokerRetryDisposition.DO_NOT_RETRY,
                message="broker acknowledged submission but returned an unreadable order payload",
            )

        if status_code in _TERMINAL_STATUS_CODES:
            return RegisteredSubmission(
                account_id=request.account_id,
                client_order_id=request.client_order_id,
                broker_order_id=_extract_broker_order_id(body),
                outcome=BrokerSubmissionOutcome.REJECTED,
                retry_disposition=BrokerRetryDisposition.DO_NOT_RETRY,
                message=_error_message(
                    status_code=status_code,
                    body=body,
                    default_message="broker rejected order submission",
                ),
            )

        if status_code in _RETRYABLE_STATUS_CODES:
            recovered_submission = self._lookup_existing_submission(
                session=session,
                account_id=request.account_id,
                client_order_id=request.client_order_id,
            )
            if recovered_submission is not None:
                return recovered_submission
            return RegisteredSubmission(
                account_id=request.account_id,
                client_order_id=request.client_order_id,
                broker_order_id=None,
                outcome=BrokerSubmissionOutcome.FAILED,
                retry_disposition=BrokerRetryDisposition.SAFE_TO_RETRY,
                message=_error_message(
                    status_code=status_code,
                    body=body,
                    default_message="broker transport failed before receipt was confirmed",
                ),
            )

        recovered_submission = self._lookup_existing_submission(
            session=session,
            account_id=request.account_id,
            client_order_id=request.client_order_id,
        )
        if recovered_submission is not None:
            return recovered_submission
        return RegisteredSubmission(
            account_id=request.account_id,
            client_order_id=request.client_order_id,
            broker_order_id=None,
            outcome=BrokerSubmissionOutcome.UNKNOWN,
            retry_disposition=BrokerRetryDisposition.DO_NOT_RETRY,
            message=_error_message(
                status_code=status_code,
                body=body,
                default_message="broker returned an unclassified submission outcome",
            ),
        )

    def _lookup_existing_submission(
        self,
        *,
        session: HTTPSession,
        account_id: str,
        client_order_id: str,
    ) -> RegisteredSubmission | None:
        lookup_url = f"{self.config.order_lookup_base_url}?{urlencode({'client_order_id': client_order_id})}"
        try:
            response = session.get(
                lookup_url,
                headers=self.config.headers(),
                timeout=self.config.timeout_seconds,
            )
        except requests.RequestException:
            return None
        if int(response.status_code) == 404:
            return None
        if not 200 <= int(response.status_code) < 300:
            return None
        body = _safe_json(response)
        return _submission_from_order_payload(
            payload=body,
            account_id=account_id,
            client_order_id=client_order_id,
        )


def load_alpaca_trading_config(*, mode: ExecutionMode) -> AlpacaTradingConfig:
    if mode is ExecutionMode.SIMULATION:
        raise ConfigurationError("alpaca trading config is not used in simulation mode")

    mode_prefix = mode.value.upper()
    api_key_id = _read_required_env(
        primary=f"AEE_ALPACA_{mode_prefix}_API_KEY_ID",
        fallback="AEE_ALPACA_API_KEY_ID",
        description=f"Alpaca {mode.value} API key id",
    )
    api_secret_key = _read_required_env(
        primary=f"AEE_ALPACA_{mode_prefix}_API_SECRET_KEY",
        fallback="AEE_ALPACA_API_SECRET_KEY",
        description=f"Alpaca {mode.value} API secret key",
    )
    trading_base_url = _read_optional_env(
        primary=f"AEE_ALPACA_{mode_prefix}_BASE_URL",
        fallback="AEE_ALPACA_BASE_URL",
    ) or _default_base_url(mode=mode)
    timeout_seconds = _read_timeout_seconds()
    return AlpacaTradingConfig(
        api_key_id=api_key_id,
        api_secret_key=api_secret_key,
        trading_base_url=trading_base_url,
        timeout_seconds=timeout_seconds,
    )


def _build_payload(*, request, config: AlpacaTradingConfig) -> dict[str, str]:
    payload: dict[str, str] = {
        "symbol": request.symbol,
        "qty": _format_decimal(request.quantity),
        "side": request.side.value,
        "type": request.order_type.value,
        "time_in_force": config.default_time_in_force,
        "client_order_id": request.client_order_id,
    }
    if request.limit_price is not None:
        payload["limit_price"] = _format_decimal(request.limit_price)
    return payload


def _submission_from_order_payload(
    *,
    payload: object,
    account_id: str,
    client_order_id: str,
) -> RegisteredSubmission | None:
    if not isinstance(payload, dict):
        return None
    order_payload: AlpacaOrderPayload = payload
    broker_order_id = order_payload.get("id")
    if not isinstance(broker_order_id, str) or not broker_order_id.strip():
        return None
    broker_client_order_id = order_payload.get("client_order_id", client_order_id)
    status = str(order_payload.get("status", "accepted"))
    return RegisteredSubmission(
        account_id=account_id,
        client_order_id=str(broker_client_order_id),
        broker_order_id=broker_order_id,
        outcome=BrokerSubmissionOutcome.ACCEPTED,
        retry_disposition=BrokerRetryDisposition.DO_NOT_RETRY,
        message=f"broker accepted order with status {status}",
    )


def _extract_broker_order_id(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    order_payload: AlpacaOrderPayload = payload
    broker_order_id = order_payload.get("id")
    if isinstance(broker_order_id, str) and broker_order_id.strip():
        return broker_order_id
    return None


def _error_message(*, status_code: int, body: object, default_message: str) -> str:
    detail = _extract_detail(body)
    if detail:
        return f"{default_message}: HTTP {status_code} - {detail}"
    return f"{default_message}: HTTP {status_code}"


def _extract_detail(body: object) -> str | None:
    if isinstance(body, dict):
        error_payload: AlpacaOrderPayload = body
        for key in ("message", "detail", "error", "code"):
            value = error_payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return json.dumps(error_payload, sort_keys=True)
    if isinstance(body, str) and body.strip():
        return body.strip()
    return None


def _safe_json(response: HTTPResponse) -> object:
    try:
        return response.json()
    except ValueError:
        text = getattr(response, "text", "")
        if isinstance(text, str) and text.strip():
            return text.strip()
        return None


def _format_decimal(value: float) -> str:
    return format(value, ".15g")


def _read_required_env(*, primary: str, fallback: str, description: str) -> str:
    value = _read_optional_env(primary=primary, fallback=fallback)
    if value is None:
        raise ConfigurationError(
            f"missing {description}; set {primary}"
            + (f" or {fallback}" if fallback else "")
        )
    return value


def _read_optional_env(*, primary: str, fallback: str | None = None) -> str | None:
    for name in (primary, fallback):
        if not name:
            continue
        raw_value = os.getenv(name)
        if raw_value is not None and raw_value.strip():
            return raw_value.strip()
    return None


def _read_timeout_seconds() -> float:
    raw_value = os.getenv("AEE_ALPACA_TIMEOUT_SECONDS")
    if raw_value is None or not raw_value.strip():
        return 10.0
    try:
        parsed = float(raw_value)
    except ValueError as exc:
        raise ConfigurationError("AEE_ALPACA_TIMEOUT_SECONDS must be numeric") from exc
    if parsed <= 0:
        raise ConfigurationError("AEE_ALPACA_TIMEOUT_SECONDS must be greater than zero")
    return parsed


def _default_base_url(*, mode: ExecutionMode) -> str:
    if mode is ExecutionMode.PAPER:
        return "https://paper-api.alpaca.markets"
    if mode is ExecutionMode.LIVE:
        return "https://api.alpaca.markets"
    raise ConfigurationError(f"unsupported Alpaca execution mode: {mode.value}")
