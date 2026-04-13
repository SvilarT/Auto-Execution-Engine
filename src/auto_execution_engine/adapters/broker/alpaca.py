from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Protocol, TypedDict
from urllib.parse import urlencode

import requests

from auto_execution_engine.adapters.broker.models import (
    BrokerOrderActivityPage,
    BrokerRetryDisposition,
    BrokerSubmissionOutcome,
    CanonicalBrokerOrderStatus,
    RawBrokerActivity,
    RawBrokerOrderSnapshot,
)
from auto_execution_engine.adapters.broker.service import BrokerStateReader, RegisteredSubmission
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
class AlpacaBrokerSubmitter(BrokerStateReader):
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

    def list_order_snapshots(self, *, account_id: str) -> tuple[RawBrokerOrderSnapshot, ...]:
        session = self.session or requests.Session()
        try:
            response = session.get(
                self.config.orders_url,
                headers=self.config.headers(),
                timeout=self.config.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"failed to load broker orders for account {account_id}: {exc}") from exc
        if not 200 <= int(response.status_code) < 300:
            raise RuntimeError(
                _error_message(
                    status_code=int(response.status_code),
                    body=_safe_json(response),
                    default_message=f"failed to load broker orders for account {account_id}",
                )
            )
        body = _safe_json(response)
        if not isinstance(body, list):
            raise RuntimeError("broker returned an unreadable order list payload")
        snapshots: list[RawBrokerOrderSnapshot] = []
        for payload in body:
            snapshot = _raw_order_snapshot_from_payload(payload=payload, account_id=account_id)
            if snapshot is not None:
                snapshots.append(snapshot)
        return tuple(snapshots)

    def list_order_activities(
        self,
        *,
        account_id: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> BrokerOrderActivityPage:
        session = self.session or requests.Session()
        query: dict[str, str] = {"direction": "asc", "page_size": str(limit)}
        if cursor is not None:
            query["page_token"] = cursor
        activity_url = f"{self.config.trading_base_url.rstrip('/')}/v2/account/activities/FILL?{urlencode(query)}"
        try:
            response = session.get(
                activity_url,
                headers=self.config.headers(),
                timeout=self.config.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"failed to load broker activities for account {account_id}: {exc}") from exc
        if not 200 <= int(response.status_code) < 300:
            raise RuntimeError(
                _error_message(
                    status_code=int(response.status_code),
                    body=_safe_json(response),
                    default_message=f"failed to load broker activities for account {account_id}",
                )
            )
        body = _safe_json(response)
        if not isinstance(body, list):
            raise RuntimeError("broker returned an unreadable activity payload")
        activities: list[RawBrokerActivity] = []
        for payload in body:
            activity = _raw_activity_from_payload(payload=payload, account_id=account_id)
            if activity is not None:
                activities.append(activity)
        next_cursor = activities[-1].activity_id if activities else cursor
        return BrokerOrderActivityPage(
            account_id=account_id,
            activities=tuple(activities),
            cursor=next_cursor,
            has_more=len(activities) >= limit,
        )

    def get_order_by_client_order_id(
        self, *, account_id: str, client_order_id: str
    ) -> RawBrokerOrderSnapshot | None:
        session = self.session or requests.Session()
        lookup_url = f"{self.config.order_lookup_base_url}?{urlencode({'client_order_id': client_order_id})}"
        try:
            response = session.get(
                lookup_url,
                headers=self.config.headers(),
                timeout=self.config.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"failed to look up broker order {client_order_id} for account {account_id}: {exc}"
            ) from exc
        if int(response.status_code) == 404:
            return None
        if not 200 <= int(response.status_code) < 300:
            raise RuntimeError(
                _error_message(
                    status_code=int(response.status_code),
                    body=_safe_json(response),
                    default_message=(
                        f"failed to look up broker order {client_order_id} for account {account_id}"
                    ),
                )
            )
        return _raw_order_snapshot_from_payload(
            payload=_safe_json(response),
            account_id=account_id,
        )

    def _lookup_existing_submission(
        self,
        *,
        session: HTTPSession,
        account_id: str,
        client_order_id: str,
    ) -> RegisteredSubmission | None:
        try:
            snapshot = self.get_order_by_client_order_id(
                account_id=account_id,
                client_order_id=client_order_id,
            )
        except RuntimeError:
            return None
        if snapshot is None:
            return None
        return RegisteredSubmission(
            account_id=account_id,
            client_order_id=snapshot.client_order_id,
            broker_order_id=snapshot.broker_order_id,
            outcome=BrokerSubmissionOutcome.ACCEPTED,
            retry_disposition=BrokerRetryDisposition.DO_NOT_RETRY,
            message=f"broker accepted order with status {snapshot.raw_status}",
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


def _raw_order_snapshot_from_payload(
    *, payload: object, account_id: str
) -> RawBrokerOrderSnapshot | None:
    if not isinstance(payload, dict):
        return None
    broker_order_id = payload.get("id")
    client_order_id = payload.get("client_order_id")
    symbol = payload.get("symbol")
    status = payload.get("status")
    if not all(isinstance(value, str) and value.strip() for value in (broker_order_id, client_order_id, symbol, status)):
        return None
    filled_quantity = _coerce_float(payload.get("filled_qty", payload.get("filled_quantity", 0.0)))
    observed_at = _coerce_string(
        payload.get("updated_at")
        or payload.get("filled_at")
        or payload.get("submitted_at")
        or payload.get("created_at")
    ) or ""
    raw_payload = {str(key): value for key, value in payload.items()}
    return RawBrokerOrderSnapshot(
        account_id=account_id,
        broker_order_id=broker_order_id.strip(),
        client_order_id=client_order_id.strip(),
        symbol=symbol.strip(),
        raw_status=status.strip(),
        canonical_status=_map_alpaca_status(status.strip()),
        filled_quantity=filled_quantity,
        raw_payload=raw_payload,
        observed_at=observed_at,
    )


def _raw_activity_from_payload(*, payload: object, account_id: str) -> RawBrokerActivity | None:
    if not isinstance(payload, dict):
        return None
    activity_id = _coerce_string(payload.get("id") or payload.get("activity_id"))
    activity_type = _coerce_string(payload.get("activity_type") or payload.get("activity_type_name") or payload.get("activity_type_id") or payload.get("transaction_type") or payload.get("side") or "fill")
    if activity_id is None or activity_type is None:
        return None
    return RawBrokerActivity(
        account_id=account_id,
        activity_id=activity_id,
        activity_type=activity_type,
        client_order_id=_coerce_string(payload.get("client_order_id") or payload.get("client_order_uuid") or payload.get("order_id")),
        broker_order_id=_coerce_string(payload.get("order_id")),
        raw_payload={str(key): value for key, value in payload.items()},
        occurred_at=_coerce_string(payload.get("transaction_time") or payload.get("date") or payload.get("timestamp")) or "",
    )


def _map_alpaca_status(status: str) -> CanonicalBrokerOrderStatus:
    normalized = status.strip().lower()
    return {
        "new": CanonicalBrokerOrderStatus.NEW,
        "accepted": CanonicalBrokerOrderStatus.NEW,
        "accepted_for_bidding": CanonicalBrokerOrderStatus.NEW,
        "pending_new": CanonicalBrokerOrderStatus.NEW,
        "done_for_day": CanonicalBrokerOrderStatus.ACKNOWLEDGED,
        "partially_filled": CanonicalBrokerOrderStatus.PARTIALLY_FILLED,
        "filled": CanonicalBrokerOrderStatus.FILLED,
        "canceled": CanonicalBrokerOrderStatus.CANCELED,
        "cancelled": CanonicalBrokerOrderStatus.CANCELED,
        "rejected": CanonicalBrokerOrderStatus.REJECTED,
        "expired": CanonicalBrokerOrderStatus.EXPIRED,
        "pending_cancel": CanonicalBrokerOrderStatus.PENDING_CANCEL,
        "pending_replace": CanonicalBrokerOrderStatus.PENDING_REPLACE,
        "replaced": CanonicalBrokerOrderStatus.REPLACED,
        "stopped": CanonicalBrokerOrderStatus.SUSPENDED,
        "suspended": CanonicalBrokerOrderStatus.SUSPENDED,
        "calculated": CanonicalBrokerOrderStatus.ACKNOWLEDGED,
    }.get(normalized, CanonicalBrokerOrderStatus.UNKNOWN)


def _coerce_string(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _coerce_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


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
