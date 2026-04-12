import requests

from auto_execution_engine.adapters.broker.alpaca import (
    AlpacaBrokerSubmitter,
    AlpacaTradingConfig,
)
from auto_execution_engine.adapters.broker.models import (
    BrokerOrderRequest,
    BrokerOrderSide,
    BrokerOrderType,
    BrokerRetryDisposition,
    BrokerSubmissionOutcome,
)


class StubResponse:
    def __init__(self, *, status_code: int, body=None, text: str = "") -> None:
        self.status_code = status_code
        self._body = body
        self.text = text

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class StubSession:
    def __init__(self, *, post_result, get_results=()) -> None:
        self._post_result = post_result
        self._get_results = list(get_results)
        self.post_calls: list[dict] = []
        self.get_calls: list[dict] = []

    def post(self, url: str, *, json, headers, timeout: float):
        self.post_calls.append(
            {
                "url": url,
                "json": json,
                "headers": headers,
                "timeout": timeout,
            }
        )
        if isinstance(self._post_result, Exception):
            raise self._post_result
        return self._post_result

    def get(self, url: str, *, headers, timeout: float):
        self.get_calls.append(
            {
                "url": url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        result = self._get_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result



def make_config() -> AlpacaTradingConfig:
    return AlpacaTradingConfig(
        api_key_id="key-id",
        api_secret_key="secret-key",
        trading_base_url="https://paper-api.alpaca.markets",
        timeout_seconds=12.5,
    )



def make_request() -> BrokerOrderRequest:
    return BrokerOrderRequest(
        account_id="acct-1",
        client_order_id="client-123",
        symbol="BTCUSD",
        side=BrokerOrderSide.BUY,
        quantity=1.25,
        order_type=BrokerOrderType.LIMIT,
        limit_price=60250.5,
    )



def test_alpaca_submitter_maps_successful_order_submission() -> None:
    session = StubSession(
        post_result=StubResponse(
            status_code=200,
            body={
                "id": "alpaca-order-1",
                "client_order_id": "client-123",
                "status": "accepted",
            },
        )
    )
    submitter = AlpacaBrokerSubmitter(config=make_config(), session=session)

    submission = submitter.submit(request=make_request())

    assert submission.outcome is BrokerSubmissionOutcome.ACCEPTED
    assert submission.retry_disposition is BrokerRetryDisposition.DO_NOT_RETRY
    assert submission.broker_order_id == "alpaca-order-1"
    assert session.post_calls[0]["url"] == "https://paper-api.alpaca.markets/v2/orders"
    assert session.post_calls[0]["json"] == {
        "symbol": "BTCUSD",
        "qty": "1.25",
        "side": "buy",
        "type": "limit",
        "time_in_force": "day",
        "client_order_id": "client-123",
        "limit_price": "60250.5",
    }
    assert session.post_calls[0]["headers"]["APCA-API-KEY-ID"] == "key-id"
    assert session.post_calls[0]["headers"]["APCA-API-SECRET-KEY"] == "secret-key"



def test_alpaca_submitter_classifies_terminal_http_rejection() -> None:
    session = StubSession(
        post_result=StubResponse(
            status_code=422,
            body={"message": "insufficient buying power"},
        )
    )
    submitter = AlpacaBrokerSubmitter(config=make_config(), session=session)

    submission = submitter.submit(request=make_request())

    assert submission.outcome is BrokerSubmissionOutcome.REJECTED
    assert submission.retry_disposition is BrokerRetryDisposition.DO_NOT_RETRY
    assert "HTTP 422" in submission.message
    assert "insufficient buying power" in submission.message



def test_alpaca_submitter_marks_transport_failure_retryable_when_lookup_finds_no_order() -> None:
    session = StubSession(
        post_result=requests.ConnectionError("network dropped"),
        get_results=[StubResponse(status_code=404, body={"message": "not found"})],
    )
    submitter = AlpacaBrokerSubmitter(config=make_config(), session=session)

    submission = submitter.submit(request=make_request())

    assert submission.outcome is BrokerSubmissionOutcome.FAILED
    assert submission.retry_disposition is BrokerRetryDisposition.SAFE_TO_RETRY
    assert submission.broker_order_id is None
    assert "transport failed before broker receipt was confirmed" in submission.message
    assert session.get_calls[0]["url"].endswith("client_order_id=client-123")



def test_alpaca_submitter_recovers_accepted_order_via_client_order_lookup() -> None:
    session = StubSession(
        post_result=StubResponse(status_code=503, body={"message": "upstream unavailable"}),
        get_results=[
            StubResponse(
                status_code=200,
                body={
                    "id": "alpaca-order-2",
                    "client_order_id": "client-123",
                    "status": "new",
                },
            )
        ],
    )
    submitter = AlpacaBrokerSubmitter(config=make_config(), session=session)

    submission = submitter.submit(request=make_request())

    assert submission.outcome is BrokerSubmissionOutcome.ACCEPTED
    assert submission.retry_disposition is BrokerRetryDisposition.DO_NOT_RETRY
    assert submission.broker_order_id == "alpaca-order-2"
    assert submission.message == "broker accepted order with status new"
