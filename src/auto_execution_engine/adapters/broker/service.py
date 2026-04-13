from dataclasses import dataclass, field
from typing import Protocol

from auto_execution_engine.adapters.broker.models import (
    BrokerOrderAck,
    BrokerOrderActivityPage,
    BrokerOrderRequest,
    BrokerOrderSide,
    BrokerOrderType,
    BrokerRetryDisposition,
    BrokerSubmissionOutcome,
    RawBrokerOrderSnapshot,
)
from auto_execution_engine.domain.orders.models import OrderAggregate, OrderSide, OrderType


class DuplicateSubmissionError(ValueError):
    """Raised when an already-submitted client order is submitted again."""


class BrokerSubmissionError(ValueError):
    """Raised when a broker submission does not end in an accepted order."""

    def __init__(self, message: str, *, submission: "RegisteredSubmission | None" = None) -> None:
        super().__init__(message)
        self.submission = submission


class RetryableBrokerSubmissionError(BrokerSubmissionError):
    """Raised when the broker failure is explicitly safe to retry."""


class TerminalBrokerSubmissionError(BrokerSubmissionError):
    """Raised when the broker terminally rejects the order."""


class UnknownBrokerSubmissionError(BrokerSubmissionError):
    """Raised when the broker outcome cannot be determined safely."""


@dataclass(frozen=True, init=False)
class RegisteredSubmission:
    account_id: str
    client_order_id: str
    broker_order_id: str | None
    outcome: BrokerSubmissionOutcome
    retry_disposition: BrokerRetryDisposition
    message: str

    def __init__(
        self,
        *,
        account_id: str,
        client_order_id: str,
        broker_order_id: str | None,
        message: str,
        outcome: BrokerSubmissionOutcome = BrokerSubmissionOutcome.ACCEPTED,
        retry_disposition: BrokerRetryDisposition = BrokerRetryDisposition.DO_NOT_RETRY,
        accepted: bool | None = None,
    ) -> None:
        if accepted is not None and outcome is BrokerSubmissionOutcome.ACCEPTED:
            outcome = (
                BrokerSubmissionOutcome.ACCEPTED
                if accepted
                else BrokerSubmissionOutcome.UNKNOWN
            )
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "client_order_id", client_order_id)
        object.__setattr__(self, "broker_order_id", broker_order_id)
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "retry_disposition", retry_disposition)
        object.__setattr__(self, "message", message)

    @property
    def accepted(self) -> bool:
        return self.outcome is BrokerSubmissionOutcome.ACCEPTED

    def to_ack(self) -> BrokerOrderAck:
        return BrokerOrderAck(
            account_id=self.account_id,
            client_order_id=self.client_order_id,
            broker_order_id=self.broker_order_id,
            accepted=self.accepted,
            outcome=self.outcome,
            retry_disposition=self.retry_disposition,
            message=self.message,
        )


class SubmissionBook(Protocol):
    def record_submission(
        self, *, request: BrokerOrderRequest, submission: RegisteredSubmission
    ) -> None: ...

    def load_submission(self, *, client_order_id: str) -> RegisteredSubmission | None: ...


@dataclass
class IdempotentSubmissionBook:
    submissions_by_client_order_id: dict[str, RegisteredSubmission] = field(
        default_factory=dict
    )

    @property
    def submitted_client_order_ids(self) -> set[str]:
        return set(self.submissions_by_client_order_id)

    def record_submission(
        self, *, request: BrokerOrderRequest, submission: RegisteredSubmission
    ) -> None:
        if request.client_order_id in self.submissions_by_client_order_id:
            raise DuplicateSubmissionError(
                f"client order {request.client_order_id} has already been submitted"
            )
        self.submissions_by_client_order_id[request.client_order_id] = submission

    def mark_submitted(
        self, *, request: BrokerOrderRequest, submission: RegisteredSubmission
    ) -> None:
        self.record_submission(request=request, submission=submission)

    def load_submission(self, *, client_order_id: str) -> RegisteredSubmission | None:
        return self.submissions_by_client_order_id.get(client_order_id)


class BrokerSubmitter(Protocol):
    def submit(self, *, request: BrokerOrderRequest) -> RegisteredSubmission: ...


class BrokerStateReader(Protocol):
    def list_order_snapshots(self, *, account_id: str) -> tuple[RawBrokerOrderSnapshot, ...]: ...

    def list_order_activities(
        self,
        *,
        account_id: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> BrokerOrderActivityPage: ...

    def get_order_by_client_order_id(
        self, *, account_id: str, client_order_id: str
    ) -> RawBrokerOrderSnapshot | None: ...


@dataclass(frozen=True)
class SyntheticBrokerSubmitter:
    def submit(self, *, request: BrokerOrderRequest) -> RegisteredSubmission:
        return RegisteredSubmission(
            account_id=request.account_id,
            client_order_id=request.client_order_id,
            broker_order_id=f"pending::{request.client_order_id}",
            outcome=BrokerSubmissionOutcome.ACCEPTED,
            retry_disposition=BrokerRetryDisposition.DO_NOT_RETRY,
            message="submission registered",
        )


class BrokerOrderRequestBuilder:
    def build(self, *, order: OrderAggregate) -> BrokerOrderRequest:
        side = {
            OrderSide.BUY: BrokerOrderSide.BUY,
            OrderSide.SELL: BrokerOrderSide.SELL,
        }[order.side]
        order_type = {
            OrderType.MARKET: BrokerOrderType.MARKET,
            OrderType.LIMIT: BrokerOrderType.LIMIT,
        }[order.order_type]
        return BrokerOrderRequest(
            account_id=order.account_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=side,
            quantity=order.quantity,
            order_type=order_type,
        )


class BrokerSubmissionService:
    def __init__(
        self,
        *,
        submission_book: SubmissionBook,
        submitter: BrokerSubmitter | None = None,
    ) -> None:
        self._submission_book = submission_book
        self._submitter = submitter or SyntheticBrokerSubmitter()

    def register_submission(self, *, request: BrokerOrderRequest) -> BrokerOrderAck:
        submission = self._submitter.submit(request=request)
        self._validate_submission(request=request, submission=submission)

        if submission.outcome is BrokerSubmissionOutcome.FAILED:
            if submission.retry_disposition is BrokerRetryDisposition.SAFE_TO_RETRY:
                raise RetryableBrokerSubmissionError(submission.message)
            raise UnknownBrokerSubmissionError(submission.message, submission=submission)

        self._submission_book.record_submission(request=request, submission=submission)

        if submission.outcome is BrokerSubmissionOutcome.ACCEPTED:
            return submission.to_ack()
        if submission.outcome is BrokerSubmissionOutcome.REJECTED:
            raise TerminalBrokerSubmissionError(submission.message, submission=submission)
        raise UnknownBrokerSubmissionError(submission.message, submission=submission)

    def load_submission(self, *, client_order_id: str) -> BrokerOrderAck | None:
        submission = self._submission_book.load_submission(
            client_order_id=client_order_id
        )
        if submission is None:
            return None
        return submission.to_ack()

    def _validate_submission(
        self, *, request: BrokerOrderRequest, submission: RegisteredSubmission
    ) -> None:
        if submission.account_id != request.account_id:
            raise ValueError("broker submission account does not match request account")
        if submission.client_order_id != request.client_order_id:
            raise ValueError(
                "broker submission client order id does not match request client order id"
            )
        if (
            submission.outcome is BrokerSubmissionOutcome.ACCEPTED
            and submission.broker_order_id is None
        ):
            raise ValueError("accepted broker submission must include a broker order id")
