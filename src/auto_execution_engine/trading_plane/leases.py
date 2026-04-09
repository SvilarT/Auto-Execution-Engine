from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta


class LeaseError(ValueError):
    """Raised when account lease ownership is invalid or unsafe."""


@dataclass(frozen=True)
class AccountLease:
    account_id: str
    owner_id: str
    acquired_at: datetime
    expires_at: datetime

    def is_active(self, *, now: datetime) -> bool:
        return now < self.expires_at

    def assert_owned_by(self, *, owner_id: str, now: datetime) -> None:
        if not self.is_active(now=now):
            raise LeaseError("lease is expired")
        if self.owner_id != owner_id:
            raise LeaseError(
                f"lease is owned by {self.owner_id}, not {owner_id}"
            )

    def renew(self, *, owner_id: str, now: datetime, ttl_seconds: int) -> "AccountLease":
        self.assert_owned_by(owner_id=owner_id, now=now)
        return replace(self, expires_at=now + timedelta(seconds=ttl_seconds))


class AccountLeaseService:
    """Simple in-memory lease authority for account worker ownership."""

    def acquire(
        self,
        *,
        existing_lease: AccountLease | None,
        account_id: str,
        owner_id: str,
        now: datetime | None = None,
        ttl_seconds: int = 30,
    ) -> AccountLease:
        current_time = now or datetime.now(UTC)

        if existing_lease is not None and existing_lease.is_active(now=current_time):
            if existing_lease.owner_id != owner_id:
                raise LeaseError(
                    f"account {account_id} is already controlled by {existing_lease.owner_id}"
                )
            return existing_lease.renew(
                owner_id=owner_id,
                now=current_time,
                ttl_seconds=ttl_seconds,
            )

        return AccountLease(
            account_id=account_id,
            owner_id=owner_id,
            acquired_at=current_time,
            expires_at=current_time + timedelta(seconds=ttl_seconds),
        )
