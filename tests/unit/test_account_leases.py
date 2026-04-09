from datetime import UTC, datetime, timedelta

import pytest

from auto_execution_engine.trading_plane.leases import AccountLeaseService, LeaseError



def test_can_acquire_new_account_lease():
    service = AccountLeaseService()
    now = datetime(2026, 1, 1, tzinfo=UTC)

    lease = service.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=now,
        ttl_seconds=30,
    )

    assert lease.account_id == "acct-1"
    assert lease.owner_id == "worker-a"
    assert lease.acquired_at == now
    assert lease.expires_at == now + timedelta(seconds=30)



def test_same_owner_can_renew_active_lease():
    service = AccountLeaseService()
    start = datetime(2026, 1, 1, tzinfo=UTC)

    lease = service.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=start,
        ttl_seconds=30,
    )

    renewed = service.acquire(
        existing_lease=lease,
        account_id="acct-1",
        owner_id="worker-a",
        now=start + timedelta(seconds=10),
        ttl_seconds=45,
    )

    assert renewed.owner_id == "worker-a"
    assert renewed.expires_at == start + timedelta(seconds=55)



def test_different_owner_is_rejected_while_lease_active():
    service = AccountLeaseService()
    start = datetime(2026, 1, 1, tzinfo=UTC)

    lease = service.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=start,
        ttl_seconds=30,
    )

    with pytest.raises(LeaseError):
        service.acquire(
            existing_lease=lease,
            account_id="acct-1",
            owner_id="worker-b",
            now=start + timedelta(seconds=5),
            ttl_seconds=30,
        )



def test_different_owner_can_acquire_after_expiry():
    service = AccountLeaseService()
    start = datetime(2026, 1, 1, tzinfo=UTC)

    lease = service.acquire(
        existing_lease=None,
        account_id="acct-1",
        owner_id="worker-a",
        now=start,
        ttl_seconds=30,
    )

    successor = service.acquire(
        existing_lease=lease,
        account_id="acct-1",
        owner_id="worker-b",
        now=start + timedelta(seconds=31),
        ttl_seconds=30,
    )

    assert successor.owner_id == "worker-b"
