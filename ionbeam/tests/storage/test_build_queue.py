# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Behavior of the Redis-backed build queue against a live Redis-protocol server
(Valkey or Redis). Gated on IONBEAM_TEST_REDIS_URL; each test flushes a throwaway
database, so point it at a disposable instance only."""

import os
from datetime import datetime, timedelta, timezone

import pytest
import redis.asyncio as redis

from ionbeam.provenance import Window
from ionbeam.storage.build_queue import RedisBuildQueue

REDIS_URL = os.getenv("IONBEAM_TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(
    REDIS_URL is None,
    reason="set IONBEAM_TEST_REDIS_URL (e.g. redis://localhost:6379/15) to run",
)

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
SPAN = timedelta(hours=1)


def _window(dataset: str, hour: int) -> Window:
    return Window(dataset, BASE + timedelta(hours=hour), SPAN)


def _past(hours: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


@pytest.fixture
async def client():
    c = redis.from_url(REDIS_URL)
    await c.flushdb()
    yield c
    await c.aclose()


async def test_claim_returns_the_earliest_eligible_window_first(client):
    queue = RedisBuildQueue(client, queue_key="q")
    later, earlier = _window("d", 0), _window("d", 1)
    await queue.schedule(later, eligible_at=_past(1))
    await queue.schedule(earlier, eligible_at=_past(5))

    first = await queue.claim_due()
    assert first.dataset_key == earlier.dataset_key


async def test_window_not_yet_eligible_is_not_claimable(client):
    queue = RedisBuildQueue(client, queue_key="q")
    window = _window("d", 0)
    await queue.schedule(window, eligible_at=datetime.now(timezone.utc) + timedelta(hours=1))

    assert await queue.claim_due() is None


async def test_completed_window_is_not_reclaimed(client):
    # lease_ttl=0 makes every lease immediately eligible for reclaim; this
    # isolates complete() as what stops redelivery.
    queue = RedisBuildQueue(client, queue_key="q", lease_ttl=0.0)
    window = _window("d", 0)
    await queue.schedule(window, eligible_at=_past(1))

    leased = await queue.claim_due()
    await queue.complete(leased, next_claim_floor=datetime.now(timezone.utc))

    assert await queue.claim_due() is None


async def test_crashed_build_lease_is_reclaimed(client):
    # A build that leases a window but never completes (its pod was killed):
    # the expired lease returns to the queue, due again.
    queue = RedisBuildQueue(client, queue_key="q", lease_ttl=0.0)
    window = _window("d", 0)
    await queue.schedule(window, eligible_at=_past(1))

    first = await queue.claim_due()
    assert first.dataset_key == window.dataset_key
    # never completed — the next claim reclaims the expired lease
    again = await queue.claim_due()
    assert again.dataset_key == window.dataset_key


async def test_reschedule_replaces_the_eligibility_time(client):
    queue = RedisBuildQueue(client, queue_key="q")
    window = _window("d", 0)
    await queue.schedule(window, eligible_at=_past(1))
    await queue.schedule(window, eligible_at=datetime.now(timezone.utc) + timedelta(hours=1))

    assert await queue.claim_due() is None


async def test_reschedule_during_a_lease_waits_for_its_release(client):
    """A record arriving mid-build re-schedules the window; the rebuild queues
    behind the lease (a concurrent second build double-assigns version numbers)
    and survives the lease release."""
    queue = RedisBuildQueue(client, queue_key="q")
    window = _window("d", 0)
    await queue.schedule(window, eligible_at=_past(1))
    leased = await queue.claim_due()
    await queue.schedule(window, eligible_at=_past(1))

    assert await queue.claim_due() is None

    await queue.complete(leased, next_claim_floor=datetime.now(timezone.utc))
    again = await queue.claim_due()
    assert again.dataset_key == window.dataset_key


async def test_completion_floor_spaces_rebuilds_but_never_hastens_them(client):
    """A window re-scheduled mid-build cannot be claimed before the completion
    floor; an eligibility already later than the floor stands."""
    queue = RedisBuildQueue(client, queue_key="q")

    deferred = _window("a", 0)
    await queue.schedule(deferred, eligible_at=_past(1))
    leased = await queue.claim_due()
    await queue.schedule(deferred, eligible_at=_past(1))
    floor = datetime.now(timezone.utc) + timedelta(minutes=10)
    await queue.complete(leased, next_claim_floor=floor)

    later = _window("b", 0)
    await queue.schedule(later, eligible_at=_past(1))
    leased = await queue.claim_due()
    assert leased.dataset_key == later.dataset_key  # "a" is held by its floor
    await queue.schedule(later, eligible_at=datetime.now(timezone.utc) + timedelta(hours=1))
    await queue.complete(leased, next_claim_floor=_past(1))

    assert await queue.claim_due() is None


async def test_claim_skips_a_leased_window_but_takes_the_next_due(client):
    queue = RedisBuildQueue(client, queue_key="q")
    building, waiting = _window("a", 0), _window("b", 0)
    await queue.schedule(building, eligible_at=_past(5))
    await queue.schedule(waiting, eligible_at=_past(1))
    first = await queue.claim_due()
    assert first.dataset_key == building.dataset_key
    await queue.schedule(building, eligible_at=_past(5))

    second = await queue.claim_due()
    assert second.dataset_key == waiting.dataset_key


async def test_requeue_releases_the_lease_and_reschedules(client):
    queue = RedisBuildQueue(client, queue_key="q")
    window = _window("d", 0)
    await queue.schedule(window, eligible_at=_past(1))
    leased = await queue.claim_due()

    await queue.requeue(leased, eligible_at=_past(1))

    again = await queue.claim_due()
    assert again.dataset_key == window.dataset_key
