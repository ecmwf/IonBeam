# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Redelivery-filter behavior through the record-store port, run against both
adapters. The Redis adapter needs a live server with the valkey-bloom BF.*
commands (the valkey-bundle image, or Redis with RedisBloom), gated on
IONBEAM_TEST_REDIS_URL; each test flushes a throwaway database, so point it at
a disposable instance only."""

import os
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
import redis.asyncio as redis

from ionbeam.storage.fingerprints import row_fingerprints
from ionbeam.storage.ingestion_record_store import RedisIngestionRecordStore
from ionbeam.storage.memory_coordination import InMemoryRecordStore

NOW = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
WINDOW_START = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
# Redis expires against its own wall clock, so the seal must be genuinely in the
# future — a fixed past date would delete the key the moment it is written.
EXPIRE_AT = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(hours=48)
CAPACITY = 10_000

REDIS_URL = os.getenv("IONBEAM_TEST_REDIS_URL")
requires_redis = pytest.mark.skipif(
    REDIS_URL is None,
    reason="set IONBEAM_TEST_REDIS_URL (e.g. redis://localhost:6379/15) to run",
)


def _table(temperatures: list[float]) -> pa.Table:
    n = len(temperatures)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return pa.table(
        {
            "time": pa.array(
                [base + timedelta(minutes=i) for i in range(n)],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "station_id": [f"station_{i}" for i in range(n)],
            "temperature": pa.array(temperatures, type=pa.float64()),
        }
    )


class TestRowFingerprints:
    def test_deterministic(self):
        table = _table([20.0, 21.5, -3.25])
        first = row_fingerprints(table)
        second = row_fingerprints(table)

        assert len(first) == 3
        assert all(len(fp) == 16 for fp in first)
        assert first == second

    def test_changed_value_changes_only_that_rows_fingerprint(self):
        original = row_fingerprints(_table([20.0, 21.5, -3.25]))
        updated = row_fingerprints(_table([20.0, 99.9, -3.25]))

        assert original[0] == updated[0]
        assert original[2] == updated[2]
        assert original[1] != updated[1]


@pytest.fixture(params=["memory", pytest.param("redis", marks=requires_redis)])
async def store(request):
    if request.param == "memory":
        yield InMemoryRecordStore()
        return
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    yield RedisIngestionRecordStore(client)
    await client.aclose()


async def _filter_unseen(store, dataset, window_start, fingerprints, now=NOW):
    return await store.filter_unseen(
        dataset, window_start, fingerprints, CAPACITY, EXPIRE_AT, now
    )


class TestFilterUnseen:
    async def test_first_delivery_is_new_and_identical_redelivery_is_seen(self, store):
        fingerprints = row_fingerprints(_table([20.0, 21.5, -3.25]))

        first = await _filter_unseen(store, "ds", WINDOW_START, fingerprints)
        second = await _filter_unseen(store, "ds", WINDOW_START, fingerprints)

        assert first.all()
        assert not second.any()

    async def test_changed_row_is_new_while_the_rest_stay_seen(self, store):
        await _filter_unseen(
            store, "ds", WINDOW_START, row_fingerprints(_table([20.0, 21.5, -3.25]))
        )

        qc_updated = row_fingerprints(_table([20.0, 99.9, -3.25]))
        mask = await _filter_unseen(store, "ds", WINDOW_START, qc_updated)

        assert mask.tolist() == [False, True, False]

    async def test_duplicate_within_one_batch_counts_once(self, store):
        """BF.INSERT checks-and-adds item by item, so the second copy of a row in
        the same batch already reads as seen — both adapters must agree."""
        fingerprint = row_fingerprints(_table([20.0]))[0]

        mask = await _filter_unseen(
            store, "ds", WINDOW_START, [fingerprint, fingerprint]
        )

        assert mask.tolist() == [True, False]

    async def test_windows_do_not_share_fingerprints(self, store):
        fingerprints = row_fingerprints(_table([20.0]))
        await _filter_unseen(store, "ds", WINDOW_START, fingerprints)

        next_window = WINDOW_START + 3600
        assert (await _filter_unseen(store, "ds", next_window, fingerprints)).all()

    async def test_datasets_do_not_share_fingerprints(self, store):
        fingerprints = row_fingerprints(_table([20.0]))
        await _filter_unseen(store, "ds_a", WINDOW_START, fingerprints)

        assert (await _filter_unseen(store, "ds_b", WINDOW_START, fingerprints)).all()


async def test_sealed_window_is_forgotten_in_memory():
    """Past its expiry a window's filter is dropped — mirroring the Redis key TTL —
    so long-lived processes do not accumulate one exact set per window forever."""
    store = InMemoryRecordStore()
    fingerprints = row_fingerprints(_table([20.0]))
    await _filter_unseen(store, "ds", WINDOW_START, fingerprints)

    after_seal = EXPIRE_AT + timedelta(seconds=1)
    assert (
        await _filter_unseen(store, "ds", WINDOW_START, fingerprints, now=after_seal)
    ).all()


@requires_redis
async def test_window_key_expires_at_the_seal():
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    try:
        redis_store = RedisIngestionRecordStore(client)
        fingerprints = row_fingerprints(_table([20.0]))
        await redis_store.filter_unseen(
            "ds", WINDOW_START, fingerprints, CAPACITY, EXPIRE_AT, NOW
        )

        expire_time = await client.expiretime(f"dedup:ds:{WINDOW_START}")
        assert expire_time == int(EXPIRE_AT.timestamp())
    finally:
        await client.aclose()


@requires_redis
async def test_batches_larger_than_one_insert_command_dedup_correctly():
    """A batch bigger than the per-command chunk spans several BF.INSERTs against
    the same filter; novelty must be judged across the whole batch, and a full
    redelivery of it must read entirely seen."""
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    try:
        redis_store = RedisIngestionRecordStore(client)
        fingerprints = row_fingerprints(_table([float(i) for i in range(25_000)]))

        first = await redis_store.filter_unseen(
            "ds", WINDOW_START, fingerprints, 50_000, EXPIRE_AT, NOW
        )
        again = await redis_store.filter_unseen(
            "ds", WINDOW_START, fingerprints, 50_000, EXPIRE_AT, NOW
        )

        # the filter is created with a 1% error target, so a handful of false
        # positives among 25k novel rows is designed-in, not a dedup failure
        assert first.sum() >= len(fingerprints) * 0.99
        assert not again.any()
    finally:
        await client.aclose()
