# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Stored-content dedup behavior through the record-store port, run against
both adapters. The Redis adapter is gated on IONBEAM_TEST_REDIS_URL; each test
flushes a throwaway database, so point it at a disposable instance only."""

import os
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
import redis.asyncio as redis

from ionbeam.storage.fingerprints import row_fingerprints
from ionbeam.storage.coordination_store import RedisCoordinationStore
from ionbeam.storage.memory_coordination import InMemoryCoordinationStore

WINDOW_START = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
# Redis expires against its own wall clock, so the seal must be genuinely in the
# future — a fixed past date would delete the key the moment it is written.
EXPIRE_AT = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(hours=48)

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
        yield InMemoryCoordinationStore()
        return
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    yield RedisCoordinationStore(client)
    await client.aclose()


class TestStoredContent:
    async def test_marked_content_reads_as_stored(self, store):
        fingerprints = row_fingerprints(_table([20.0, 21.5, -3.25]))

        before = await store.stored_content("ds", WINDOW_START, fingerprints)
        await store.mark_content_stored("ds", WINDOW_START, fingerprints, EXPIRE_AT)
        after = await store.stored_content("ds", WINDOW_START, fingerprints)

        assert not before.any()
        assert after.all()

    async def test_changed_row_is_novel_while_the_rest_stay_stored(self, store):
        await store.mark_content_stored(
            "ds", WINDOW_START, row_fingerprints(_table([20.0, 21.5, -3.25])), EXPIRE_AT
        )

        qc_updated = row_fingerprints(_table([20.0, 99.9, -3.25]))
        mask = await store.stored_content("ds", WINDOW_START, qc_updated)

        assert mask.tolist() == [True, False, True]

    async def test_windows_and_datasets_do_not_share_content(self, store):
        fingerprints = row_fingerprints(_table([20.0]))
        await store.mark_content_stored("ds", WINDOW_START, fingerprints, EXPIRE_AT)

        next_window = await store.stored_content("ds", WINDOW_START + 3600, fingerprints)
        other_dataset = await store.stored_content("other", WINDOW_START, fingerprints)
        assert not next_window.any()
        assert not other_dataset.any()


@requires_redis
async def test_stored_content_key_expires_at_the_seal():
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    try:
        redis_store = RedisCoordinationStore(client)
        fingerprints = row_fingerprints(_table([20.0]))
        await redis_store.mark_content_stored("ds", WINDOW_START, fingerprints, EXPIRE_AT)

        expire_time = await client.expiretime(f"ionbeam:delta_stored:ds:{WINDOW_START}")
        assert expire_time == int(EXPIRE_AT.timestamp())
    finally:
        await client.aclose()
