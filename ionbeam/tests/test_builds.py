# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Build artifact layout on the store: key naming, version succession, discovery
by day, and superseded-build retention."""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from ionbeam.builds import (
    SUPERSEDED_GRACE,
    build_file_key,
    current_build_keys,
    manifest_key,
    next_version,
    parse_build,
    stored_builds,
    superseded,
)
from ionbeam.provenance import Window, WindowBuildState
from ionbeam.storage.arrow_store import LocalFileSystemStore, StoredObject

WINDOW = Window("acronet", datetime(2024, 1, 1, 10, tzinfo=timezone.utc), timedelta(hours=1))
DAY = Window("acronet", datetime(2024, 1, 1, tzinfo=timezone.utc), timedelta(days=1))
NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)
OLD = NOW - SUPERSEDED_GRACE
HASH = "3f9c2a1bdeadbeef"


def _stored(
    window: Window,
    version: int,
    record_hash: str = HASH,
    written_at: datetime = OLD,
) -> list[StoredObject]:
    return [StoredObject(build_file_key(window, version, record_hash), written_at)]


def test_a_build_is_one_file_under_its_windows_day():
    """Every build sits under its start day's hive partition, bounding a
    directory to one day's builds, and parses back to the window that wrote it.
    Manifests live outside the partitions and are not builds."""
    assert build_file_key(WINDOW, 3, HASH) == (
        "acronet/ib_year=2024/ib_month=01/ib_day=01/20240101T100000_PT1H-v3-3f9c2a1b"
    )
    assert build_file_key(DAY, 1, HASH) == (
        "acronet/ib_year=2024/ib_month=01/ib_day=01/20240101T000000_P1D-v1-3f9c2a1b"
    )
    assert manifest_key(WINDOW) == "acronet/_manifests/20240101T100000_PT1H.json"

    assert parse_build(build_file_key(DAY, 7, HASH)) == (
        ("acronet", "20240101T000000_P1D"),
        7,
    )
    assert parse_build(manifest_key(DAY)) is None
    assert parse_build("acronet/oddball") is None


def test_next_version_advances_past_both_state_and_store():
    state = WindowBuildState(record_ids_hash="h", version=4, timestamp=NOW)

    assert next_version(None, []) == 1
    assert next_version(state, []) == 5  # store wiped: state carries the count
    assert next_version(None, _stored(WINDOW, 6)) == 7  # state wiped: store carries it
    assert next_version(state, _stored(WINDOW, 6)) == 7


def test_superseded_keeps_the_current_build_and_one_rollback_margin():
    other = Window("acronet", datetime(2024, 1, 2, tzinfo=timezone.utc), timedelta(days=1))
    stored = (
        _stored(DAY, 1, "aaaa1111")
        + _stored(DAY, 2, "bbbb2222")
        + _stored(DAY, 3, "cccc3333")
        + _stored(other, 1)  # sole build of its window: never doomed
        + [StoredObject("acronet/oddball", OLD)]  # outside the layout: never touched
    )

    assert sorted(superseded(stored, NOW)) == sorted(
        obj.key for obj in _stored(DAY, 1, "aaaa1111")
    )

    # a window that never got past its rollback margin keeps everything
    assert superseded(_stored(DAY, 1, "aaaa1111") + _stored(DAY, 2, "bbbb2222"), NOW) == []


def test_a_current_build_younger_than_the_grace_protects_its_predecessors():
    stored = _stored(DAY, 1) + _stored(DAY, 2, "bbbb2222", written_at=NOW)
    assert superseded(stored, NOW) == []


def test_windows_sharing_a_dataset_stay_distinct():
    first = Window("acronet", datetime(2024, 1, 1, tzinfo=timezone.utc), timedelta(minutes=10))
    second = Window(
        "acronet", datetime(2024, 1, 1, 0, 10, tzinfo=timezone.utc), timedelta(minutes=10)
    )
    # a rebuild of one window leaves its neighbour's sole build untouched,
    # including DAY's, which shares first's start stamp under a different span
    stored = (
        _stored(first, 1) + _stored(first, 2, "bbbb2222") + _stored(first, 3, "cccc3333")
        + _stored(second, 1)
        + _stored(DAY, 1)
    )
    assert superseded(stored, NOW) == [obj.key for obj in _stored(first, 1)]
    assert next_version(None, _stored(second, 1)) == 2


async def _write(store, key: str) -> None:
    async def batches():
        yield pa.record_batch({"x": pa.array([1])})
    await store.write_record_batches(key, batches(), schema=pa.schema([("x", pa.int64())]))


@pytest.mark.asyncio
async def test_build_discovery_finds_each_windows_builds_by_day(tmp_path):
    # Discovery lists each window's day directory by exact prefix. A build is
    # found regardless of how many other days' builds crowd the dataset (an
    # object store short-pages a flat listing).
    store = LocalFileSystemStore(tmp_path)
    w0 = Window("acronet", datetime(2024, 1, 1, 23, tzinfo=timezone.utc), timedelta(hours=1))
    w1 = Window("acronet", datetime(2024, 1, 2, 0, tzinfo=timezone.utc), timedelta(hours=1))
    await _write(store, build_file_key(w0, 1, HASH))
    await _write(store, build_file_key(w1, 1, HASH))
    await _write(store, build_file_key(w1, 2, HASH))  # rebuild: v2 current

    keys = await current_build_keys(
        store, "acronet", w0.start, w1.start + timedelta(hours=1)
    )
    assert keys == [build_file_key(w0, 1, HASH), build_file_key(w1, 2, HASH)]

    assert {o.key for o in await stored_builds(store, w1)} == {
        build_file_key(w1, 1, HASH),
        build_file_key(w1, 2, HASH),
    }
