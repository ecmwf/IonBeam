# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

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
    assert build_file_key(WINDOW, 3, HASH) == (
        "acronet/ib_year=2024/ib_month=01/ib_day=01/20240101T100000_PT1H-v3-3f9c2a1b"
    )
    assert build_file_key(DAY, 1, HASH) == (
        "acronet/ib_year=2024/ib_month=01/ib_day=01/20240101T000000_P1D-v1-3f9c2a1b"
    )
    assert manifest_key(WINDOW) == "acronet/_manifests/20240101T100000_PT1H.json"


def test_parse_build_round_trips_and_rejects_foreign_keys():
    key = build_file_key(DAY, 7, HASH)
    assert parse_build(key) == (("acronet", "20240101T000000_P1D"), 7)
    assert parse_build("acronet/_manifests/20240101T000000_P1D.json") is None
    assert parse_build("acronet/oddball") is None


def test_next_version_advances_past_both_state_and_store():
    state = WindowBuildState(record_ids_hash="h", version=4, timestamp=NOW)

    assert next_version(None, []) == 1
    assert next_version(state, []) == 5  # store wiped: state carries the count
    assert next_version(None, _stored(WINDOW, 6)) == 7  # state wiped: store carries it
    assert next_version(state, _stored(WINDOW, 6)) == 7


def test_superseded_keeps_current_and_previous_once_the_current_settles():
    other = Window("acronet", datetime(2024, 1, 2, tzinfo=timezone.utc), timedelta(days=1))
    stored = (
        _stored(DAY, 1, "aaaa1111")
        + _stored(DAY, 2, "bbbb2222")
        + _stored(DAY, 3, "cccc3333")
        + _stored(other, 1)  # sole build of its window: never doomed
    )

    doomed = superseded(stored, NOW)

    # v3 current, v2 the rollback margin; only v1 is reaped
    assert sorted(doomed) == sorted(
        obj.key for obj in _stored(DAY, 1, "aaaa1111")
    )


def test_superseded_keeps_a_windows_only_two_versions():
    stored = _stored(DAY, 1, "aaaa1111") + _stored(DAY, 2, "bbbb2222")
    assert superseded(stored, NOW) == []


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


def test_superseded_never_touches_keys_outside_the_layout():
    stored = [StoredObject("acronet/oddball", OLD)] + _stored(WINDOW, 1)
    assert superseded(stored, NOW) == []


async def _write(store, key: str) -> None:
    async def batches():
        yield pa.record_batch({"x": pa.array([1])})
    await store.write_record_batches(key, batches(), schema=pa.schema([("x", pa.int64())]))


@pytest.mark.asyncio
async def test_current_build_keys_and_stored_builds_find_builds_by_day(tmp_path):
    # Discovery lists each window's day directory by exact prefix. A build is
    # found regardless of how many other days' builds crowd the dataset (an
    # object store short-pages a flat listing).
    store = LocalFileSystemStore(tmp_path)
    w0 = Window("acronet", datetime(2024, 1, 1, 23, tzinfo=timezone.utc), timedelta(hours=1))
    w1 = Window("acronet", datetime(2024, 1, 2, 0, tzinfo=timezone.utc), timedelta(hours=1))
    await _write(store, build_file_key(w0, 1, HASH))
    await _write(store, build_file_key(w1, 1, HASH))
    await _write(store, build_file_key(w1, 2, HASH))  # rebuild: v2 current

    # a range spanning both days finds the current build of each window
    keys = await current_build_keys(
        store, "acronet", w0.start, w1.start + timedelta(hours=1)
    )
    assert keys == [build_file_key(w0, 1, HASH), build_file_key(w1, 2, HASH)]

    # stored_builds sees a window's full history from its own day
    assert {o.key for o in await stored_builds(store, w1)} == {
        build_file_key(w1, 1, HASH),
        build_file_key(w1, 2, HASH),
    }


def test_builds_fan_out_across_day_partitions():
    # Every build sits under its start day's hive partition: a dataset's
    # objects spread across day directories, bounding directory size to a
    # day's builds. The partition keys live in the reserved ib_ namespace,
    # separate from declared columns.
    import re
    from collections import Counter

    keys = [
        build_file_key(
            Window("acronet", datetime(2024, 1, day, hour, tzinfo=timezone.utc),
                   timedelta(hours=1)),
            version, HASH,
        )
        for day in range(1, 8)
        for hour in range(24)
        for version in (1, 2, 3)
    ]
    directories = Counter(key.rsplit("/", 1)[0] for key in keys)
    assert all(
        re.fullmatch(r"[^/]+/ib_year=\d{4}/ib_month=\d{2}/ib_day=\d{2}", d)
        for d in directories
    )
    assert max(directories.values()) <= 24 * 3
