# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Canonical-store layout and build naming.

A window's build is a single file under its dataset's start-day partition:
``<dataset>/ib_year=YYYY/ib_month=MM/ib_day=DD/<window start stamp>_<span>
-v<version>-<record-set hash>``. The day partition fans a dataset's builds
across many small directories rather than one flat prefix that an S3 lister
short-pages, and spells the day as hive ``key=value`` segments: an engine
pointed at the store can opt into hive parsing and prune on the keys, which
live in the platform's reserved ``ib_`` namespace so no declared column can
collide with them. A reader that does not opt in sees inert path segments.
The stamp and span name the window (matching its ``_manifests/`` entry), the
version orders that window's builds, and the hash ties the file to its
manifest entry and its footer's ``ionbeam.build``. Aggregation spans divide
one day and windows are epoch-aligned (see
:class:`~ionbeam.datasets.DatasetProductionConfig`), so every window nests
inside its partition: the rows under an ``ib_day`` are exactly that day's
rows. Readers also prune by the window interval in the name or by the time
column's parquet statistics; predicates on the declared time column never
prune against the partition keys.

A rebuild writes the next version's file beside the current one and never
touches an existing file. A window's current build is its highest version.
Once the current build is older than :data:`SUPERSEDED_GRACE` — its write is
complete and any read that resolved an earlier build has finished — every
version but the current and the one before it is deleted by the sweep."""

import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from isodate import duration_isoformat

from .provenance import Window, WindowBuildState
from .storage.arrow_store import ArrowStore, StoredObject

# longest a build write or an in-flight read of a superseded build can last
SUPERSEDED_GRACE = timedelta(minutes=30)

_BUILD_FILE = re.compile(
    r"^(?P<dataset>[^/]+)/ib_year=\d{4}/ib_month=\d{2}/ib_day=\d{2}/"
    r"(?P<window>\d{8}T\d{6}_[^-/]+)-v(?P<version>\d+)-[0-9a-f]+$"
)

_DAY_PARTITION = "ib_year=%Y/ib_month=%m/ib_day=%d"


def window_name(window: Window) -> str:
    """``<start stamp>_<span>`` — the window's identity within its dataset."""
    return (
        f"{window.start.strftime('%Y%m%dT%H%M%S')}"
        f"_{duration_isoformat(window.aggregation)}"
    )


def manifest_key(window: Window) -> str:
    """The window's build-history document, under an underscore prefix so
    standard dataset discovery skips it."""
    return f"{window.dataset}/_manifests/{window_name(window)}.json"


def build_file_key(window: Window, version: int, record_ids_hash: str) -> str:
    day = window.start.strftime(_DAY_PARTITION)
    return (
        f"{window.dataset}/{day}/{window_name(window)}"
        f"-v{version}-{record_ids_hash[:8]}"
    )


def parse_build(key: str) -> Optional[tuple[tuple[str, str], int]]:
    """A stored key as ((dataset, window name), version), or None for a key
    outside the layout — such keys are never pruned."""
    match = _BUILD_FILE.match(key)
    if match is None:
        return None
    return (match["dataset"], match["window"]), int(match["version"])


def next_version(
    state: Optional[WindowBuildState], stored: Iterable[StoredObject]
) -> int:
    """One past the highest version known to either the coordination state or
    the store — the store heals a wiped state, the state heals a wiped store."""
    versions = [
        parsed[1] for obj in stored if (parsed := parse_build(obj.key)) is not None
    ]
    return max([state.version if state else 0, *versions]) + 1


def _day_prefixes(dataset: str, start: datetime, end: datetime) -> list[str]:
    """The day-partition prefixes a window starting in ``[start, end)`` can
    live under. A window's day is its start day, so the range of start days is
    exactly ``[start.date, end.date]`` inclusive — listed one day at a time
    because an object store short-pages a listing of the whole dataset
    directory once it holds enough builds."""
    day = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    last = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)
    prefixes = []
    while day <= last:
        prefixes.append(f"{dataset}/{day.strftime(_DAY_PARTITION)}")
        day += timedelta(days=1)
    return prefixes


async def _list_days(
    store: ArrowStore, dataset: str, start: datetime, end: datetime
) -> list[StoredObject]:
    objects: list[StoredObject] = []
    for prefix in _day_prefixes(dataset, start, end):
        objects += await store.list_keys(prefix)
    return objects


async def stored_builds(store: ArrowStore, window: Window) -> list[StoredObject]:
    """Every stored build file of the window."""
    identity = (window.dataset, window_name(window))
    return [
        obj
        for obj in await _list_days(store, window.dataset, window.start, window.end)
        if (parsed := parse_build(obj.key)) is not None and parsed[0] == identity
    ]


def _window_start(name: str) -> datetime:
    return datetime.strptime(name.split("_", 1)[0], "%Y%m%dT%H%M%S").replace(
        tzinfo=timezone.utc
    )


async def current_build_keys(
    store: ArrowStore, dataset: str, start: datetime, end: datetime
) -> list[str]:
    """Current build of every window of ``dataset`` starting in ``[start, end)``."""
    current: dict[str, tuple[int, str]] = {}
    for obj in await _list_days(store, dataset, start, end):
        if (parsed := parse_build(obj.key)) is None:
            continue
        (_, name), version = parsed
        if start <= _window_start(name) < end and version > current.get(name, (0, ""))[0]:
            current[name] = (version, obj.key)
    return [key for _, (_, key) in sorted(current.items())]


def superseded(stored: Iterable[StoredObject], now: datetime) -> list[str]:
    """Keys of build files below their window's current and previous versions,
    for windows whose current build is older than the grace period — a write
    still in progress, being younger, protects the versions beneath it. The
    previous version is kept as a one-deep rollback margin."""
    windows: dict[tuple[str, str], dict[int, list[StoredObject]]] = {}
    for obj in stored:
        parsed = parse_build(obj.key)
        if parsed is None:
            continue
        window, version = parsed
        windows.setdefault(window, {}).setdefault(version, []).append(obj)
    cutoff = now - SUPERSEDED_GRACE
    doomed = []
    for versions in windows.values():
        kept = sorted(versions)[-2:]  # current + previous
        current = kept[-1]
        if max(obj.written_at for obj in versions[current]) <= cutoff:
            doomed += [
                obj.key
                for version, objs in versions.items()
                if version not in kept
                for obj in objs
            ]
    return doomed


async def prune_superseded(store: ArrowStore, prefix: str = "") -> int:
    """Delete every superseded build file under ``prefix`` (the whole store
    when empty); the number deleted."""
    doomed = superseded(await store.list_keys(prefix), datetime.now(timezone.utc))
    for key in doomed:
        await store.delete(key)
    return len(doomed)
