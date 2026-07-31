# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""The coordinator's windowing decisions: which windows an event schedules for
build, defers, or leaves alone. Single-shot decisions are table-driven
scenarios; the stateful behaviours (build lifecycle, measured-lateness settle,
rebuild debounce, sealing) are individual flows.

The aggregation span and the finaliser thresholds are server-side production
config (see :class:`DatasetRegistry`). The windowing scenarios pin them
through ``_registry``, not the event."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from ionbeam.datasets import DatasetBuildConfig, DatasetRegistry
from ionbeam.handlers.dataset_coordinator import (
    DatasetCoordinatorConfig,
    DatasetCoordinator,
)
from ionbeam.provenance import (
    CoverageClaim,
    RecordSet,
    Window,
    WindowBuildState,
    align_to_aggregation,
)
from ionbeam.storage.lateness_histogram import bucket_for
from ionbeam_client.models import (
    CfSemantics,
    DataAvailableEvent,
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    WindowRecord,
    geographic_point_coordinates,
)

T0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

# Windows at T0 are years old; a far horizon keeps every window provisional,
# isolating the windowing decision under test.
_NEVER_FINAL = timedelta(days=3650)


def at(hours: int, minutes: int = 0) -> datetime:
    return T0 + timedelta(hours=hours, minutes=minutes)


def _metadata() -> IngestionMetadata:
    return IngestionMetadata(
        name="test_dataset",
        dataset_schema=DatasetSchema(
            time=TimeCoordinate(),
            coordinates=geographic_point_coordinates(),
            variables=[
                Variable(
                    name="temperature",
                    semantics=CfSemantics(standard_name="air_temperature"),
                    unit="deg_C",
                )
            ],
            tags=[Tag(name="station_id")],
        ),
    )


def _registry(
    span: timedelta = timedelta(hours=1),
    debounce: timedelta = timedelta(0),
) -> DatasetRegistry:
    return DatasetRegistry(
        {
            "test_dataset": DatasetBuildConfig(
                aggregation_span=span,
                rebuild_debounce=debounce,
            )
        }
    )


def _event(
    start: datetime,
    end: datetime,
    metadata: IngestionMetadata,
    span: timedelta = timedelta(hours=1),
) -> DataAvailableEvent:
    """A coverage claim as ingestion emits it: one record per spanned window,
    since these claims delivered rows across their whole range."""
    records = []
    window_start = align_to_aggregation(start, span)
    while window_start < end:
        records.append(WindowRecord(id=uuid4(), window_start=window_start))
        window_start += span
    return DataAvailableEvent(
        id=uuid4(),
        metadata=metadata,
        start_time=start,
        end_time=end,
        arrived_at=datetime.now(timezone.utc),
        records=records,
    )


def _handler(store, queue, metrics, registry=None, **config) -> DatasetCoordinator:
    config.setdefault("retention", _NEVER_FINAL)
    return DatasetCoordinator(
        DatasetCoordinatorConfig(**config), store, queue, metrics, registry or _registry()
    )


@dataclass(frozen=True)
class Scenario:
    id: str
    events: list  # [(start, end)] handled as DataAvailableEvents, in order
    expect: list  # window starts that must be scheduled afterwards — and only these
    claims: list = field(default_factory=list)  # [(start, end)] pre-seeded, unhandled
    span: timedelta = timedelta(hours=1)


SCENARIOS = [
    Scenario(
        id="one fully covered window schedules once",
        events=[(at(10), at(11))],
        expect=[at(10)],
    ),
    Scenario(
        id="hourly checkpoints aggregate into one daily window",
        span=timedelta(days=1),
        events=[(at(h), at(h + 1)) for h in range(24)],
        expect=[at(0)],
    ),
    Scenario(
        id="one event spanning three windows schedules each",
        events=[(at(10), at(13))],
        expect=[at(10), at(11), at(12)],
    ),
    Scenario(
        id="a coverage gap holds the window back",
        events=[(at(10), at(10, 30)), (at(10, 45), at(11)), (at(11), at(12))],
        expect=[at(11)],
    ),
    Scenario(
        id="half-covered window is held back",
        events=[(at(10), at(10, 30))],
        expect=[],
    ),
    Scenario(
        id="an earlier unspanned window is left alone",
        claims=[(at(8), at(9))],
        events=[(at(10), at(11))],
        expect=[at(10)],
    ),
]


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.id)
async def test_window_decisions(
    scenario, coordination_store, build_queue, coordinator_metrics
):
    handler = _handler(
        coordination_store,
        build_queue,
        coordinator_metrics,
        registry=_registry(span=scenario.span),
    )
    metadata = _metadata()

    for start, end in scenario.claims:
        await coordination_store.save_coverage_claim(
            "test_dataset",
            CoverageClaim(id=uuid4(), start_time=start, end_time=end, arrived_at=end),
        )
    events = [
        _event(start, end, metadata, span=scenario.span)
        for start, end in scenario.events
    ]
    for event in events:
        await handler.handle(event)

    queue = build_queue.get_queue_dict()
    starts = sorted(scenario.expect)
    expected_keys = [
        Window("test_dataset", start, scenario.span).dataset_key for start in starts
    ]
    assert sorted(queue) == sorted(expected_keys)

    # a cold lateness histogram means no settle delay: every scheduled window is
    # eligible the moment it ends; older windows are claimed first
    assert [queue[key] for key in expected_keys] == [
        start + scenario.span for start in starts
    ]

    # every scheduled window's desired set holds every record delivered to it
    for start in starts:
        window = Window("test_dataset", start, scenario.span)
        ids = set(
            await coordination_store.get_desired_record_ids(window)
        )
        delivered = {
            str(record.id)
            for event in events
            for record in event.records
            if record.window_start == window.start
        }
        assert delivered <= ids


async def test_window_build_lifecycle(
    coordination_store, build_queue, coordinator_metrics
):
    """A gap holds the window back; backfill completes and schedules it; once built,
    a replayed event leaves it alone and a genuinely new event rebuilds it."""
    handler = _handler(coordination_store, build_queue, coordinator_metrics)
    metadata = _metadata()
    window = Window("test_dataset", at(10), timedelta(hours=1))

    def delivered(*events) -> set[str]:
        return {str(r.id) for event in events for r in event.records}

    # partial coverage with a gap at 10:00-10:09 — tracked but not scheduled
    b = _event(at(10, 9), at(10, 30), metadata)
    c = _event(at(10, 30), at(11), metadata)
    for event in (b, c):
        await handler.handle(event)
    assert build_queue.get_queue_dict() == {}
    ids = await coordination_store.get_desired_record_ids(window)
    assert set(ids) == delivered(b, c)

    # backfill fills the gap: the window becomes complete and is scheduled
    a = _event(at(10), at(10, 20), metadata)
    await handler.handle(a)
    assert list(build_queue.get_queue_dict()) == [window.dataset_key]
    ids = await coordination_store.get_desired_record_ids(window)
    assert set(ids) == delivered(a, b, c)

    # the builder builds exactly the desired set and drains the queue
    await coordination_store.set_window_state(
        window,
        WindowBuildState(
            record_ids_hash=RecordSet.from_list(ids).hash, timestamp=at(10)
        ),
    )
    await build_queue.claim_due()

    # a replayed claim re-delivers the same record ids: desired still matches
    replay = b.model_copy(update={"arrived_at": datetime.now(timezone.utc)})
    await handler.handle(replay)
    assert build_queue.get_queue_dict() == {}

    # a genuinely new correction changes the desired set and reschedules
    d = _event(at(10), at(11), metadata)
    await handler.handle(d)
    assert list(build_queue.get_queue_dict()) == [window.dataset_key]
    ids = await coordination_store.get_desired_record_ids(window)
    assert set(ids) == delivered(a, b, c, d)


async def test_measured_lateness_defers_eligibility(
    coordination_store, build_queue, coordinator_metrics
):
    """The settle delay is driven by the source's observed arrival lateness: once
    the histogram says data settles ~6h late, a window whose data could still be
    arriving is scheduled but not yet claimable, while an older one past its settle
    time is claimed immediately."""
    span = timedelta(hours=1)
    handler = _handler(
        coordination_store,
        build_queue,
        coordinator_metrics,
        settle_min_samples=3,
    )
    metadata = _metadata()

    # this source has historically arrived ~6h late (p95 ≈ 6h): 100 datums, each
    # bucketed at 6h, seeded straight into the histogram the ingestion path fills
    six_hours = bucket_for(timedelta(hours=6).total_seconds())
    await coordination_store.record_lateness(
        "test_dataset", {six_hours: 100}, 168
    )

    # a just-completed window: its data could still be arriving, so its build
    # is scheduled for after the measured settle time
    now = datetime.now(timezone.utc)
    fresh_start = align_to_aggregation(now, span) - span
    fresh = Window("test_dataset", fresh_start, span)
    await handler.handle(_event(fresh_start, fresh_start + span, metadata))
    assert build_queue.get_queue_dict()[fresh.dataset_key] > now

    # a window older than the measured settle time is due immediately; claiming
    # yields it and leaves the fresh window waiting its turn
    old_start = align_to_aggregation(now - timedelta(hours=10), span)
    old = Window("test_dataset", old_start, span)
    await handler.handle(_event(old_start, old_start + span, metadata))

    claimed = await build_queue.claim_due()
    assert claimed.dataset_key == old.dataset_key
    assert await build_queue.claim_due() is None
    assert fresh.dataset_key in build_queue.get_queue_dict()


async def test_rebuild_is_debounced(
    coordination_store, build_queue, coordinator_metrics
):
    """A late arrival against a built provisional window schedules the revision a
    debounce after its arrival; each further arrival slides it later."""
    debounce = timedelta(minutes=10)
    handler = _handler(
        coordination_store,
        build_queue,
        coordinator_metrics,
        registry=_registry(debounce=debounce),
    )
    metadata = _metadata()
    window = Window("test_dataset", at(10), timedelta(hours=1))

    first = _event(at(10), at(11), metadata)
    await handler.handle(first)
    await coordination_store.set_window_state(
        window,
        WindowBuildState(
            record_ids_hash=RecordSet.from_list(
                [str(r.id) for r in first.records]
            ).hash,
            timestamp=datetime.now(timezone.utc),
        ),
    )
    await build_queue.claim_due()

    before = datetime.now(timezone.utc)
    await handler.handle(_event(at(10), at(11), metadata))
    eligible = build_queue.get_queue_dict()[window.dataset_key]
    assert before + debounce <= eligible <= datetime.now(timezone.utc) + debounce

    await handler.handle(_event(at(10), at(11), metadata))
    assert build_queue.get_queue_dict()[window.dataset_key] >= eligible


async def test_sealed_window_drops_late_arrivals(
    coordination_store, build_queue, coordinator_metrics
):
    """Past the retention floor a window is sealed and its records expire with
    the hot store. A late arrival cannot schedule any build, built or not."""
    handler = _handler(
        coordination_store,
        build_queue,
        coordinator_metrics,
        retention=timedelta(hours=1),
    )
    metadata = _metadata()
    built = Window("test_dataset", at(10), timedelta(hours=1))
    await coordination_store.set_window_state(
        built,
        WindowBuildState(record_ids_hash="stale", timestamp=at(11)),
    )

    await handler.handle(_event(at(10), at(11), metadata))
    assert built.dataset_key not in build_queue.get_queue_dict()

    await handler.handle(_event(at(12), at(13), metadata))
    assert build_queue.get_queue_dict() == {}
