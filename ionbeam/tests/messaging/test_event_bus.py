# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from ionbeam_client.models import DatasetMetadata

from ionbeam.messaging import (
    DataSetAvailableEvent,
    InMemoryEventBus,
    StartSourceCommand,
)

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _dataset_event(dataset: str) -> DataSetAvailableEvent:
    return DataSetAvailableEvent(
        id=uuid4(),
        metadata=DatasetMetadata(
            name=dataset, description="test", source_links=[], keywords=[]
        ),
        dataset_locations=[f"{dataset}/w"],
        start_time=BASE,
        end_time=BASE + timedelta(hours=1),
    )


def _trigger(source_name: str) -> StartSourceCommand:
    return StartSourceCommand(
        id=uuid4(),
        source_name=source_name,
        start_time=BASE,
        end_time=BASE + timedelta(hours=1),
    )


async def test_dataset_subscription_delivers_and_filters_by_name():
    bus = InMemoryEventBus()
    sub = await bus.subscribe_datasets("exp", {"weather"})

    await bus.publish_dataset_available(_dataset_event("air_quality"))  # filtered out
    await bus.publish_dataset_available(_dataset_event("weather"))  # delivered

    event = await sub.next(1.0)
    assert event.metadata.name == "weather"
    # nothing else pending -> timeout returns None
    assert await sub.next(0.05) is None
    await sub.close()


async def test_dataset_subscription_without_filter_receives_everything():
    bus = InMemoryEventBus()
    sub = await bus.subscribe_datasets("exp")

    await bus.publish_dataset_available(_dataset_event("air_quality"))
    await bus.publish_dataset_available(_dataset_event("weather"))

    first = await sub.next(1.0)
    second = await sub.next(1.0)
    assert {first.metadata.name, second.metadata.name} == {"air_quality", "weather"}
    await sub.close()


async def test_trigger_subscription_routed_by_source_name():
    bus = InMemoryEventBus()
    sub = await bus.subscribe_triggers("meteotracker")

    await bus.publish_source_trigger(_trigger("ioncannon"))  # other source
    await bus.publish_source_trigger(_trigger("meteotracker"))

    trigger = await sub.next(1.0)
    assert trigger.source_name == "meteotracker"
    assert await sub.next(0.05) is None
    await sub.close()


async def test_closed_subscription_stops_receiving():
    bus = InMemoryEventBus()
    sub = await bus.subscribe_triggers("s")
    await sub.close()
    # publishing after close reaches no subscriber; a fresh wait times out
    await bus.publish_source_trigger(_trigger("s"))
    assert await sub.next(0.05) is None


async def test_closed_dataset_subscription_unregisters():
    bus = InMemoryEventBus()
    sub = await bus.subscribe_datasets("exp")
    await sub.close()
    await bus.publish_dataset_available(_dataset_event("weather"))
    assert await sub.next(0.05) is None
