# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Behavior of the Redis-Streams EventBus against a live Redis-protocol server
(Valkey or Redis). Gated on IONBEAM_TEST_REDIS_URL; each test runs in a flushed
throwaway database, so point it at a disposable instance only."""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import redis.asyncio as redis
from ionbeam_client.models import DatasetMetadata
from prometheus_client import CollectorRegistry

from ionbeam.messaging import (
    DataSetAvailableEvent,
    RedisStreamsEventBus,
    StartSourceCommand,
)
from ionbeam.observability import EventBusMetrics

REDIS_URL = os.getenv("IONBEAM_TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(
    REDIS_URL is None,
    reason="set IONBEAM_TEST_REDIS_URL (e.g. redis://localhost:6379/15) to run",
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


@pytest.fixture
async def bus():
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    yield RedisStreamsEventBus(client, EventBusMetrics(CollectorRegistry()))
    await client.aclose()


async def test_dataset_subscription_delivers_and_filters_by_name(bus):
    sub = await bus.subscribe_datasets("exp", {"weather"})

    await bus.publish_dataset_available(_dataset_event("air_quality"))  # filtered out
    await bus.publish_dataset_available(_dataset_event("weather"))  # delivered

    event = await sub.next(2.0)
    assert event.metadata.name == "weather"
    assert await sub.next(0.05) is None
    await sub.close()


async def test_each_exporter_identity_sees_every_event_once(bus):
    odb = await bus.subscribe_datasets("odb")
    pygeoapi = await bus.subscribe_datasets("pygeoapi")

    await bus.publish_dataset_available(_dataset_event("weather"))

    assert (await odb.next(2.0)).metadata.name == "weather"
    assert (await pygeoapi.next(2.0)).metadata.name == "weather"
    await odb.close()
    await pygeoapi.close()


async def test_instances_of_the_same_identity_compete(bus):
    first = await bus.subscribe_datasets("odb")
    second = await bus.subscribe_datasets("odb")

    await bus.publish_dataset_available(_dataset_event("a"))
    await bus.publish_dataset_available(_dataset_event("b"))

    received = [await first.next(2.0), await second.next(2.0)]
    assert sorted(e.metadata.name for e in received) == ["a", "b"]
    assert await first.next(0.05) is None
    await first.close()
    await second.close()


async def test_events_published_while_offline_are_delivered_on_resubscribe(bus):
    sub = await bus.subscribe_datasets("odb")
    await sub.close()  # exporter goes away; its group (identity) remains

    await bus.publish_dataset_available(_dataset_event("weather"))

    again = await bus.subscribe_datasets("odb")
    assert (await again.next(2.0)).metadata.name == "weather"
    await again.close()


async def test_delivered_event_is_acked_and_not_redelivered(bus):
    sub = await bus.subscribe_datasets("odb")
    await bus.publish_dataset_available(_dataset_event("weather"))

    event = await sub.next(2.0)
    assert event.metadata.name == "weather"
    await sub.ack()
    await sub.close()

    # acked: a fresh consumer in the group has nothing left to reclaim
    again = await bus.subscribe_datasets("odb")
    assert await again.next(0.2) is None
    await again.close()


async def test_unacked_event_is_redelivered_to_a_new_consumer():
    # A subscriber that reads but dies before ack (a disconnect mid-delivery)
    # leaves the event pending: a reconnecting consumer reclaims it.
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    bus = RedisStreamsEventBus(
        client, EventBusMetrics(CollectorRegistry()), reclaim_min_idle_ms=0
    )

    sub = await bus.subscribe_datasets("odb")
    await bus.publish_dataset_available(_dataset_event("weather"))

    delivered = await sub.next(2.0)
    assert delivered.metadata.name == "weather"
    await sub.close()  # closes without ack — entry stays pending

    again = await bus.subscribe_datasets("odb")
    redelivered = await again.next(2.0)
    assert redelivered.metadata.name == "weather"
    await again.ack()
    assert await again.next(0.2) is None
    await again.close()
    await client.aclose()


async def test_dead_consumer_names_are_reaped_on_subscribe():
    # A SIGKILLed subscriber never reaches close(); its consumer name stays
    # registered in the group. A new subscription sweeps idle names with
    # nothing pending, leaving only live subscribers registered.
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    bus = RedisStreamsEventBus(
        client, EventBusMetrics(CollectorRegistry()), reap_consumer_min_idle_ms=500
    )

    dead = await bus.subscribe_datasets("odb")
    assert await dead.next(0.05) is None  # registers its consumer name
    consumers = await client.xinfo_consumers("ionbeam:datasets", "odb")
    (dead_name,) = [c["name"] for c in consumers]
    # dies here without close(): the name stays registered, nothing pending

    live = await bus.subscribe_datasets("odb")
    await asyncio.sleep(1.0)
    assert await live.next(0.05) is None  # live: idle stays near zero

    await bus.subscribe_datasets("odb")
    names = {c["name"] for c in await client.xinfo_consumers("ionbeam:datasets", "odb")}
    assert dead_name not in names
    assert len(names) == 1  # only the live consumer survives

    await bus.publish_dataset_available(_dataset_event("weather"))
    assert (await live.next(2.0)).metadata.name == "weather"
    await live.ack()
    await live.close()
    await client.aclose()


async def test_triggers_route_by_source_name(bus):
    meteo = await bus.subscribe_triggers("meteotracker")
    cannon = await bus.subscribe_triggers("ioncannon")

    await bus.publish_source_trigger(_trigger("meteotracker"))

    received = await meteo.next(2.0)
    assert received.source_name == "meteotracker"
    assert received.start_time == BASE
    assert await cannon.next(0.05) is None
    await meteo.close()
    await cannon.close()


async def test_poison_event_is_dropped_after_max_deliveries():
    # An event whose handler dies on every delivery blocks everything behind it
    # if redelivered forever. Past the delivery cap it is dropped with an error.
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    registry = CollectorRegistry()
    bus = RedisStreamsEventBus(client, EventBusMetrics(registry), reclaim_min_idle_ms=0)

    first = await bus.subscribe_datasets("odb")
    await bus.publish_dataset_available(_dataset_event("weather"))

    deliveries = 0
    sub = first
    for _ in range(20):  # more attempts than the cap allows deliveries
        event = await sub.next(0.5)
        await sub.close()  # never acked — handler "died"
        if event is None:
            break
        deliveries += 1
        sub = await bus.subscribe_datasets("odb")
    else:
        pytest.fail("poison event was never dropped")

    assert deliveries == 8  # _MAX_DELIVERIES

    # dead-lettered means acked: gone for every future consumer
    again = await bus.subscribe_datasets("odb")
    assert await again.next(0.2) is None
    await again.close()

    # parked with payload and provenance
    parked = await client.xrange("ionbeam:dead-letter")
    assert len(parked) == 1
    _, fields = parked[0]
    assert fields[b"stream"] == b"ionbeam:datasets"
    assert fields[b"group"] == b"odb"
    assert b"weather" in fields[b"event"]
    assert (
        registry.get_sample_value(
            "ionbeam_eventbus_dead_lettered_total",
            {"stream": "ionbeam:datasets", "group": "odb"},
        )
        == 1
    )
    await client.aclose()
