# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Event distribution through the bus port: subscription semantics on both
adapters, plus the Redis-Streams durability contract. The Redis adapter is
gated on IONBEAM_TEST_REDIS_URL; each test flushes a throwaway database, so
point it at a disposable instance only."""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import redis.asyncio as redis
from ionbeam_client.models import DatasetMetadata
from prometheus_client import CollectorRegistry

from ionbeam.messaging.streams import DATASET_STREAM, DEAD_LETTER_STREAM
from ionbeam.messaging import (
    DataSetAvailableEvent,
    InMemoryEventBus,
    RedisStreamsEventBus,
    StartSourceCommand,
)
from ionbeam.observability import EventBusMetrics

REDIS_URL = os.getenv("IONBEAM_TEST_REDIS_URL")
requires_redis = pytest.mark.skipif(
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
async def redis_client():
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.fixture(params=["memory", pytest.param("redis", marks=requires_redis)])
async def any_bus(request):
    """The bus port over each adapter, so the subscription contract is one suite."""
    if request.param == "memory":
        yield InMemoryEventBus()
        return
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    yield RedisStreamsEventBus(client, EventBusMetrics(CollectorRegistry()))
    await client.aclose()


@pytest.fixture
async def bus(redis_client):
    return RedisStreamsEventBus(redis_client, EventBusMetrics(CollectorRegistry()))


async def test_dataset_subscription_delivers_and_filters_by_name(any_bus):
    sub = await any_bus.subscribe_datasets("exp", {"weather"})

    await any_bus.publish_dataset_available(_dataset_event("air_quality"))
    await any_bus.publish_dataset_available(_dataset_event("weather"))

    event = await sub.next(2.0)
    assert event.metadata.name == "weather"
    assert await sub.next(0.05) is None
    await sub.close()


async def test_dataset_subscription_without_filter_receives_everything(any_bus):
    sub = await any_bus.subscribe_datasets("exp")

    await any_bus.publish_dataset_available(_dataset_event("air_quality"))
    await any_bus.publish_dataset_available(_dataset_event("weather"))

    first = await sub.next(2.0)
    second = await sub.next(2.0)
    assert {first.metadata.name, second.metadata.name} == {"air_quality", "weather"}
    await sub.close()


async def test_triggers_route_by_source_name(any_bus):
    sub = await any_bus.subscribe_triggers("meteotracker")

    await any_bus.publish_source_trigger(_trigger("ioncannon"))
    await any_bus.publish_source_trigger(_trigger("meteotracker"))

    trigger = await sub.next(2.0)
    assert trigger.source_name == "meteotracker"
    assert await sub.next(0.05) is None
    await sub.close()


async def test_a_closed_in_memory_subscription_receives_nothing_more():
    """The in-memory bus keeps no durable group: closing unregisters both channels
    and events published afterwards are gone. The Streams adapter deliberately
    differs, see test_events_published_while_offline_are_delivered_on_resubscribe."""
    bus = InMemoryEventBus()
    triggers = await bus.subscribe_triggers("s")
    datasets = await bus.subscribe_datasets("exp")
    await triggers.close()
    await datasets.close()

    await bus.publish_source_trigger(_trigger("s"))
    await bus.publish_dataset_available(_dataset_event("weather"))

    assert await triggers.next(0.05) is None
    assert await datasets.next(0.05) is None


@requires_redis
async def test_each_exporter_identity_sees_every_event_once(bus):
    odb = await bus.subscribe_datasets("odb")
    pygeoapi = await bus.subscribe_datasets("pygeoapi")

    await bus.publish_dataset_available(_dataset_event("weather"))

    assert (await odb.next(2.0)).metadata.name == "weather"
    assert (await pygeoapi.next(2.0)).metadata.name == "weather"
    await odb.close()
    await pygeoapi.close()


@requires_redis
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


@requires_redis
async def test_events_published_while_offline_are_delivered_on_resubscribe(bus):
    sub = await bus.subscribe_datasets("odb")
    await sub.close()  # exporter goes away; its group (identity) remains

    await bus.publish_dataset_available(_dataset_event("weather"))

    again = await bus.subscribe_datasets("odb")
    assert (await again.next(2.0)).metadata.name == "weather"
    await again.close()


@requires_redis
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


@requires_redis
async def test_unacked_event_is_redelivered_to_a_new_consumer():
    """A subscriber that reads but dies before ack leaves the event pending: a
    reconnecting consumer reclaims it."""
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


@requires_redis
async def test_dead_consumer_names_are_reaped_on_subscribe():
    """A SIGKILLed subscriber never reaches close(); a new subscription sweeps
    idle consumer names with nothing pending, leaving only live subscribers
    registered."""
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
    assert len(names) == 1

    await bus.publish_dataset_available(_dataset_event("weather"))
    assert (await live.next(2.0)).metadata.name == "weather"
    await live.ack()
    await live.close()
    await client.aclose()


@requires_redis
async def test_poison_event_is_dropped_after_max_deliveries():
    """An event whose handler dies on every delivery is dropped with an error
    once past the delivery cap."""
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

    assert deliveries == 8  # the delivery cap

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


@requires_redis
async def test_a_reconnecting_subscriber_takes_back_its_own_pending_event():
    """A subscriber whose connection drops mid-handler leaves its event pending.
    Reattaching under the same identity redelivers it at once: the reclaim
    threshold covers a subscriber that never comes back, and waiting it out
    would stall every reconnect."""
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    bus = RedisStreamsEventBus(client, EventBusMetrics(CollectorRegistry()))

    sub = await bus.subscribe_datasets("odb", subscriber="exporter-1")
    await bus.publish_dataset_available(_dataset_event("weather"))
    assert (await sub.next(2.0)).metadata.name == "weather"
    await sub.close()  # drops without ack

    again = await bus.subscribe_datasets("odb", subscriber="exporter-1")
    redelivered = await again.next(2.0)
    assert redelivered.metadata.name == "weather"
    await again.ack()
    await again.close()
    await client.aclose()


@requires_redis
async def test_reconnecting_leaves_one_consumer_per_subscriber():
    """Reconnects name the same consumer, so a group's consumer list tracks
    subscribers rather than growing with every dropped connection."""
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    bus = RedisStreamsEventBus(client, EventBusMetrics(CollectorRegistry()))

    for _ in range(3):
        sub = await bus.subscribe_datasets("odb", subscriber="exporter-1")
        await bus.publish_dataset_available(_dataset_event("weather"))
        assert (await sub.next(2.0)).metadata.name == "weather"
        await sub.close()  # drops without ack, leaving the entry pending

    consumers = await client.xinfo_consumers("ionbeam:datasets", "odb")
    assert [c["name"] for c in consumers] == [b"exporter-1"]
    await client.aclose()


@requires_redis
async def test_coverage_read_finds_a_span_starting_long_before_the_window():
    """Coverage is read over the window being decided, and a sweep can start
    well before it — a long backfill covering days reaches into an hour-long
    window whose range it does not begin in."""
    from datetime import timedelta as _td

    from ionbeam.provenance import CoverageClaim as _Claim
    from ionbeam.storage.coordination_store import RedisCoordinationStore

    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    store = RedisCoordinationStore(client, retention=_td(days=21))

    # inside the retention floor, which the write prunes below
    recent = datetime.now(timezone.utc) - _td(days=5)
    backfill_start = recent
    backfill_end = recent + _td(days=4)
    await store.save_coverage_claim(
        "weather",
        _Claim(
            id=uuid4(),
            start_time=backfill_start,
            end_time=backfill_end,
            arrived_at=backfill_end,
        ),
    )

    window_start = recent + _td(days=3)
    spans = await store.get_coverage_spans(
        "weather", window_start, window_start + _td(hours=1)
    )
    assert spans == [(backfill_start, backfill_end)]
    await client.aclose()


@requires_redis
async def test_a_poison_event_a_subscriber_keeps_inheriting_still_parks():
    """A subscriber that reconnects takes back its own pending entry. If its
    handler never gets through that entry, redelivery must still count toward
    the dead-letter bound — otherwise the entry blocks the stream forever."""
    client = redis.from_url(REDIS_URL)
    await client.flushdb()
    bus = RedisStreamsEventBus(client, EventBusMetrics(CollectorRegistry()))

    # subscribe first: a group starts at the stream tip, so an event published
    # before it exists is never delivered
    sub = await bus.subscribe_datasets("odb", subscriber="exporter-1")
    await bus.publish_dataset_available(_dataset_event("weather"))
    assert (await sub.next(2.0)) is not None
    await sub.close()  # handler "died" — never acked

    for _ in range(12):
        sub = await bus.subscribe_datasets("odb", subscriber="exporter-1")
        event = await sub.next(1.0)
        await sub.close()
        if event is None:
            break  # parked: nothing left to inherit

    assert await client.xlen(DEAD_LETTER_STREAM) == 1
    summary = await client.xpending(DATASET_STREAM, "odb")
    assert summary["pending"] == 0
    await client.aclose()
