# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""``EventBus`` over Redis-protocol streams (Valkey in the deployments).

One stream per source for triggers, one shared stream for dataset availability.
A consumer group per subscriber identity gives durable fan-out semantics: every
identity sees each event once, instances of the same identity compete for events,
and a group accumulates events published while its subscriber is offline (from the
group's creation onwards — the first-ever subscribe starts at the stream tip).
This is what lets multiple ionbeam replicas share one control plane: an exporter
connected to any replica receives events published by any other.

Streams are trimmed to a bounded length. An event is acknowledged only once the
subscriber confirms its handler *completed the work* (:meth:`Subscription.ack` —
the Flight server calls it after the client's completion ack arrives on the
exchange), so a client that errors or dies mid-handler leaves the event pending,
not lost. On reconnect a fresh consumer reclaims entries left pending by a dead
consumer (``XAUTOCLAIM`` past an idle threshold), making the work at-least-once.
A redelivery is harmless: exports and rebuilds are idempotent and re-fired
triggers re-fetch the same window. A poison event is parked on the bounded
``ionbeam:dead-letter`` stream (payload + provenance, oldest trimmed) after
``_MAX_DELIVERIES`` attempts rather than blocking its stream forever.
"""

import asyncio
from typing import Callable, Generic, Optional, Set, TypeVar
from uuid import uuid4

import redis.asyncio as redis
import structlog

from ionbeam.observability import EventBusMetrics

from .event_bus import (
    DataSetAvailableEvent,
    EventBus,
    StartSourceCommand,
    Subscription,
)

logger = structlog.get_logger(__name__)

T = TypeVar("T")

DATASET_STREAM = "ionbeam:datasets"
TRIGGER_STREAM_PREFIX = "ionbeam:triggers:"
_MAX_STREAM_LENGTH = 8192
# A pending entry idle longer than this is reclaimed as abandoned. MUST exceed
# the slowest handler's end-to-end run time (an ack certifies work completion,
# so an entry stays pending for the whole fetch+ingest): sized above the
# deepest meteotracker backfill.
_RECLAIM_MIN_IDLE_MS = 1_200_000  # 20 min
# A consumer name idle this long with nothing pending was left registered by an
# unclean shutdown — a live consumer's blocking reads keep its idle near zero.
# Each new subscription deregisters such names so a group's consumer list
# reflects live subscribers.
_REAP_CONSUMER_MIN_IDLE_MS = 1_800_000
# An event whose handler keeps dying (a poison event) would redeliver forever
# and block everything behind it; past this many deliveries it is parked on the
# dead-letter stream instead.
_MAX_DELIVERIES = 8
# Dead letters carry the full payload so they can be inspected and re-fired.
# They should be rare — the small bound keeps a pathological run of poison
# events from growing the stream without limit (oldest are trimmed first).
DEAD_LETTER_STREAM = "ionbeam:dead-letter"
_MAX_DEAD_LETTERS = 1024


def _trigger_stream(source_name: str) -> str:
    return f"{TRIGGER_STREAM_PREFIX}{source_name}"


class _StreamSubscription(Subscription[T], Generic[T]):
    """One consumer in a consumer group, reading a stream with a blocking cursor.

    Reads new entries (``>``) but first reclaims any left pending by a dead
    consumer, so nothing read by an interrupted subscriber is lost. An entry is
    acked only after the caller confirms delivery via :meth:`ack`."""

    def __init__(
        self,
        client: redis.Redis,
        stream: str,
        group: str,
        parse: Callable[[bytes], T],
        metrics: EventBusMetrics,
        matches: Optional[Callable[[T], bool]] = None,
        reclaim_min_idle_ms: int = _RECLAIM_MIN_IDLE_MS,
    ):
        self._client = client
        self._stream = stream
        self._group = group
        self._consumer = uuid4().hex
        self._parse = parse
        self._metrics = metrics
        self._matches = matches
        self._reclaim_min_idle_ms = reclaim_min_idle_ms
        self._delivered_unacked: Optional[bytes] = None
        self._reclaim_cursor = b"0-0"

    async def next(self, timeout: float) -> Optional[T]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            entry = await self._reclaim_one()
            if entry is None:
                entry = await self._read_new(block_ms=max(1, int(remaining * 1000)))
            if entry is None:
                continue
            entry_id, fields = entry
            self._delivered_unacked = entry_id
            self._metrics.delivered(self._stream, self._group)
            event = self._parse(fields[b"event"])
            if self._matches is None or self._matches(event):
                return event
            # filtered out for this subscriber: ack and keep draining
            await self.ack()

    async def _read_new(self, block_ms: int):
        entries = await self._client.xreadgroup(
            self._group,
            self._consumer,
            {self._stream: ">"},
            count=1,
            block=block_ms,
        )
        if not entries:
            return None
        _, messages = entries[0]
        return messages[0] if messages else None

    async def _reclaim_one(self):
        cursor, messages, _ = await self._client.xautoclaim(
            self._stream,
            self._group,
            self._consumer,
            min_idle_time=self._reclaim_min_idle_ms,
            start_id=self._reclaim_cursor,
            count=1,
        )
        self._reclaim_cursor = cursor
        if not messages:
            return None
        entry_id, fields = messages[0]
        self._metrics.reclaimed(self._stream, self._group)
        pending = await self._client.xpending_range(
            self._stream, self._group, min=entry_id, max=entry_id, count=1
        )
        if pending and pending[0]["times_delivered"] > _MAX_DELIVERIES:
            logger.error(
                "Dead-lettering event after repeated failed deliveries",
                stream=self._stream,
                group=self._group,
                entry=entry_id,
                deliveries=pending[0]["times_delivered"],
                dead_letter_stream=DEAD_LETTER_STREAM,
            )
            # Park first, then ack: a crash in between redelivers and parks a
            # duplicate — never the reverse (acked but not parked = lost).
            await self._client.xadd(
                DEAD_LETTER_STREAM,
                {
                    "stream": self._stream,
                    "group": self._group,
                    "entry": entry_id,
                    "event": fields.get(b"event", b""),
                },
                maxlen=_MAX_DEAD_LETTERS,
                approximate=True,
            )
            await self._client.xack(self._stream, self._group, entry_id)
            self._metrics.dead_lettered(self._stream, self._group)
            return None
        return entry_id, fields

    async def ack(self) -> None:
        if self._delivered_unacked is not None:
            await self._client.xack(self._stream, self._group, self._delivered_unacked)
            self._delivered_unacked = None
            self._metrics.acked(self._stream, self._group)

    async def close(self) -> None:
        # Deleting a consumer discards its pending entries, so only reap it once
        # nothing is left unacked; otherwise leave it for reclaim.
        if self._delivered_unacked is None:
            await self._client.xgroup_delconsumer(
                self._stream, self._group, self._consumer
            )


class RedisStreamsEventBus(EventBus):
    def __init__(
        self,
        client: redis.Redis,
        metrics: EventBusMetrics,
        reclaim_min_idle_ms: int = _RECLAIM_MIN_IDLE_MS,
        reap_consumer_min_idle_ms: int = _REAP_CONSUMER_MIN_IDLE_MS,
    ):
        self._client = client
        self._metrics = metrics
        self._reclaim_min_idle_ms = reclaim_min_idle_ms
        self._reap_consumer_min_idle_ms = reap_consumer_min_idle_ms

    async def publish_source_trigger(self, command: StartSourceCommand) -> None:
        await self._publish(_trigger_stream(command.source_name), command.model_dump_json())

    async def subscribe_triggers(
        self, source_name: str
    ) -> Subscription[StartSourceCommand]:
        stream = _trigger_stream(source_name)
        await self._ensure_group(stream, source_name)
        await self._reap_dead_consumers(stream, source_name)
        return _StreamSubscription(
            self._client,
            stream,
            source_name,
            StartSourceCommand.model_validate_json,
            self._metrics,
            reclaim_min_idle_ms=self._reclaim_min_idle_ms,
        )

    async def publish_dataset_available(self, event: DataSetAvailableEvent) -> None:
        await self._publish(DATASET_STREAM, event.model_dump_json())

    async def subscribe_datasets(
        self, exporter_name: str, datasets: Optional[Set[str]] = None
    ) -> Subscription[DataSetAvailableEvent]:
        await self._ensure_group(DATASET_STREAM, exporter_name)
        await self._reap_dead_consumers(DATASET_STREAM, exporter_name)
        matches = None if datasets is None else (lambda e: e.metadata.name in datasets)
        return _StreamSubscription(
            self._client,
            DATASET_STREAM,
            exporter_name,
            DataSetAvailableEvent.model_validate_json,
            self._metrics,
            matches,
            reclaim_min_idle_ms=self._reclaim_min_idle_ms,
        )

    async def _publish(self, stream: str, payload: str) -> None:
        await self._client.xadd(
            stream, {"event": payload}, maxlen=_MAX_STREAM_LENGTH, approximate=True
        )
        self._metrics.published(stream)

    async def _reap_dead_consumers(self, stream: str, group: str) -> None:
        # Deleting a consumer discards its pending entries, so a corpse with
        # pending is left for XAUTOCLAIM to drain; a later subscribe reaps it.
        for consumer in await self._client.xinfo_consumers(stream, group):
            if consumer["pending"] > 0:
                continue
            if consumer["idle"] > self._reap_consumer_min_idle_ms:
                await self._client.xgroup_delconsumer(
                    stream, group, consumer["name"]
                )

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._client.xgroup_create(stream, group, id="$", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
