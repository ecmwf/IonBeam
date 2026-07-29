# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""In-process ``EventBus`` for the single-service deployment and tests."""

import asyncio
from typing import Callable, Dict, List, Optional, Set

from ionbeam_client.models import DataSetAvailableEvent, StartSourceCommand

from .event_bus import EventBus, Subscription


class _QueueSubscription(Subscription):
    """A subscription backed by an asyncio queue; ``close`` unregisters it.

    ``next`` waits on the queue with a timeout — cancelling the wait on timeout is
    safe (the queue is untouched), so the subscription survives idle periods.
    """

    def __init__(self, unregister: Callable[["_QueueSubscription"], None]):
        self._queue: "asyncio.Queue" = asyncio.Queue()
        self._unregister = unregister

    def deliver(self, event) -> None:
        self._queue.put_nowait(event)

    async def next(self, timeout: float):
        try:
            return await asyncio.wait_for(self._queue.get(), timeout)
        except asyncio.TimeoutError:
            return None

    async def ack(self) -> None:
        # In-process delivery has no redelivery channel; once next() hands the
        # event over there is nothing to acknowledge.
        return None

    async def close(self) -> None:
        self._unregister(self)


class InMemoryEventBus(EventBus):
    def __init__(self):
        self._dataset_subs: List[tuple] = []  # (datasets, subscription)
        self._trigger_subs: Dict[str, List[_QueueSubscription]] = {}

    async def publish_source_trigger(self, command: StartSourceCommand) -> None:
        for sub in list(self._trigger_subs.get(command.source_name, [])):
            sub.deliver(command)

    async def subscribe_triggers(self, source_name: str) -> Subscription:
        def unregister(sub):
            self._trigger_subs.get(source_name, []).remove(sub)

        sub = _QueueSubscription(unregister)
        self._trigger_subs.setdefault(source_name, []).append(sub)
        return sub

    async def publish_dataset_available(self, event: DataSetAvailableEvent) -> None:
        for datasets, sub in list(self._dataset_subs):
            if datasets is None or event.metadata.name in datasets:
                sub.deliver(event)

    async def subscribe_datasets(
        self, exporter_name: str, datasets: Optional[Set[str]] = None
    ) -> Subscription:
        entry_holder = {}

        def unregister(sub):
            self._dataset_subs.remove(entry_holder["entry"])

        sub = _QueueSubscription(unregister)
        entry_holder["entry"] = (datasets, sub)
        self._dataset_subs.append(entry_holder["entry"])
        return sub
