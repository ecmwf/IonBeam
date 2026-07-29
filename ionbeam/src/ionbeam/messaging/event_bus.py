# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""EventBus port: the internal control plane behind the public Flight boundary.

Carries only cheap control messages — source triggers and dataset availability —
never bulk data. Integrators never see this; the Flight service subscribes on their
behalf and pushes events down ``DoExchange`` streams.

Subscriptions are pull-with-timeout (``next(timeout)`` returning ``None`` on timeout)
rather than infinite async generators: the Flight server polls so it can notice
client disconnects without destroying the subscription.
"""

from abc import ABC, abstractmethod
from typing import Generic, Optional, Set, TypeVar

from ionbeam_client.models import DataSetAvailableEvent, StartSourceCommand

T = TypeVar("T")


class Subscription(ABC, Generic[T]):
    @abstractmethod
    async def next(self, timeout: float) -> Optional[T]:
        """Next event, or ``None`` if ``timeout`` seconds elapse first.

        The event stays unacknowledged until :meth:`ack`; a subscriber that dies
        after ``next`` but before ``ack`` leaves it for redelivery, so delivery is
        at-least-once (call ``ack`` only once the event is safely handed on)."""

    @abstractmethod
    async def ack(self) -> None:
        """Confirm the event last returned by :meth:`next` was delivered."""

    @abstractmethod
    async def close(self) -> None:
        ...


class EventBus(ABC):
    @abstractmethod
    async def publish_source_trigger(self, command: StartSourceCommand) -> None:
        ...

    @abstractmethod
    async def subscribe_triggers(
        self, source_name: str
    ) -> Subscription[StartSourceCommand]:
        ...

    @abstractmethod
    async def publish_dataset_available(self, event: DataSetAvailableEvent) -> None:
        ...

    @abstractmethod
    async def subscribe_datasets(
        self, exporter_name: str, datasets: Optional[Set[str]] = None
    ) -> Subscription[DataSetAvailableEvent]:
        """Subscribe to dataset availability, optionally filtered by dataset name."""
