# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from abc import ABC, abstractmethod
from typing import Optional, Tuple

import redis.asyncio as redis

from ..models import Window


class OrderedQueue(ABC):
    @abstractmethod
    async def enqueue(self, window: Window, priority: int) -> None:
        pass

    @abstractmethod
    async def dequeue_highest_priority(self) -> Optional[Tuple[Window, int]]:
        pass

    @abstractmethod
    async def get_size(self) -> int:
        pass


class RedisOrderedQueue(OrderedQueue):
    def __init__(self, client: redis.Redis, queue_key: str = "build_queue"):
        self.client = client
        self.queue_key = "build_queue"

    async def enqueue(self, window: Window, priority: int) -> None:
        """Add a window to the queue with the given priority.

        Uses Redis sorted set where score is the priority.
        Higher scores = higher priority (processed first).
        """
        await self.client.zadd(self.queue_key, {window.dataset_key: priority})

    async def dequeue_highest_priority(self) -> Optional[Tuple[Window, int]]:
        """Remove and return the highest priority window.

        Uses ZPOPMAX to atomically get and remove the highest scored item.
        """
        result = await self.client.zpopmax(self.queue_key, count=1)
        if not result:
            return None

        dataset_key, priority = result[0]
        window = Window.from_dataset_key(dataset_key.decode("utf-8"))
        return window, int(priority)

    async def get_size(self) -> int:
        return await self.client.zcard(self.queue_key)
