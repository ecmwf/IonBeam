# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from abc import ABC, abstractmethod
from typing import List, Optional

import redis.asyncio as redis


class EventStore(ABC):
    """Interface for storing and retrieving ingestion events."""

    @abstractmethod
    async def get_event(self, key: str) -> Optional[str]:
        """Get a single event by key."""
        pass

    @abstractmethod
    async def store_event(
        self, key: str, event_json: str, ttl: Optional[int] = None
    ) -> None:
        """Store an event with the given key. TTL in seconds; None means no expiry."""
        pass

    @abstractmethod
    async def get_events(self, pattern: str) -> List[str]:
        """Get all events matching the pattern."""
        pass


class RedisEventStore(EventStore):
    """Redis implementation using shared client."""

    def __init__(self, client: redis.Redis):
        self.client = client

    async def get_event(self, key: str) -> Optional[str]:
        """Get a single event by key."""
        result = await self.client.get(key)
        return result.decode("utf-8") if result else None

    async def store_event(
        self, key: str, event_json: str, ttl: Optional[int] = None
    ) -> None:
        """Store an event with the given key."""
        await self.client.set(key, event_json, ex=ttl)

    async def get_events(self, pattern: str) -> List[str]:
        """Get all events matching the pattern."""
        keys = await self.client.keys(pattern)
        if not keys:
            return []

        values = await self.client.mget(keys)
        return [value.decode("utf-8") for value in values if value is not None]
