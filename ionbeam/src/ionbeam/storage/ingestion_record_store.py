# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
from abc import ABC, abstractmethod
from typing import List, Optional

import redis.asyncio as redis

from ionbeam_client.constants import MaximumCachePeriod
from ..models import IngestionRecord, Window, WindowBuildState


class IngestionRecordStore(ABC):
    """Interface for storing and retrieving ingestion records and window state."""

    @abstractmethod
    async def save_ingestion_record(self, record: IngestionRecord) -> None:
        """Save an ingestion record."""
        pass

    @abstractmethod
    async def get_ingestion_records(self, dataset: str) -> List[IngestionRecord]:
        """Get all ingestion records for a dataset."""
        pass

    @abstractmethod
    async def get_desired_event_ids(self, window: Window) -> List[str]:
        """Get the list of desired event IDs for a window."""
        pass

    @abstractmethod
    async def set_desired_event_ids(self, window: Window, event_ids: List[str]) -> None:
        """Set the list of desired event IDs for a window."""
        pass

    @abstractmethod
    async def get_window_state(self, window: Window) -> Optional[WindowBuildState]:
        """Get the build state for a window."""
        pass

    @abstractmethod
    async def set_window_state(self, window: Window, state: WindowBuildState) -> None:
        """Set the build state for a window."""
        pass


class RedisIngestionRecordStore(IngestionRecordStore):
    """Redis implementation of IngestionRecordStore."""

    def __init__(self, client: redis.Redis):
        self.client = client
        self._ttl = int(MaximumCachePeriod.total_seconds())

    def _ingestion_record_key(self, dataset: str, event_id: str) -> str:
        return f"ingestion_events:{dataset}:{event_id}"

    def _desired_events_key(self, window: Window) -> str:
        return f"{window.dataset_key}:event_ids"

    def _window_state_key(self, window: Window) -> str:
        return f"{window.dataset_key}:state"

    async def save_ingestion_record(self, record: IngestionRecord) -> None:
        """Save an ingestion record with TTL."""
        key = self._ingestion_record_key(record.metadata.dataset.name, record.id)
        await self.client.set(key, record.model_dump_json(), ex=self._ttl)

    async def get_ingestion_records(self, dataset: str) -> List[IngestionRecord]:
        """Get all ingestion records for a dataset."""
        pattern = f"ingestion_events:{dataset}:*"
        keys = await self.client.keys(pattern)
        if not keys:
            return []

        values = await self.client.mget(keys)
        return [
            IngestionRecord.model_validate_json(value.decode("utf-8"))
            for value in values
            if value is not None
        ]

    async def get_desired_event_ids(self, window: Window) -> List[str]:
        """Get the list of desired event IDs for a window."""
        key = self._desired_events_key(window)
        result = await self.client.get(key)
        if not result:
            return []

        return json.loads(result.decode("utf-8"))

    async def set_desired_event_ids(self, window: Window, event_ids: List[str]) -> None:
        """Set the list of desired event IDs for a window with TTL."""
        key = self._desired_events_key(window)
        await self.client.set(key, json.dumps(sorted(event_ids)), ex=self._ttl)

    async def get_window_state(self, window: Window) -> Optional[WindowBuildState]:
        """Get the build state for a window."""
        key = self._window_state_key(window)
        result = await self.client.get(key)
        if not result:
            return None

        return WindowBuildState.model_validate_json(result.decode("utf-8"))

    async def set_window_state(self, window: Window, state: WindowBuildState) -> None:
        """Set the build state for a window with TTL."""
        key = self._window_state_key(window)
        await self.client.set(key, state.model_dump_json(), ex=self._ttl)
