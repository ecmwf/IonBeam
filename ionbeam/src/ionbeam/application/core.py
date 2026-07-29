# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional, Set
from uuid import UUID, uuid4

import pyarrow as pa
import structlog
from ionbeam_client.models import (
    DataAvailableEvent,
    DataSetAvailableEvent,
    IngestionMetadata,
    StartSourceCommand,
)

from ionbeam.builds import current_build_keys, prune_superseded
from ionbeam.handlers import (
    DatasetBuilder,
    DatasetCoordinator,
    Ingestion,
)
from ionbeam.messaging import EventBus, Subscription
from ionbeam.provenance import RegisteredDatasetMetadata
from ionbeam.storage.arrow_store import ArrowStore
from ionbeam.storage.coordination_store import CoordinationStore

SWEEP_INTERVAL = timedelta(hours=1)


class IonbeamCore:
    """Dataset registration, observation ingestion, window coordination, and
    dataset builds, under one lifecycle."""

    def __init__(
        self,
        ingestion: Ingestion,
        coordinator: DatasetCoordinator,
        builder: DatasetBuilder,
        record_store: CoordinationStore,
        arrow_store: ArrowStore,
        event_bus: EventBus,
    ):
        self._ingestion = ingestion
        self._coordinator = coordinator
        self._builder = builder
        self._record_store = record_store
        self._arrow_store = arrow_store
        self._event_bus = event_bus
        self._sweep: Optional[asyncio.Task] = None
        self._logger = structlog.get_logger(__name__)

    async def register_dataset(self, metadata: IngestionMetadata) -> str:
        schema_hash = metadata.schema_hash()
        existing = await self._record_store.get_registered_metadata(metadata.name)
        if existing is not None:
            if existing.metadata.version == metadata.version and existing.schema_hash != schema_hash:
                raise ValueError(
                    f"dataset '{metadata.name}' is already registered at "
                    f"version {metadata.version} with a different schema hash"
                )
            if existing.schema_hash == schema_hash:
                return schema_hash

        registered = RegisteredDatasetMetadata(
            metadata=metadata,
            schema_hash=schema_hash,
            registered_at=datetime.now(timezone.utc),
        )
        await self._record_store.save_registered_metadata(registered)

        # durable registration log; first sighting wins, so a re-register
        # after a cache wipe keeps the original document
        log_key = f"registrations/{metadata.name}/{schema_hash}.json"
        if await self._arrow_store.read_json(log_key) is None:
            await self._arrow_store.write_json(log_key, registered.model_dump_json())
        return schema_hash

    async def registered_dataset(self, dataset: str) -> Optional[RegisteredDatasetMetadata]:
        return await self._record_store.get_registered_metadata(dataset)

    async def ingest(
        self,
        ingestion_id: UUID,
        metadata: IngestionMetadata,
        start_time: datetime,
        end_time: datetime,
        batches: AsyncIterator[pa.RecordBatch],
    ) -> DataAvailableEvent:
        # coverage checkpoints flow to the coordinator while the stream is open, so
        # settled windows build without waiting for the stream to end
        return await self._ingestion.ingest(
            ingestion_id,
            metadata,
            start_time,
            end_time,
            batches,
            on_data_available=self._coordinator.handle,
        )

    async def current_builds(
        self, dataset: str, start: datetime, end: datetime
    ) -> list[str]:
        return await current_build_keys(self._arrow_store, dataset, start, end)

    def open_dataset(self, location: str) -> AsyncIterator[pa.RecordBatch]:
        return self._arrow_store.read_record_batches(location)

    def dataset_schema(self, location: str) -> pa.Schema:
        return self._arrow_store.read_schema(location)

    async def subscribe_triggers(
        self, source_name: str
    ) -> Subscription[StartSourceCommand]:
        return await self._event_bus.subscribe_triggers(source_name)

    async def subscribe_datasets(
        self, exporter_name: str, datasets: Optional[Set[str]] = None
    ) -> Subscription[DataSetAvailableEvent]:
        return await self._event_bus.subscribe_datasets(exporter_name, datasets)

    async def trigger_source(
        self,
        source_name: str,
        start: datetime,
        end: datetime,
        id: Optional[UUID] = None,
    ) -> None:
        await self._event_bus.publish_source_trigger(
            StartSourceCommand(
                id=id or uuid4(),
                source_name=source_name,
                start_time=start,
                end_time=end,
            )
        )

    async def start(self) -> None:
        await self._builder.start()
        self._sweep = asyncio.create_task(self._sweep_superseded())

    async def _sweep_superseded(self) -> None:
        """The sole deleter of canonical-store objects: periodically drop
        build files superseded for longer than the grace period."""
        while True:
            try:
                deleted = await prune_superseded(self._arrow_store)
                if deleted:
                    self._logger.info("Pruned superseded build files", deleted=deleted)
            except Exception:
                self._logger.exception("Superseded sweep failed; will retry")
            await asyncio.sleep(SWEEP_INTERVAL.total_seconds())

    async def stop(self) -> None:
        if self._sweep is not None:
            self._sweep.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweep
        await self._builder.stop()
