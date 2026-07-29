# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Common test fixtures and utilities for ionbeam tests."""

from datetime import datetime
from typing import AsyncIterator, Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from prometheus_client import CollectorRegistry

from ionbeam.observability import (
    IngestionMetrics,
    CoordinatorMetrics,
    BuilderMetrics,
)
from ionbeam.storage.arrow_store import LocalFileSystemStore
from ionbeam.storage.memory_coordination import InMemoryBuildQueue, InMemoryCoordinationStore
from ionbeam.storage.timeseries import TimeSeriesDatabase


class InspectableBuildQueue(InMemoryBuildQueue):
    def get_queue_dict(self) -> Dict[str, datetime]:
        """Helper for tests to inspect the schedule: {dataset_key: eligible_at}."""
        return dict(self._scheduled)


class FakeTimeSeriesDatabase(TimeSeriesDatabase):
    """Mock wide-Arrow timeseries database for ionbeam testing."""

    def __init__(self) -> None:
        self.write_calls: List[dict] = []

    async def query_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        timestamp_column: str,
        record_ids=None,
    ) -> AsyncIterator[pa.RecordBatch]:
        time_range = pd.date_range(start_time, end_time, freq="1min", tz="UTC")[:-1]

        # Wide format (one column per field/tag), canonical names throughout;
        # rows honor the record-scoped contract by carrying the requested tags.
        ids = list(record_ids or ["untracked"])
        df = pd.DataFrame(
            {
                timestamp_column: time_range,
                "temperature": [20.0 + i * 0.1 for i in range(len(time_range))],
                "lat": 52.5,
                "lon": 13.4,
                "station_id": "test_station",
                "ib_record_id": [ids[i % len(ids)] for i in range(len(time_range))],
            }
        )
        yield pa.RecordBatch.from_pandas(df, preserve_index=False)

    async def write(
        self,
        table: pa.Table,
        measurement: str,
        tag_columns: List[str],
        timestamp_column: str,
    ) -> None:
        self.write_calls.append(
            {
                "table": table,
                "measurement": measurement,
                "tag_columns": tag_columns,
                "timestamp_column": timestamp_column,
            }
        )


class FailingTimeSeriesDatabase(TimeSeriesDatabase):
    """Mock timeseries database that always fails for testing error handling."""

    async def query_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        timestamp_column: str,
        record_ids=None,
    ) -> AsyncIterator[pa.RecordBatch]:
        raise Exception("Simulated database failure")
        yield

    async def write(
        self,
        table: pa.Table,
        measurement: str,
        tag_columns: List[str],
        timestamp_column: str,
    ) -> None:
        raise Exception("Simulated database failure")


class InspectableFileSystemStore(LocalFileSystemStore):
    """LocalFileSystemStore plus synchronous inspection helpers for assertions."""

    def stored_keys(self) -> List[str]:
        return sorted(
            str(p.relative_to(self.base_path)).removesuffix(".parquet")
            for p in self.base_path.rglob("*.parquet")
        )

    def get_total_rows(self, key: str) -> int:
        return pq.ParquetFile(self._get_path(key)).metadata.num_rows


@pytest.fixture
def metrics_registry() -> CollectorRegistry:
    return CollectorRegistry()


@pytest.fixture
def ingestion_metrics(metrics_registry) -> IngestionMetrics:
    return IngestionMetrics(metrics_registry)


@pytest.fixture
def coordinator_metrics(metrics_registry) -> CoordinatorMetrics:
    return CoordinatorMetrics(metrics_registry)


@pytest.fixture
def builder_metrics(metrics_registry) -> BuilderMetrics:
    return BuilderMetrics(metrics_registry)


@pytest.fixture
def coordination_store() -> InMemoryCoordinationStore:
    return InMemoryCoordinationStore()


@pytest.fixture
def build_queue() -> InspectableBuildQueue:
    return InspectableBuildQueue()


@pytest.fixture
def timeseries_db() -> FakeTimeSeriesDatabase:
    return FakeTimeSeriesDatabase()


@pytest.fixture
def failing_timeseries_db() -> FailingTimeSeriesDatabase:
    return FailingTimeSeriesDatabase()


@pytest.fixture
def arrow_store(tmp_path) -> InspectableFileSystemStore:
    return InspectableFileSystemStore(tmp_path / "arrow_store")


@pytest.fixture
def arrow_store_writer(arrow_store):
    async def write(key: str, df: pd.DataFrame) -> int:
        table = pa.Table.from_pandas(df, preserve_index=False)

        async def batches():
            for batch in table.to_batches():
                yield batch

        return await arrow_store.write_record_batches(key, batches())

    return write
