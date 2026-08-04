# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Shared fixtures: the weather dataset shape and in-memory backends every flow runs on."""

from datetime import datetime
from typing import Dict, List, Optional

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
from ionbeam.storage.memory_timeseries import InMemoryTimeSeriesDatabase
from ionbeam_client.canonical_stream import canonical_arrow_schema
from ionbeam_client.models import (
    CfSemantics,
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)


def weather_metadata(name: str = "test_dataset") -> IngestionMetadata:
    """A geographic point station feed reporting one variable: the shape every
    handler, the builder and the Flight surface are exercised against."""
    return IngestionMetadata(
        name=name,
        dataset_schema=DatasetSchema(
            time=TimeCoordinate(),
            coordinates=geographic_point_coordinates(),
            variables=[
                Variable(
                    name="temperature",
                    semantics=CfSemantics(standard_name="air_temperature"),
                    unit="deg_C",
                )
            ],
            tags=[Tag(name="station_id")],
        ),
    )


def observation_frame(times, temperatures=None, stations=None) -> pd.DataFrame:
    """Rows for :func:`weather_metadata`, all at one Berlin station unless
    ``stations`` distinguishes them."""
    n = len(times)
    return pd.DataFrame(
        {
            "time": list(times),
            "lat": [52.5] * n,
            "lon": [13.4] * n,
            "temperature": list(temperatures) if temperatures is not None else [20.0] * n,
            "station_id": list(stations) if stations is not None else ["test_station"] * n,
        }
    )


@pytest.fixture
def metadata() -> IngestionMetadata:
    return weather_metadata()


@pytest.fixture
def canonical_batch(metadata):
    """Batches in the shape the ingestion contract demands: canonical column
    names and types for the ``metadata`` fixture, schema hash stamped."""

    def _batch(times, temperatures=None, stations=None, schema=None) -> pa.RecordBatch:
        frame = observation_frame(times, temperatures, stations)
        return pa.RecordBatch.from_pydict(
            {column: frame[column].tolist() for column in frame},
            schema=schema if schema is not None else canonical_arrow_schema(metadata),
        )

    return _batch


class InspectableBuildQueue(InMemoryBuildQueue):
    def get_queue_dict(self) -> Dict[str, datetime]:
        """Helper for tests to inspect the schedule: {dataset_key: eligible_at}."""
        return dict(self._scheduled)


class InspectableCoordinationStore(InMemoryCoordinationStore):
    def lateness(self, dataset: str) -> Dict[int, int]:
        """Helper for tests to inspect the histogram: {bucket: count}."""
        return dict(self._lateness.get(dataset, {}))


class InspectableTimeSeriesDatabase(InMemoryTimeSeriesDatabase):
    """InMemoryTimeSeriesDatabase plus a spy on the write contract and a
    synchronous stored-table view."""

    def __init__(self) -> None:
        super().__init__()
        self.last_write: Optional[tuple[str, List[str], str]] = None

    def stored(self, measurement: str) -> Optional[pa.Table]:
        return self._data.get(measurement)

    async def write(
        self,
        table: pa.Table,
        measurement: str,
        tag_columns: List[str],
        timestamp_column: str,
    ) -> None:
        self.last_write = (measurement, list(tag_columns), timestamp_column)
        await super().write(table, measurement, tag_columns, timestamp_column)


class InspectableFileSystemStore(LocalFileSystemStore):
    """LocalFileSystemStore plus synchronous inspection helpers for assertions."""

    def stored_keys(self) -> List[str]:
        return sorted(
            str(p.relative_to(self.base_path)).removesuffix(".parquet")
            for p in self.base_path.rglob("*.parquet")
        )

    def table(self, key: str) -> pa.Table:
        return pq.read_table(self._get_path(key))

    def schema_of(self, key: str) -> pa.Schema:
        return pq.read_schema(self._get_path(key))


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
def coordination_store() -> InspectableCoordinationStore:
    return InspectableCoordinationStore()


@pytest.fixture
def build_queue() -> InspectableBuildQueue:
    return InspectableBuildQueue()


@pytest.fixture
def timeseries_db() -> InspectableTimeSeriesDatabase:
    return InspectableTimeSeriesDatabase()


@pytest.fixture
def arrow_store(tmp_path) -> InspectableFileSystemStore:
    return InspectableFileSystemStore(tmp_path / "arrow_store")
