# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from ionbeam_client.models import (
    Coordinate,
    DatasetSchema,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)

from ionbeam.handlers.canonicalize import canonicalize


def _geo_map() -> DatasetSchema:
    return DatasetSchema(
        time=TimeCoordinate(),
        coordinates=geographic_point_coordinates(),
        variables=[Variable(name="temperature")],
        tags=[Tag(name="station_id")],
    )


def _geo_batch() -> pa.RecordBatch:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return pa.RecordBatch.from_pydict(
        {
            "time": [base, base.replace(minute=5), base.replace(minute=1)],
            "lat": [52.5, 52.5, 52.5],
            "lon": [13.4, 13.4, 13.4],
            "temperature": [20.0, 20.5, 20.2],
            "station_id": ["A", "A", "A"],
        }
    )


def test_canonicalize_with_geographic_map_sorts_by_time():
    result = canonicalize(_geo_batch(), _geo_map())

    assert result is not None
    assert set(result.table.column_names) == {"time", "lat", "lon", "temperature", "station_id"}
    assert result.tag_columns == ["station_id"]
    assert result.timestamp_column == "time"

    times = result.table.column("time").to_pylist()
    assert times == sorted(times)
    assert result.start_time == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert result.end_time == datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)


def test_canonicalize_with_non_geographic_map():
    map_ = DatasetSchema(
        coordinates=[
            Coordinate(name="degree", dtype="int64"),
            Coordinate(name="order", dtype="int64"),
        ],
        variables=[Variable(name="power", dtype="float64")],
    )
    batch = pa.RecordBatch.from_pydict(
        {
            "time": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            "degree": [2],
            "order": [1],
            "power": [42.0],
        }
    )

    result = canonicalize(batch, map_)

    assert result is not None
    assert result.table.column_names == ["time", "degree", "order", "power"]
    assert result.tag_columns == []


def test_canonicalize_missing_declared_column_raises_with_name():
    batch = pa.RecordBatch.from_pydict(
        {
            "time": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            "lat": [52.5],
            "temperature": [20.0],
            "station_id": ["A"],
        }
    )

    with pytest.raises(ValueError, match="lon"):
        canonicalize(batch, _geo_map())


def test_ancillary_only_rows_are_dropped_by_empty_row_check():
    map_ = DatasetSchema(
        variables=[
            Variable(name="temperature"),
            Variable(name="qc", dtype="int64", ancillary_of=["temperature"]),
        ]
    )
    batch = pa.RecordBatch.from_pydict(
        {
            "time": [datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)],
            "temperature": [None, 20.0],
            "qc": [1, None],
        }
    )

    result = canonicalize(batch, map_)

    assert result is not None
    df = result.table.to_pandas()
    assert len(df) == 1
    assert df["temperature"].iloc[0] == 20.0


def test_canonicalize_returns_none_when_all_primary_variables_null():
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    batch = pa.RecordBatch.from_pydict(
        {
            "time": [base],
            "lat": [52.5],
            "lon": [13.4],
            "temperature": [None],
            "station_id": ["A"],
        }
    )
    assert canonicalize(batch, _geo_map()) is None
