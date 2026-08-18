# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""The declaration contract: what a source may declare, the schema hash, and how
declared frames become canonical Arrow streams."""

import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from ionbeam_client.canonical_stream import canonical_arrow_schema, canonical_record_batches
from ionbeam_client.alignment import align_to_schema, coerce_types
from ionbeam_client.models import (
    CfSemantics,
    Coordinate,
    DatasetSchema,
    DatasetMetadata,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)
from ionbeam_client.schema_metadata import (
    ancillaries_of,
    dataset_metadata,
    find_coordinates,
    semantics,
    tag_fields,
    time_field,
    value_fields,
)


def dataset_schema(**overrides):
    data = {
        "coordinates": geographic_point_coordinates(),
        "variables": [
            Variable(
                name="temperature",
                semantics=CfSemantics(standard_name="air_temperature", cell_method="point"),
                unit="K",
            )
        ],
        "tags": [Tag(name="station_id")],
    }
    data.update(overrides)
    return DatasetSchema(**data)


def metadata(**overrides):
    data = {
        "name": "weather",
        "dataset_schema": dataset_schema(),
    }
    data.update(overrides)
    return IngestionMetadata(**data)


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"variables": [Variable(name="lat")]}, "duplicate column names"),
        ({"variables": [Variable(name="ib_time")]}, "platform prefix"),
        ({"tags": [Tag(name="ib_geometry")]}, "platform prefix"),
        ({"time": TimeCoordinate(name="ib_year")}, "platform prefix"),
        ({"time": TimeCoordinate(name="_measurement")}, "must match"),
        ({"variables": [Variable(name="Temperature")]}, "must match"),
    ],
    ids=[
        "a name already taken",
        "a variable in the platform namespace",
        "a tag in the platform namespace",
        "a time axis in the platform namespace",
        "a name that is not an identifier",
        "a capitalised name",
    ],
)
def test_declaration_rejected(kwargs, message):
    """A dataset schema names its own columns; the platform reserves the ib_
    namespace and requires identifiers it can carry through Arrow."""
    with pytest.raises(ValidationError, match=message):
        dataset_schema(**kwargs)


def test_a_column_declaration_must_stand_on_its_own():
    """Column rules bind at the column, before any schema sees it: a crs claims
    an axis role, and a dtype must be one Arrow can carry."""
    with pytest.raises(ValidationError, match="crs requires an axis role"):
        Coordinate(name="lat", crs="EPSG:4326")
    with pytest.raises(ValidationError):
        Variable(name="temperature", dtype="float128")


def test_plain_standard_words_are_free_to_declare():
    """A source's native vocabulary — time, source, year — is free to declare;
    the declared time name survives as the structural axis."""
    schema = dataset_schema(
        time=TimeCoordinate(name="obs_time"),
        variables=[Variable(name="temperature"), Variable(name="time")],
        tags=[Tag(name="source"), Tag(name="year")],
    )
    assert schema.canonical_columns[0] == "obs_time"
    assert {"time", "source", "year"} < set(schema.canonical_columns)


@pytest.mark.parametrize(
    "variables, message",
    [
        (
            [Variable(name="qc", dtype="int64", ancillary_of=["temperature"])],
            "unknown variable",
        ),
        ([Variable(name="qc", dtype="int64", ancillary_of=["qc"])], "cannot qualify itself"),
        (
            [
                Variable(name="primary"),
                Variable(name="a", ancillary_of=["b"]),
                Variable(name="b", ancillary_of=["a"]),
            ],
            "ancillary cycle",
        ),
        # a finite schema where every variable is ancillary always contains a cycle
        (
            [Variable(name="a", ancillary_of=["b"]), Variable(name="b", ancillary_of=["a"])],
            "ancillary cycle",
        ),
    ],
    ids=["dangling reference", "self reference", "cycle", "no primary variable at all"],
)
def test_ancillary_references_must_resolve_to_a_primary_variable(variables, message):
    with pytest.raises(ValidationError, match=message):
        dataset_schema(variables=variables)


def test_schema_hash_covers_the_contract_a_source_owns():
    """The hash trips on the contract a source declares — its name, version, and
    the structural schema."""
    map_ = dataset_schema()
    base = IngestionMetadata(name="weather", dataset_schema=map_)

    bumped_version = IngestionMetadata(name="weather", dataset_schema=map_, version=2)
    changed_schema = IngestionMetadata(
        name="weather",
        dataset_schema=dataset_schema(
            variables=[
                Variable(
                    name="temperature",
                    semantics=CfSemantics(standard_name="air_temperature", cell_method="point"),
                    unit="degC",  # a declared unit change is a contract change
                )
            ]
        ),
    )

    assert base.schema_hash() != bumped_version.schema_hash()
    assert base.schema_hash() != changed_schema.schema_hash()


def test_schema_metadata_round_trips_through_parquet(tmp_path):
    meta = metadata(
        dataset_schema=dataset_schema(
            variables=[
                Variable(
                    name="temperature",
                    semantics=CfSemantics(standard_name="air_temperature", cell_method="point"),
                    unit="K",
                ),
                Variable(
                    name="humidity",
                    semantics=CfSemantics(standard_name="relative_humidity"),
                    unit="%",
                ),
                Variable(
                    name="qc_flag",
                    dtype="int64",
                    semantics=CfSemantics(standard_name="status_flag"),
                    ancillary_of=["temperature", "humidity"],
                ),
            ]
        )
    )
    schema = canonical_arrow_schema(
        meta, DatasetMetadata(name="weather", description="Weather observations")
    )
    table = pa.Table.from_arrays(
        [
            pa.array([], type=field.type)
            for field in schema
        ],
        schema=schema,
    )
    path = tmp_path / "dataset.parquet"
    pq.write_table(table, path)
    restored = pq.read_schema(path)

    assert dataset_metadata(restored).name == "weather"
    assert time_field(restored).name == "time"
    assert [field.name for field in find_coordinates(restored, axis="x")] == ["lon"]
    assert [field.name for field in value_fields(restored, primary_only=True)] == [
        "temperature",
        "humidity",
    ]
    assert [field.name for field in tag_fields(restored)] == ["station_id"]
    assert [field.name for field in ancillaries_of(restored, "temperature")] == ["qc_flag"]
    qc_meta = {k.decode(): v.decode() for k, v in restored.field("qc_flag").metadata.items()}
    assert json.loads(qc_meta["ionbeam.ancillary_of"]) == ["temperature", "humidity"]
    # semantics round-trip as the typed union, per scheme
    assert semantics(restored.field("temperature")) == CfSemantics(
        standard_name="air_temperature", cell_method="point"
    )
    assert semantics(restored.field("station_id")) is None


async def test_canonical_record_batches_pins_declared_schema_across_frames():
    """The stream schema comes from the declaration; ragged dtypes across
    frames coerce to the declared types."""
    meta = metadata(
        dataset_schema=dataset_schema(
            variables=[
                Variable(name="temperature"),
                Variable(name="qc", dtype="int64"),
            ]
        )
    )
    first = pd.DataFrame(  # qc absent, temperature already numeric
        {
            "time": ["2025-01-01T00:00:00Z"],
            "lat": [51.0],
            "lon": [0.1],
            "temperature": [280.0],
            "station_id": ["a"],
        }
    )
    second = pd.DataFrame(  # temperature as strings, qc as integral floats
        {
            "time": ["2025-01-01T00:05:00Z"],
            "lat": [51.0],
            "lon": [0.1],
            "temperature": ["281.5"],
            "qc": [2.0],
            "station_id": ["b"],
        }
    )

    batches = [batch async for batch in canonical_record_batches([first, second], meta)]

    declared = canonical_arrow_schema(meta)
    assert all(batch.schema.equals(declared, check_metadata=True) for batch in batches)
    assert batches[0].column("qc").null_count == 1
    assert batches[1].column("temperature").to_pylist() == [281.5]
    assert batches[1].column("qc").to_pylist() == [2]


def test_align_to_schema_fills_missing_declared_columns_with_typed_nulls():
    meta = metadata()
    raw = pd.DataFrame(
        {
            "time": ["2025-01-01T00:00:00Z"],
            "lat": [51.0],
            "lon": [0.1],
            "station_id": ["abc"],
        }
    )

    aligned = align_to_schema(raw, meta)

    assert list(aligned.columns) == [
        "time", "lat", "lon", "temperature", "station_id"
    ]
    assert pd.isna(aligned.loc[0, "temperature"])
    assert str(aligned["temperature"].dtype) == "float64"


def test_align_to_schema_rejects_undeclared_columns():
    raw = pd.DataFrame(
        {
            "time": ["2025-01-01T00:00:00Z"],
            "lat": [51.0],
            "lon": [0.1],
            "temperature": [280.0],
            "station_id": ["abc"],
            "extra": ["boom"],
        }
    )

    with pytest.raises(ValueError, match="undeclared columns"):
        align_to_schema(raw, metadata())


def test_coerce_types_applies_declared_dtypes_and_utc_time():
    map_ = dataset_schema(
        coordinates=[Coordinate(name="degree", dtype="int64")],
        variables=[Variable(name="power", dtype="float32")],
        tags=[Tag(name="instrument")],
    )
    frame = pd.DataFrame(
        {
            "time": ["2025-01-01T00:00:00+02:00", "not a time"],
            "degree": [1, None],
            "power": ["1.25", "2.5"],
            "instrument": [7, 8],
        }
    )

    coerced = coerce_types(frame, map_)

    assert list(coerced.columns) == ["time", "degree", "power", "instrument"]
    assert len(coerced) == 1
    assert str(coerced["time"].dtype) == "datetime64[ns, UTC]"
    assert str(coerced["degree"].dtype) == "Int64"
    assert str(coerced["power"].dtype) == "float32"
    assert str(coerced["instrument"].dtype) == "string"
