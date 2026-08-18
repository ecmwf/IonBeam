# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Geospatial projection of built datasets: WKB geometry and deterministic
self-locating row ids."""

import re
import struct
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa

from ionbeam_client.canonical_stream import canonical_arrow_schema
from ionbeam_client.geo import geographic_axes, geospatial_projection
from ionbeam_client.models import (
    CfSemantics,
    DatasetSchema,
    IngestionMetadata,
    Tag,
    Variable,
    geographic_point_coordinates,
)
from ionbeam_client.schema_metadata import time_field


def _geographic_metadata():
    return IngestionMetadata(
        name="weather",
        dataset_schema=DatasetSchema(
            coordinates=geographic_point_coordinates(),
            variables=[Variable(name="temperature", semantics=CfSemantics(standard_name="air_temperature"), unit="K")],
            tags=[Tag(name="station_id")],
        ),
    )


def _decode_point(wkb: bytes) -> tuple[float, float]:
    assert wkb[0] == 1  # little-endian
    assert struct.unpack_from("<I", wkb, 1)[0] == 1  # Point
    return struct.unpack_from("<dd", wkb, 5)


def _batch(schema: pa.Schema) -> pa.RecordBatch:
    x_name, y_name = geographic_axes(schema)
    t0 = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 1, 13, tzinfo=timezone.utc)
    frame = pd.DataFrame(
        {
            time_field(schema).name: [t0, t0, t1],
            y_name: [50.0, 50.0, 55.0],
            x_name: [10.0, 10.0, float("nan")],  # row 2 has no position
            "temperature": [1.0, 2.0, 3.0],
            "station_id": ["a", "b", "a"],  # row 1 differs only by tag
        }
    )
    return pa.Table.from_pandas(frame, schema=schema, preserve_index=False).to_batches()[0]


def test_geographic_dataset_gets_geometry_and_id():
    schema = canonical_arrow_schema(_geographic_metadata())
    out_schema, transform = geospatial_projection(schema)

    assert transform is not None
    assert out_schema.field("ib_geometry").type == pa.binary()
    assert out_schema.field("ib_id").type == pa.string()
    assert b"geo" in out_schema.metadata  # valid GeoParquet footer
    geometry_meta = out_schema.field("ib_geometry").metadata
    assert geometry_meta[b"ARROW:extension:name"] == b"geoarrow.wkb"

    out = transform(_batch(schema))
    assert out.schema.field("ib_geometry").metadata == geometry_meta  # rides the stream
    geometry = out.column("ib_geometry").to_pylist()
    ids = out.column("ib_id").to_pylist()

    assert _decode_point(geometry[0]) == (10.0, 50.0)
    assert _decode_point(geometry[1]) == (10.0, 50.0)
    assert geometry[2] is None  # null coordinate -> null geometry
    assert ids[0] != ids[1]  # identity includes tags

    # Self-locating: the row's UTC second in clear, then the identity hash,
    # so an id lookup can prune to the covering dataset window.
    assert re.fullmatch(r"20240101T120000-[0-9a-f]{16}", ids[0])
    assert re.fullmatch(r"20240101T120000-[0-9a-f]{16}", ids[1])
    assert ids[2].startswith("20240101T130000-")


def test_id_is_deterministic_across_rebuilds():
    schema = canonical_arrow_schema(_geographic_metadata())
    _, transform = geospatial_projection(schema)
    batch = _batch(schema)

    assert transform(batch).column("ib_id").to_pylist() == transform(batch).column("ib_id").to_pylist()


def test_non_geographic_dataset_is_untouched():
    metadata = IngestionMetadata(
        name="prices",
        dataset_schema=DatasetSchema(
            coordinates=[],
            variables=[Variable(name="price")],
            tags=[Tag(name="market")],
        ),
    )
    schema = canonical_arrow_schema(metadata)
    out_schema, transform = geospatial_projection(schema)

    assert transform is None
    assert out_schema is schema
