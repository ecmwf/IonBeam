# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""GeoParquet projection for built datasets.

A dataset whose schema carries geographic point coordinates gains two derived
columns at build time: a WKB ``ib_geometry`` and a stable ``ib_id``.
The geometry is encoded by hand (a point is 21 trivial bytes) so the core never
depends on shapely or geopandas. The ``geo`` schema metadata makes the written
Parquet a valid GeoParquet, readable directly by pygeoapi and every GeoArrow
consumer.

The id is self-locating: ``<UTC second stamp>-<sha1 of the identity columns>``,
e.g. ``20260717T103000-41c3100d85287e3d``. The hash alone identifies the row;
the stamp exposes the row's time in clear so an id lookup over the
time-windowed dataset files can prune to the covering window instead of
scanning them all, and so per-file id column statistics cluster by time for
engines that only see the Parquet.
"""

import hashlib
import json

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .schema_meta import find_coordinates, tag_fields, time_field

GEOMETRY_FIELD = "ib_geometry"
ID_FIELD = "ib_id"


def geographic_axes(schema: pa.Schema) -> tuple[str, str] | None:
    """The (x, y) coordinate column names if the schema is geographic, else None."""
    xs = find_coordinates(schema, axis="x", crs_kind="geographic")
    ys = find_coordinates(schema, axis="y", crs_kind="geographic")
    if xs and ys:
        return xs[0].name, ys[0].name
    return None


def _identity_columns(schema: pa.Schema) -> list[str]:
    """The columns that identify an observation row: time, coordinates, and tags
    (the series key). Measured values are deliberately excluded."""
    names = [time_field(schema).name]
    names += [field.name for field in find_coordinates(schema)]
    names += [field.name for field in tag_fields(schema)]
    return names


def geoparquet_metadata() -> dict[bytes, bytes]:
    """The GeoParquet ``geo`` schema metadata for a WKB point ``geometry`` column.
    Public so consumers persisting a streamed dataset can restore the footer if
    the transport dropped schema metadata."""
    # GeoParquet 1.1; crs omitted defaults to OGC:CRS84 (lon, lat), matching x/y.
    geo = {
        "version": "1.1.0",
        "primary_column": GEOMETRY_FIELD,
        "columns": {
            GEOMETRY_FIELD: {"encoding": "WKB", "geometry_types": ["Point"]}
        },
    }
    return {b"geo": json.dumps(geo).encode()}


def geoarrow_field_metadata() -> dict[bytes, bytes]:
    """GeoArrow extension metadata for the WKB ``geometry`` field, making the
    dataset self-describing to Arrow-native geo consumers (DuckDB spatial,
    geopandas, lonboard) on the Flight stream as well as in the written file."""
    return {
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": json.dumps(
            {"crs": "OGC:CRS84", "crs_type": "authority_code"}
        ).encode(),
    }


def _augment_schema(schema: pa.Schema) -> pa.Schema:
    fields = list(schema) + [
        pa.field(GEOMETRY_FIELD, pa.binary(), metadata=geoarrow_field_metadata()),
        pa.field(ID_FIELD, pa.string()),
    ]
    metadata = dict(schema.metadata or {})
    metadata.update(geoparquet_metadata())
    return pa.schema(fields, metadata=metadata)


def _wkb_points(x: np.ndarray, y: np.ndarray) -> pa.Array:
    n = len(x)
    valid = ~(np.isnan(x) | np.isnan(y))
    buf = np.zeros((n, 21), dtype=np.uint8)
    buf[:, 0] = 1  # little-endian byte order
    buf[:, 1] = 1  # geometry type 1 (Point) as uint32 LE -> 01 00 00 00
    buf[:, 5:13] = np.ascontiguousarray(x.astype("<f8")).view(np.uint8).reshape(n, 8)
    buf[:, 13:21] = np.ascontiguousarray(y.astype("<f8")).view(np.uint8).reshape(n, 8)
    # Wrap the packed buffer as fixed-size binary (zero-copy, no per-row Python
    # list), then null out rows with a missing coordinate.
    points = pa.FixedSizeBinaryArray.from_buffers(
        pa.binary(21), n, [None, pa.py_buffer(buf.reshape(-1))]
    )
    return pc.if_else(
        pa.array(valid), points, pa.scalar(None, type=pa.binary(21))
    ).cast(pa.binary())


def _row_ids(batch: pa.RecordBatch, time_name: str, identity_names: list[str]) -> pa.Array:
    # Arrow-native join of the identity columns (no pandas, no per-row axis=1 loop);
    # the sha1 stays per-row but over an Arrow-joined string.
    parts = [batch.column(name).cast(pa.string()) for name in identity_names]
    joined = pc.binary_join_element_wise(
        *parts, "\x1f", null_handling="replace", null_replacement=""
    )
    hashes = pa.array(
        [hashlib.sha1(value.encode()).hexdigest()[:16] for value in joined.to_pylist()],
        type=pa.string(),
    )
    # Whole-second stamp: %S renders fractional digits for sub-second units,
    # so floor to seconds first.
    seconds = pc.floor_temporal(batch.column(time_name), unit="second").cast(
        pa.timestamp("s", tz="UTC")
    )
    stamps = pc.strftime(seconds, format="%Y%m%dT%H%M%S")
    return pc.binary_join_element_wise(
        stamps, hashes, "-", null_handling="replace", null_replacement=""
    )


def geospatial_projection(schema: pa.Schema):
    """Return ``(schema, transform)`` for the built dataset.

    For a geographic dataset the schema gains ``geometry``/``id`` fields plus the
    GeoParquet ``geo`` metadata, and ``transform`` appends those two columns to
    each record batch. For a non-geographic dataset the schema is returned
    unchanged and ``transform`` is ``None`` (the profile gate)."""
    axes = geographic_axes(schema)
    if axes is None:
        return schema, None

    x_name, y_name = axes
    time_name = time_field(schema).name
    identity_names = _identity_columns(schema)
    augmented = _augment_schema(schema)

    def transform(batch: pa.RecordBatch) -> pa.RecordBatch:
        x = batch.column(x_name).to_numpy(zero_copy_only=False).astype("float64")
        y = batch.column(y_name).to_numpy(zero_copy_only=False).astype("float64")
        arrays = list(batch.columns) + [
            _wkb_points(x, y),
            _row_ids(batch, time_name, identity_names),
        ]
        return pa.RecordBatch.from_arrays(arrays, schema=augmented)

    return augmented, transform
