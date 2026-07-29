# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Arrow utilities for converting dataframes to record batches."""

import asyncio
from collections.abc import AsyncIterable, Iterable
from typing import AsyncIterator, Union

import pandas as pd
import pyarrow as pa

from .alignment import align_to_schema, coerce_types
from .models import DatasetMetadata, IngestionMetadata, ScalarDType
from .schema_metadata import attach_metadata

DataFrameStream = Union[AsyncIterable[pd.DataFrame], Iterable[pd.DataFrame]]


def _arrow_type_from_dtype(dtype: ScalarDType) -> pa.DataType:
    if dtype == "float64":
        return pa.float64()
    if dtype == "float32":
        return pa.float32()
    if dtype == "int64":
        return pa.int64()
    if dtype == "uint64":
        return pa.uint64()
    if dtype == "bool":
        return pa.bool_()
    if dtype == "string":
        return pa.string()
    if dtype == "datetime":
        return pa.timestamp("ns", tz="UTC")
    raise ValueError(f"unsupported scalar dtype: {dtype!r}")


def canonical_arrow_schema(
    metadata: IngestionMetadata, dataset: DatasetMetadata | None = None
) -> pa.Schema:
    """Build the canonical Arrow schema: canonical column names, declared dtypes,
    full ionbeam schema- and field-level metadata attached. ``dataset`` is the
    server-side production descriptor, attached only when the builder writes an
    output dataset file — source-produced streams omit it."""
    dataset_schema = metadata.dataset_schema
    fields: list[pa.Field] = [
        pa.field(dataset_schema.time.name, pa.timestamp("ns", tz="UTC"))
    ]
    fields.extend(
        pa.field(coordinate.name, _arrow_type_from_dtype(coordinate.dtype))
        for coordinate in dataset_schema.coordinates
    )
    fields.extend(
        pa.field(variable.name, _arrow_type_from_dtype(variable.dtype))
        for variable in dataset_schema.variables
    )
    fields.extend(pa.field(tag.name, pa.string()) for tag in dataset_schema.tags)

    return attach_metadata(pa.schema(fields), metadata, dataset)


async def _iterate_dataframes(stream: DataFrameStream) -> AsyncIterator[pd.DataFrame]:
    if isinstance(stream, AsyncIterable):
        async for df in stream:
            yield df
    else:
        for df in stream:
            yield df
            await asyncio.sleep(0)


async def canonical_record_batches(
    dataframes: DataFrameStream, metadata: IngestionMetadata
) -> AsyncIterator[pa.RecordBatch]:
    """Turn raw source frames into the canonical stream ``IonbeamClient.ingest`` sends.

    Each frame is projected onto the registered schema (canonical names, typed
    nulls for missing declared columns, undeclared columns an error) and coerced
    to the declared dtypes. Batch schemas come from the declaration rather than
    pandas inference: inferred dtypes shift with frame contents (an all-null
    column, digit-only tags), and the client and core verify every batch against
    the declared schema's stamped hash, so an inferred schema would fail the
    contract whenever a frame's contents drift.
    """
    schema = canonical_arrow_schema(metadata)
    async for df in _iterate_dataframes(dataframes):
        if df is None or df.empty:
            continue

        coerced = coerce_types(align_to_schema(df, metadata), metadata.dataset_schema)
        if coerced.empty:
            continue

        # from_pandas merges its own pandas key into the schema metadata; drop it
        # so every batch carries exactly the canonical schema.
        table = pa.Table.from_pandas(
            coerced, schema=schema, preserve_index=False
        ).replace_schema_metadata(schema.metadata)
        for batch in table.to_batches():
            yield batch
        await asyncio.sleep(0)
