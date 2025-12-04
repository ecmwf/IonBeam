# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""Arrow utilities for converting dataframes to record batches."""

import asyncio
from collections.abc import AsyncIterable, Iterable
from typing import AsyncIterator, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa

from .constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from .models import DataIngestionMap

DataFrameStream = Union[AsyncIterable[pd.DataFrame], Iterable[pd.DataFrame]]


def _arrow_type_from_dtype(dtype: Optional[object]) -> pa.DataType:
    if dtype is None:
        return pa.float64()

    if isinstance(dtype, str):
        normalized = dtype.lower()
        if normalized in {"string", "str", "object"}:
            return pa.string()
        if normalized in {"bool", "boolean"}:
            return pa.bool_()

    try:
        np_dtype = np.dtype(dtype)
    except (TypeError, ValueError):
        return pa.string()

    try:
        return pa.from_numpy_dtype(np_dtype)
    except (NotImplementedError, TypeError, ValueError):
        if np.issubdtype(np_dtype, np.bool_):
            return pa.bool_()
        return pa.string()


def schema_from_ingestion_map(ingestion_map: DataIngestionMap) -> pa.Schema:
    fields: list[pa.Field] = []
    seen: set[str] = set()

    def add_field(name: Optional[str], arrow_type: pa.DataType) -> None:
        if not name or name in seen:
            return
        fields.append(pa.field(name, arrow_type))
        seen.add(name)

    datetime_col = (
        ingestion_map.datetime.from_col
        if ingestion_map.datetime and getattr(ingestion_map.datetime, "from_col", None)
        else ObservationTimestampColumn
    )
    lat_col = (
        ingestion_map.lat.from_col
        if ingestion_map.lat and getattr(ingestion_map.lat, "from_col", None)
        else LatitudeColumn
    )
    lon_col = (
        ingestion_map.lon.from_col
        if ingestion_map.lon and getattr(ingestion_map.lon, "from_col", None)
        else LongitudeColumn
    )

    add_field(datetime_col, pa.timestamp("ns", tz="UTC"))
    add_field(lat_col, pa.float64())
    add_field(lon_col, pa.float64())

    for var in getattr(ingestion_map, "canonical_variables", []) or []:
        arrow_type = _arrow_type_from_dtype(getattr(var, "dtype", None))
        add_field(var.column, arrow_type)

    for var in getattr(ingestion_map, "metadata_variables", []) or []:
        arrow_type = _arrow_type_from_dtype(getattr(var, "dtype", None))
        add_field(var.column, arrow_type)

    return pa.schema(fields)


async def _iterate_dataframes(stream: DataFrameStream) -> AsyncIterator[pd.DataFrame]:
    if isinstance(stream, AsyncIterable):
        async for df in stream:
            yield df
    else:
        for df in stream:
            yield df
            await asyncio.sleep(0)


def _enforce_schema(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    if schema is None:
        return df

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas.DataFrame, got {type(df)!r}")

    result = df.copy()

    for field in schema:
        name = field.name
        if name not in result.columns:
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                result[name] = ""
            elif pa.types.is_boolean(field.type):
                result[name] = False
            else:
                result[name] = np.nan

    result = result[[field.name for field in schema]]

    return result


async def dataframes_to_tables(
    dataframes: DataFrameStream,
    *,
    schema: Optional[pa.Schema] = None,
    preserve_index: bool = False,
) -> AsyncIterator[pa.Table]:
    async for df in _iterate_dataframes(dataframes):
        if df is None or df.empty:
            continue

        chunk = _enforce_schema(df, schema) if schema is not None else df
        table = pa.Table.from_pandas(
            chunk, schema=schema, preserve_index=preserve_index
        )
        yield table
        await asyncio.sleep(0)


async def dataframes_to_record_batches(
    dataframes: DataFrameStream,
    *,
    schema: Optional[pa.Schema] = None,
    preserve_index: bool = False,
) -> AsyncIterator[pa.RecordBatch]:
    async for table in dataframes_to_tables(
        dataframes,
        schema=schema,
        preserve_index=preserve_index,
    ):
        for batch in table.to_batches():
            yield batch
            await asyncio.sleep(0)
