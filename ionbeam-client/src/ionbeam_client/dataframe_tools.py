# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""DataFrame utilities for ionbeam data processing."""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd
import structlog

from .models import DatasetSchema, IngestionMetadata, ScalarDType

logger = structlog.get_logger(__name__)


def typed_null_series(index: pd.Index, dtype: ScalarDType | Literal["tag", "time"]) -> pd.Series:
    """An all-null series of the pandas dtype that round-trips to the declared Arrow type."""
    if dtype == "time" or dtype == "datetime":
        return pd.Series(pd.NaT, index=index, dtype="datetime64[ns, UTC]")
    if dtype == "float64":
        return pd.Series(np.nan, index=index, dtype="float64")
    if dtype == "float32":
        return pd.Series(np.nan, index=index, dtype="float32")
    if dtype == "int64":
        return pd.Series(pd.NA, index=index, dtype="Int64")
    if dtype == "uint64":
        return pd.Series(pd.NA, index=index, dtype="UInt64")
    if dtype == "bool":
        return pd.Series(pd.NA, index=index, dtype="boolean")
    if dtype == "string" or dtype == "tag":
        return pd.Series(pd.NA, index=index, dtype="string")
    raise ValueError(f"unsupported scalar dtype: {dtype!r}")


def _declared_columns(
    dataset_schema: DatasetSchema,
) -> list[tuple[str, ScalarDType | Literal["tag", "time"]]]:
    """(canonical column, dtype) for every declared column."""
    columns: list[tuple[str, ScalarDType | Literal["tag", "time"]]] = [
        (dataset_schema.time.name, "time")
    ]
    columns.extend(
        (coordinate.name, coordinate.dtype)
        for coordinate in dataset_schema.coordinates
    )
    columns.extend(
        (variable.name, variable.dtype) for variable in dataset_schema.variables
    )
    columns.extend((tag.name, "tag") for tag in dataset_schema.tags)
    return columns


def align_to_schema(df: pd.DataFrame, metadata: IngestionMetadata) -> pd.DataFrame:
    """Project a canonical-named frame onto the registered schema.

    Frames arrive here already renamed by the source's own transform. Missing
    declared columns become typed nulls (logged); undeclared columns raise —
    declare them or drop them at the source, never ship them silently.
    """
    declared = _declared_columns(metadata.dataset_schema)
    names = {name for name, _ in declared}
    undeclared = [column for column in df.columns if column not in names]
    if undeclared:
        raise ValueError(f"undeclared columns in frame: {undeclared}")

    result = pd.DataFrame(index=df.index)
    for name, dtype in declared:
        if name in df.columns:
            result[name] = df[name]
        else:
            logger.debug(
                "Filling missing declared column with nulls",
                dataset=metadata.name,
                column=name,
                rows=len(result),
            )
            result[name] = typed_null_series(df.index, dtype)

    return result


def drop_undeclared_columns(df: pd.DataFrame, dataset_schema: DatasetSchema) -> pd.DataFrame:
    """Keep only the declared columns.

    The explicit source-side drop: a source whose frames carry working columns
    beyond the declared schema projects them away here, before alignment — which
    treats any remaining undeclared column as an error.
    """
    declared = set(dataset_schema.canonical_columns)
    return df[[column for column in df.columns if column in declared]]


def _coerce_series(series: pd.Series, dtype: ScalarDType | Literal["tag", "time"]) -> pd.Series:
    if dtype == "time" or dtype == "datetime":
        return pd.to_datetime(series, utc=True, errors="coerce")
    if dtype == "float64":
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if dtype == "float32":
        return pd.to_numeric(series, errors="coerce").astype("float32")
    if dtype == "int64":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if dtype == "uint64":
        return pd.to_numeric(series, errors="coerce").astype("UInt64")
    if dtype == "bool":
        return series.astype("boolean")
    if dtype == "string" or dtype == "tag":
        return series.astype("string")
    raise ValueError(f"unsupported scalar dtype: {dtype!r}")


def coerce_types(df: pd.DataFrame, dataset_schema: DatasetSchema) -> pd.DataFrame:
    """Coerce a canonical-named frame to its declared dtypes.

    The structural time column is coerced to UTC nanosecond timestamps. Rows whose
    structural time cannot be parsed are dropped and counted in the debug log.
    """
    result = df.copy()
    declared = _declared_columns(dataset_schema)
    expected = [name for name, _ in declared]
    missing = [name for name in expected if name not in result.columns]
    if missing:
        raise KeyError(f"missing declared columns: {missing}")

    for name, dtype in declared:
        result[name] = _coerce_series(result[name], dtype)

    time_name = dataset_schema.time.name
    bad_time = int(result[time_name].isna().sum())
    if bad_time:
        logger.debug("Dropping rows with invalid time", column=time_name, rows=bad_time)
        result = result[result[time_name].notna()]

    return result[expected]
