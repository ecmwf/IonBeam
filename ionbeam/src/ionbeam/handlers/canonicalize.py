# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Pure, streaming canonicalization of a single raw observation batch.

One raw ``RecordBatch`` in, one canonical Arrow table out (or ``None`` when the
batch holds no usable rows). No I/O, no metrics, no async — this is the transform
ingestion applies per batch as observations stream into the timeseries store.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import pandas as pd
import pyarrow as pa
from ionbeam_client import coerce_types
from ionbeam_client.models import DatasetSchema


@dataclass(frozen=True)
class CanonicalBatch:
    """Canonical observations ready to write to the timeseries store."""

    table: pa.Table
    tag_columns: List[str]
    timestamp_column: str
    start_time: datetime
    end_time: datetime


def canonicalize(
    batch: pa.RecordBatch, dataset_schema: DatasetSchema
) -> Optional[CanonicalBatch]:
    """Canonicalize one canonical-named batch. Returns ``None`` if nothing usable remains."""
    columns = dataset_schema.canonical_columns
    df = batch.to_pandas(
        types_mapper={pa.string(): pd.StringDtype(storage="python")}.get
    )

    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"missing declared columns: {missing}")

    df = coerce_types(df[columns], dataset_schema)

    time_name = dataset_schema.time.name
    df = df.sort_values([time_name], kind="mergesort")

    primary_columns = [variable.name for variable in dataset_schema.primary_variables]
    df = df.dropna(subset=primary_columns, how="all")

    if len(df) == 0:
        return None

    times = df[time_name]
    return CanonicalBatch(
        table=pa.Table.from_pandas(df, preserve_index=False),
        tag_columns=[tag.name for tag in dataset_schema.tags],
        timestamp_column=time_name,
        start_time=times.min().to_pydatetime(),
        end_time=times.max().to_pydatetime(),
    )
