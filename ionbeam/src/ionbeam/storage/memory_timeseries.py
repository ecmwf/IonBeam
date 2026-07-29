# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from .timeseries import RECORD_ID_COLUMN, TimeSeriesDatabase


class InMemoryTimeSeriesDatabase(TimeSeriesDatabase):
    """In-memory implementation (Arrow tables) for tests and local execution.
    Unlike InfluxDB, writes do not upsert on (tags, time): a duplicate delivery
    stores a duplicate row."""

    def __init__(self):
        self._data: Dict[str, pa.Table] = {}

    async def query_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        timestamp_column: str,
        record_ids: Optional[List[str]] = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        table = self._data.get(measurement)
        if table is None or table.num_rows == 0:
            return

        time_col = table.column(timestamp_column)
        mask = pc.and_(
            pc.greater_equal(time_col, pa.scalar(start_time, type=time_col.type)),
            pc.less(time_col, pa.scalar(end_time, type=time_col.type)),
        )
        if record_ids is not None:
            mask = pc.and_(
                mask,
                pc.is_in(
                    table.column(RECORD_ID_COLUMN),
                    value_set=pa.array(sorted(record_ids), type=pa.string()),
                ),
            )
        for batch in table.filter(mask).to_batches():
            if batch.num_rows > 0:
                yield batch

    async def write(
        self,
        table: pa.Table,
        measurement: str,
        tag_columns: List[str],
        timestamp_column: str,
    ) -> None:
        if table.num_rows == 0:
            return

        existing = self._data.get(measurement)
        combined = (
            pa.concat_tables([existing, table]) if existing is not None else table
        )
        self._data[measurement] = combined.sort_by([(timestamp_column, "ascending")])
