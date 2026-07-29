# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""InfluxDB 3 adapter for the ``TimeSeriesDatabase`` port — the scalable backbone.

Writes canonical rows via the line-protocol client; range-queries via SQL and
streams the result batch-by-batch off the Flight reader. InfluxDB stores the
timestamp under its own reserved name ``time``; that name never leaves this
adapter — the declared timestamp column maps onto it at write and back at
read. Blocking InfluxDB calls are pushed off the event loop with
``asyncio.to_thread``.
"""

import asyncio
from datetime import datetime
from typing import AsyncIterator, List, Optional

import pyarrow as pa
import structlog
from influxdb_client_3 import InfluxDBClient3

from .timeseries import RECORD_ID_COLUMN, TimeSeriesDatabase

logger = structlog.get_logger(__name__)

INFLUX_TIME_COLUMN = "time"

# InfluxDB's documented optimum is 10k lines per request; the server caps a
# request at 10 MB, which 10k canonical rows stay well under even with long tags
# (and the body ships gzipped besides).
_WRITE_CHUNK_ROWS = 10_000


class InfluxTimeSeriesDatabase(TimeSeriesDatabase):
    def __init__(
        self,
        host: str,
        database: str,
        token: Optional[str] = None,
        org: Optional[str] = None,
    ):
        # Native v3 write endpoint, all-or-nothing (a partial window must fail the
        # ingest so the bus redelivers it whole), gzipped bodies, and no_sync —
        # don't wait for the WAL fsync: ingestion is at-least-once and windows
        # rebuild from the record store, so a sub-second tail lost to a crash is
        # re-ingested, not gone.
        self._client = InfluxDBClient3(
            host=host,
            database=database,
            token=token,
            org=org,
            enable_gzip=True,
            write_use_v2_api=False,
            write_no_sync=True,
            write_accept_partial=False,
        )

    async def write(
        self,
        table: pa.Table,
        measurement: str,
        tag_columns: List[str],
        timestamp_column: str,
    ) -> None:
        if table.num_rows == 0:
            return
        if (
            INFLUX_TIME_COLUMN in table.column_names
            and timestamp_column != INFLUX_TIME_COLUMN
        ):
            raise ValueError(
                f"column '{INFLUX_TIME_COLUMN}' collides with InfluxDB's timestamp "
                f"(declared time column is '{timestamp_column}')"
            )
        await asyncio.to_thread(
            self._write_sync, table, measurement, tag_columns, timestamp_column
        )

    def _write_sync(self, table, measurement, tag_columns, timestamp_column) -> None:
        df = table.to_pandas()
        for start in range(0, len(df), _WRITE_CHUNK_ROWS):
            self._client.write(
                record=df.iloc[start : start + _WRITE_CHUNK_ROWS],
                data_frame_measurement_name=measurement,
                data_frame_tag_columns=tag_columns,
                data_frame_timestamp_column=timestamp_column,
            )

    async def query_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        timestamp_column: str,
        record_ids: Optional[List[str]] = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        # Stream off the Flight reader rather than reading the whole window into one
        # table: a wide window is millions of rows, and materializing it held the
        # entire window in core's memory at once. Errors propagate — a "not found"
        # mid-stream means the hot store lost the window's objects, and ending the
        # stream cleanly instead would let a partially drained record publish as a
        # complete build.
        reader = await asyncio.to_thread(
            self._open_reader, measurement, start_time, end_time, record_ids
        )
        while True:
            batch = await asyncio.to_thread(next, reader, None)
            if batch is None:
                break
            if batch.num_rows > 0:
                yield self._restore_timestamp(batch, timestamp_column)

    def _open_reader(
        self, measurement, start_time, end_time, record_ids=None
    ) -> pa.RecordBatchReader:
        # No ORDER BY: a global time sort buffers the whole window in server
        # memory (spill is disabled — it OOMs the DB, taking ingestion down).
        # The builder sorts in its own memory instead.
        sql = (
            f'SELECT * FROM "{measurement}" '
            "WHERE time >= CAST($start AS TIMESTAMP) "
            "AND time < CAST($end AS TIMESTAMP)"
        )
        if record_ids is not None:
            # Our own uuid strings, inlined: the client's query_parameters are
            # scalar-only, and a few hundred quoted uuids stay a small statement.
            quoted = ", ".join(f"'{record_id}'" for record_id in sorted(record_ids))
            sql += f" AND {RECORD_ID_COLUMN} IN ({quoted})"
        return self._client.query(
            sql,
            language="sql",
            mode="reader",
            query_parameters={
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            },
        )

    @staticmethod
    def _restore_timestamp(
        batch: pa.RecordBatch, timestamp_column: str
    ) -> pa.RecordBatch:
        names = [
            timestamp_column if n == INFLUX_TIME_COLUMN else n
            for n in batch.schema.names
        ]
        return pa.RecordBatch.from_arrays(list(batch.columns), names=names)
