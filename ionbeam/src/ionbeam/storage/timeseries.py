# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncIterator, List, Optional

import pyarrow as pa

# The provenance tag: every stored row carries the id of the ingestion record
# that delivered it, so a build can select exactly its desired record set.
RECORD_ID_COLUMN = "ib_record_id"


class TimeSeriesDatabase(ABC):
    """Port for the time-series measurement buffer.

    Measurements are written and read **wide** (one column per field/tag) as
    Arrow, under their canonical column names: rows read back with the names
    they were written with, ``timestamp_column`` naming the time axis on both
    sides. Any engine-internal naming stays behind the adapter.
    """

    @abstractmethod
    def query_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        timestamp_column: str,
        record_ids: Optional[List[str]] = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Rows in ``[start_time, end_time)``. With ``record_ids``, only rows
        whose ``record_id`` tag is in the set — a record-scoped read, so a build
        composes exactly its desired records rather than whatever the range
        holds at query time."""
        pass

    @abstractmethod
    async def write(
        self,
        table: pa.Table,
        measurement: str,
        tag_columns: List[str],
        timestamp_column: str,
    ) -> None:
        pass
