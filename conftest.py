# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncIterator, Awaitable, Callable, List, Optional

import pandas as pd
import pyarrow as pa
import pytest


class FakeArrowStore:
    """In-memory, write-once Arrow store for the exporter tests."""

    def __init__(self) -> None:
        self._storage: dict[str, List[pa.RecordBatch]] = {}

    async def write_record_batches(
        self,
        key: str,
        batch_stream: AsyncIterator[pa.RecordBatch],
        schema: Optional[pa.Schema] = None,
    ) -> int:
        if key in self._storage:
            raise FileExistsError(f"Object already exists for key '{key}'")

        batches: List[pa.RecordBatch] = []
        total_rows = 0
        async for batch in batch_stream:
            batches.append(batch)
            total_rows += batch.num_rows

        self._storage[key] = batches
        return total_rows

    def read_record_batches(self, key: str) -> AsyncIterator[pa.RecordBatch]:
        async def _generator():
            for batch in self._storage.get(key, []):
                yield batch

        return _generator()

    async def exists(self, key: str) -> bool:
        return key in self._storage

    def list_keys(self) -> List[str]:
        return list(self._storage.keys())


@pytest.fixture
def arrow_store() -> FakeArrowStore:
    """Provide a mock Arrow store for tests."""
    return FakeArrowStore()


@pytest.fixture
def arrow_store_writer(
    arrow_store: FakeArrowStore,
) -> Callable[[str, pd.DataFrame, Optional[pa.Schema]], Awaitable[int]]:
    async def _writer(
        key: str,
        df: pd.DataFrame,
        schema: Optional[pa.Schema] = None,
    ) -> int:
        if schema is None:
            schema = pa.Table.from_pandas(df, preserve_index=False).schema

        async def stream():
            yield pa.RecordBatch.from_pandas(df, schema=schema, preserve_index=False)

        return await arrow_store.write_record_batches(key, stream(), schema=schema)

    return _writer
