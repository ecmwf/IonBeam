from typing import AsyncIterator, Awaitable, Callable, List, Optional

import pandas as pd
import pyarrow as pa
import pytest


class MockArrowStore:
    """In-memory mock Arrow store for testing dataset builds."""

    def __init__(self) -> None:
        self._storage: dict[str, List[pa.RecordBatch]] = {}
        self._schemas: dict[str, Optional[pa.Schema]] = {}

    async def write_record_batches(
        self,
        key: str,
        batch_stream: AsyncIterator[pa.RecordBatch],
        schema: Optional[pa.Schema] = None,
        overwrite: bool = False,
    ) -> int:
        if key in self._storage and not overwrite:
            raise FileExistsError(f"Object already exists for key '{key}'")

        if overwrite:
            self._storage.pop(key, None)
            self._schemas.pop(key, None)

        batches: List[pa.RecordBatch] = []
        total_rows = 0
        actual_schema = schema
        async for batch in batch_stream:
            batches.append(batch)
            total_rows += batch.num_rows
            if actual_schema is None:
                actual_schema = batch.schema

        self._storage[key] = batches
        self._schemas[key] = actual_schema
        return total_rows

    def read_record_batches(
        self,
        key: str,
        batch_size: Optional[int] = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        async def _generator():
            for batch in self._storage.get(key, []):
                yield batch

        return _generator()

    async def delete(self, key: str) -> None:
        self._storage.pop(key, None)
        self._schemas.pop(key, None)

    async def exists(self, key: str) -> bool:
        return key in self._storage

    def list_keys(self) -> List[str]:
        return list(self._storage.keys())

    def get_batches(self, key: str) -> List[pa.RecordBatch]:
        return self._storage.get(key, [])

    def get_total_rows(self, key: str) -> int:
        return sum(batch.num_rows for batch in self._storage.get(key, []))

    def clear(self) -> None:
        self._storage.clear()
        self._schemas.clear()


@pytest.fixture
def mock_arrow_store() -> MockArrowStore:
    """Provide a mock Arrow store for tests."""
    return MockArrowStore()


@pytest.fixture
def arrow_store_writer(
    mock_arrow_store: MockArrowStore,
) -> Callable[[str, pd.DataFrame, Optional[pa.Schema], bool], Awaitable[int]]:
    async def _writer(
        key: str,
        df: pd.DataFrame,
        schema: Optional[pa.Schema] = None,
        overwrite: bool = False,
    ) -> int:
        if schema is None:
            schema = pa.Table.from_pandas(df, preserve_index=False).schema

        async def stream():
            yield pa.RecordBatch.from_pandas(df, schema=schema, preserve_index=False)

        return await mock_arrow_store.write_record_batches(
            key,
            stream(),
            schema=schema,
            overwrite=overwrite,
        )

    return _writer
