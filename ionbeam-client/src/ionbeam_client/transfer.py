# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""Arrow data transfer interface and implementations for ionbeam."""

import asyncio
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import AsyncIterator, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import structlog


class ArrowStore(ABC):
    @abstractmethod
    async def write_record_batches(
        self,
        key: str,
        batch_stream: AsyncIterator[pa.RecordBatch],
        schema: Optional[pa.Schema] = None,
        overwrite: bool = False,
    ) -> int:
        pass

    @abstractmethod
    def read_record_batches(
        self, key: str, batch_size: Optional[int] = None
    ) -> AsyncIterator[pa.RecordBatch]:
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        pass


class LocalFileSystemStore(ArrowStore):
    def __init__(self, base_path: Path):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = structlog.get_logger(__name__)

    def _get_path(self, key: str) -> Path:
        if not key.endswith(".parquet"):
            key = f"{key}.parquet"
        return self.base_path / key

    async def write_record_batches(
        self,
        key: str,
        batch_stream: AsyncIterator[pa.RecordBatch],
        schema: Optional[pa.Schema] = None,
        overwrite: bool = False,
    ) -> int:
        path = self._get_path(key)

        if path.exists() and not overwrite:
            raise FileExistsError(f"Object already exists at {path}")

        path.parent.mkdir(parents=True, exist_ok=True)

        writer = None
        total_rows = 0
        temp_path: Optional[Path] = None

        try:
            async for batch in batch_stream:
                if writer is None:
                    actual_schema = schema or batch.schema
                    temp_path = path.parent / f"{path.name}.tmp-{uuid.uuid4().hex}"
                    writer = pq.ParquetWriter(temp_path, actual_schema)

                writer.write_batch(batch)
                total_rows += batch.num_rows
        except Exception:
            if writer is not None:
                writer.close()
            if temp_path and temp_path.exists():
                temp_path.unlink()
            raise
        finally:
            if writer is not None:
                writer.close()

        if writer is not None and temp_path is not None:
            try:
                temp_path.replace(path)
            except Exception:
                if temp_path.exists():
                    temp_path.unlink()
                raise

            self.logger.debug(
                "Wrote record batches to filesystem",
                key=key,
                rows=total_rows,
                path=str(path),
                overwrite=overwrite,
            )
        else:
            self.logger.debug(
                "No record batches written (empty stream)",
                key=key,
                path=str(path),
                overwrite=overwrite,
            )

        return total_rows

    async def read_record_batches(
        self, key: str, batch_size: Optional[int] = None
    ) -> AsyncIterator[pa.RecordBatch]:
        path = self._get_path(key)
        parquet_file = pq.ParquetFile(path)

        actual_batch_size = batch_size or 65536

        for batch in parquet_file.iter_batches(batch_size=actual_batch_size):
            await asyncio.sleep(0)
            yield batch

    async def delete(self, key: str) -> None:
        path = self._get_path(key)
        if path.exists():
            path.unlink()

    async def exists(self, key: str) -> bool:
        return self._get_path(key).exists()
