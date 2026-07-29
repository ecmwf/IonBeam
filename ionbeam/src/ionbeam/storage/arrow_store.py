# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""ArrowStore port: keyed storage for Arrow RecordBatch streams and the JSON
documents that describe them (window manifests, registration records).
RecordBatch objects are write-once; replacement is the caller's concern
(see :mod:`ionbeam.builds`)."""

import asyncio
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, NamedTuple, Optional

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import structlog
from ionbeam_client.parquet import BufferedParquetWriter


def arrow_store_from_config(cfg: Optional[dict]) -> "ArrowStore":
    """The configured adapter for a YAML ``arrow_store:`` section."""
    cfg = cfg or {}
    adapter = cfg.get("type", "local_filesystem")
    if adapter == "s3":
        return S3ObjectStore(
            bucket=cfg["bucket"],
            prefix=cfg.get("prefix") or "datasets",
            endpoint=cfg.get("endpoint"),
            region=cfg.get("region"),
        )
    if adapter == "local_filesystem":
        return LocalFileSystemStore(Path(cfg.get("data_path", "./data/datasets")))
    raise ValueError(f"unknown arrow_store type {adapter!r}")


class StoredObject(NamedTuple):
    key: str
    written_at: datetime


class ArrowStore(ABC):
    @abstractmethod
    async def write_record_batches(
        self,
        key: str,
        batch_stream: AsyncIterator[pa.RecordBatch],
        schema: Optional[pa.Schema] = None,
        sorted_by: Optional[str] = None,
    ) -> int:
        """Persist a batch stream under ``key``; raises FileExistsError if the
        key is already written. ``sorted_by`` declares a column the stream is
        already ordered by; it is recorded for readers, not sorted."""
        pass

    @abstractmethod
    def read_record_batches(self, key: str) -> AsyncIterator[pa.RecordBatch]:
        pass

    @abstractmethod
    def read_schema(self, key: str) -> pa.Schema:
        pass

    @abstractmethod
    async def list_keys(self, prefix: str) -> list[StoredObject]:
        """Every RecordBatch key under ``prefix/`` (the whole store when
        empty), with its write time."""
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        pass

    @abstractmethod
    async def write_json(self, key: str, document: str) -> None:
        """Store ``document`` verbatim at ``key`` (which carries its own
        extension), replacing any previous version."""
        pass

    @abstractmethod
    async def read_json(self, key: str) -> Optional[str]:
        """The document at ``key``, or None if absent."""
        pass


class LocalFileSystemStore(ArrowStore):
    def __init__(self, base_path: Path):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = structlog.get_logger(__name__)

    def _get_path(self, key: str) -> Path:
        return self.base_path / f"{key}.parquet"

    async def write_record_batches(
        self,
        key: str,
        batch_stream: AsyncIterator[pa.RecordBatch],
        schema: Optional[pa.Schema] = None,
        sorted_by: Optional[str] = None,
    ) -> int:
        path = self._get_path(key)
        if path.exists():
            raise FileExistsError(f"Object already exists at {path}")
        path.parent.mkdir(parents=True, exist_ok=True)

        writer = None
        total_rows = 0
        temp_path: Optional[Path] = None

        try:
            async for batch in batch_stream:
                if writer is None:
                    temp_path = path.parent / f"{path.name}.tmp-{uuid.uuid4().hex}"
                    writer = BufferedParquetWriter(
                        temp_path, schema or batch.schema, sorted_by=sorted_by
                    )

                writer.write_batch(batch)
                total_rows += batch.num_rows
        except Exception:
            if temp_path and temp_path.exists():
                temp_path.unlink()
            raise
        finally:
            if writer is not None:
                writer.close()

        if temp_path is not None:
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
            )
        else:
            self.logger.debug(
                "No record batches written (empty stream)",
                key=key,
                path=str(path),
            )

        return total_rows

    async def read_record_batches(self, key: str) -> AsyncIterator[pa.RecordBatch]:
        parquet_file = pq.ParquetFile(self._get_path(key))
        for batch in parquet_file.iter_batches(batch_size=65536):
            await asyncio.sleep(0)
            yield batch

    def read_schema(self, key: str) -> pa.Schema:
        return pq.read_schema(self._get_path(key))

    async def list_keys(self, prefix: str) -> list[StoredObject]:
        root = self.base_path / prefix if prefix else self.base_path
        if not root.is_dir():
            return []
        return sorted(
            StoredObject(
                str(path.relative_to(self.base_path)).removesuffix(".parquet"),
                datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc),
            )
            for path in root.rglob("*.parquet")
        )

    async def delete(self, key: str) -> None:
        path = self._get_path(key)
        if path.exists():
            path.unlink()

    async def exists(self, key: str) -> bool:
        return self._get_path(key).exists()

    async def write_json(self, key: str, document: str) -> None:
        path = self.base_path / key
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.parent / f"{path.name}.tmp-{uuid.uuid4().hex}"
        temp_path.write_text(document)
        temp_path.replace(path)

    async def read_json(self, key: str) -> Optional[str]:
        path = self.base_path / key
        if not path.exists():
            return None
        return path.read_text()


class S3ObjectStore(ArrowStore):
    """S3-compatible object storage (AWS, Ceph RGW, MinIO, SeaweedFS).

    Credentials come from the standard AWS environment variables or the
    default AWS credential chain; they are never part of ionbeam config.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "datasets",
        endpoint: Optional[str] = None,
        region: Optional[str] = None,
    ):
        self.filesystem = pafs.S3FileSystem(
            endpoint_override=endpoint or None,
            region=region or None,
            allow_bucket_creation=True,
            # Overlap part uploads with parquet encoding; the writer/sink close()
            # still awaits completion, so a failed stream never publishes a partial.
            background_writes=True,
        )
        self.base_path = f"{bucket}/{prefix}".rstrip("/")
        self.filesystem.create_dir(bucket)
        self.logger = structlog.get_logger(__name__)

    def _get_path(self, key: str) -> str:
        return f"{self.base_path}/{key}.parquet"

    async def write_record_batches(
        self,
        key: str,
        batch_stream: AsyncIterator[pa.RecordBatch],
        schema: Optional[pa.Schema] = None,
        sorted_by: Optional[str] = None,
    ) -> int:
        path = self._get_path(key)
        if await self.exists(key):
            raise FileExistsError(f"Object already exists at {path}")

        # A failed stream never closes, so the multipart upload never completes
        # and no partial object is published.
        writer = None
        sink = None
        total_rows = 0

        try:
            async for batch in batch_stream:
                if writer is None:
                    sink = await asyncio.to_thread(
                        self.filesystem.open_output_stream, path
                    )
                    writer = BufferedParquetWriter(
                        sink, schema or batch.schema, sorted_by=sorted_by
                    )
                await asyncio.to_thread(writer.write_batch, batch)
                total_rows += batch.num_rows
        except Exception:
            self.logger.warning("Write failed mid-stream; upload abandoned", key=key)
            raise

        if writer is None:
            self.logger.debug(
                "No record batches written (empty stream)", key=key, path=path
            )
            return 0

        await asyncio.to_thread(writer.close)
        await asyncio.to_thread(sink.close)
        self.logger.debug(
            "Wrote record batches to object store",
            key=key,
            rows=total_rows,
            path=path,
        )
        return total_rows

    async def read_record_batches(self, key: str) -> AsyncIterator[pa.RecordBatch]:
        def _open() -> pq.ParquetFile:
            return pq.ParquetFile(self.filesystem.open_input_file(self._get_path(key)))

        parquet_file = await asyncio.to_thread(_open)

        batches = parquet_file.iter_batches(batch_size=65536)
        while True:
            batch = await asyncio.to_thread(next, batches, None)
            if batch is None:
                break
            yield batch

    def read_schema(self, key: str) -> pa.Schema:
        return pq.read_schema(self.filesystem.open_input_file(self._get_path(key)))

    async def list_keys(self, prefix: str) -> list[StoredObject]:
        root = f"{self.base_path}/{prefix}" if prefix else self.base_path

        def _list() -> list[StoredObject]:
            infos = self.filesystem.get_file_info(
                pafs.FileSelector(root, recursive=True, allow_not_found=True)
            )
            return sorted(
                StoredObject(
                    info.path[len(self.base_path) + 1 : -len(".parquet")],
                    info.mtime,
                )
                for info in infos
                if info.type == pafs.FileType.File and info.path.endswith(".parquet")
            )

        return await asyncio.to_thread(_list)

    async def delete(self, key: str) -> None:
        if await self.exists(key):
            await asyncio.to_thread(self.filesystem.delete_file, self._get_path(key))

    async def exists(self, key: str) -> bool:
        info = await asyncio.to_thread(
            self.filesystem.get_file_info, self._get_path(key)
        )
        return info.type != pafs.FileType.NotFound

    async def write_json(self, key: str, document: str) -> None:
        path = f"{self.base_path}/{key}"

        def _write() -> None:
            with self.filesystem.open_output_stream(path) as sink:
                sink.write(document.encode("utf-8"))

        await asyncio.to_thread(_write)

    async def read_json(self, key: str) -> Optional[str]:
        path = f"{self.base_path}/{key}"

        def _read() -> Optional[str]:
            if self.filesystem.get_file_info(path).type == pafs.FileType.NotFound:
                return None
            with self.filesystem.open_input_stream(path) as source:
                return source.read().decode("utf-8")

        return await asyncio.to_thread(_read)
