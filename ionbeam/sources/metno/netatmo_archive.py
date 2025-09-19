import asyncio
import codecs
import json
import tarfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import AsyncIterator, Iterable, List, Optional
from uuid import uuid4

from pydantic import BaseModel

from ...core.handler import BaseHandler
from ...models.models import IngestDataCommand, StartSourceCommand
from ...utilities.parquet_tools import stream_dataframes_to_parquet
from .netatmo import netatmo_metadata
from .netatmo_processing import netatmo_dataframe_stream


class NetAtmoArchiveConfig(BaseModel):
    """Configuration for NetAtmo Archive source."""
    archives_path: Optional[Path] = None  # Backwards-compat: single archive file
    data_path: Path
    countries: Optional[List[str]] = None
    batch_size: int = 50000


class NetAtmoArchiveSource(BaseHandler[StartSourceCommand, Optional[IngestDataCommand]]):
    """Read Netatmo JSONL tar archives from a directory and emit an IngestDataCommand like a normal source."""

    def __init__(self, config: NetAtmoArchiveConfig):
        super().__init__("NetAtmoArchiveSource")
        self.config = config
        self.metadata = netatmo_metadata

    def _iter_archives_in_dir(self, archives_dir: Path) -> Iterable[Path]:
        try:
            for p in sorted(archives_dir.iterdir()):
                if not p.is_file():
                    continue
                name = p.name.lower()
                if name.endswith((".tar.gz", ".tgz", ".tar")):
                    yield p
        except Exception:
            self.logger.exception("Failed to list archives in %s", archives_dir)

    def _iter_jsonl_members(self, tar: tarfile.TarFile) -> Iterable[tarfile.TarInfo]:
        for member in tar:
            if member.isfile() and member.name.endswith(".jsonl"):
                yield member

    async def _message_stream(
        self,
        archives: Iterable[Path],
        start_time: datetime,
        end_time: datetime,
        countries: Optional[set[str]],
    ) -> AsyncIterator[dict]:
        """Async generator yielding JSON objects from JSONL members within the time/country filters across multiple archives."""
        for archive_path in archives:
            self.logger.info("Scanning archive %s", archive_path)
            try:
                with tarfile.open(name=str(archive_path), mode="r|*") as tar:
                    for member in self._iter_jsonl_members(tar):
                        # Yield control between files to keep amqp happy
                        await asyncio.sleep(0)
                        p = PurePosixPath(member.name)
                        try:
                            country = p.parts[-2].lower()
                            ts_str = p.stem  # e.g. 2025091008
                            file_ts = datetime.strptime(ts_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)
                        except Exception as e:
                            self.logger.debug("Skipping member with unexpected path format: %s (%s)", member.name, e)
                            continue

                        if countries is not None and country not in countries:
                            continue
                        if not (start_time <= file_ts <= end_time):
                            continue

                        exfile = tar.extractfile(member)
                        if exfile is None:
                            continue
                        with closing(exfile):
                            for idx, _line in enumerate(codecs.iterdecode(exfile, "utf-8", errors="replace"), start=1):
                                s = _line.rstrip("\n")
                                if not s:
                                    continue
                                try:
                                    obj = json.loads(s)
                                except Exception:
                                    continue
                                yield obj
                                if (idx % 100000) == 0:
                                    await asyncio.sleep(0)
            except Exception:
                self.logger.exception("Failed reading archive %s", archive_path)

    async def _handle(self, event: StartSourceCommand) -> Optional[IngestDataCommand]:
        self.logger.info("Processing Netatmo archive for %s to %s", event.start_time, event.end_time)

        try:
            self.config.data_path.mkdir(parents=True, exist_ok=True)
            output_path = self.config.data_path / f"{self.metadata.dataset.name}_{event.start_time}-{event.end_time}_{datetime.now(timezone.utc)}.parquet"

            country_filter = set(c.lower() for c in self.config.countries) if self.config.countries else None

            # Determine archives to process
            if self.config.archives_path is not None:
                archives = list(self._iter_archives_in_dir(self.config.archives_path))
            else:
                raise ValueError("NetAtmoArchiveConfig requires either 'archives_dir' (preferred) or 'archive_path' to be set")

            df_stream = netatmo_dataframe_stream(
                self._message_stream(archives, event.start_time, event.end_time, country_filter),
                batch_size=self.config.batch_size,
                logger=self.logger,
            )

            total_rows = await stream_dataframes_to_parquet(df_stream, output_path, schema_fields=None)

            if total_rows == 0:
                self.logger.warning("No data written from archive based on provided filters/window")
                return None

            self.logger.info("Saved %s rows to %s", total_rows, output_path)

            return IngestDataCommand(
                id=uuid4(),
                metadata=self.metadata,
                payload_location=output_path,
                start_time=event.start_time,
                end_time=event.end_time,
            )
        except Exception:
            self.logger.exception("Failed processing Netatmo archive")
            return None
