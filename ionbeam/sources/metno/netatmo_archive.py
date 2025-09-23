import asyncio
import codecs
import json
import re
import tarfile
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
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
                        # Only process members whose hour overlaps the window (right-exclusive end)
                        file_start = file_ts
                        file_end = file_ts + timedelta(hours=1)
                        if file_end <= start_time or file_start >= end_time:
                            self.logger.info("Skipping %s", member.name)
                            continue

                        # Member accepted by filters; start processing
                        self.logger.info("Processing member %s (country=%s ts=%s)", member.name, country, file_ts.isoformat())

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

                                # Per-message time filter [start, end)
                                props = obj.get("properties") or {}
                                dt_str = props.get("datetime")
                                if not dt_str:
                                    continue
                                try:
                                    msg_dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                                except Exception:
                                    continue
                                if not (start_time <= msg_dt < end_time):
                                    continue

                                if (idx % 100000) == 0:
                                    await asyncio.sleep(0)
                                yield obj
            except Exception:
                self.logger.exception("Failed reading archive %s", archive_path)

    async def _handle(self, event: StartSourceCommand) -> Optional[IngestDataCommand]:
        try:
            self.config.data_path.mkdir(parents=True, exist_ok=True)
            output_path = self.config.data_path / f"{self.metadata.dataset.name}_{event.start_time}-{event.end_time}_{datetime.now(timezone.utc)}.parquet"

            country_filter = set(c.lower() for c in self.config.countries) if self.config.countries else None
            self.logger.info("Filters: countries=%s, batch_size=%d", sorted(country_filter) if country_filter else None, self.config.batch_size)

            # Determine archives to process (filter by hour range encoded in filename: netatmo-raw-YYYYMMDDHH-YYYYMMDDHH.tar.gz)
            if self.config.archives_path is not None:
                all_archives = list(self._iter_archives_in_dir(self.config.archives_path))
                self.logger.info("Found %d archive files in %s", len(all_archives), self.config.archives_path)

                pattern = re.compile(r'^netatmo-raw-(\d{10})-(\d{10})\.(?:tar\.gz|tgz|tar)$')
                selected: List[Path] = []
                unparsable = 0

                for p in all_archives:
                    m = pattern.match(p.name)
                    if not m:
                        unparsable += 1
                        continue
                    try:
                        a_start = datetime.strptime(m.group(1), "%Y%m%d%H").replace(tzinfo=timezone.utc)
                        # Archive end is inclusive by hour; use half-open interval by adding 1 hour
                        a_end_exclusive = datetime.strptime(m.group(2), "%Y%m%d%H").replace(tzinfo=timezone.utc) + timedelta(hours=1)
                    except Exception:
                        unparsable += 1
                        continue

                    # Overlap test with requested [start_time, end_time)
                    if a_end_exclusive <= event.start_time or a_start >= event.end_time:
                        continue

                    selected.append(p)

                if not selected:
                    self.logger.warning(
                        "No archives overlapping requested window based on filename ranges; parsable=%d unparsable=%d",
                        len(all_archives) - unparsable,
                        unparsable,
                    )
                    archives = []
                else:
                    # Process in chronological order by filename (lexicographic aligns with YYYYMMDDHH)
                    archives = sorted(selected, key=lambda x: x.name)
                    self.logger.info("Selected %d archives overlapping window; skipped %d unparsable", len(archives), unparsable)
            else:
                raise ValueError("NetAtmoArchiveConfig requires either 'archives_dir' (preferred) or 'archive_path' to be set")

            df_stream = netatmo_dataframe_stream(
                self._message_stream(archives, event.start_time, event.end_time, country_filter),
                batch_size=self.config.batch_size,
                logger=self.logger,
            )

            write_t0 = time.perf_counter()
            total_rows = await stream_dataframes_to_parquet(df_stream, output_path, schema_fields=None)
            write_elapsed = time.perf_counter() - write_t0
            self.logger.info("Parquet write completed: rows=%d elapsed=%.2fs (%.0f rows/s)", total_rows, write_elapsed, (total_rows / write_elapsed) if write_elapsed > 0 else float("inf"))

            if total_rows == 0:
                self.logger.warning("No data written from archive based on provided filters/window")
                return None

            self.logger.info("Wrote parquet file", rows=total_rows, path=str(output_path))

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
