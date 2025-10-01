import asyncio
import codecs
import json
import re
import tarfile
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import AsyncIterator, Iterable, List, Optional, Set
from uuid import uuid4

from pydantic import BaseModel

from ...core.handler import BaseHandler
from ...models.models import IngestDataCommand, StartSourceCommand
from ...observability.metrics import IonbeamMetricsProtocol
from ...utilities.parquet_tools import stream_dataframes_to_parquet
from .netatmo import netatmo_metadata
from .netatmo_processing import netatmo_dataframe_stream


class NetAtmoArchiveConfig(BaseModel):
    """Configuration for NetAtmo Archive source."""
    archives_path: Optional[Path] = None  # Backwards-compat: single archive file
    data_path: Path
    countries: Optional[List[str]] = None
    batch_size: int = 50000
    process_all: bool = True


class NetAtmoArchiveSource(BaseHandler[StartSourceCommand, Optional[List[IngestDataCommand]]]):
    """Read Netatmo JSONL tar archives from a directory and emit IngestDataCommand"""

    def __init__(self, config: NetAtmoArchiveConfig, metrics: IonbeamMetricsProtocol):
        super().__init__("NetAtmoArchiveSource", metrics)
        self.config = config
        self.metadata = netatmo_metadata

    @staticmethod
    def _format_ts_for_path(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y%m%dT%H%M%SZ")

    def _collect_time_bounds(self, messages: List[dict]) -> Optional[tuple[datetime, datetime]]:
        times: List[datetime] = []
        for obj in messages:
            props = obj.get("properties") or {}
            dt_str = props.get("datetime")
            if not dt_str:
                continue
            try:
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                times.append(dt)
            except Exception:
                continue
        if not times:
            return None
        return min(times), max(times)

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

    def _filter_archives_by_time_window(
        self,
        archives: List[Path],
        start_time: datetime,
        end_time: datetime,
    ) -> List[Path]:
        """Filter archives based on filename timestamp ranges overlapping the requested window."""
        pattern = re.compile(r"^netatmo-raw-(\d{10})-(\d{10})\.(?:tar\.gz|tgz|tar)$")
        selected: List[Path] = []
        unparsable = 0

        for p in archives:
            m = pattern.match(p.name)
            if not m:
                unparsable += 1
                continue
            try:
                a_start = datetime.strptime(m.group(1), "%Y%m%d%H").replace(tzinfo=timezone.utc)
                a_end_exclusive = datetime.strptime(m.group(2), "%Y%m%d%H").replace(tzinfo=timezone.utc) + timedelta(hours=1)
            except Exception:
                unparsable += 1
                continue

            if a_end_exclusive <= start_time or a_start >= end_time:
                continue

            selected.append(p)

        if not selected:
            self.logger.warning(
                "No archives overlapping requested window based on filename ranges; parsable=%d unparsable=%d",
                len(archives) - unparsable,
                unparsable,
            )
        else:
            self.logger.info(
                "Selected %d archives overlapping window; skipped %d unparsable",
                len(selected),
                unparsable,
            )

        return sorted(selected, key=lambda x: x.name)

    async def _message_stream(
        self,
        archives: Iterable[Path],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        countries: Optional[Set[str]],
    ) -> AsyncIterator[dict]:
        """Async generator yielding JSON objects with optional time/country filters across multiple archives."""

        for archive_path in archives:
            self.logger.info("Scanning archive %s", archive_path)
            try:
                with tarfile.open(name=str(archive_path), mode="r|*") as tar:
                    for member in self._iter_jsonl_members(tar):
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

                        if start_time or end_time:
                            file_start = file_ts
                            file_end = file_ts + timedelta(hours=1)
                            if start_time and file_end <= start_time:
                                self.logger.debug("Skipping %s; before start window", member.name)
                                continue
                            if end_time and file_start >= end_time:
                                self.logger.debug("Skipping %s; after end window", member.name)
                                continue

                        self.logger.info(
                            "Processing member %s (country=%s ts=%s)",
                            member.name,
                            country,
                            file_ts.isoformat(),
                        )

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

                                props = obj.get("properties") or {}
                                dt_str = props.get("datetime")
                                if not dt_str:
                                    continue
                                try:
                                    msg_dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                                except Exception:
                                    continue
                                if start_time and msg_dt < start_time:
                                    continue
                                if end_time and msg_dt >= end_time:
                                    continue

                                if (idx % 100000) == 0:
                                    await asyncio.sleep(0)
                                yield obj
            except Exception:
                self.logger.exception("Failed reading archive %s", archive_path)

    async def _process_batch(self, batch: List[dict]) -> Optional[IngestDataCommand]:
        if not batch:
            return None
        source_name = self.metadata.dataset.name
        try:
            bounds = self._collect_time_bounds(batch)
            if bounds is None:
                self.logger.warning("Batch contains no valid datetimes; skipping")
                self.metrics.sources.record_ingestion_request(source_name, "no_data")
                return None
            start_time, end_time = bounds
            now_utc = datetime.now(timezone.utc)

            start_s = self._format_ts_for_path(start_time)
            end_s = self._format_ts_for_path(end_time)
            now_s = self._format_ts_for_path(now_utc)
            path = self.config.data_path / f"{self.metadata.dataset.name}_{start_s}-{end_s}_{now_s}.parquet"

            async def batch_stream():
                for obj in batch:
                    yield obj

            df_stream = netatmo_dataframe_stream(
                batch_stream(),
                batch_size=self.config.batch_size,
                logger=self.logger,
            )
            rows = await stream_dataframes_to_parquet(df_stream, path, schema_fields=None)

            if rows == 0:
                self.logger.warning("No rows written for batch", path=str(path))
                self.metrics.sources.record_ingestion_request(source_name, "no_data")
                return None

            self.metrics.sources.record_ingestion_request(source_name, "success")
            self.metrics.sources.observe_request_rows(source_name, int(rows))
            lag_seconds = max(0.0, (now_utc - start_time).total_seconds())
            self.metrics.sources.observe_data_lag(source_name, lag_seconds)

            self.logger.info(
                "Batch parquet write completed",
                rows=rows,
                path=str(path),
                start=start_time.isoformat(),
                end=end_time.isoformat(),
            )

            return IngestDataCommand(
                id=uuid4(),
                metadata=self.metadata,
                payload_location=path,
                start_time=start_time,
                end_time=end_time,
            )
        except Exception:
            self.metrics.sources.record_ingestion_request(source_name, "error")
            self.logger.exception("Failed to process batch")
            return None

    async def _process_in_batches(
        self,
        archives: List[Path],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        country_filter: Optional[Set[str]],
    ) -> List[IngestDataCommand]:
        """Process archives in batches, yielding multiple IngestDataCommands."""
        commands: List[IngestDataCommand] = []
        batch: List[dict] = []
        chunk_size = max(1, self.config.batch_size)

        message_stream = self._message_stream(archives, start_time, end_time, country_filter)

        async for message in message_stream:
            batch.append(message)
            if len(batch) >= chunk_size:
                batch_snapshot = list(batch)
                batch.clear()
                command = await self._process_batch(batch_snapshot)
                if command is not None:
                    commands.append(command)
                await asyncio.sleep(0)

        if batch:
            command = await self._process_batch(list(batch))
            if command is not None:
                commands.append(command)

        return commands

    async def _handle(self, event: StartSourceCommand) -> Optional[List[IngestDataCommand]]:
        source_name = self.metadata.dataset.name
        fetch_started = time.perf_counter()

        try:
            self.config.data_path.mkdir(parents=True, exist_ok=True)

            if self.config.archives_path is None:
                raise ValueError("NetAtmoArchiveConfig requires 'archives_path' to be set")

            all_archives = list(self._iter_archives_in_dir(self.config.archives_path))
            self.logger.info("Found %d archive files in %s", len(all_archives), self.config.archives_path)

            if not all_archives:
                self.logger.warning("No archive files found at %s", self.config.archives_path)
                self.metrics.sources.record_ingestion_request(source_name, "no_data")
                return []

            country_filter = set(c.lower() for c in self.config.countries) if self.config.countries else None
            countries_log = sorted(country_filter) if country_filter else None
            self.logger.info(
                "Filters: countries=%s, batch_size=%d, process_all=%s",
                countries_log,
                self.config.batch_size,
                self.config.process_all,
            )

            # Determine which archives to process and what time filters to apply
            if self.config.process_all:
                archives_to_process = sorted(all_archives, key=lambda x: x.name)
                commands = await self._process_in_batches(archives_to_process, None, None, country_filter)
            else:
                archives_to_process = self._filter_archives_by_time_window(
                    all_archives,
                    event.start_time,
                    event.end_time,
                )
                if not archives_to_process:
                    self.metrics.sources.record_ingestion_request(source_name, "no_data")
                    return []
                commands = await self._process_in_batches(
                    archives_to_process,
                    event.start_time,
                    event.end_time,
                    country_filter,
                )

            fetch_duration = time.perf_counter() - fetch_started
            self.metrics.sources.observe_fetch_duration(source_name, fetch_duration)

            if not commands:
                self.logger.warning("No data written from archives")
                self.metrics.sources.record_ingestion_request(source_name, "no_data")
            else:
                self.logger.info("Processing produced %d ingestion command(s)", len(commands))

            return commands

        except Exception:
            self.logger.exception("Failed processing Netatmo archive")
            self.metrics.sources.record_ingestion_request(source_name, "error")
            fetch_duration = time.perf_counter() - fetch_started
            self.metrics.sources.observe_fetch_duration(source_name, fetch_duration)
            return None
