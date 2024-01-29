# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import logging
import pandas as pd
import re
from typing import Literal, Tuple, List, Iterable

from pathlib import Path

import dataclasses

from ...core.bases import FileMessage, Source, MetaData, InputColumn
from ...core.history import PreviousActionInfo, ActionInfo

from .meteotracker import MeteoTracker_API, MeteoTracker_API_Error

import shapely
import yaml
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MeteoTrackerSource(Source):
    secrets_file: Path
    cache_directory: Path

    "The time interval to ingest, can be overidden by globals.source_timespan"
    start_date: str
    end_date: str

    "How many messages to emit before stopping, useful to debug runs."
    finish_after: int | None = None

    "The value to insert as source"
    source: str = "meteotracker"

    "A bounding polygon represented in WKT (well known text) format."
    wkt_bounding_polygon: str | None = None  # Use geojson.io to generate

    """A list of patterns used to generate author names to check. eg 
        author_patterns:
            Bologna:
                bologna_living_lab_{}: range(1,21)
            Barcelona:
                barcelona_living_lab_{}: range(1, 16)
                Barcelona_living_lab_{}: range(1, 16)
    """
    author_patterns: dict[str, dict[str, str]] = dataclasses.field(default_factory=dict)

    def init(self, globals):
        super().init(globals)
        logger.debug(f"Initialialised MeteoTracker source with {self.start_date=}, {self.end_date=}")
        
        # If the timespan of interest is being overridden
        if self.globals.ingestion_time_constants is not None:
            self.start_date, self.end_date = self.globals.ingestion_time_constants.query_timespan
        else:
            self.start_date = datetime.fromisoformat(self.start_date)
            self.end_date = datetime.fromisoformat(self.end_date)

        self.cache_directory = self.resolve_path(self.cache_directory, type="data")

        if self.wkt_bounding_polygon is not None:
            self.bounding_polygon = shapely.from_wkt(self.wkt_bounding_polygon)

        secrets_file = self.resolve_path(self.secrets_file, type="config")
        with open(secrets_file, "r") as f:
            parsed_yaml = yaml.safe_load(f)
            self.api_headers = parsed_yaml["headers"]
            self.credentials = parsed_yaml["MeteoTracker_API"]

        logger.debug(f"cache_directory: {self.cache_directory}")

    def __str__(self):
        cls = self.__class__.__name__
        return f"{cls}({self.start_date} - {self.end_date})"

    def in_bounds(self, df):
        if self.wkt_bounding_polygon is None:
            return True
        lat, lon = df.lat.values[0], df.lon.values[0]
        point = shapely.geometry.point.Point(lat, lon)
        return self.bounding_polygon.contains(point)

    def all_filters(self, path, data: pd.DataFrame):
        # bail out if the author is not one of the living labs
        if data.empty:
            logger.warning(f"{path} contains empty CSV!")
            return False

        # bail out if this data is not in the target region
        if not self.in_bounds(data):
            logger.debug(f"Skipping {path} because it's not in bounds")
            return False

        return True

    def process_sessions(self, sessions):
        self.cache_directory.mkdir(parents=True, exist_ok=True)
        for session in sessions:
            filename = f"MeteoTracker_{session.id}.csv"
            path = Path(self.cache_directory) / filename

            if not path.exists():
                try:
                    data = self.api.get_session_data(session)
                except MeteoTracker_API_Error as e:
                    logger.warning(f"get_session_data failed for session {session.id}\n{e}")
                    continue
                print(session)
                for field in dataclasses.fields(session):
                    value = getattr(session, field.name)
                    if value is None or field.name == "columns": continue
                    data[field.name] = value

                data.to_csv(path, index=False)

            data = pd.read_csv(path)

            if not self.all_filters(path, data):
                continue

            logger.debug(f"Yielding meteotracker CSV file with columns {data.columns}")
            logger.debug(f"Author = {data['author'].iloc[0]}")
            logger.debug(f"Living_lab = {data['living_lab'].iloc[0]}")
            yield FileMessage(
                metadata=self.generate_metadata(
                    filepath=path,
                ),
            )

    def online_generate_by_daterange(self) -> Iterable[FileMessage]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        self.api = MeteoTracker_API(self.credentials, self.api_headers)
        dates = pd.date_range(start=self.start_date, end=self.end_date, freq="3D")
        date_ranges = list(zip(dates[:-1], dates[1:]))
        emitted_messages = 0

        for timespan in date_ranges:
            try:
                sessions = self.api.query_sessions(timespan, items=1000)
            except MeteoTracker_API_Error as e:
                logger.warning(f"Query_sessions failed for timespan {timespan}\n{e}")
                continue

            logger.debug(f"Retrieved {len(sessions)} sessions for date {timespan[0].isoformat()}")

            for message in self.process_sessions(sessions):
                yield message
                emitted_messages += 1
                if self.finish_after is not None and emitted_messages >= self.finish_after:
                    return

    def online_generate_by_author(self) -> Iterable[FileMessage]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        self.api = MeteoTracker_API(self.credentials, self.api_headers)
        timespan = (self.start_date, self.end_date)

        # Set up the list of target authors
        authors = []
        for living_lab, patterns in self.author_patterns.items():
            for pattern, range_expression in patterns.items():
                for item in eval(range_expression):
                    author = pattern.format(item)
                    authors.append((living_lab, author))

        sessions = []
        for living_lab, author in authors:
            try:
                author_sessions = self.api.query_sessions(author=author, timespan=timespan, items=1000)
            except MeteoTracker_API_Error as e:
                logger.warning(f"Query_sessions failed for author {author}\n{e}")
                continue
            if author_sessions:
                logger.debug(f"Retrieved {len(author_sessions)} sessions for {author=}")

            for s in author_sessions:
                s.living_lab = living_lab
            sessions.extend(author_sessions)

        logger.debug(f"Sorting sessions oldest to newest, make sure the TimeAggregator knows that!")
        sessions = sorted(sessions, key=lambda s: s.start_time)

        emitted_messages = 0
        for message in self.process_sessions(sessions):
            yield message
            emitted_messages += 1
            if self.finish_after is not None and emitted_messages >= self.finish_after:
                return

    def offline_generate(self) -> Iterable[FileMessage]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        dates = pd.date_range(start=self.start_date, end=self.end_date, freq="1D")
        list(zip(dates[:-1], dates[1:]))
        emitted_messages = 0

        for path in self.cache_directory.iterdir():
            try:
                data = pd.read_csv(path)
            except UnicodeDecodeError:
                logger.warning("{path} is not unicode!")
                continue

            if not self.all_filters(path, data):
                continue

            yield FileMessage(
                metadata=self.generate_metadata(
                    filepath=path,
                ),
                history=[
                    PreviousActionInfo(
                        action=ActionInfo(name=self.__class__.__name__, code=self.globals.code_source),
                        message=None,
                    )
                ],
            )
            emitted_messages += 1

            if self.finish_after is not None and emitted_messages >= self.finish_after:
                return

    def generate(self) -> Iterable[FileMessage]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        if not self.globals.offline:
            logger.debug("Starting in online mode...")
            # return self.online_generate_by_daterange()
            return self.online_generate_by_author()
        else:
            logger.debug("Starting in offline mode...")
            return self.offline_generate()


source = MeteoTrackerSource
