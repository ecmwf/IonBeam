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
from typing import Iterable
import dataclasses
from ...core.bases import TabularMessage
from .meteotracker import MeteoTracker_API, MeteoTracker_API_Error
from ..API_sources_base import RESTSource
import shapely
from pathlib import Path
from datetime import datetime
from .metadata import add_meteotracker_track_to_metadata_store

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MeteoTrackerSource(RESTSource):
    "The value to insert as source"
    source: str = "meteotracker"
    cache_directory: Path = Path(f"inputs/meteotracker")

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

        assert(self.globals is not None)
        assert(self.globals.ingestion_time_constants is not None)
        assert(self.globals.secrets is not None)
        
        self.start_date, self.end_date = self.globals.ingestion_time_constants.query_timespan
        self.cache_directory = self.resolve_path(self.cache_directory, type="data")

        if self.wkt_bounding_polygon is not None:
            self.bounding_polygon = shapely.from_wkt(self.wkt_bounding_polygon)


        self.api_headers = self.globals.secrets["headers"]
        self.credentials = self.globals.secrets["MeteoTracker_API"]

        logger.debug(f"MeteoTracker cache_directory: {self.cache_directory}")
        logger.debug(f"Initialialised MeteoTracker source with {self.start_date=}, {self.end_date=}")

    def in_bounds(self, df):
        if self.wkt_bounding_polygon is None:
            return True
        lat, lon = df.lat.values[0], df.lon.values[0]
        point = shapely.geometry.point.Point(lat, lon)
        return self.bounding_polygon.contains(point)

    def all_filters(self, session, data: pd.DataFrame):
        # bail out if this data is not in the target region
        if not self.in_bounds(data):
            logger.debug(f"Skipping {session.id} because it's not in bounds")
            return False

        return True

    def online_get_chunks_by_author(self, start_date : datetime, end_date: datetime) -> Iterable[dict]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        self.api = MeteoTracker_API(self.credentials, self.api_headers)
        timespan = (start_date, end_date)

        # Set up the list of target authors
        authors = []
        for living_lab, patterns in self.author_patterns.items():
            for pattern, range_expression in patterns.items():
                for item in eval(range_expression):
                    author = pattern.format(item)
                    authors.append((living_lab, author))

        for living_lab, author in authors:
            try:
                author_sessions = self.api.query_sessions(author=author, timespan=timespan, items=1000)
            except MeteoTracker_API_Error as e:
                logger.warning(f"Query_sessions failed for author {author}\n{e}")
                continue
            if author_sessions:
                logger.debug(f"Retrieved {len(author_sessions)} sessions for {author=}")

            for session in author_sessions:
                session.living_lab = living_lab
                yield dict(
                    key = f"MeteoTracker_{session.id}.pickle",
                    session=session,
                )

    def offline_get_chunks(self, start_date : datetime, end_date: datetime) -> Iterable[dict]:
        for path in self.cache_directory.iterdir():
            chunk, data = self.load_data_from_cache(path=path)

            def filter_by_date(data):
                if data.empty: return False
                print(data.columns)
                t = data.time.apply(pd.Timestamp)
                return (t.min() <= end_date) and (t.max() >= start_date)
            

            if not filter_by_date(data):
                continue
            if not self.all_filters(path, data):
                continue

            yield dict(
                key = path.name,
                filepath=path,
            )

    def get_chunks(self, start_date : datetime, end_date: datetime) -> Iterable[dict]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        if not self.globals.offline:
            logger.debug("Starting in online mode...")
            return self.online_get_chunks_by_author(start_date, end_date)
        else:
            logger.debug("Starting in offline mode...")
            return self.offline_get_chunks(start_date, end_date)
        
    def download_chunk(self, chunk: dict): 
        # Try to load data from the cache first
        try:
            chunk, data = self.load_data_from_cache(chunk)
        except KeyError:
            logger.debug(f"Downloading from API chunk with key {chunk['key']}")
            try:
                data = self.api.get_session_data(chunk["session"])
            except MeteoTracker_API_Error as e:
                logger.warning(f"get_session_data failed for session {chunk['session'].id}\n{e}")
                return
            self.save_data_to_cache(chunk, data)

        if not self.all_filters(chunk["session"], data):
            return
        
        already_there, _ = add_meteotracker_track_to_metadata_store(self, chunk["session"], data)


        raw_metadata = dict(session = dataclasses.asdict(chunk["session"]))
        
        yield TabularMessage(
            metadata=self.generate_metadata(
                unstructured = raw_metadata,
            ),
            data = data,
        )


source = MeteoTrackerSource
