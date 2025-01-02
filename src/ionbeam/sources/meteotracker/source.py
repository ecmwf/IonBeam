# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import dataclasses
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd
import shapely

from ...core.bases import TabularMessage, TimeSpan
from ...core.time import round_datetime
from ..API_sources_base import DataStream, RESTSource
from .metadata import add_meteotracker_track_to_metadata_store
from .meteotracker import MeteoTracker_API, MeteoTracker_API_Error

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MeteoTrackerSource(RESTSource):
    "The value to insert as source"
    source: str = "meteotracker"
    cache_directory: Path = Path("inputs/meteotracker")

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

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

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
        self.api = MeteoTracker_API(self.credentials, self.api_headers)

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
    
    def get_cache_keys(self, time_span: TimeSpan) -> Iterator[DataStream]:
        """Return the possible cache keys for this source, for this date range"""
    
        # Set up the list of target authors
        authors = []
        for living_lab, patterns in self.author_patterns.items():
            for pattern, range_expression in patterns.items():
                for item in eval(range_expression):
                    author = pattern.format(item)
                    authors.append((living_lab, author))

        for living_lab, author in authors:
            try:
                author_sessions = self.api.query_sessions(author=author, time_span=time_span, items=1000)
            except MeteoTracker_API_Error as e:
                logger.warning(f"Query_sessions failed for author {author}\n{e}")
                continue
            if author_sessions:
                logger.debug(f"Retrieved {len(author_sessions)} sessions for {author=}")

            for session in author_sessions:
                session.living_lab = living_lab
                yield DataStream(
                    key = f"meteotracker:{session.id}",
                    data=session,
                )
    
    
    def download_chunk(self, cache_key: DataStream, time_span: TimeSpan) -> pd.DataFrame:
        chunk = cache_key.data
        if not self.all_filters(chunk["session"], data):
            return
        
        already_there, _ = add_meteotracker_track_to_metadata_store(self, chunk["session"], data)
        
        # Round start time to nearest 5 minute chunk
        granularity = self.globals.ingestion_time_constants.granularity
        time_span = TimeSpan(
            start = round_datetime(chunk["session"].start_time, round_to = granularity, method="floor"),
            end = round_datetime(datetime.fromisoformat(data.time.iloc[-1]), round_to = granularity, method="ceil")
        )

        yield TabularMessage(
            metadata=self.generate_metadata(
                time_span = time_span,
                unstructured = dict(session = dataclasses.asdict(chunk["session"])),
            ),
            data = data,
        )

