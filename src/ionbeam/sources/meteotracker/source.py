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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import shapely

from ...core.bases import RawVariable, TabularMessage
from ...core.time import TimeSpan
from ..API_sources_base import AbstractDataSourceMixin, DataChunk, DataStream, RESTSource
from .api import MeteoTracker_API, MeteoTracker_API_Error

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MeteoTrackerSource(RESTSource, AbstractDataSourceMixin):
    """
    The retrieval here works like this:

    """
    source: str = "meteotracker"
    version = 1

    mappings: List[RawVariable] = dataclasses.field(default_factory=list)

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
    cache_directory: Path = dataclasses.field(default_factory=Path)
    author_patterns: dict[str, dict[str, str]] = dataclasses.field(default_factory=dict)

    maximum_request_size: timedelta = timedelta(days = 10)
    max_time_downloading: timedelta = timedelta(seconds = 10)
    

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

        assert(self.globals is not None)
        assert(self.globals.ingestion_time_constants is not None)
        assert(self.globals.secrets is not None)
        

        self.cache_directory = self.resolve_path(self.cache_directory, type="data")

        if self.wkt_bounding_polygon is not None:
            self.bounding_polygon = shapely.from_wkt(self.wkt_bounding_polygon)


        self.api_headers = self.globals.secrets["headers"]
        self.credentials = self.globals.secrets["MeteoTracker_API"]
        self.timespan = self.globals.ingestion_time_constants.query_timespan

        logger.debug(f"MeteoTracker cache_directory: {self.cache_directory}")
        logger.debug(f"Initialialised MeteoTracker source with {self.timespan.start = }, {self.timespan.end = }")
        
        self.api = MeteoTracker_API(self.credentials, self.api_headers)

        # Set up the list of target authors
        self.authors = []
        for living_lab, patterns in self.author_patterns.items():
            for pattern, range_expression in patterns.items():
                for item in eval(range_expression):
                    author = pattern.format(item)
                    self.authors.append((living_lab, author))


    def get_data_streams(self, time_span: TimeSpan) -> Iterable[DataStream]:
        yield DataStream(
            source = self.source,
            key = "meteotracker:all",
            version = self.version,
            data = {},
        )

    def download_chunk(self, data_stream: DataStream, time_span: TimeSpan) -> DataChunk:
        sessions = {}
        all_session_data = pd.DataFrame()
        for living_lab, author in self.authors:
            try:
                author_sessions = self.api.query_sessions(author=author, time_span=time_span, items=1000)
            
            except MeteoTracker_API_Error as e:
                logger.warning(f"Query_sessions failed for author {author}\n{e}")
                continue



            for session in author_sessions:
                # Can for example find tracks for author = 'genova_living_lab_28'  with the search 'genova_living_lab_2'
                if session.id in sessions:
                    continue

                logger.debug(f"Retrieved session {session.id} for {author=}")
                data = self.api.get_session_data(session)
                data["id"] = session.id
                all_session_data = pd.concat([all_session_data, data])

                session = dataclasses.asdict(session)
                session["living_lab"] = living_lab
                session["start_time"] = session["start_time"].isoformat()
                session["end_time"] = session["end_time"].isoformat() if session["end_time"] is not None else None
                sessions[session["id"]] = session
        
        logger.debug(f"Retrieved {len(sessions)} in total.")

        return DataChunk(
            key = "meteotracker:all",
            time_span=time_span,
            source=self.source,
            version=self.version,
            empty = len(sessions) == 0,
            json=sessions,
            data=all_session_data,
        )
    
    def affected_time_spans(self, chunk: DataChunk, granularity: timedelta) -> Iterable[TimeSpan]:
        assert chunk.source == self.source, f"{chunk.source} != {self.source}"
        for session_id, session in chunk.json.items():
            start_time = datetime.fromisoformat(session["start_time"])
            yield TimeSpan.from_point(start_time, granularity)

    def emit_messages(self, relevant_chunks : Iterable[DataChunk], time_spans: Iterable[TimeSpan]) -> Iterable[TabularMessage]:
        time_spans = set(time_spans)
        for chunk in relevant_chunks:
            for session_id, session in chunk.json.items():
                assert chunk.data is not None

                data = chunk.data[chunk.data.id == session_id].copy()
                data["time"] = pd.to_datetime(data["time"])

                start_time = data["time"].min()
                end_time = data["time"].max()
                data_time_span = TimeSpan(start=start_time, end=end_time)

                if not any(start_time in ts for ts in time_spans):
                    logger.debug(f"Skipping {session_id} with {start_time = } because it's not in any of the time spans")
                    continue

                if not self.in_bounds(data):
                    logger.debug(f"Skipping {session_id} because it's not in bounds")
                    return False

    
                yield TabularMessage(
                    metadata=self.generate_metadata(
                        external_id=session_id,
                        time_span=data_time_span,
                        unstructured=session,
                    ),
                    data=data,
                )

    def in_bounds(self, df):
        if self.wkt_bounding_polygon is None:
            return True
        lat, lon = df.lat.values[0], df.lon.values[0]
        point = shapely.geometry.point.Point(lat, lon)
        return self.bounding_polygon.contains(point)

