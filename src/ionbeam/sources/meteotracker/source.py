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
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from time import time
from typing import Iterable

import numpy as np
import pandas as pd
import shapely

from ...core.bases import CanonicalVariable, RawVariable, TabularMessage
from ...core.time import TimeSpan, fmt_time
from ..API_sources_base import AbstractDataSourceMixin, DataChunk, DataStream, RESTSource
from .api import MeteoTracker_API

logger = logging.getLogger(__name__)

@dataclass
class MeteoTrackerSource(RESTSource, AbstractDataSourceMixin):
    """
    The retrieval here works like this:

    """
    source: str = "meteotracker"
    version = 1

    "A bounding polygon represented in WKT (well known text) format."
    wkt_bounding_polygon: str | None = None  # Use geojson.io to generate
    
    cache_directory: Path = field(default_factory=Path)
    author_patterns: dict[str, list[str]] = field(default_factory=dict)

    maximum_request_size: timedelta = timedelta(days = 10)
    minimum_request_size: timedelta = timedelta(minutes=5)
    max_time_downloading: timedelta = timedelta(seconds = 30)

    

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

        assert(self.globals is not None)
        assert(self.globals.ingestion_time_constants is not None)
        assert(self.globals.secrets is not None)
        
        self.cache_directory = Path(self.globals.cache_path) / "meteotracker_cache"

        if self.wkt_bounding_polygon is not None:
            self.bounding_polygon = shapely.from_wkt(self.wkt_bounding_polygon)


        self.api_headers = self.globals.secrets["headers"]
        self.credentials = self.globals.secrets["MeteoTracker_API"]
        self.timespan = self.globals.ingestion_time_constants.query_timespan

        logger.debug(f"MeteoTracker cache_directory: {self.cache_directory}")
        logger.debug(f"Initialialised MeteoTracker source with {self.timespan.start = }, {self.timespan.end = }")
        
        self.api = MeteoTracker_API(self.credentials, self.api_headers)

        self.author_regex_to_living_lab = {re.compile(pattern): living_lab 
            for living_lab, patterns in self.author_patterns.items()
            for pattern in patterns}


    def get_data_streams(self, time_span: TimeSpan) -> Iterable[DataStream]:
        yield DataStream(
            source = self.source,
            key = "meteotracker:all",
            version = self.version,
            data = {},
        )

    def download_chunk(self, data_stream: DataStream, time_span: TimeSpan) -> DataChunk:
        sessions : dict = {session.id : dict(
            meta = session,
            data = None,
        ) for session in self.api.query_sessions(time_span=time_span)}
        
        logger.info(f"Meteotracker will download {len(sessions)} sessions for this time period.")

        t0 = time()
        for session in sessions.values():
            meta = session["meta"]
            session["data"] = self.api.get_session_data(meta, raw=True)
            session["meta"] = dataclasses.asdict(meta)
        
        if sessions:
            logger.info(f"Meteotracker retrieved {len(sessions)} sessions in {fmt_time((time() - t0)/len(sessions))}")


        return DataChunk(
            key = "meteotracker:all",
            time_span=time_span,
            source=self.source,
            version=self.version,
            empty = len(sessions) == 0,
            json=sessions,
            data=None,
        )
    
    # Got rid of this because it requires looking through the data to decide what time granules are affected
    # This negates any performance benefit
    # def affected_time_spans(self, chunk: DataChunk, granularity: timedelta) -> Iterable[TimeSpan]:
    #     assert chunk.source == self.source, f"{chunk.source} != {self.source}"
    #     for session in chunk.json.values():
    #         start_time = datetime.fromisoformat(session["meta"]["start_time"])
    #         yield TimeSpan.from_point(start_time, granularity)

    def emit_messages(self, relevant_chunks : Iterable[DataChunk], time_spans: Iterable[TimeSpan]) -> Iterable[TabularMessage]:
        time_spans = set(time_spans)
        columns : dict[str, RawVariable | CanonicalVariable ] = {}
        data_frames = []
        for chunk in relevant_chunks:
            for session_id, session in chunk.json.items():
                data = session["data"]
                meta = session["meta"]

                data = pd.DataFrame.from_records(data)
                if len(data) == 0:
                    continue
                
                data[["lat", "lon"]] = np.array(data["lo"].tolist())[:, [1, 0]]
                data.drop(columns=["lo"], inplace=True)
                
                # determine the living lab
                living_labs = set(living_lab for pattern, living_lab in self.author_regex_to_living_lab.items() if pattern.match(meta['author']))
                if len(living_labs) == 0:
                    living_lab = "unknown"
                elif len(living_labs) == 1:
                    living_lab = living_labs.pop()
                    # logger.debug(f"Matched {meta['author'] = } to {living_lab = }")
                else:
                    raise ValueError(f"Multiple living labs matched for {session.author = } {living_labs = }")

                # Copy all relevant metadata into columns
                
                data.rename(columns={"time": "datetime"}, inplace=True)
                data["datetime"] = pd.to_datetime(data["datetime"], utc = True, format = "ISO8601")
                meta["living_lab"] = living_lab
                self.perform_copy_metadata_columns(data, meta, columns)
                data_frames.append(data)

        combined_df = pd.concat(
            [
                df.set_index(['datetime', 'external_station_id'])
                for df in data_frames
            ],
            verify_integrity=False
        )
        combined_df.reset_index(inplace=True)
        combined_df.set_index('datetime', inplace=True)
        combined_df.sort_index(inplace=True)

        for time_span in time_spans:
            data = combined_df.loc[time_span.start : time_span.end]
            if len(data) == 0:
                continue

            yield TabularMessage(
                metadata=self.generate_metadata(
                    time_span=time_span,
                    columns=columns,
                ),
                data=data,
            )

    # def in_bounds(self, df):
    #     if self.wkt_bounding_polygon is None:
    #         return True
    #     lat, lon = df.lat.values[0], df.lon.values[0]
    #     point = shapely.geometry.point.Point(lat, lon)
    #     return self.bounding_polygon.contains(point)