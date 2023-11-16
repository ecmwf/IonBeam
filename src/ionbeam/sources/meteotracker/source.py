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
from typing import Literal, Tuple, List, Iterable

from pathlib import Path

import dataclasses

from ...core.bases import FileMessage, Source, MetaData, InputColumn
from ...core.history import PreviousActionInfo, ActionInfo

from .meteotracker import MeteoTracker_API, MeteoTracker_API_Error

import shapely

logger = logging.getLogger(__name__)

meteoname_map = {
    "genova": "GENOALL",
    "jerusalem": "TAULL",
    "llwa": "LLWA",
    "barcelona": "UBLL",
    "hasselt": "UHASSELT",
}


@dataclasses.dataclass
class MeteoTrackerSource(Source):
    secrets_file: Path
    cache_directory: Path
    start_date: str
    end_date: str
    static_metadata_columns: List[InputColumn]
    finish_after: int | None = None
    source: str = "meteotracker"
    wkt_bounding_polygon: str | None = None  # Use geojson.io to generate

    def init(self, globals):
        super().init(globals)
        logger.debug(f"Initialialised MeteoTracker source with {self.start_date=}, {self.end_date=}")
        self.secrets_file = self.resolve_path(self.secrets_file, type="config")
        self.cache_directory = self.resolve_path(self.cache_directory, type="data")
        self.cache_directory.mkdir(parents=True, exist_ok=True)

        if self.wkt_bounding_polygon is not None:
            self.bounding_polygon = shapely.from_wkt(self.wkt_bounding_polygon)

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

    def online_generate(self) -> Iterable[FileMessage]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        self.api = MeteoTracker_API(self.secrets_file)
        dates = pd.date_range(start=self.start_date, end=self.end_date, freq="1D")
        date_ranges = list(zip(dates[:-1], dates[1:]))
        emitted_messages = 0

        for timespan in date_ranges:
            try:
                sessions = self.api.query_sessions(timespan, items=1000)
            except MeteoTracker_API_Error as e:
                logger.warning(f"Query_sessions failed for timespan {timespan}\n{e}")
                continue

            logger.debug(f"Retrieved {len(sessions)} sessions for date {timespan[0].isoformat()}")

            for session in sessions:
                filename = f"MeteoTracker_{session.id}.csv"
                path = Path(self.cache_directory) / filename

                if not path.exists():
                    try:
                        data = self.api.get_session_data(session)
                    except MeteoTracker_API_Error as e:
                        logger.warning(f"get_session_data failed for session {session.id}\n{e}")
                        continue

                    for column in self.static_metadata_columns:
                        if hasattr(session, column.key):
                            data[column.name] = getattr(session, column.key)

                    data.to_csv(path, index=False)

                data = pd.read_csv(path)

                # bail out if this data is not in the target region
                if not self.in_bounds(data):
                    logger.debug(f"Skipping {path} because it's not in bounds")
                    continue

                logger.debug(f"Yielding meteotracker CSV file with columns {data.columns}")
                yield FileMessage(
                    metadata=self.generate_metadata(
                        filepath=path,
                    ),
                )
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

            if data.empty:
                logger.warning(f"{path} contains empty CSV!")
                continue

            if self.in_bounds(data):
                logger.debug(f"Skipping {path} because it's not in bounds")
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
        if self.globals.online:
            return self.online_generate()
        else:
            return self.offline_generate()


source = MeteoTrackerSource
