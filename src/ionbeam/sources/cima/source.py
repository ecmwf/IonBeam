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

from ...core.bases import InputColumn, TabularMessage
from ..API_sources_base import RESTSource
from .cima import CIMA_API

logger = logging.getLogger(__name__)

def round_datetime(dt: datetime, round_to: int = 5, method: str = "floor") -> datetime:
    if round_to <= 0:
        raise ValueError("round_to must be a positive integer")
    if method not in {"floor", "ceil"}:
        raise ValueError("method must be 'floor' or 'ceil'")
    
    # Calculate the number of seconds since the start of the day
    total_seconds = (dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    
    # Calculate the rounded total seconds
    rounding_seconds = round_to * 60
    if method == "floor":
        rounded_seconds = (total_seconds // rounding_seconds) * rounding_seconds
    else:  # method == "ceil"
        rounded_seconds = ((total_seconds + rounding_seconds - 1) // rounding_seconds) * rounding_seconds
    
    # Return the rounded datetime
    return dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=rounded_seconds)


@dataclasses.dataclass
class AcronetSource(RESTSource):
    """
    """
    cache_directory: Path = Path("inputs/acronet")
    endpoint = "https://webdrops.cimafoundation.org/app/"
    # endpoints_url = "https://testauth.cimafoundation.org/auth/realms/webdrops/.well-known/openid-configuration"
    cache_version = 5 # increment this to invalidate the cache
    mappings: List[InputColumn] = dataclasses.field(default_factory=list)

    def init(self, globals):
        super().init(globals)
        self.api = CIMA_API(globals.secrets["ACRONET"],
                            cache_file=Path(self.cache_directory) / "cache.pickle",
                            headers = globals.secrets["headers"],)


    def get_chunks(self, start_date : datetime, end_date: datetime) -> Iterable[dict]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        """
        # The maximum number time range we can request is 3 days
        start = round_datetime(self.start_date, round_to=5, method="floor")
        end = round_datetime(self.start_date, round_to=5, method="ceil")
        dates = pd.date_range(start, end, freq="5min")
        
        # Ensure the last date is included
        if dates[-1] != self.end_date:
            dates = pd.DatetimeIndex(
                dates.union(
                    [
                        self.end_date,
                    ]
                )
            )

        # Convert to ranges
        date_ranges = list(zip(dates[:-1], dates[1:]))

        for start, end in date_ranges:
            for station in self.api.stations.values():
                yield dict(
                    key = f"{station.id}_{start.isoformat()}_{end.isoformat()}.pickle",
                    station = dataclasses.asdict(station),
                    start = start,
                    end = end,
                )

    def download_chunk(self, chunk: dict):
            station = chunk["station"]
            try:
                _, data = self.load_data_from_cache(chunk)
            except KeyError:
                data = self.api.get_data_by_station(
                    station_name=station["name"],
                    start_date=chunk["start"],
                    end_date=chunk["end"],
                    aggregation_time_seconds=60,
                )
                self.save_data_to_cache(chunk, data)

            data["author"] = "Acronet"

            yield TabularMessage(
                metadata=self.generate_metadata(
                    unstructured = dict(station = station, start = chunk["start"], end = chunk["end"])
                ),
                data = data
            )