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
from typing import Literal, List, Iterable
from pathlib import Path
from datetime import datetime

import dataclasses

from ...core.bases import TabularMessage, Source, MetaData, InputColumn
from ..API_sources_base import RESTSource

from .cima import CIMA_API

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class AcronetSource(RESTSource):
    """
    """
    cache_directory: Path = Path(f"inputs/acronet")
    endpoint = "https://webdrops.cimafoundation.org/app/"
    # endpoints_url = "https://testauth.cimafoundation.org/auth/realms/webdrops/.well-known/openid-configuration"
    cache_version = 3 # increment this to invalidate the cache

    def init(self, globals):
        super().init(globals)
        self.api = CIMA_API(globals.secrets["ACRONET"],
                            cache_file=Path(self.cache_directory) / "cache.pickle",
                            headers = globals.secrets["headers"],)


    def get_chunks(self, start_date : datetime, end_date: datetime) -> Iterable[dict]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        """
        # The maximum number of days we can request is 3
        dates = pd.date_range(start=self.start_date, end=self.end_date, freq="3D")
        
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
                    station = station,
                    start = start,
                    end = end,
                )

    def download_chunk(self, chunk: dict):
            station = chunk["station"]
            try:
                data = self.load_data_from_cache(chunk["key"])
            except KeyError:
                data = self.api.get_data_by_station(
                    station_name=station.name,
                    start_date=chunk["start"],
                    end_date=chunk["end"],
                    aggregation_time_seconds=60,
                )
                self.save_data_to_cache(data, chunk["key"])

            # Copy data in from the station object
            for column in self.copy_metadata_to_columns:
                if hasattr(station, column.key):
                    data[column.name] = getattr(station, column.key)
            
            data["author"] = "Acronet"
            

            yield TabularMessage(
                metadata=self.generate_metadata(
                    variables = list(data.columns),
                    unstructured = dict(station = station)
                ),
                data = data
            )