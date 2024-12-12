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

from ...core.bases import InputColumn, TabularMessage, TimeSpan
from ...core.time import round_datetime, split_time_interval_into_chunks
from ..API_sources_base import RESTSource
from .cima import CIMA_API

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class AcronetSource(RESTSource):
    """
    """
    source: str = "acronet"
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


    def get_chunks(self, start_date : datetime, end_date: datetime, chunk_size: timedelta) -> Iterable[dict]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        """     
        dates = split_time_interval_into_chunks(self.start_date, self.end_date, chunk_size)
        date_ranges = list(zip(dates[:-1], dates[1:]))

        for start, end in date_ranges:
            for station in list(self.api.stations.values())[:1]:
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


            granularity = self.globals.ingestion_time_constants.granularity
            time_span = TimeSpan(
                start = round_datetime(chunk["start"], round_to=granularity, method="floor"),
                end = round_datetime(chunk["end"], round_to=granularity, method="ceil")
            )

            yield TabularMessage(
                metadata=self.generate_metadata(
                    time_span = time_span,
                    unstructured = dict(station = station, 
                    start = chunk["start"], end = chunk["end"])
                ),
                data = data
            )