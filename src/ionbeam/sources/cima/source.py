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
from typing import Literal, List
from pathlib import Path

import dataclasses

from ...core.bases import FileMessage, Source, MetaData, InputColumn

from .cima import CIMA_API

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CIMASource(Source):
    secrets_file: Path
    cache_directory: Path
    start_date: str
    end_date: str
    frequency: str
    static_metadata_columns: List[InputColumn]
    finish_after: int | None = None

    def __str__(self):
        cls = self.__class__.__name__
        return f"{cls}({self.start_date} - {self.end_date})"

    def init(self, global_config):
        super().init(global_config)
        logger.debug(
            f"Initialialised CIMA source with {self.start_date=}, {self.end_date=}, {self.finish_after=}"
        )
        self.secrets_file = self.resolve_path(self.secrets_file, type="config")
        self.cache_directory = self.resolve_path(self.cache_directory, type="data")

    def generate(self):
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        self.api = CIMA_API(
            self.secrets_file, cache_file=Path(self.cache_directory) / "cache.pickle"
        )
        dates = pd.date_range(
            start=self.start_date, end=self.end_date, freq=self.frequency
        )
        if dates[-1] != self.end_date:
            dates = pd.DatetimeIndex(
                dates.union(
                    [
                        self.end_date,
                    ]
                )
            )

        date_ranges = list(zip(dates[:-1], dates[1:]))

        emitted_messages = 0
        for s, e in date_ranges:
            for station in self.api.stations.values():
                filename = f"CIMA_{station.id}_{s.isoformat()}_{e.isoformat()}.csv"
                path = Path(self.cache_directory) / filename

                if not path.exists():
                    data = self.api.get_data_by_station(
                        station_name=station.name,
                        start_date=s,
                        end_date=e,
                        aggregation_time_seconds=60,
                    )

                    for column in self.static_metadata_columns:
                        if hasattr(station, column.key):
                            data[column.name] = getattr(station, column.key)

                    data.to_csv(path, index=True, index_label="time")
                    logger.debug(
                        f"Yielding meteotracker CSV file with columns {data.columns}"
                    )

                yield FileMessage(
                    metadata=self.generate_metadata(
                        filepath=path,
                    ),
                )
                emitted_messages += 1
                if (
                    self.finish_after is not None
                    and emitted_messages >= self.finish_after
                ):
                    return


source = CIMASource
