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
from typing import Literal, Iterable

from pathlib import Path

import dataclasses

from ...core.bases import FileMessage, MetaData
from .online_source import MeteoTrackerSource


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MeteoTrackerOfflineSource(MeteoTrackerSource):
    name: Literal["MeteoTrackerOfflineSource"]

    def __post_init__(self):
        logger.debug(f"Initialialised MeteoTrackerOfflineSource source with {self.start_date=}, {self.end_date=}")
        self.cache_directory = self.resolve_path(self.cache_directory)

    def generate(self) -> Iterable[FileMessage]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        dates = pd.date_range(start=self.start_date, end=self.end_date, freq="1D")
        list(zip(dates[:-1], dates[1:]))
        emitted_messages = 0

        for path in (Path(__file__) / "../../../../../data/inputs/meteotracker/").resolve().iterdir():
            data = pd.read_csv(path)
            if self.in_bounds(data):
                continue

            yield FileMessage(
                metadata=MetaData(
                    # source="meteotracker",
                    source=self.source,
                    observation_variable=None,  # don't know this yet
                    time_slice=None,  # don't know this yet
                    filepath=path,
                ),
            )
            emitted_messages += 1

            if self.finish_after is not None and emitted_messages >= self.finish_after:
                return


source = MeteoTrackerOfflineSource
