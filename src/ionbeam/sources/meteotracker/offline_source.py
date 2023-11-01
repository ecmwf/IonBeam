# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import os
import logging
import pandas as pd
from typing import Literal, Iterable

from pathlib import Path

import dataclasses

from ...core.bases import FileMessage, Source, MetaData
from .online_source import MeteoTrackerSource


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MeteoTrackerOfflineSource(MeteoTrackerSource):
    def init(self, global_config):
        super().init(global_config)
        logger.debug(
            f"Initialialised MeteoTrackerOfflineSource source with {self.start_date=}, {self.end_date=}"
        )
        self.cache_directory = self.resolve_path(self.cache_directory, type="data")
        self.data_path = self.resolve_path(self.cache_directory, type="data")

    def generate(self) -> Iterable[FileMessage]:
        # Do  API requests in chunks larger than the data granularity, upto 3 days
        dates = pd.date_range(start=self.start_date, end=self.end_date, freq="1D")
        list(zip(dates[:-1], dates[1:]))
        emitted_messages = 0

        for path in self.data_path.iterdir():
            data = pd.read_csv(path)
            if self.in_bounds(data):
                continue

            yield FileMessage(
                metadata=self.generate_metadata(
                    filepath=path,
                ),
            )
            emitted_messages += 1

            if self.finish_after is not None and emitted_messages >= self.finish_after:
                return


source = MeteoTrackerOfflineSource
