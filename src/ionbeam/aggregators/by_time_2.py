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
from collections import defaultdict
from dataclasses import field
from datetime import datetime, time, timedelta, timezone
from typing import Iterable, Literal

import pandas as pd

from ..core.bases import (
    Aggregator,
    FinishMessage,
    TabularMessage,
)

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class NewTimeAggregator(Aggregator):
    "How much time a data granule represents."
    granularity_hours: int = 1

    "Is data arriving chronologically or the reverse?"
    time_direction: Literal["forwards", "backwards"] = "forwards"

    """Wait this long before emitting a message, this allows for data to
     be up to this many hours late and still make it into the data granule."""
    min_emit_after_hours: int = 10

    """Where T is the timespan of the longest message seen so far, always wait at least
    T2 = max(min_emit_after_hours, T) * emit_after_multiplier 
    Then don't emit any data until its it at least T2 older (younger) that the data we're currently processing.
    """
    emit_after_multiplier: int = 5

    time_chunks: dict[datetime, list] = field(default_factory=lambda: defaultdict(list))

    """If we're going forwards, the frontier is the latest __start time__ of messages seen
       If we're going backwards, the frontier is the earliest __end time__ of messages seen
    """
    time_frontier: datetime | None = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.match}, {self.granularity}, {self.time_direction})"

    def init(self, globals):
        super().init(globals)
        self.metadata = dataclasses.replace(self.metadata, state="time_aggregated")

        if self.globals.ingestion_time_constants is not None:
            self.granularity = self.globals.ingestion_time_constants.granularity
            self.time_direction = self.globals.ingestion_time_constants.time_direction

        assert self.granularity_hours == 1
        assert self.time_direction in ["forwards", "backwards"]

    def bucket_to_message(self, bucket):
        data = [m.data for m in bucket]
        return TabularMessage(
            metadata=self.metadata,
            data=pd.concat(data),
        )

    def update_time_frontier(self, start_time, end_time):
        assert start_time.tzinfo is not None
        assert end_time.tzinfo is not None

        if self.time_frontier is None:
            self.time_frontier = start_time if self.time_direction == "forwards" else end_time
            return
        if self.time_direction == "forwards":
            self.time_frontier = max(self.time_frontier, start_time)
        elif self.time_direction == "backwards":
            self.time_frontier = min(self.time_frontier, end_time)

    def process(self, message: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(message, FinishMessage):
            for _, bucket in self.time_chunks.items():
                yield self.bucket_to_message(bucket)
            return

        message_timedelta = message.data.time.max() - message.data.time.min()
        self.min_emit_after_hours = max(self.min_emit_after_hours, message_timedelta.total_seconds() / 60**2)
        self.update_time_frontier(start_time=message.data.time.min(), end_time=message.data.time.max())

        print(
            f"""
              Got message spanning time {message.data.time.min()} - {message.data.time.max()}
              Spanning {message.data.time.max() - message.data.time.min()}
              Going {self.time_direction}
              Emit frontier = {self.time_frontier - timedelta(hours=self.min_emit_after_hours)}
              {self.min_emit_after_hours=}
              {self.time_frontier=}    
              """
        )

        # convert timezone to utc and convert to time naive
        message.data.time = message.data.time.dt.tz_convert("UTC").dt.tz_localize(None)
        for (date, hour), data_chunk in message.data.groupby([message.data.time.dt.date, message.data.time.dt.hour]):
            if data_chunk.empty:
                continue
            chunked_message = dataclasses.replace(message, data=data_chunk)
            start_time = datetime.combine(date, time(hour=hour, tzinfo=timezone.utc))
            self.time_chunks[start_time].append(chunked_message)

        assert self.time_frontier is not None
        if self.time_direction == "forwards":
            t2 = self.time_frontier - timedelta(hours=self.min_emit_after_hours)

            def ready_to_emit(t):
                return t < t2

        elif self.time_direction == "backwards":
            t2 = self.time_frontier + timedelta(hours=self.min_emit_after_hours)

            def ready_to_emit(t):
                return t > t2

        for t, bucket in list(self.time_chunks.items()):
            if ready_to_emit(t):
                yield self.bucket_to_message(bucket)
                del self.time_chunks[t]
