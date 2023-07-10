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
from typing import Literal, Iterable, List, Dict
import logging
from collections import defaultdict
from dataclasses import field
import itertools as it

import pandas as pd

from datetime import timezone, timedelta

from ..core.bases import Aggregator, MetaData, TabularMessage, FinishMessage

logger = logging.getLogger()


@dataclasses.dataclass
class TimeSliceBucket:
    """
    A sequence of messages that correspond to a unique
    (source, observation_variable, period) tuple
    """

    source: str
    observation_variable: str
    period: pd.Period
    messages: List[TabularMessage] = field(default_factory=list)

    def add_message(self, msg: TabularMessage) -> None:
        assert msg.metadata.source == self.source
        assert msg.metadata.observation_variable == self.observation_variable

        # For some reason pd.Period objects are timezone naive,
        # While the timestamps in the data are timezone aware and in UTC.
        # These periods have been generated from UTC data
        # so it should be safe to just localise them to UTC
        start = self.period.start_time.tz_localize("UTC")
        end = self.period.end_time.tz_localize("UTC")
        time = msg.data.time
        assert all((start <= time) & (time < end))

        self.messages.append(msg)

    def aggregate(self) -> TabularMessage | None:
        "Return the aggregated contents of this bucket"

        # Make an id which is source + period_start + period_size
        self.period.to_timestamp().isoformat()
        source = self.source
        observation = self.observation_variable

        # How are we going to aggregate metadata?

        data = pd.concat([m.data for m in self.messages], ignore_index=True)
        if len(data) == 0:
            return

        message = TabularMessage(
            data=data,
            metadata=MetaData(
                source=source,
                observation_variable=observation,
                time_slice=self.period,
                filepath=None,
            ),
        )
        return message


@dataclasses.dataclass
class BucketContainer:
    "Holds a set of TimeSliceBuckets and implements the logic of when to empty them"
    source: str
    observation_variable: str
    granularity: str = "1H"
    buckets: Dict[pd.Period, TimeSliceBucket] = field(default_factory=defaultdict, init=False)

    def add_message(self, msg: TabularMessage) -> None:
        "Take an incoming message, split it up into 1H granules and the store it internally"
        assert msg.metadata.source == self.source
        assert msg.metadata.observation_variable == self.observation_variable

        # Incoming messages can cover multiple time slices
        # So we have to split them up
        for period, resampled_data in msg.data.resample(
            on="time", rule=self.granularity, kind="period", origin="epoch"
        ):
            message_slice = TabularMessage(
                data=resampled_data,
                metadata=msg.metadata,
            )
            if period not in self.buckets:
                self.buckets[period] = TimeSliceBucket(self.source, self.observation_variable, period)

            self.buckets[period].add_message(message_slice)

    def emit(
        self, age: timedelta = timedelta(hours=10), direction: Literal["oldest", "youngest"] = "oldest"
    ) -> Iterable[TabularMessage]:
        """Look at the current data we have aggreated
        and emit the data that is old/young enough that we don't
        expect to see any more data in that bin"""
        periods = sorted(self.buckets.keys())

        min_p, max_p = periods[0], periods[-1]
        pivot_value = max_p - age if direction == "oldest" else min_p + age

        # put the values we want to emit at the begining
        if direction == "youngest":
            periods = periods[::-1]

        if direction == "oldest":
            to_emit = it.takewhile(lambda p: p < pivot_value, periods)
        else:
            to_emit = it.takewhile(lambda p: p > pivot_value, periods)

        for period in to_emit:
            msg = self.buckets[period].aggregate()
            if msg is not None:
                yield msg
            del self.buckets[period]

    def emit_all(self) -> Iterable[TabularMessage]:
        """Call this when no new data is coming, so clean the pipeline up nicely
        by emitting everything we have."""
        for bucket in self.buckets.values():
            msg = bucket.aggregate()
            if msg is not None:
                yield msg


@dataclasses.dataclass
class TimeAggregator(Aggregator):
    """
    Take in data messages.
    Stratify by them by key = (source, observed_variable)
    Hold each (source, observed_var, timeslice) until we're sure now more data is coming for it with `emit_after_hours`.
    Then emit a timeslice as an aggregated message.

    Internally this is done with a BucketContainer responsible for managing all data for a particular
    (source, observed_variable). The BucketContainer checks when each timeslice is reader to emit.
    """

    granularity: str = "1H"
    direction: Literal["oldest", "youngest"] = "youngest"
    emit_after_hours: int = 10

    name: Literal["TimeAggregator"] = "TimeAggregator"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.match}, {self.granularity}, {self.direction})"

    def __post_init__(self):
        self.bucket_containers: Dict[tuple, BucketContainer] = {}

    def process(self, message: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        # A finish message here means there's nothing else coming down the line,
        # so just return everything we have
        if isinstance(message, FinishMessage):
            for bucket_container in self.bucket_containers.values():
                for msg in bucket_container.emit_all():
                    yield msg
            return

        assert message.metadata.observation_variable is not None

        start_time = message.data.time.iloc[0]

        if not start_time.tzinfo == timezone.utc:
            logger.warning(f"{message}: Time is not in UTC!")

        period = pd.Period(start_time, freq=self.granularity)

        # if period.freq != pd.Period("2000/01/01", "1H").freq:
        #     logger.warning(f"granularity != 1H has not been tested! got: {period.freq}")

        # We aggregate anything that maps to this key
        key = (message.metadata.source, message.metadata.observation_variable, period)

        # Find the bucket container that manages this key and give the message to it
        # the container handles splitting the message up
        if key not in self.bucket_containers:
            self.bucket_containers[key] = BucketContainer(
                message.metadata.source, message.metadata.observation_variable, self.granularity
            )

        bucket_container = self.bucket_containers[key]
        bucket_container.add_message(message)

        # tell the bucket to go through what it has and emit all data older/younger
        # than the current data by a certain amount.
        # Direction depends on the order in which we're ingesting data
        for msg in bucket_container.emit(age=timedelta(hours=self.emit_after_hours), direction=self.direction):
            yield msg
