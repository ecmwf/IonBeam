import math
from copy import copy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Iterable, Self

import pandas as pd


@dataclass(eq=True, frozen=True)
class TimeSpan:
    start: datetime
    end: datetime

    def __str__(self) -> str:
        return f"{self.start.isoformat()} - {self.end.isoformat()}"

    def __post_init__(self):
        if self.start > self.end:
            raise ValueError("start must not be after end")
        if self.start.strftime('%Z') != 'UTC' or self.end.strftime('%Z') != 'UTC':
            raise ValueError("start and end must be in UTC")
        
    def union(self, other: Self) -> Self:
        return type(self)(min(self.start, other.start), max(self.end, other.end))
    
    @classmethod
    def parse(cls, value: dict[str, str]) -> Self:
        """
        Takes a dict of the form: {start : python expression involving datetime and timedelta, end : ...} and outputs a time span
        """
        def f(s): return eval(
                    s, dict(datetime=datetime, timedelta=timedelta, timezone=timezone)
                )
        return cls(f(value["start"]), f(value["end"]))
    
    @classmethod
    def from_json(cls, value: dict[str, str]) -> Self:
        """
        Takes a dict of the form: {start : isodatetime, end : isodatetime} and outputs a time span
        """
        f = datetime.fromisoformat
        return cls(f(value["start"]), f(value["end"]))
    
    def as_json(self) -> dict[str, str]:
        """
        Outputs a dict of the form: {start : isoformat, end : isoformat}
        """
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}
    
    @classmethod
    def last(cls, days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0):
        "Return a time span that ends now and starts the given time before now i.e TimeSpan.last(days=1) returns the last 24 hours"
        now = datetime.now(UTC)
        delta = timedelta(
                days=days,
                seconds=seconds,
                microseconds=microseconds,
                milliseconds=milliseconds,
                minutes=minutes, hours=hours, weeks=weeks)
        return cls(now - delta, now)
        
    def delta(self) -> timedelta:
        """Return the duration of the time span as a timedelta"""
        return self.end - self.start
        
    def remove(self, coverage: list[Self]) -> list[Self]:
        """Return all the parts of time_span not covered by one of the time spans to coverage."""
        result = [self]

        for r in sorted(coverage, key=lambda ts: ts.start):
            new_result = []
            for ts in result:
                if r.end <= ts.start or r.start >= ts.end:
                    # No overlap
                    new_result.append(ts)
                else:
                    # There is overlap; split the time_span
                    if ts.start < r.start:
                        new_result.append(TimeSpan(ts.start, r.start))
                    if ts.end > r.end:
                        new_result.append(TimeSpan(r.end, ts.end))
            result = new_result

        return result
    
    def split(self, maximum_size: timedelta) -> list[Self]:
        """Split the time span into chunks of maximum_size"""
        if self.delta() <= maximum_size:
            return [self]
        
        n_chunks = math.floor(self.delta() / maximum_size)
        return [type(self)(self.start + i * maximum_size, self.start + (i + 1) * maximum_size)
                for i in range(n_chunks)] + [type(self)(self.start + n_chunks * maximum_size, self.end)]
    
    def split_rounded(self, size: timedelta) -> list[Self]:
        """Like split but first rounds the start and end times to the nearest multiple of size
        So if size = timedelta(minutes=5) the returned time spans will start and end at even multiples of 5 minutes
        The given time_span will also be expanded if it partially overlaps with a multiple of size
        """
        start = round_datetime(self.start, round_to = size, method="floor")
        end = round_datetime(self.end, round_to = size, method="ceil")
        
        n_points = math.floor((end - start) / size)
        return [type(self)(start = start + (i * size),
                           end  = start + ((i + 1) * size))
                            for i in range(n_points)]
    @classmethod
    def from_point(cls, dt: datetime, granularity: timedelta) -> Self:
        start = round_datetime(dt, round_to = granularity, method="floor")
        end = start + granularity
        return cls(start, end)
    

    def __contains__(self, other: datetime) -> bool:
        if not isinstance(other, datetime):
            raise TypeError('Tried to do "o in TimeSpan()" where d was not a datetime object')
        if not other.tzinfo:
            raise ValueError('Tried to do "o in TimeSpan()" where o was a naive datetime object')
        return self.start <= other < self.end

def round_datetime(dt: datetime, round_to: timedelta, method: str = "floor") -> datetime:
    if round_to.total_seconds() <= 0:
        raise ValueError("round_to must represent a positive duration")
    if method not in {"floor", "ceil"}:
        raise ValueError("method must be 'floor' or 'ceil'")
    
    # Calculate the number of seconds since the start of the day
    total_seconds = (dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()

    # Get the total number of seconds for the rounding interval
    rounding_seconds = round_to.total_seconds()

    if method == "floor":
        rounded_seconds = (total_seconds // rounding_seconds) * rounding_seconds
    else:  # method == "ceil"
        rounded_seconds = ((total_seconds + rounding_seconds - 1) // rounding_seconds) * rounding_seconds
    
    # Return the rounded datetime
    return dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=rounded_seconds)


def collapse_time_spans_upto_max(time_spans : list[TimeSpan], maximum_request_size : timedelta) -> list[tuple[TimeSpan, tuple[TimeSpan, ...]]]:
    """
    Given a list of date ranges, return a list of date ranges that are as large as possible but
    do not exceed the maximum_request_size
    """
    collapsed_time_spans = []
    current_spans = []
    current_range = None
    for ts in time_spans:
        if current_range is None:
            current_range = copy(ts)
            current_spans = [ts]

        # Check if we can add ts to the current time span without exceeding the maximum_request_size
        # also that the time spans are adjacent
        elif ts.end - current_range.start < maximum_request_size and ts.start == current_range.end:
            current_range.end = ts.end
            current_spans.append(ts)

        # Otherwise start a new time span
        else:
            collapsed_time_spans.append((current_range, tuple(current_spans)))
            current_range = copy(ts)
            current_spans = [ts]

    # Finalize the last group if it exists
    if current_range:
        collapsed_time_spans.append((current_range, tuple(current_spans)))

    return collapsed_time_spans


def split_df_into_time_chunks(full_df, time_span_chunks, on = "time") -> Iterable[tuple[TimeSpan, pd.DataFrame]]:
    """
    Given a dataframe and a list of time spans, return a list of dataframes that are split on the time column
    'on' is the name of the time column
    """
    for ts in time_span_chunks:
        yield ts, full_df[(full_df[on] >= ts.start) & (full_df[on] < ts.end)]

