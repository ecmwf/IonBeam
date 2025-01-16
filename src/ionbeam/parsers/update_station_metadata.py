import dataclasses
import logging
from typing import Iterable

import pandas as pd
from shapely.geometry import Point, box

from ..core.bases import CanonicalVariable, Parser, RawVariable, TabularMessage
from ..core.time import TimeSpan
from ..metadata import db

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class SetConstants(Parser):
    """
    Consume incoming messages and split them into chunks based on the value of the column specified in the action configuration.
    """
    set: dict[str, str]

    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        for k, v in self.set.items():
            msg.data[k] = v
        yield msg

@dataclasses.dataclass
class SplitOnColumnValue(Parser):
    """
    Consume incoming messages and split them into chunks based on the value of the column specified in the action configuration.
    """
    column: str

    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        if self.column not in msg.data.columns:
            raise ValueError(f"Column {self.column} not found in the input message")

        for value, data_chunk in msg.data.groupby(self.column):
            data_chunk[self.column] = value
            metadata = self.generate_metadata(
                message=msg,
            )

            output_msg = TabularMessage(
                metadata=metadata,
                data=data_chunk,
            )

            yield output_msg


@dataclasses.dataclass
class UpdateStationMetadata(Parser):
    """
    Split the incoming data up into groups by the column "station_id"


    """
    def create_properties_from_columns(self, columns: list[CanonicalVariable | RawVariable]) -> Iterable[dict]:
        for column in columns:
            yield dict(
                name = column.name,
                description = column.description,
                unit = column.unit,
            )

    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        assert msg.data.index.is_monotonic_increasing, "Data must be sorted by time"
        assert "station_id" in msg.data.columns, "Data must have a station_id column"
        assert isinstance(msg.data.index, pd.DatetimeIndex), "Data must have a datetime index"
        assert "lat" in msg.data.columns, "Data must have a lattitude column"
        assert "lon" in msg.data.columns, "Data must have a longitude column"

        for station_id, data_chunk in msg.data.groupby("station_id"):
            data_chunk.dropna(axis=1, how="all", inplace=True)
            data_chunk.dropna(axis=0, how="all", inplace=True)
            if data_chunk.empty:
                logger.warning(f"Data chunk for station {station_id} is empty after throwing away NaNs")
                continue

            columns = [c for c in msg.metadata.columns.values() if c.name in data_chunk.columns]

            def get(column):
                assert column in data_chunk.columns, f"Column {column} not found in the data"
                assert data_chunk[column].nunique() == 1, f"Got multiple values for {column} in the same station"
                return data_chunk[column].iloc[0]

            time_span = TimeSpan(
                start = data_chunk.index.min(),
                end = data_chunk.index.max(),
            )
            
            lat, lon = data_chunk["lat"], data_chunk["lon"]
            location = Point(lon.iloc[0], lat.iloc[0])
            bbox = box(
                lon.min(),
                lat.min(),
                lon.max(),
                lat.max(),
            )

            properties = list(self.create_properties_from_columns(columns))

            with self.globals.sql_session.begin() as db_session:
                db.Station.upsert(session = db_session, 
                                  name = get("station_name"),
                                  internal_id= get("station_id"),
                                  external_id= get("external_station_id"),
                                  aggregation_type = get("aggregation_type"),
                                  platform=get("platform"),
                                  time_span = time_span,
                                  location = location,
                                  authors=[{"name": get("author")}],
                                  bbox = bbox,
                                  properties = properties)


        yield msg