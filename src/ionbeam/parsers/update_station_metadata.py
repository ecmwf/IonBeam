import dataclasses
import logging
import warnings
from copy import deepcopy
from time import time
from typing import Iterable

import pandas as pd
from pandas.api.types import pandas_dtype
from shapely.geometry import Point, box

from ..core.bases import CanonicalVariable, Parser, RawVariable, TabularMessage
from ..core.time import TimeSpan
from ..metadata import db
from ..singleprocess_pipeline import fmt_time

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class SetConstants(Parser):
    """
    Consume incoming messages and split them into chunks based on the value of the column specified in the action configuration.
    """
    set: dict[str, str]

    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        for k, v in self.set.items():
            assert k in self.globals.canonical_variables_by_name, f"SetConstant: Column {k} not found in the canonical variables"
            msg.data.insert(0, k, v, allow_duplicates=False)
            msg.data[k] = msg.data[k].astype(pandas_dtype(self.globals.canonical_variables_by_name[k].dtype or "string"))
            msg.metadata.columns[k] = self.globals.canonical_variables_by_name[k]
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
                columns = deepcopy(msg.metadata.columns),
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

        t0 = time()
        stations = 0

        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            groups = msg.data.groupby("station_id")
            
        for station_id, data_chunk in groups:
            stations += 1
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
            if time_span.start is None or time_span.end is None:
                raise ValueError(f"Time span for station {station_id} is None")
            
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


        logger.debug(f"UpdateStationId Updated {stations} stations in {fmt_time(time() - t0)} {fmt_time((time() - t0) / stations)} per station")
        yield msg
