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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from ...core.bases import TabularMessage
from ...core.time import TimeSpan
from ..API_sources_base import DataChunk, DataStream, RESTSource
from .api import CIMA_API

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class AcronetSource(RESTSource):
    """
    The retrieval here works like this:
    1) Get list of stations, sensors and sensor_class (rain, wind, etc)
    2) Construct sensor_id -> (station, sensor) mapping
    3) Use `sensors/data/{sensor_class}/all` endpoint to get all data in a time range for one sensor class
    4) 

    """
    source: str = "acronet"
    version = 1
    endpoint = "https://webdrops.cimafoundation.org/app/"
    maximum_request_size: timedelta = timedelta(days = 2)
    max_time_downloading: timedelta = timedelta(seconds = 10)

    # mappings: List[RawVariable] = dataclasses.field(default_factory=list)

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        self.api = CIMA_API(globals.secrets["ACRONET"],
                            cache_file=Path(self.globals.cache_path) / "cima_cache.pickle",
                            headers = globals.secrets["headers"],)
        
        self.sensor_id_to_station = {
            sensor.id : (station.name, sensor_name)
            for station in self.api.stations.values()
            for sensor_name, sensor in station.sensors.items()
        }


    def get_data_streams(self, time_span: TimeSpan) -> Iterable[DataStream]:
        for sensor_class_name, sensor_class in self.api.sensors.items():
            yield DataStream(
                source = self.source,
                key = f"acronet:{sensor_class_name}",
                version = self.version,
                data = sensor_class,
            )

    def download_chunk(self, data_stream: DataStream, time_span: TimeSpan) -> DataChunk:
        sensor_class = data_stream.data
        # logger.debug(f"Downloading all data in timespan for sensor_class = {sensor_class.name}")
        r = self.api.get(
                self.api.api_url + f"sensors/data/{sensor_class.name}/all",
                params={
                "from":time_span.start.strftime("%Y%m%d%H%M"),
                "to": time_span.end.strftime("%Y%m%d%H%M"),
                "aggr": 60,
                "date_as_string": True,
        })
        
        r.raise_for_status()
        json = r.json()

        times = set(datetime.strptime(t, "%Y%m%d%H%M")
                            for readings in json
                            for t in readings["timeline"])

        time_span = TimeSpan(
            start = min(times).replace(tzinfo=UTC),
            end = max(times).replace(tzinfo=UTC),
        )

        return DataChunk(
            source=data_stream.source,
            key = data_stream.key,
            version = data_stream.version,
            time_span = time_span,
            json = json,
            data = None,
        )

    def emit_messages(self, relevant_chunks : Iterable[DataChunk], time_spans: Iterable[TimeSpan]) -> Iterable[TabularMessage]:
        # Go through all the returned data group it by station
        data_by_station = defaultdict(lambda: defaultdict(list))

        relevant_chunks = list(relevant_chunks)
        t0 = datetime.now()

        columns = {}

        for chunk in relevant_chunks:
            for data in chunk.json:
                # logger.debug(f"Processing data for sensor {data}")
                if data["sensorId"] not in self.sensor_id_to_station:
                    continue

                sensor_id = data["sensorId"]
                station_name, sensor_class = self.sensor_id_to_station[sensor_id]
                sensor = self.api.sensors[sensor_class]
                
                column_name = f"{sensor_class} [{sensor.unit}]"
                
                values = np.array(data["values"], dtype=float)
                values[values < -9000] = np.nan  # or None

                df = pd.DataFrame({
                    "datetime": pd.to_datetime(data["timeline"], format="%Y%m%d%H%M", utc = True),
                    column_name: values,
                }).set_index("datetime")

                data_by_station[station_name][column_name].append(df)

        logger.debug(f"Acronet processed {len(relevant_chunks)} chunks in {datetime.now() - t0}")

        station_dataframes = []
        t0 = datetime.now()
        for station_name, column_dict in data_by_station.items():
            station = self.api.stations[station_name]
            station_dfs = []
            for col_name, partial_dfs in column_dict.items():
                partial_df = pd.concat(partial_dfs, axis=0)
                # deduplicate by time
                partial_df = partial_df[~partial_df.index.duplicated(keep='first')]
                partial_df = partial_df.sort_index()
                station_dfs.append(partial_df)
            
            df = pd.concat(station_dfs, axis=1, copy = False)

            # Copy relevant station metadata as columns into the dataframe
            self.perform_copy_metadata_columns(df, dataclasses.asdict(station))
            station_dataframes.append(df)

        combined_df = pd.concat(
            [
                df.reset_index()  # moves the current DatetimeIndex into a column named 'datetime'
                .set_index(['datetime', 'external_station_id'])
                for df in station_dataframes
            ],
            verify_integrity=True   # raises an error if duplicates exist
        )
        combined_df["author"] = "acronet"
        combined_df.reset_index(inplace=True)
        combined_df.set_index("datetime", inplace=True)
        combined_df.sort_index(inplace=True)

        for time_span in sorted(time_spans, key=lambda x: x.start):
            data = combined_df.loc[time_span.start : time_span.end]

            msg = TabularMessage(
                metadata=self.generate_metadata(
                    time_span=time_span,
                ),
                data=data,
            )
            yield msg
        logger.debug(f"Acronet: Collected by station in {datetime.now() - t0}")