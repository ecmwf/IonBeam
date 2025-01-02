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
from functools import reduce
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from ...core.bases import RawVariable, TabularMessage
from ...core.time import TimeSpan, split_df_into_time_chunks
from ..API_sources_base import DataChunk, DataStream, RESTSource
from .cima import CIMA_API

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

    mappings: List[RawVariable] = dataclasses.field(default_factory=list)

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
        logger.debug(f"Downloading all data in timespan for sensor_class = {sensor_class.name}")
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

        return DataChunk(
            source=data_stream.source,
            key = data_stream.key,
            version = data_stream.version,
            time_span = time_span,
            json = json,
            data = None,
        )

    def emit_messages(self, relevant_chunks, time_spans: list[TimeSpan]) -> Iterable[TabularMessage]:
        data_by_sensor_id = defaultdict(lambda : defaultdict(list))
        # Go through all the returned data group it by sensor_id
        for chunk in relevant_chunks:
            for data in chunk.json:
                if data["sensorId"] not in self.sensor_id_to_station:
                    continue
                
                sensor_id = data["sensorId"]
                data_by_sensor_id[sensor_id]["values"].extend(data["values"])
                data_by_sensor_id[sensor_id]["timeline"].extend(data["timeline"])

        # Regroup the data by station
        data_by_station = defaultdict(list)
        for sensor_id, data in data_by_sensor_id.items():
            station_name, sensor_class = self.sensor_id_to_station[sensor_id]
            sensor = self.api.sensors[sensor_class]
            new_data = {}
            new_data["station_name"] = station_name
            new_data["sensor_class"] = sensor_class
            new_data["sensor_unit"] = sensor.unit
            new_data["values"] = data["values"]
            new_data["timeline"] = data["timeline"]
            data_by_station[station_name].append(new_data)
        
        station_dfs = self.make_dataframes(data_by_station)
        
        for station_name, df in station_dfs.items():
            station = self.api.stations[station_name]

            metadata = dict(
                            station = dataclasses.asdict(station),
                            author = "acronet",
                        )

            for time_span, df_chunk in split_df_into_time_chunks(df, time_spans):
                yield TabularMessage(
                    metadata=self.generate_metadata(
                        time_span=time_span,
                        unstructured=metadata,
                    ),
                    data=df_chunk,
                )


    
    def make_dataframes(self, data_by_station: dict) -> dict[str, pd.DataFrame]:
        station_dfs = {}
        for station_name, data in data_by_station.items():
            sensor_dfs = []
            for d in data:
                col_name = f"{d['sensor_class']} [{d['sensor_unit']}]"
                
                single_df = pd.DataFrame({
                    col_name : d["values"],
                    "time": pd.to_datetime(d["timeline"], format="%Y%m%d%H%M", utc = True),
                })

                # Values below -9000 (specifically -9998.0 but somehow sometimes others?)
                # are used as a NaN sentinel value by Acronet
                # Filter these rows out
                single_df = single_df[single_df[col_name] != -9998.0]
                single_df = single_df[~single_df[col_name].isnull()]
                
                # If this sensor has any data
                if not single_df[col_name].isnull().all():
                    sensor_dfs.append(single_df)

            # If this station has at least one sensor with data
            if sensor_dfs:
                df = reduce(
                    lambda left, right: pd.merge(left, right, on=["time"], how="outer"),
                    sensor_dfs
                )
                # df["station_id"] =  self.api.stations[station_name].id
                # df["station_name"] = station_name
                # df["author"] = "acronet"
                # df["lat"] = 
                
                station_dfs[station_name] = df
        return station_dfs