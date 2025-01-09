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

import pandas as pd

from ...core.bases import TabularMessage
from ...core.time import TimeSpan
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
    maximum_request_size: timedelta = timedelta(minutes = 20)
    max_time_downloading: timedelta = timedelta(seconds = 5)

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

    def emit_messages(self, relevant_chunks : Iterable[DataChunk], time_span: TimeSpan) -> Iterable[TabularMessage]:
        data_by_sensor_id = defaultdict(lambda : defaultdict(list))
        
        # Go through all the returned data group it by sensor_id
        relevant_chunks = list(relevant_chunks)
        logger.debug(f"Processing {len(relevant_chunks)} chunks")
        for chunk in relevant_chunks:
            for data in chunk.json:
                if data["sensorId"] not in self.sensor_id_to_station:
                    continue
                
                sensor_id = data["sensorId"]
                for time, value in zip(data["timeline"], data["values"]):
                    # Filter out NaN values
                    if value < -9000:
                        continue

                    if not time_span.start <= datetime.strptime(time, "%Y%m%d%H%M").replace(tzinfo=UTC) < time_span.end:
                        continue

                    data_by_sensor_id[sensor_id]["values"].append(value)
                    data_by_sensor_id[sensor_id]["timeline"].append(time)

        del relevant_chunks

        logger.debug("Regrouping data by station")
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
        
        logger.debug("Making dataframes")
        station_dfs = self.make_dataframes(data_by_station)
        del data_by_station
        
        for station_name, df in station_dfs.items():
            station = self.api.stations[station_name]

            metadata = dict(
                            station = dataclasses.asdict(station),
                            author = "acronet",
                        )


            yield TabularMessage(
                metadata=self.generate_metadata(
                    time_span=time_span,
                    unstructured=metadata,
                ),
                data=df,
            )
            # break


    def make_dataframes(self, data_by_station: dict) -> dict[str, pd.DataFrame]:
        station_dfs = {}

        for station_name, data in data_by_station.items():
            by_time = defaultdict(lambda: {"time": None})
            for d in data:
                col_name = f"{d['sensor_class']} [{d['sensor_unit']}]"
                for time, value in zip(d["timeline"], d["values"]):
                    by_time[time]["time"] = time
                    by_time[time][col_name] = value

            # Merge all sensor data for the station
            if by_time:
                df = pd.DataFrame.from_records(list(by_time.values()))
                df["time"] = pd.to_datetime(df["time"], format="%Y%m%d%H%M", utc = True)
                df = df.sort_values("time").reset_index(drop=True)
                station_dfs[station_name] = df


        return station_dfs