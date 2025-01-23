import dataclasses
import logging
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep, time
from typing import Iterable
from unicodedata import normalize

import numpy as np
import pandas as pd
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey

from ...core.bases import Mappings, RawVariable, TabularMessage, TimeSpan
from ...singleprocess_pipeline import fmt_time
from ..API_sources_base import DataChunk, DataStream, RESTSource

logger = logging.getLogger(__name__)

def saltedmethodkey(salt):
    def _hash(self, *args, **kwargs):
        return hashkey(salt, *args, **kwargs)

    return _hash


@dataclass
class SmartCitizenKitSource(RESTSource):
    """
    API Documentation: https://developer.smartcitizen.me/#summary
    """
    mappings: Mappings = field(kw_only=True)

    maximum_request_size: timedelta = timedelta(days=10)
    minimum_request_size: timedelta = timedelta(minutes=5)
    max_time_downloading: timedelta = timedelta(minutes=1)

    cache_directory: Path = Path("inputs/smart_citizen_kit")
    endpoint = "https://api.smartcitizen.me/v0"
    cache = TTLCache(maxsize=1e5, ttl=20 * 60)  # Cache API responses for 20 minutes
    version=1

    @cachedmethod(lambda self: self.cache, key=saltedmethodkey("devices_by_tag"))
    def get_devices_by_tag(self, tag: str):
        return self.get(f"/devices?with_tags={tag}")

    @cachedmethod(lambda self: self.cache, key=saltedmethodkey("users"))
    def get_users(self, username_contains):
        return self.get(f"/users?q[username_cont]={username_contains}")

    @cachedmethod(lambda self: self.cache, key=saltedmethodkey("device"))
    def get_device(self, device_id):
        return self.get(f"/devices/{device_id}")

    @cachedmethod(lambda self: self.cache, key=saltedmethodkey("sensor"))
    def get_sensor(self, sensor_id):
        return self.get(f"/sensors/{sensor_id}")

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        self.mappings.link(globals.canonical_variables)
        self.mappings_variable_unit_dict = {(column.key, column.unit): column for column in self.mappings}
        self.source = "smart_citizen_kit"

    # @cachedmethod(lambda self: self.cache, key=saltedmethodkey("readings"))
    def get_readings(self, device_id : int, sensor_id : int, time_span: TimeSpan):
        params={
                "device_id": device_id,
                "sensor_id": sensor_id,
                "rollup": "1s",
                "function": "avg",
                "from": time_span.start.isoformat(),
                "to": time_span.end.isoformat(),
                "all_intervals": "false",
            }
        
        return self.get(
            f"/devices/{device_id}/readings",
            params=params,
        )

    def get_ICHANGE_devices(self):
        tags = ["Barcelona", "I-CHANGE"]
        devices = []
        for tag in tags:
            tag_devices = self.get_devices_by_tag(tag)
            logger.debug(f"Tag '{tag}' has {len(tag_devices)} devices")
            devices.extend(tag_devices)

        users = self.get_users("ichange")
        for user in users:
            user_devices = [self.get_device(device["id"]) for device in user["devices"]]
            logger.debug(f"User '{user['username']}' has {len(user_devices)}")
            devices.extend(user_devices)

        logger.debug(f"Found {len(devices)} devices overall for I-CHANGE.")
        return devices
    
    def get_devices_in_date_range(self, time_span: TimeSpan) -> list[dict]:
        devices = self.get_ICHANGE_devices()

        def filter_by_dates(device):


            device_timespan = TimeSpan.from_set((
                dt
                for sensor in device["data"]["sensors"]
                for dt in [
                    datetime.fromisoformat(sensor["created_at"]) if sensor["created_at"] is not None else None,
                    datetime.fromisoformat(sensor["last_reading_at"]) if sensor["last_reading_at"] is not None else None
            ]))
            if device_timespan is None:
                return False
            
            device["timespan"] = device_timespan.as_json()

            return device_timespan.overlaps(time_span)
        
        devices_in_date_range = [d for d in devices if filter_by_dates(d)]

        return devices_in_date_range


        """Get all the sensor readings in the rawest possible form,
        leave any formatting decisions for after the caching layer

        The object returned by get_readings has structure:
        ```
        {'device_id': 16030,
        'sensor_key': 'tvoc',
        'sensor_id': 113,
        'component_id': 71464,
        'rollup': '1s',
        'function': 'avg',
        'from': '2024-07-11T09:52:37Z',
        'to': '2024-07-18T09:52:37Z',
        'sample_size': 9596,
        'readings': [
            ['2024-07-18T09:52:34Z', 1],
            ...
            ]
        }
        ```
        Unfortunately the readings returned by different sensors for a device are not
        guaranteed to be at the same time points, so we have to carefully merge them.
        """


    
    def get_data_streams(self, time_span: TimeSpan) -> Iterable[DataStream]:
        """Return the possible cache keys for this source, for this date range"""
        devices_in_date_range = self.get_devices_in_date_range(time_span)
        logger.debug(f"{len(devices_in_date_range)} of those might have data in the requested date range.")
        return [DataStream(
                key = f"sck:id={device['id']}",
                source=self.source,
                data = device,
                version=self.version,
        ) for device in devices_in_date_range]
    
    # Example sensor object
    # {
    #     'id': 10,
    #     'ancestry': None,
    #     'name': 'Battery SCK',
    #     'description': 'Custom Circuit',
    #     'unit': '%',
    #     'created_at': '2015-02-02T18:18:00Z',
    #     'updated_at': '2020-12-11T16:12:40Z',
    #     'uuid': 'c9ff2784-53a7-4a84-b0fc-90ecc7e313f9',
    #     'default_key': 'bat',
    #     'datasheet': None,
    #     'unit_definition': None,
    #     'measurement': {'id': 7,
    #     'name': 'battery',
    #     'description': 'The SCK remaining battery level in percentage.',
    #     'unit': None,
    #     'uuid': 'c5964926-c2d2-4714-98b5-18f84c6f95c1',
    #     'definition': None},
    #     'value': 71.7,
    #     'prev_value': 71.7,
    #     'last_reading_at': '2014-04-04T18:30:16Z',
    #     'tags': []},

    # Example readings object
    #  {'device_id': 28,
    # 'sensor_key': 'bat',
    # 'sensor_id': 10,
    # 'component_id': 9956,
    # 'rollup': '1s',
    # 'function': 'avg',
    # 'from': '2025-01-12T10:09:38Z',
    # 'to': '2025-01-14T10:09:38Z',
    # 'sample_size': 0,
    # 'readings': []})

    # Some of the sck sensors have a hierarchy, that looks like this:
    #            |-- "DHT11 - Temperature"
    # - "DHT22" -|
    #            |-- "DHT11 - Humidity"
    # This is a little redundant, as the child sensors don't really carry any more information
    # except the measurement that we extract above
    # for s in device["data"]["sensors"]:
    #     if s["ancestry"] is not None:
    #         s = self.get_sensor(s["ancestry"])
    
    def download_chunk(self, data_stream: DataStream, time_span: TimeSpan) -> DataChunk:
        device = data_stream.data
        device_id = device["id"]
        device_timespan = time_span.from_json(device["timespan"])
        
        # Quick exit if the time spans don't overlap
        if not device_timespan.overlaps(time_span):
            return DataChunk.make_empty_chunk(data_stream, time_span)


        # logger.debug(f"Downloading data for device {device_id} in {time_span} with sensors {[s['name'] for s in device['data']['sensors']]}")
        logger.debug(f"Downloading data for device {device_id} in {time_span}")
        
        # Get the readings for each sensor
        t0 = time()
        min_time = None
        max_time = None
        sensor_data = []
        for sensor in device["data"]["sensors"]:
            # Skip if the dates don't overlap
            if sensor["last_reading_at"] is None or datetime.fromisoformat(sensor["last_reading_at"]) < time_span.start:
                # logger.debug(f"Skipping sensor {sensor['name']} because it has no readings in the requested time span")
                continue

            readings = self.get_readings(device_id, sensor["id"], time_span)
            
            # Try to reduce how often we get rate limited by SCK
            sleep(0.1)
            
            # Skip if there are no readings
            if not readings["readings"]:
                logger.debug(f"No readings returned for {sensor['name']}, even though the date metadata suggested there should be.")
                continue

            # Extract the maximum and minimum times for the chunk
            df = pd.DataFrame(readings["readings"], columns=["time", "data"])
            times = pd.to_datetime(df.time, utc=True)
            min_time = times.min() if min_time is None else min(min_time, times.min())
            max_time = times.max() if max_time is None else max(max_time, times.max())

            sensor_data.append(dict(
                sensor = sensor,
                sensor_key = readings["sensor_key"],
                readings = readings["readings"]
            ))

        if not sensor_data or min_time is None or max_time is None:
            # raise ValueError(f"No data for {device_id = } in {time_span = }")
            logger.debug(f"No data for {device_id = } in {time_span = }")
            return DataChunk.make_empty_chunk(data_stream, time_span)

        logger.debug(f"Got data for SCK device {device_id} in {fmt_time(time() - t0)}")
        return DataChunk(
            source=self.source,
            key = data_stream.key,
            version = self.version,
            time_span = TimeSpan(
                start = min_time,
                end = max_time,
                ),
            json = dict(
                sensor_data = sensor_data,
                device = device,
            ),
            data = df,
        )

    def emit_messages(self, relevant_chunks : Iterable[DataChunk], time_spans: Iterable[TimeSpan]) -> Iterable[TabularMessage]:
        """
        Emit messages corresponding to the data downloaded from the API in this time_span
        This is separate from download_data_stream_chunks to allow for more complex processing.
        This could for example involve merging data from multiple data streams.
        This happens for the Acronet API where we can download data for each sensor class
        but we want to emit messages for each station. So this method regroups the messages.
        """

        relevant_chunks = list(relevant_chunks)

        all_dfs = []
        column_metadata = {}
        for chunk in relevant_chunks:
            device = chunk.json["device"]
            sensor_data = chunk.json["sensor_data"]

            all_times : list[np.ndarray] = []
            all_readings : list[np.ndarray] = []
            all_sensor_keys : list[np.ndarray] = []
            
            for d in sensor_data:
                sensor = d["sensor"]
                sensor_key = d["sensor_key"] # e.g "tvoc" the name of the column
                readings = d["readings"]
                unit = normalize("NFKD", sensor["unit"])
                # logger.debug(f"Processing sensor {sensor_key =} {sensor['name'] = } {unit = }")
               
                mapping_key = (sensor_key, unit)
                if mapping_key not in self.mappings_variable_unit_dict:
                    logger.warning(f"Sensor {sensor_key} with unit {unit} not found in mappings!")
                    raw_variable = RawVariable(
                        key = sensor_key,
                        name = sensor["name"],
                        unit = unit,
                        canonical_variable = None,
                    )
                else:
                    raw_variable = self.mappings_variable_unit_dict[mapping_key]

                column_metadata[sensor_key] = dataclasses.replace(
                    raw_variable,
                    metadata = sensor | dict(
                        sensor_key = sensor_key,
                    )
                )

                column_metadata["external_station_id"] = self.globals.canonical_variables_by_name["external_station_id"]
                
                # logger.debug(f"Processing sensor {sensor_key} for device {device_id}")
                array = np.array(readings)
                if len(array) == 0:
                    continue
                
                times, values = array.T
                
                all_times.append(times)
                all_readings.append(values.astype(np.float64))
                all_sensor_keys.append(np.array([sensor_key] * values.shape[0]))

            a_all_times = np.concatenate(all_times)
            a_all_readings = np.concatenate(all_readings, dtype = np.float64)
            a_all_sensor_keys = np.concatenate(all_sensor_keys)

            df_long = pd.DataFrame({
                "datetime": pd.to_datetime(a_all_times, utc=True),
                "sensor_key": pd.Series(a_all_sensor_keys, dtype = "string"),
                "value": a_all_readings,
            },
            ).set_index("datetime")

            # column_metadata["datetime"] = self.mappings_variable_unit_dict[("datetime", None)]

            # 4) Pivot to get one column per sensor_key
            df_wide = df_long.pivot(columns="sensor_key", values="value")

            self.perform_copy_metadata_columns(df_wide, dict(
                device = device,
            ),
            columns = column_metadata)

            all_dfs.append(df_wide)

        if not all_dfs:
            return
        

        combined_df = pd.concat(
                    [
                        df.reset_index()  # moves the current DatetimeIndex into a column named 'datetime'
                        .set_index(['datetime', 'external_station_id'])
                        for df in all_dfs
                    ],
                    verify_integrity=False
                )
        combined_df["author"] = "smart_citizen_kit"
        combined_df["author"] = combined_df["author"].astype("string")
        
        combined_df.reset_index(inplace=True)
        combined_df.set_index("datetime", inplace=True)
        combined_df.sort_index(inplace=True)
        combined_df["external_station_id"] = combined_df["external_station_id"].astype("string")

        if "chunk_date" in combined_df.columns:
            raise ValueError("chunk_date already in the data")

        for time_span in sorted(time_spans, key=lambda x: x.start):
                data = combined_df.loc[time_span.start : time_span.end]

                msg = TabularMessage(
                    metadata=self.generate_metadata(
                        columns = deepcopy(column_metadata),
                        time_span=time_span,
                    ),
                    data=data,
                )
                yield msg