import dataclasses
import logging
import time
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import Iterable

import pandas as pd
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey

from ...core.bases import TimeSpan
from ..API_sources_base import DataStream, RESTSource
from .metadata import construct_sck_metadata

logger = logging.getLogger(__name__)

def saltedmethodkey(salt):
    def _hash(self, *args, **kwargs):
        return hashkey(salt, *args, **kwargs)

    return _hash


@dataclasses.dataclass
class SmartCitizenKitSource(RESTSource):
    """
    API Documentation: https://developer.smartcitizen.me/#summary
    """

    source = "smart_citizen_kit"
    maximum_request_size = timedelta(days=10)
    cache_directory: Path = Path("inputs/smart_citizen_kit")
    endpoint = "https://api.smartcitizen.me/v0"
    cache = TTLCache(maxsize=1e5, ttl=20 * 60)  # Cache API responses for 20 minutes

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

    # def get_sensors(self, device_id):
    #     sensors = self.get_device(device_id)["data"]["sensors"]
    #     return sensors

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        self.mappings_variable_unit_dict = {(column.key, column.unit): column for column in self.mappings}

    @cachedmethod(lambda self: self.cache, key=saltedmethodkey("readings"))
    def get_readings(self, device_id : int, sensor_id : int, time_span: TimeSpan):
        return self.get(
            f"/devices/{device_id}/readings",
            params={
                "sensor_id": sensor_id,
                "rollup": "1s",
                "function": "avg",
                "from": time_span.start.isoformat() + "Z",
                "to": time_span.end.isoformat() + "Z",
            },
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
            if device["last_reading_at"] is None or device["created_at"] is None:
                return False
            device_start_date = datetime.fromisoformat(device["created_at"])
            device_end_date = datetime.fromisoformat(device["last_reading_at"])
            # see https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap
            return (device_start_date <= time_span.end) and (device_end_date >= time_span.start)

        devices_in_date_range = [d for d in devices if filter_by_dates(d)]

        return devices_in_date_range


    def get_all_sensor_data(self, chunk: DataStream, time_span : TimeSpan) -> list[dict]:
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
        device_id = chunk.data["device"]["id"]
        readings = []
        for sensor in chunk.data["data"]["sensors"]:
            readings.append(
                self.get_readings(device_id, sensor["id"], time_span)
            )   
            time.sleep(0.5)
        return readings

    def get_cache_keys(self, time_span: TimeSpan) -> list[DataStream]:
        """Return the possible cache keys for this source, for this date range"""
        devices_in_date_range = self.get_devices_in_date_range(time_span)
        logger.debug(f"{len(devices_in_date_range)} of those might have data in the requested date range.")
        return [DataStream(
                key = f"source=sck:device_id={device['id']}",
                data = device
        ) for device in devices_in_date_range]
    
    
    def download_chunk(self, cache_key: DataStream, time_span: TimeSpan) -> Iterable[tuple[dict, pd.DataFrame]]:
        chunk = cache_key.data
        sensor_data = self.get_all_sensor_data(chunk, time_span)
        station = construct_sck_metadata(self, chunk["device"], start_date = chunk["start_date"], end_date = chunk["end_date"])

        raw_metadata = dict(
            station = station,
            device = chunk["device"]
        )

        dfs = []
        def make_df(col_name, s):
            df = pd.DataFrame(s["readings"], columns=["time", col_name])
            df.time = pd.to_datetime(df.time, utc=True)
            return df


        for readings in sensor_data:
            col_name = readings["sensor_key"]
            df = make_df(col_name, readings)
            if not df[col_name].isnull().all():
                dfs.append(df)

        df = reduce(
            lambda left, right: pd.merge(left, right, on=["time"], how="outer"),
            dfs
        )
        df["station_id"] =  chunk["device"]["id"]
        df["station_name"] = chunk["device"]["name"]

        yield raw_metadata, df

