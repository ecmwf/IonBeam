import dataclasses
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from unicodedata import normalize

import pandas as pd
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey

from ...core.bases import TabularMessage, TimeSpan
from ...core.time import split_time_interval_into_chunks
from ..API_sources_base import RESTSource
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

    def init(self, globals):
        super().init(globals)
        self.mappings_variable_unit_dict = {(column.key, column.unit): column for column in self.mappings}

    @cachedmethod(lambda self: self.cache, key=saltedmethodkey("readings"))
    def get_readings(self, device_id, sensor_id, start_date, end_date):
        return self.get(
            f"/devices/{device_id}/readings",
            params={
                "sensor_id": sensor_id,
                "rollup": "1s",
                "function": "avg",
                "from": start_date.isoformat() + "Z",
                "to": end_date.isoformat() + "Z",
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

    def get_chunks(self, start_date: datetime, end_date: datetime, chunk_size: timedelta) -> Iterable[dict]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        """
        devices = self.get_ICHANGE_devices()

        def filter_by_dates(device):
            if device["last_reading_at"] is None or device["created_at"] is None:
                return False
            device_start_date = datetime.fromisoformat(device["created_at"])
            device_end_date = datetime.fromisoformat(device["last_reading_at"])
            # see https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap
            return (device_start_date <= end_date) and (device_end_date >= start_date)

        devices_in_date_range = [d for d in devices if filter_by_dates(d)]
        logger.debug(f"{len(devices_in_date_range)} of those might have data in the requested date range.")

        dates = split_time_interval_into_chunks(self.start_date, self.end_date, chunk_size)
        date_ranges = list(zip(dates[:-1], dates[1:]))

        for start_date, end_date in date_ranges:
            for device in devices_in_date_range:
                logger.debug(f"Working on device with id {device['id']}")
                yield dict(
                    key=f"device:{device['id']}_{start_date.isoformat()}_{end_date.isoformat()}.pickle",
                    device_id=device["id"],
                    start_date=start_date,
                    end_date=end_date,
                    device=device,
                )

    def get_all_sensor_data(self, chunk: dict) -> list[dict]:
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
        readings = []
        for sensor in chunk["device"]["data"]["sensors"]:
            readings.append(
                self.get_readings(chunk["device_id"], sensor["id"], chunk["start_date"], chunk["end_date"])
            )   
            time.sleep(0.5)
        return readings

    def download_chunk(self, chunk: dict):
        # Try to load data from the cache first
        try:
            chunk, sensor_data = self.load_data_from_cache(chunk)
        except KeyError:
            logger.debug(f"Downloading from API chunk with key {chunk['key']}")
            sensor_data = self.get_all_sensor_data(chunk)
            self.save_data_to_cache(chunk, sensor_data)

        # Construct the metadata for the station
        # and add it to the postgres database
        station = construct_sck_metadata(self, chunk["device"], start_date = chunk["start_date"], end_date = chunk["end_date"])

        raw_metadata = {}
        raw_metadata["station"] = station
        raw_metadata["device"] = chunk["device"]
        raw_metadata["readings"] = []

        dfs = []

        def make_df(s):
            df = pd.DataFrame(s["readings"], columns=["time", s["sensor_key"]])
            df.time = pd.to_datetime(df.time, utc=True)
            df = df.set_index("time")
            return df

        sensors = chunk["device"]["data"]["sensors"]
        for readings, sensor in zip(sensor_data, sensors):
            if readings:
                # Check that this variable,unit pair is known to us.
                variable, unit = readings["sensor_key"], normalize("NFKD", sensor["unit"])
                canonical_form = self.mappings_variable_unit_dict.get((variable, unit))

                # If not emit a warning, though in future this will be a message sent to an operator
                if canonical_form is None:
                    logger.warning(
                        f"Variable ('{variable}', '{unit}') not found in mappings for Smart Citizen Kit\n\n"
                        f"Sensor: {sensor}\n"
                    )
                    continue

                # If this data has been explicitly marked for discarding, silently drop it.
                if canonical_form.discard:
                    continue

                # Save the readings metadata
                raw_metadata["readings"].append({k: v for k, v in readings.items() if k != "readings"})

                dfs.append(make_df(readings))

        df = pd.concat(dfs, axis=1)

        # Remove time as the index and just have it as a normal column
        df = df.reset_index()

        granularity = self.globals.ingestion_time_constants.granularity
        time_span = TimeSpan(
            start = chunk["start_date"],
            end = chunk["end_date"],
        )

        yield TabularMessage(
            metadata=self.generate_metadata(
                time_span=time_span,
                unstructured=raw_metadata,
            ),
            data=df,
        )
