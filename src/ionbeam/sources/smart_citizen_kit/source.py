

import logging
import pandas as pd
from typing import Iterable
from pathlib import Path
import dataclasses
from ...core.bases import TabularMessage
from ..API_sources_base import RESTSource
from datetime import datetime
from unicodedata import normalize
from .metadata import construct_sck_metadata


from cachetools import cachedmethod, TTLCache
from cachetools.keys import hashkey

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
    cache_directory: Path = Path(f"inputs/smart_citizen_kit")
    endpoint = "https://api.smartcitizen.me/v0"
    cache = TTLCache(maxsize=1e5, ttl=20*60) # Cache API responses for 20 minutes
        
    @cachedmethod(lambda self: self.cache, key=saltedmethodkey('devices_by_tag'))
    def get_devices_by_tag(self, tag : str):
        return self.get(f"/devices?with_tags={tag}")
    
    @cachedmethod(lambda self: self.cache, key=saltedmethodkey('users'))
    def get_users(self, username_contains):
        return self.get(f"/users?q[username_cont]={username_contains}")

    @cachedmethod(lambda self: self.cache, key=saltedmethodkey('device'))
    def get_device(self, device_id):
        return self.get(f"/devices/{device_id}")
    
    @cachedmethod(lambda self: self.cache, key=saltedmethodkey('sensor'))
    def get_sensor(self, sensor_id):
        return self.get(f"/sensors/{sensor_id}")

    # def get_sensors(self, device_id):
    #     sensors = self.get_device(device_id)["data"]["sensors"]
    #     return sensors

    def init(self, globals):
        super().init(globals)
        self.mappings_variable_unit_dict = {(column.key, column.unit) : column for column in self.mappings}

    def get_readings(self, device_id, sensor_id, start_date, end_date):
        return self.get(f"/devices/{device_id}/readings",
                    params = {
                        "sensor_id" : sensor_id,
                        "rollup" : "1s",
                        "function" : "avg",
                        "from" : start_date.isoformat() + "Z",
                        "to" : end_date.isoformat() + "Z",
                        
                    })


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

    def get_chunks(self, start_date : datetime, end_date: datetime) -> Iterable[dict]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        In this case (device_id, sensor_id) tuples
        """
        devices = self.get_ICHANGE_devices()
        
        def filter_by_dates(device):
            if device['last_reading_at'] is None or device['created_at'] is None: return False
            device_start_date = datetime.fromisoformat(device['created_at'])
            device_end_date = datetime.fromisoformat(device['last_reading_at'])
            # see https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap
            return (device_start_date <= end_date) and (device_end_date >= start_date) 

        devices_in_date_range = [d for d in devices if filter_by_dates(d)]
        logger.debug(f"{len(devices_in_date_range)} of those might have data in the requested date range.")
        
        for device in devices_in_date_range:
            logger.debug(f"Working on device with id {device['id']}")
            construct_sck_metadata(self, device)
            for sensor in device["data"]["sensors"]:
                logger.debug(f"Working on sensor with id {sensor['id']}")
                yield dict(
                           key = f"device:{device['id']}_sensor:{sensor['id']}_{start_date.isoformat()}_{end_date.isoformat()}.pickle",
                           device_id = device["id"],
                           sensor_id = sensor["id"],
                           start_date = start_date,
                           end_date = end_date,
                           device = device,
                           sensor = sensor,
                           )

    def download_chunk(self, chunk: dict): 
        # Try to load data from the cache first
        try:
            chunk, readings = self.load_data_from_cache(chunk)
        except KeyError:
            logger.debug(f"Downloading from API chunk with key {chunk['key']}")
            readings = self.get_readings(chunk["device_id"], chunk["sensor_id"], chunk["start_date"], chunk["end_date"])
            self.save_data_to_cache(chunk, readings)

        if readings["readings"]: 
            variable = readings["sensor_key"]
            unit = normalize("NFKD", chunk["sensor"]["unit"])

            canonical_form = self.mappings_variable_unit_dict.get((variable, unit))
            if canonical_form is None:
                logger.warning(f"Variable ('{variable}', '{unit}') not found in mappings for Smart Citizen Kit\n\n"
                                   f"Sensor: {chunk['sensor']}\n"
                                   )
                return
            
            # Check if we should discard this data
            if canonical_form.discard:
                return

            raw_metadata = {k : v for k, v in readings.items() if k != "readings"}
            raw_metadata["device"] = chunk["device"]
            raw_metadata["sensor"] = chunk["sensor"]
            df = pd.DataFrame(readings["readings"], columns = ["time", variable])
            
            yield TabularMessage(
                metadata=self.generate_metadata(
                    unstructured = raw_metadata,
                ),
                data = df,
            )