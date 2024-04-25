

import logging
import pandas as pd
import re
from typing import Literal, Tuple, List, Iterable

from pathlib import Path

import dataclasses

from ...core.bases import Message, TabularMessage, Source, MetaData, InputColumn
from ...core.history import PreviousActionInfo, ActionInfo

from ..API_sources_base import APISource

import yaml
from datetime import datetime
import requests

import cachetools
from datetime import datetime, timedelta

devices_by_tag_cache = cachetools.TTLCache(maxsize=1e5, ttl=timedelta(hours=1), timer=datetime.now)
device_cache = cachetools.TTLCache(maxsize=1e5, ttl=timedelta(hours=1), timer=datetime.now)
sensor_cache = cachetools.TTLCache(maxsize=1e5, ttl=timedelta(hours=1), timer=datetime.now)
readings_cache = cachetools.TTLCache(maxsize=1e6, ttl=timedelta(hours=1), timer=datetime.now)

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class SCK_Chunk():
    device_id : int
    sensor_id : int
    start_date : datetime
    

@dataclasses.dataclass
class SmartCitizenKitSource(APISource):
    """
    API Documentation: https://developer.smartcitizen.me/#summary
    """
    cache_directory: Path = Path(f"inputs/smart_citizen_kit")
    endpoint = "https://api.smartcitizen.me/v0"
    session = requests.Session()

    def get(self, *args, **kwargs):
        r = self.session.get(*args, **kwargs)
        r.raise_for_status()
        return r.json()
        
    @cachetools.cachedmethod(cache = lambda self : devices_by_tag_cache, key = lambda self, it : tuple(it))
    def get_devices_by_tag(self, tags : Iterable[str]):
        tags = ",".join(tags)
        return self.get(f"{self.endpoint}/devices/world_map?with_tags={tags}")

    @cachetools.cachedmethod(cache = lambda self : device_cache)
    def get_device(self, device_id):
        return self.get(f"{self.endpoint}/devices/{device_id}")
    
    @cachetools.cachedmethod(cache = lambda self : sensor_cache)
    def get_sensor(self, sensor_id):
        return self.get(f"{self.endpoint}/sensors/{sensor_id}")

    def get_sensors(self, device_id):
        sensors = self.get_device(device_id)["data"]["sensors"]
        return sensors

    @cachetools.cachedmethod(cache = lambda self : readings_cache)
    def get_readings(self, device_id, sensor_id, start_date, end_date):
        return self.get(f"https://api.smartcitizen.me/v0/devices/{device_id}/readings",
                    params = {
                        "sensor_id" : sensor_id,
                        "rollup" : "1s",
                        "function" : "avg",
                        "from" : start_date.isoformat() + "Z",
                        "to" : end_date.isoformat() + "Z",
                        
                    })


    def get_chunks(self, start_date : datetime, end_date: datetime) -> Iterable[dict]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        In this case (device_id, sensor_id) tuples
        """
        devices = self.get_devices_by_tag(["I-CHANGE", "I-change"])
        logger.debug(f"Found {len(devices)} devices with I-CHANGE tag")
        
        def filter_by_dates(device):
            device_start_date = datetime.fromisoformat(device['created_at'])
            device_end_date = datetime.fromisoformat(device['last_reading_at'])
            # see https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap
            return (device_start_date <= end_date) and (device_end_date >= start_date) 

        devices_in_date_range = [d for d in devices if filter_by_dates(d)]
        logger.debug(f"{len(devices_in_date_range)} of those might have data in the requested date range.")
        
        for device in devices_in_date_range:
            for sensor in self.get_sensors(device["id"]):
                yield dict(device_id = device["id"],
                           sensor_id = sensor["id"],
                           start_date = start_date,
                           end_date = end_date)

    def download_chunk(self, chunk: dict): 
        readings = self.get_readings(chunk["device_id"], chunk["sensor_id"], chunk["start_date"], chunk["end_date"])
        if readings["readings"]: 
            variable = readings["sensor_key"]
            raw_metadata = {k : v for k, v in readings.items() if k != "readings"}
            raw_metadata["device"] = self.get_device(chunk["device_id"])
            raw_metadata["sensor"] = self.get_sensor(chunk["sensor_id"])
            df = pd.DataFrame(readings["readings"], columns = ["datetime", variable]).set_index("datetime")
            
            yield TabularMessage(
                metadata=self.generate_metadata(
                    unstructured = raw_metadata,
                ),
                data = df,
            )