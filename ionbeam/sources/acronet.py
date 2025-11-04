import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

import httpx
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ionbeam.core.handler import BaseHandler
from ionbeam.models.models import (
    CanonicalVariable,
    DataIngestionMap,
    DatasetMetadata,
    IngestDataCommand,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    StartSourceCommand,
    TimeAxis,
)
from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.storage.arrow_store import ArrowStore, LocalFileSystemStore
from ionbeam.utilities.arrow_tools import dataframes_to_record_batches, schema_from_ingestion_map
from ionbeam.utilities.cache import cached


@dataclass(frozen=True)
class SensorCatalogEntry:
    sensor_id: str
    station_name: str
    station_id: str
    latitude: float
    longitude: float
    unit: Optional[str]
    sensor_class: str


class AcronetConfig(BaseModel):
    """Configuration for the Acronet data source."""

    base_url: Optional[str] = None
    token_endpoint: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    timeout_seconds: float = 30.0
    max_retries: int = 3
    aggregation_minutes: int = 60
    maximum_request_size: timedelta = Field(default=timedelta(days=2))
    station_group: str = "ComuneLive%IChange"
    geo_window: Tuple[float, float, float, float] = (6.0, 36.0, 18.6, 47.5)
    headers: Optional[Dict[str, str]] = None
    verify_ssl: bool = True
    cache_enabled: bool = True


class AcronetSource(BaseHandler[StartSourceCommand, Optional[IngestDataCommand]]):
    """Implementation of the Acronet data source."""

    AUTHOR = "acronet"

    # Unit name normalization (no conversion, just string matching for validation)
    UNIT_NAME_NORMALIZATION = {
        "°C": "degC",
        "Degrees": "degree",
        "m/s": "m s-1",
        "W/m^2": "W m-2",
        "Knots": "m s-1",  # Name normalization only
    }

    # Actual unit conversions (sensor_class -> conversion function)
    UNIT_CONVERSIONS: Dict[str, Callable[[np.ndarray], np.ndarray]] = {
        "ANEMOMETRO_RAFFICA": lambda x: x * 0.514444,  # Knots to m/s
    }

    def __init__(self, config: AcronetConfig, metrics: IonbeamMetricsProtocol | None, arrow_store: ArrowStore) -> None:
        super().__init__("AcronetSource", metrics)
        self.config = config
        self.arrow_store = arrow_store

        self.metadata = IngestionMetadata(
            dataset=DatasetMetadata(
                name="acronet",
                description="IoT observations collected from the CIMA Acronet network.",
                aggregation_span=timedelta(days=1),
                subject_to_change_window=timedelta(days=0),
                source_links=[],
                keywords=["acronet", "iot", "weather"],
            ),
            ingestion_map=DataIngestionMap(
                datetime=TimeAxis(from_col="time"),
                lat=LatitudeAxis(from_col="lat", standard_name="latitude", cf_unit="degrees_north"),
                lon=LongitudeAxis(from_col="lon", standard_name="longitude", cf_unit="degrees_east"),
                canonical_variables=[
                    CanonicalVariable(
                        column="PLUVIOMETRO",
                        standard_name="precipitation_amount",
                        cf_unit="mm",
                    ),
                    CanonicalVariable(
                        column="TERMOMETRO",
                        standard_name="air_temperature",
                        cf_unit="degC",
                    ),
                    CanonicalVariable(
                        column="IGROMETRO",
                        standard_name="relative_humidity",
                        cf_unit="%",
                    ),
                    CanonicalVariable(
                        column="DIREZIONEVENTO",
                        standard_name="wind_from_direction",
                        cf_unit="degree",
                    ),
                    CanonicalVariable(
                        column="ANEMOMETRO",
                        standard_name="wind_speed",
                        cf_unit="m s-1",
                    ),
                    CanonicalVariable(
                        column="BAROMETRO",
                        standard_name="air_pressure",
                        cf_unit="hPa",
                    ),
                    CanonicalVariable(
                        column="RADIOMETRO",
                        standard_name="surface_downwelling_shortwave_flux_in_air",
                        cf_unit="W m-2",
                    ),
                    CanonicalVariable(
                        column="BATTERIA",
                        standard_name="battery_level",
                        cf_unit="V",
                    ),
                    CanonicalVariable(
                        column="TERMOMETRO_INTERNA",
                        standard_name="indoor_air_temperature",
                        cf_unit="degC",
                    ),
                    CanonicalVariable(
                        column="DIREZIONEVENTO_RAFFICA",
                        standard_name="wind_from_direction_of_gust",
                        cf_unit="degree",
                    ),
                    CanonicalVariable(
                        column="ANEMOMETRO_RAFFICA",
                        standard_name="wind_speed_of_gust",
                        cf_unit="m s-1",
                    ),
                    CanonicalVariable(
                        column="TERMOMETRO_MIN",
                        standard_name="minimum_air_temperature",
                        cf_unit="degC",
                    ),
                    CanonicalVariable(
                        column="TERMOMETRO_MAX",
                        standard_name="maximum_air_temperature",
                        cf_unit="degC",
                    ),
                    CanonicalVariable(
                        column="SIGNAL_STRENGTH",
                        standard_name="signal_strength",
                        cf_unit="CSQ",
                    ),
                ],
                metadata_variables=[
                    MetadataVariable(column="station_id"),
                    MetadataVariable(column="station_name"),
                    MetadataVariable(column="author"),
                ],
            ),
            version=1,
        )

        self._arrow_schema = schema_from_ingestion_map(self.metadata.ingestion_map)
        self._configured_sensor_classes = {
            var.column: var for var in self.metadata.ingestion_map.canonical_variables
        }

        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

        self._auth_lock = asyncio.Lock()
        self._sensors_by_id: Dict[str, SensorCatalogEntry] = {}
        self._sensors_by_class: Dict[str, List[SensorCatalogEntry]] = {}
        self._available_sensor_classes: set[str] = set()
        self._unit_mismatches_logged: set[str] = set()

    async def _handle(self, event: StartSourceCommand) -> Optional[IngestDataCommand]:
        request_start = perf_counter()
        total_rows = 0

        await self._ensure_sensor_inventory(event)

        classes_to_fetch = sorted(self._available_sensor_classes)
        if not classes_to_fetch:
            self.logger.warning("No configured sensor classes available from Acronet API")
            return None

        async def dataframe_stream() -> AsyncIterator[pd.DataFrame]:
            station_records: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            failed_sensors: Dict[str, int] = defaultdict(int)

            for chunk_start, chunk_end in self._iter_time_chunks(event.start_time, event.end_time):
                for sensor_class in classes_to_fetch:
                    payload = await self._fetch_sensor_class_data(event, sensor_class, chunk_start, chunk_end)
                    if not payload:
                        self.logger.warning(
                            "No data returned for sensor class",
                            sensor_class=sensor_class,
                            chunk_start=chunk_start.isoformat(),
                            chunk_end=chunk_end.isoformat(),
                        )
                        continue

                    for entry in payload:
                        sensor_id = str(entry.get("sensorId") or entry.get("sensor_id") or "")

                        if not sensor_id:
                            failed_sensors["missing_sensor_id"] += 1
                            continue

                        sensor = self._sensors_by_id.get(sensor_id)
                        if sensor is None:
                            failed_sensors[f"unknown_sensor_{sensor_id}"] += 1
                            continue

                        timeline = entry.get("timeline") or entry.get("times")
                        values = entry.get("values")

                        if not timeline or values is None:
                            failed_sensors[sensor_id] += 1
                            continue

                        if len(timeline) != len(values):
                            self.logger.debug(
                                "Timeline/value length mismatch",
                                sensor_id=sensor_id,
                                sensor_class=sensor_class,
                                timeline_len=len(timeline),
                                values_len=len(values),
                            )
                            failed_sensors[sensor_id] += 1
                            continue

                        values_array = np.array(values, dtype=float)
                        values_array[values_array < -9000] = np.nan

                        # Apply unit conversion where necessary
                        conversion_func = self.UNIT_CONVERSIONS.get(sensor_class)
                        if conversion_func is not None:
                            values_array = conversion_func(values_array)

                        for timestamp_str, value in zip(timeline, values_array):
                            if np.isnan(value):
                                continue

                            try:
                                timestamp = pd.to_datetime(timestamp_str, format="%Y%m%d%H%M", utc=True)
                            except Exception:
                                failed_sensors[sensor_id] += 1
                                continue

                            station_records[sensor.station_id].append({
                                'time': timestamp,
                                'station_id': sensor.station_id,
                                'station_name': sensor.station_name,
                                'lat': sensor.latitude,
                                'lon': sensor.longitude,
                                'author': self.AUTHOR,
                                sensor_class: value,
                            })

            if failed_sensors:
                self.logger.warning(
                    "Sensor data parsing failures",
                    failed_count=len(failed_sensors),
                    total_failures=sum(failed_sensors.values()),
                    top_failures=dict(sorted(failed_sensors.items(), key=lambda x: x[1], reverse=True)[:10]),
                )

            for station_id, records in station_records.items():
                if not records:
                    continue

                station_df = pd.DataFrame(records)
                station_df = station_df.groupby(
                    ['time', 'station_id', 'station_name', 'lat', 'lon', 'author'],
                    dropna=False
                ).first().reset_index()
                station_df.sort_values('time', inplace=True)

                yield station_df

        object_key = self._build_object_key(event.start_time, event.end_time)

        batch_stream = dataframes_to_record_batches(
            dataframe_stream(),
            schema=self._arrow_schema,
            preserve_index=False,
        )

        total_rows = await self.arrow_store.write_record_batches(
            object_key,
            batch_stream,
            schema=self._arrow_schema,
        )

        request_duration = perf_counter() - request_start

        if total_rows == 0:
            self.logger.warning("Acronet ingestion produced no rows", key=object_key)
            return None

        self.logger.info("Stored Acronet data", rows=int(total_rows), key=object_key)

        return IngestDataCommand(
            id=uuid4(),
            metadata=self.metadata,
            payload_location=object_key,
            start_time=event.start_time,
            end_time=event.end_time,
        )

    async def _ensure_sensor_inventory(self, event: StartSourceCommand) -> None:
        """Build inventory of available sensors from the Acronet API."""
        if self._sensors_by_id:
            return

        classes = await self._list_sensor_classes(event)
        if not classes:
            self.logger.warning("Failed to retrieve sensor classes from Acronet API")
            return

        for sensor_class in classes:
            if sensor_class not in self._configured_sensor_classes:
                continue

            sensors = await self._fetch_sensor_list_for_class(event, sensor_class)
            if not sensors:
                continue

            self._sensors_by_class[sensor_class] = sensors
            for sensor in sensors:
                self._sensors_by_id[sensor.sensor_id] = sensor
                self._validate_sensor_unit(sensor)

            self._available_sensor_classes.add(sensor_class)

    def _validate_sensor_unit(self, sensor: SensorCatalogEntry) -> None:
        """Validate that the API-reported unit matches our metadata configuration."""
        expected_var = self._configured_sensor_classes.get(sensor.sensor_class)
        if not expected_var:
            return

        expected_unit = expected_var.cf_unit
        api_unit = sensor.unit

        if not api_unit:
            return

        unit_key = f"{sensor.sensor_class}:{api_unit}"
        if unit_key in self._unit_mismatches_logged:
            return

        normalized_api_unit = api_unit.strip()
        normalized_expected_unit = expected_unit.strip()

        # Apply unit name normalization for validation
        normalized_api_unit = self.UNIT_NAME_NORMALIZATION.get(normalized_api_unit, normalized_api_unit)

        if normalized_api_unit != normalized_expected_unit:
            self.logger.warning(
                "Unit mismatch between API and metadata",
                sensor_class=sensor.sensor_class,
                api_unit=api_unit,
                expected_unit=expected_unit,
                sensor_id=sensor.sensor_id,
                station_name=sensor.station_name,
            )
            self._unit_mismatches_logged.add(unit_key)

    async def _list_sensor_classes(self, event: StartSourceCommand) -> List[str]:
        @cached("sensor_classes", event_id=event.id, cache_enabled=self.config.cache_enabled)
        async def _fetch():
            response = await self._make_request("GET", "sensors/classes")
            if response is None:
                return []
            
            # Handle empty response body
            if not response.text or response.text.strip() == "":
                self.logger.warning("Empty response body from sensors/classes endpoint")
                return []
            
            try:
                return response.json()
            except Exception as exc:
                self.logger.error("Failed to parse JSON response from sensors/classes", error=str(exc))
                return []
        
        payload = await _fetch()
        
        if isinstance(payload, list):
            return [str(item.get("name", item)) if isinstance(item, dict) else str(item) for item in payload]

        return []

    async def _fetch_sensor_list_for_class(self, event: StartSourceCommand, sensor_class: str) -> List[SensorCatalogEntry]:
        params: Dict[str, Any] = {
            "stationgroup": self.config.station_group,
            "geowin": ",".join(str(coord) for coord in self.config.geo_window),
        }

        @cached(lambda x: f"sensor_list_{sensor_class}_{json.dumps(x)}", event_id=event.id, cache_enabled=self.config.cache_enabled)
        async def _fetch(params):
            response = await self._make_request("GET", f"sensors/list/{sensor_class}", params=params)
            if response is None:
                return []
            return response.json()

        payload = await _fetch(params)
        entries: List[SensorCatalogEntry] = []

        for item in payload:
            try:
                sensor_id = str(item["id"])
                station_name = str(item["name"])
                latitude = float(item["lat"])
                longitude = float(item["lng"])
                unit = str(item.get("mu") or item.get("unit") or "")
            except (KeyError, TypeError, ValueError):
                continue

            station_id = self._normalize_station_name(station_name)
            entries.append(
                SensorCatalogEntry(
                    sensor_id=sensor_id,
                    station_name=station_name,
                    station_id=station_id,
                    latitude=latitude,
                    longitude=longitude,
                    unit=unit,
                    sensor_class=sensor_class,
                )
            )

        return entries

    async def _fetch_sensor_class_data(
        self,
        event: StartSourceCommand,
        sensor_class: str,
        window_start: datetime,
        window_end: datetime,
    ) -> List[Dict[str, Any]]:
        params = {
            "from": self._format_timestamp(window_start),
            "to": self._format_timestamp(window_end),
            "aggr": max(1, int(self.config.aggregation_minutes)),
            "date_as_string": True,
        }

        @cached(
            lambda x: f"sensor_data_{sensor_class}_{x['from']}_{x['to']}_{x['aggr']}",
            event_id=event.id,
            cache_enabled=self.config.cache_enabled
        )
        async def _fetch(params):
            response = await self._make_request("GET", f"sensors/data/{sensor_class}/all", params=params)
            if response is None:
                return []
            return response.json()

        payload = await _fetch(params)
        if isinstance(payload, list):
            return payload

        return []

    def _iter_time_chunks(self, start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
        chunk_size = self.config.maximum_request_size
        if chunk_size <= timedelta(0):
            yield start, end
            return

        current = start
        while current < end:
            chunk_end = min(current + chunk_size, end)
            yield current, chunk_end
            if chunk_end == end:
                break
            current = chunk_end

    async def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        require_auth: bool = True,
    ) -> Optional[httpx.Response]:
        url = path if path.lower().startswith("http") else f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"

        if require_auth:
            await self._ensure_access_token()
            if not self._access_token:
                self.logger.error("Missing access token for authenticated request", url=url)
                return None

        for attempt in range(self.config.max_retries + 1):
            headers = dict(self.config.headers or {})
            if require_auth and self._access_token:
                headers["Authorization"] = f"Bearer {self._access_token}"

            try:
                async with httpx.AsyncClient(
                    timeout=self.config.timeout_seconds,
                    headers=headers,
                    follow_redirects=True,
                    verify=self.config.verify_ssl,
                ) as client:
                    if method.upper() == "GET":
                        response = await client.get(url, params=params or {})
                    elif method.upper() == "POST":
                        response = await client.post(url, params=params or {}, json=json_data)
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")

                    if response.status_code == 401 and require_auth and attempt < self.config.max_retries:
                        self.logger.info("Received 401, attempting token refresh", url=url, attempt=attempt + 1)
                        if await self._refresh_access_token() or await self._authenticate():
                            continue
                        self.logger.error("Unable to refresh authentication", url=url)
                        return None

                    response.raise_for_status()
                    return response

            except httpx.TimeoutException:
                self.logger.warning(
                    "Request timeout",
                    url=url,
                    timeout_seconds=self.config.timeout_seconds,
                    attempt=attempt + 1,
                    max_retries=self.config.max_retries,
                )
                if attempt == self.config.max_retries:
                    return None
                await asyncio.sleep(min(2 ** attempt, 10))

            except httpx.HTTPStatusError as exc:
                self.logger.warning(
                    "HTTP error during request",
                    url=url,
                    status=exc.response.status_code,
                    attempt=attempt + 1,
                    max_retries=self.config.max_retries,
                )
                if attempt == self.config.max_retries:
                    return None
                await asyncio.sleep(min(2 ** attempt, 10))

            except httpx.RequestError as exc:
                self.logger.warning(
                    "Network error during request",
                    url=url,
                    error=str(exc),
                    attempt=attempt + 1,
                    max_retries=self.config.max_retries,
                )
                if attempt == self.config.max_retries:
                    return None
                await asyncio.sleep(min(2 ** attempt, 10))

        return None

    async def _ensure_access_token(self) -> None:
        now = datetime.now(timezone.utc)
        if self._access_token and self._token_expiry and self._token_expiry > now + timedelta(seconds=30):
            return

        if self._refresh_token:
            refreshed = await self._refresh_access_token()
            if refreshed:
                return

        await self._authenticate()

    async def _authenticate(self) -> bool:
        async with self._auth_lock:
            if not self.config.username or not self.config.password or not self.config.client_id:
                self.logger.error("Acronet credentials are not fully configured")
                return False

            data = {
                "grant_type": "password",
                "username": self.config.username,
                "password": self.config.password,
                "client_id": self.config.client_id,
            }
            if self.config.client_secret:
                data["client_secret"] = self.config.client_secret

            try:
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds, verify=self.config.verify_ssl) as client:
                    response = await client.post(self.config.token_endpoint, data=data)
                    response.raise_for_status()

                    payload = response.json()
                    self._access_token = payload.get("access_token")
                    self._refresh_token = payload.get("refresh_token")

                    expires_in = int(payload.get("expires_in", 0)) or 0
                    self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 30)

                    if not self._access_token:
                        self.logger.error("Authentication response missing access token")
                        return False

                    return True

            except Exception as exc:
                self.logger.error("Authentication failed", error=str(exc), error_type=type(exc).__name__)
                self._access_token = None
                self._refresh_token = None
                self._token_expiry = None
                return False

    async def _refresh_access_token(self) -> bool:
        async with self._auth_lock:
            if not self._refresh_token or not self.config.client_id:
                return False

            data = {
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self.config.client_id,
            }
            if self.config.client_secret:
                data["client_secret"] = self.config.client_secret

            try:
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds, verify=self.config.verify_ssl) as client:
                    response = await client.post(self.config.token_endpoint, data=data)
                    response.raise_for_status()

                    payload = response.json()
                    self._access_token = payload.get("access_token")
                    self._refresh_token = payload.get("refresh_token", self._refresh_token)
                    expires_in = int(payload.get("expires_in", 0)) or 0
                    self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 30)

                    if not self._access_token:
                        self.logger.error("Token refresh response missing access token")
                        return False

                    return True

            except Exception as exc:
                self.logger.warning("Token refresh failed", error=str(exc), error_type=type(exc).__name__)
                self._access_token = None
                self._refresh_token = None
                self._token_expiry = None
                return False

    @staticmethod
    def _normalize_station_name(name: str) -> str:
        return name.strip().lower().replace(" ", "_").replace("-", "_") or "unknown_station"

    def _build_object_key(self, start_time: datetime, end_time: datetime) -> str:
        start_s = start_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        end_s = end_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        now_s = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"raw/{self.metadata.dataset.name}/{start_s}-{end_s}_{now_s}"

    @staticmethod
    def _format_timestamp(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M")


async def _manual_run() -> None:
    config = AcronetConfig(
        username="",
        password="",
        client_id="",
        client_secret="",
    )

    arrow_store = LocalFileSystemStore(Path("./acronet_output"))

    source = AcronetSource(config, None, arrow_store)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=5)

    command = StartSourceCommand(
        id=uuid4(),
        source_name="acronet",
        start_time=start_time,
        end_time=end_time,
    )

    result = await source._handle(command)
    if result is None:
        print("Ingestion produced no data.")
    else:
        print("Ingestion succeeded:")
        print(f"  Payload key : {result.payload_location}")
        print(f"  Start time  : {result.start_time.isoformat()}")
        print(f"  End time    : {result.end_time.isoformat()}")
        print("  Rows stored : written to arrow store")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(_manual_run())
    except KeyboardInterrupt:
        print("Aborted by user.")
