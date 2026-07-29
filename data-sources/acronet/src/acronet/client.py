# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Acronet data source client implementation."""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Callable, Iterable, Optional
from uuid import UUID

import httpx
from httpx_retries import Retry, RetryTransport
import numpy as np
import pandas as pd
import structlog
from ionbeam_client import IonbeamClient
from ionbeam_client.canonical_stream import canonical_record_batches
from ionbeam_client.models import (
    CfSemantics,
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    cf,
    geographic_point_coordinates,
)

from .models import AcronetConfig, SensorCatalogEntry

# Acronet sensor class -> the canonical column it lands in.
SENSOR_CLASSES = {
    "PLUVIOMETRO": "precipitation_amount",
    "TERMOMETRO": "air_temperature",
    "IGROMETRO": "relative_humidity",
    "DIREZIONEVENTO": "wind_from_direction",
    "ANEMOMETRO": "wind_speed",
    "BAROMETRO": "air_pressure",
    "RADIOMETRO": "surface_downwelling_shortwave_flux_in_air",
    "ANEMOMETRO_RAFFICA": "wind_speed_of_gust",
    "TERMOMETRO_MIN": "minimum_air_temperature",
    "TERMOMETRO_MAX": "maximum_air_temperature",
    "BATTERIA": "battery_level",
    "TERMOMETRO_INTERNA": "indoor_air_temperature",
    "DIREZIONEVENTO_RAFFICA": "wind_from_direction_of_gust",
    "SIGNAL_STRENGTH": "signal_strength",
}


class AcronetSource:
    """Implementation of the Acronet data source."""

    AUTHOR = "acronet"

    UNIT_NAME_NORMALIZATION = {
        "°C": "degC",
        "Degrees": "degree",
        "m/s": "m s-1",
        "W/m^2": "W m-2",
        "Knots": "m s-1",
        "%": "percent",
        "CSQ": "1",
    }

    UNIT_CONVERSIONS: dict[str, Callable[[np.ndarray], np.ndarray]] = {
        "ANEMOMETRO_RAFFICA": lambda x: x * 0.514444,  # Knots to m/s
    }

    def __init__(self, config: AcronetConfig) -> None:
        self.config = config
        self.logger = structlog.get_logger(__name__)

        self.metadata = IngestionMetadata(
            # min/max temperature are CF air_temperature under a cell_method;
            # station telemetry (battery, signal, indoor temperature, gust
            # direction) carries no CF claim — those names are not in the CF table.
            version=2,
            name="acronet",
            dataset_schema=DatasetSchema(
                time=TimeCoordinate(),
                coordinates=geographic_point_coordinates(),
                variables=[
                    cf("precipitation_amount", "mm"),
                    cf("air_temperature", "degC"),
                    cf("relative_humidity", "percent"),
                    cf("wind_from_direction", "degree"),
                    cf("wind_speed", "m s-1"),
                    cf("air_pressure", "hPa"),
                    cf("surface_downwelling_shortwave_flux_in_air", "W m-2"),
                    cf("wind_speed_of_gust", "m s-1"),
                    Variable(
                        name="minimum_air_temperature",
                        semantics=CfSemantics(
                            standard_name="air_temperature", cell_method="minimum"
                        ),
                        unit="degC",
                    ),
                    Variable(
                        name="maximum_air_temperature",
                        semantics=CfSemantics(
                            standard_name="air_temperature", cell_method="maximum"
                        ),
                        unit="degC",
                    ),
                    Variable(name="battery_level", unit="V"),
                    Variable(name="indoor_air_temperature", unit="degC"),
                    Variable(name="wind_from_direction_of_gust", unit="degree"),
                    Variable(name="signal_strength", unit="1"),
                ],
                tags=[
                    Tag(name="station_id"),
                    Tag(name="station_name"),
                    Tag(name="author"),
                ],
            ),
        )

        variables_by_name = {
            var.name: var for var in self.metadata.dataset_schema.variables
        }
        self._configured_sensor_classes = {
            sensor_class: variables_by_name[canonical]
            for sensor_class, canonical in SENSOR_CLASSES.items()
        }

        self._access_token: str | None = None
        self._http = httpx.AsyncClient(
            timeout=config.timeout_seconds,
            headers=config.headers or {},
            follow_redirects=True,
            verify=config.verify_ssl,
            transport=RetryTransport(
                retry=Retry(total=config.max_retries, backoff_factor=0.5)
            ),
        )
        self._sensors_by_id: dict[str, SensorCatalogEntry] = {}
        self._sensors_by_class: dict[str, list[SensorCatalogEntry]] = {}
        self._available_sensor_classes: set[str] = set()
        self._unit_mismatches_logged: set[str] = set()

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        client: IonbeamClient,
        ingestion_id: Optional[UUID] = None,
    ) -> None:
        """Fetch and ingest Acronet data for the given time window."""
        await self._ensure_sensor_inventory()

        classes_to_fetch = sorted(self._available_sensor_classes)
        if not classes_to_fetch:
            self.logger.warning(
                "No configured sensor classes available from Acronet API"
            )
            return

        async def dataframe_stream() -> AsyncIterator[pd.DataFrame]:
            station_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
            failed_sensors: dict[str, int] = defaultdict(int)

            for chunk_start, chunk_end in self._iter_time_chunks(start_time, end_time):
                for sensor_class in classes_to_fetch:
                    payload = await self._fetch_sensor_class_data(
                        sensor_class, chunk_start, chunk_end
                    )
                    if not payload:
                        self.logger.warning(
                            "No data returned for sensor class",
                            sensor_class=sensor_class,
                            chunk_start=chunk_start.isoformat(),
                            chunk_end=chunk_end.isoformat(),
                        )
                        continue

                    for entry in payload:
                        sensor_id = str(
                            entry.get("sensorId") or entry.get("sensor_id") or ""
                        )

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

                        conversion_func = self.UNIT_CONVERSIONS.get(sensor_class)
                        if conversion_func is not None:
                            values_array = conversion_func(values_array)

                        for timestamp_str, value in zip(timeline, values_array):
                            if np.isnan(value):
                                continue

                            try:
                                timestamp = pd.to_datetime(
                                    timestamp_str, format="%Y%m%d%H%M", utc=True
                                )
                            except Exception:
                                failed_sensors[sensor_id] += 1
                                continue

                            station_records[sensor.station_id].append(
                                {
                                    "time": timestamp,
                                    "station_id": sensor.station_id,
                                    "station_name": sensor.station_name,
                                    "lat": sensor.latitude,
                                    "lon": sensor.longitude,
                                    "author": self.AUTHOR,
                                    SENSOR_CLASSES[sensor_class]: value,
                                }
                            )

            if failed_sensors:
                self.logger.warning(
                    "Sensor data parsing failures",
                    failed_count=len(failed_sensors),
                    total_failures=sum(failed_sensors.values()),
                    top_failures=dict(
                        sorted(
                            failed_sensors.items(), key=lambda x: x[1], reverse=True
                        )[:10]
                    ),
                )

            for station_id, records in station_records.items():
                if not records:
                    continue

                station_df = pd.DataFrame(records)
                station_df = (
                    station_df.groupby(
                        ["time", "station_id", "station_name", "lat", "lon", "author"],
                        dropna=False,
                    )
                    .first()
                    .reset_index()
                )
                station_df.sort_values("time", inplace=True)

                yield station_df

        batch_stream = canonical_record_batches(dataframe_stream(), self.metadata)

        await client.ingest(
            batch_stream=batch_stream,
            metadata=self.metadata,
            start_time=start_time,
            end_time=end_time,
            ingestion_id=ingestion_id,
        )

        self.logger.info(
            "Acronet ingestion completed",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

    async def _ensure_sensor_inventory(self) -> None:
        if self._sensors_by_id:
            return

        classes = await self._list_sensor_classes()
        if not classes:
            self.logger.warning("Failed to retrieve sensor classes from Acronet API")
            return

        for sensor_class in classes:
            if sensor_class not in self._configured_sensor_classes:
                continue

            sensors = await self._fetch_sensor_list_for_class(sensor_class)
            if not sensors:
                continue

            self._sensors_by_class[sensor_class] = sensors
            for sensor in sensors:
                self._sensors_by_id[sensor.sensor_id] = sensor
                self._validate_sensor_unit(sensor)

            self._available_sensor_classes.add(sensor_class)

    def _validate_sensor_unit(self, sensor: SensorCatalogEntry) -> None:
        expected_var = self._configured_sensor_classes.get(sensor.sensor_class)
        if not expected_var:
            return

        expected_unit = expected_var.unit
        api_unit = sensor.unit

        if not api_unit:
            return

        unit_key = f"{sensor.sensor_class}:{api_unit}"
        if unit_key in self._unit_mismatches_logged:
            return

        normalized_api_unit = api_unit.strip()
        normalized_expected_unit = expected_unit.strip()
        normalized_api_unit = self.UNIT_NAME_NORMALIZATION.get(
            normalized_api_unit, normalized_api_unit
        )

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

    async def _list_sensor_classes(self) -> list[str]:
        response = await self._get("sensors/classes")
        if not response.text or response.text.strip() == "":
            self.logger.warning("Empty response body from sensors/classes endpoint")
            return []

        try:
            payload = response.json()
        except Exception as exc:
            self.logger.error(
                "Failed to parse JSON response from sensors/classes", error=str(exc)
            )
            return []

        if isinstance(payload, list):
            return [
                str(item.get("name", item)) if isinstance(item, dict) else str(item)
                for item in payload
            ]

        return []

    async def _fetch_sensor_list_for_class(
        self, sensor_class: str
    ) -> list[SensorCatalogEntry]:
        params: dict[str, Any] = {
            "stationgroup": self.config.station_group,
            "geowin": ",".join(str(coord) for coord in self.config.geo_window),
        }

        response = await self._get(f"sensors/list/{sensor_class}", params=params)
        payload = response.json()
        entries: list[SensorCatalogEntry] = []

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
        sensor_class: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, Any]]:
        params = {
            "from": self._format_timestamp(window_start),
            "to": self._format_timestamp(window_end),
            "aggr": max(1, int(self.config.aggregation_minutes)),
            "date_as_string": True,
        }

        response = await self._get(f"sensors/data/{sensor_class}/all", params=params)
        payload = response.json()
        if isinstance(payload, list):
            return payload

        return []

    def _iter_time_chunks(
        self, start: datetime, end: datetime
    ) -> Iterable[tuple[datetime, datetime]]:
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

    async def _get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        """One authenticated GET; transient failures retry in the transport.
        A 401 re-authenticates once — a second 401 raises, and any other
        failure raises so the trigger redelivers the whole fetch."""
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        if not self._access_token:
            await self._authenticate()
        response = await self._http.get(url, params=params or {}, headers=self._auth)
        if response.status_code == 401:
            await self._authenticate()
            response = await self._http.get(url, params=params or {}, headers=self._auth)
        response.raise_for_status()
        return response

    @property
    def _auth(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_token}"}

    async def _authenticate(self) -> None:
        if (
            not self.config.username
            or not self.config.password
            or not self.config.client_id
        ):
            raise RuntimeError("Acronet credentials are not fully configured")

        response = await self._http.post(
            self.config.token_endpoint,
            data={
                "grant_type": "password",
                "username": self.config.username,
                "password": self.config.password,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            },
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        if not token:
            raise RuntimeError("authentication response missing access token")
        self._access_token = token

    @staticmethod
    def _normalize_station_name(name: str) -> str:
        return (
            name.strip().lower().replace(" ", "_").replace("-", "_")
            or "unknown_station"
        )

    @staticmethod
    def _format_timestamp(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M")
