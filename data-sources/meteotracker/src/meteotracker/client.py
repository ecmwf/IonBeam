# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import asyncio
import re
from datetime import datetime, timedelta
from typing import Any, AsyncIterator

import httpx
import pandas as pd
import structlog

from ionbeam_client import IonbeamClient
from ionbeam_client.models import (
    IngestionMetadata,
    DatasetMetadata,
    DataIngestionMap,
    TimeAxis,
    LatitudeAxis,
    LongitudeAxis,
    CanonicalVariable,
    MetadataVariable,
)
from ionbeam_client.arrow_tools import dataframes_to_record_batches, schema_from_ingestion_map
from ionbeam_client.constants import LatitudeColumn, LongitudeColumn

from .models import MeteoTrackerConfig, MT_Session


class MeteoTrackerSource:
    def __init__(self, config: MeteoTrackerConfig):
        self.config = config
        self.logger = structlog.get_logger(__name__)
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self.metadata: IngestionMetadata = IngestionMetadata(
            dataset=DatasetMetadata(
                name="meteotracker",
                description="raw IoT data collected from MeteoTracker",
                subject_to_change_window=timedelta(days=0),
                aggregation_span=timedelta(days=1),
                source_links=[],
                keywords=["meteotracker", "iot", "data"],
            ),
            ingestion_map=DataIngestionMap(
                datetime=TimeAxis(from_col="time"),
                lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
                lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
                canonical_variables=[
                    CanonicalVariable(
                        column="a", standard_name="altitude", cf_unit="m"
                    ),
                    CanonicalVariable(
                        column="P", standard_name="air_pressure", cf_unit="mbar"
                    ),
                    CanonicalVariable(
                        column="T0", standard_name="air_temperature", cf_unit="degC"
                    ),
                    CanonicalVariable(
                        column="H", standard_name="relative_humidity", cf_unit="1"
                    ),
                    CanonicalVariable(
                        column="tp",
                        standard_name="air_potential_temperature",
                        cf_unit="K",
                    ),
                    CanonicalVariable(
                        column="td",
                        standard_name="dew_point_temperature",
                        cf_unit="degC",
                    ),
                    CanonicalVariable(
                        column="HDX", standard_name="humidity_index", cf_unit="degC"
                    ),
                    CanonicalVariable(
                        column="i",
                        standard_name="air_temperature_lapse_rate",
                        cf_unit="degC/100m",
                    ),
                    CanonicalVariable(
                        column="s", standard_name="wind_speed", cf_unit="km h-1"
                    ),
                    CanonicalVariable(
                        column="L", standard_name="solar_radiation_index", cf_unit="1"
                    ),
                    CanonicalVariable(
                        column="bt", standard_name="bluetooth_RSSI", cf_unit="dBm"
                    ),
                    CanonicalVariable(
                        column="CO2",
                        standard_name="mole_fraction_of_carbon_dioxide_in_air",
                        cf_unit="1e-6",
                    ),
                    CanonicalVariable(
                        column="CO",
                        standard_name="mole_fraction_of_carbon_monoxide_in_air",
                        cf_unit="1e-6",
                    ),
                    CanonicalVariable(
                        column="SO2",
                        standard_name="mole_fraction_of_sulphur_dioxide_in_air",
                        cf_unit="1e-6",
                    ),
                    CanonicalVariable(
                        column="m1",
                        standard_name="mass_concentration_of_pm1_ambient_aerosol_in_air",
                        cf_unit="ug m-3",
                    ),
                    CanonicalVariable(
                        column="m2",
                        standard_name="mass_concentration_of_pm2p5_ambient_aerosol_in_air",
                        cf_unit="ug m-3",
                    ),
                    CanonicalVariable(
                        column="m4",
                        standard_name="mass_concentration_of_pm4_ambient_aerosol_in_air",
                        cf_unit="ug m-3",
                    ),
                    CanonicalVariable(
                        column="m10",
                        standard_name="mass_concentration_of_pm10_ambient_aerosol_in_air",
                        cf_unit="ug m-3",
                    ),
                    CanonicalVariable(
                        column="n0",
                        standard_name="particulate_matter_particle_number_0_5",
                        cf_unit="cm-3",
                    ),
                    CanonicalVariable(
                        column="n1",
                        standard_name="particulate_matter_particle_number_1",
                        cf_unit="cm-3",
                    ),
                    CanonicalVariable(
                        column="n2",
                        standard_name="particulate_matter_particle_number_2_5",
                        cf_unit="cm-3",
                    ),
                    CanonicalVariable(
                        column="n4",
                        standard_name="particulate_matter_particle_number_4",
                        cf_unit="cm-3",
                    ),
                    CanonicalVariable(
                        column="n10",
                        standard_name="particulate_matter_particle_number_10",
                        cf_unit="cm-3",
                    ),
                    CanonicalVariable(
                        column="tps",
                        standard_name="typical_particle_size",
                        cf_unit="um",
                    ),
                    CanonicalVariable(
                        column="EAQ", standard_name="epa_air_quality", cf_unit="1"
                    ),
                    CanonicalVariable(
                        column="FAQ", standard_name="fast_air_quality", cf_unit="1"
                    ),
                    CanonicalVariable(
                        column="O3", standard_name="ozone", cf_unit="1e-9"
                    ),
                ],
                metadata_variables=[
                    MetadataVariable(column="station_id"),
                    MetadataVariable(column="living_lab"),
                    MetadataVariable(column="author"),
                ],
            ),
        )

        self.author_regex_to_living_lab = {
            re.compile(pattern): living_lab
            for living_lab, patterns in self.config.author_patterns.items()
            for pattern in patterns
        }

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        client: IonbeamClient,
    ) -> None:
        sessions_metadata = await self._fetch_session_metadata(start_time, end_time)
        self.logger.debug("Fetched session metadata", count=len(sessions_metadata))

        if not sessions_metadata:
            self.logger.warning("No session metadata available")
            return

        # Create schema from ingestion map to enforce column ordering
        schema = schema_from_ingestion_map(self.metadata.ingestion_map)
        
        async def dataframe_stream() -> AsyncIterator[pd.DataFrame]:
            for session in sessions_metadata:
                df = await self._fetch_session_data(session)
                if df is not None and not df.empty:
                    yield df

        batch_stream = dataframes_to_record_batches(
            dataframe_stream(),
            schema=schema,
            preserve_index=False,
        )

        await client.ingest(
            batch_stream=batch_stream,
            metadata=self.metadata,
            start_time=start_time,
            end_time=end_time,
        )

        self.logger.info(
            "MeteoTracker ingestion completed",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

    async def _authenticate(self) -> bool:
        if not self.config.username or not self.config.password:
            self.logger.error("Username and password required for authentication")
            return False

        auth_data = {"email": self.config.username, "password": self.config.password}

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            try:
                self.logger.debug(f"Authenticating with {self.config.token_endpoint}")
                response = await client.post(
                    self.config.token_endpoint,
                    json=auth_data,
                    headers={"Content-Type": "application/json"},
                )
                response.raise_for_status()

                auth_result = response.json()
                self._access_token = auth_result.get("accessToken")
                self._refresh_token = auth_result.get("refreshToken")

                if self._access_token:
                    self.logger.debug("Authentication successful")
                    return True

                self.logger.error("No access token in authentication response")
                return False

            except httpx.HTTPStatusError as e:
                self.logger.error(
                    f"Authentication failed: HTTP {e.response.status_code}"
                )
                self.logger.debug(f"Response: {e.response.text}")
                return False
            except Exception as e:
                self.logger.error(f"Authentication error: {e}")
                return False

    async def _refresh_access_token(self) -> bool:
        if not self._refresh_token:
            self.logger.error("No refresh token available")
            return False

        refresh_data = {"refresh_token": self._refresh_token}

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            try:
                self.logger.debug(f"Refreshing token at {self.config.refresh_endpoint}")
                response = await client.post(
                    self.config.refresh_endpoint,
                    json=refresh_data,
                    headers={"Content-Type": "application/json"},
                )
                response.raise_for_status()

                refresh_result = response.json()
                self._access_token = refresh_result.get("access_token")

                if self._access_token:
                    self.logger.debug("Token refresh successful")
                    return True

                self.logger.error("No access token in refresh response")
                return False

            except httpx.HTTPStatusError as e:
                self.logger.error(
                    f"Token refresh failed: HTTP {e.response.status_code}"
                )
                self.logger.debug(f"Response: {e.response.text}")
                return False
            except Exception as e:
                self.logger.error(f"Token refresh error: {e}")
                return False

    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        require_auth: bool = True,
    ) -> httpx.Response | None:
        if require_auth and not self._access_token:
            if not await self._authenticate():
                self.logger.error(
                    "Authentication failed, cannot make authenticated request"
                )
                return None

        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        request_headers = dict(self.config.headers) if self.config.headers else {}
        if require_auth and self._access_token:
            request_headers["Authorization"] = f"Bearer {self._access_token}"

        async with httpx.AsyncClient(
            timeout=self.config.timeout, headers=request_headers, follow_redirects=True
        ) as client:
            for attempt in range(self.config.max_retries + 1):
                try:
                    self.logger.debug(
                        f"Making {method} request to {url} (attempt {attempt + 1})"
                    )

                    if method.upper() == "GET":
                        response = await client.get(url, params=params)
                    elif method.upper() == "POST":
                        response = await client.post(url, params=params, json=json_data)
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")

                    response.raise_for_status()
                    self.logger.debug(
                        f"Successfully fetched {url} - Status: {response.status_code}"
                    )
                    return response

                except httpx.HTTPStatusError as e:
                    self.logger.warning(
                        f"HTTP error for {url}: {e.response.status_code}"
                    )

                    if (
                        e.response.status_code == 401
                        and require_auth
                        and self._refresh_token
                    ):
                        self.logger.info("Received 401, attempting to refresh token")
                        if await self._refresh_access_token():
                            request_headers["Authorization"] = (
                                f"Bearer {self._access_token}"
                            )
                            client.headers.update(request_headers)
                            continue

                        self.logger.error("Token refresh failed, re-authenticating")
                        if await self._authenticate():
                            request_headers["Authorization"] = (
                                f"Bearer {self._access_token}"
                            )
                            client.headers.update(request_headers)
                            continue

                    if attempt == self.config.max_retries:
                        self.logger.error(
                            f"Failed to fetch {url} after {self.config.max_retries + 1} attempts"
                        )
                        return None

                except httpx.RequestError as e:
                    self.logger.warning(f"Request error for {url}: {e}")
                    if attempt == self.config.max_retries:
                        self.logger.error(
                            f"Failed to fetch {url} after {self.config.max_retries + 1} attempts"
                        )
                        return None

                except Exception as e:
                    self.logger.error(f"Unexpected error fetching {url}: {e}")
                    return None

                wait_time = 2**attempt
                self.logger.debug(f"Waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)

        return None

    async def _fetch_session_metadata(
        self, start_time: datetime, end_time: datetime
    ) -> list[MT_Session]:
        t1, t2 = (int(t.timestamp()) for t in [start_time, end_time])
        params = {
            "startTime": (f'{{"$gte":{t1},"$lte":{t2}}}',),
            "dataType": "all",
            "items": 1000,
        }

        sessions_metadata = []
        consecutive_failures = 0
        max_consecutive_failures = 3

        for i in range(self.config.max_queries):
            params["page"] = i
            response = await self._make_request("/sessions", params=params)

            if response is None:
                consecutive_failures += 1
                self.logger.warning(
                    "Failed to fetch page %s (consecutive failures: %s)",
                    i,
                    consecutive_failures,
                )

                if consecutive_failures >= max_consecutive_failures:
                    self.logger.error(
                        "Multiple consecutive request failures detected. Failing fast."
                    )
                    raise Exception(
                        f"Unable to connect to MeteoTracker after {consecutive_failures} consecutive failures"
                    )

                continue

            consecutive_failures = 0
            payload = response.json()

            sessions_metadata.append(payload)
            if len(payload) < params["items"]:
                break

        out = [s for session in sessions_metadata for s in session]
        return [MT_Session(**j) for j in out]

    async def _fetch_session_data(self, session: MT_Session):
        self.logger.debug("Session data fetching")
        variables = session.columns + ["time", "lo"]
        params = dict(id=session.id, data=" ".join(variables))

        response = await self._make_request("/points/session", params=params)

        if response is None:
            self.logger.error("failed to fetch session data for %s ", session.id)
            return None

        payload = response.json()

        df = pd.DataFrame.from_records(payload)
        if df.empty:
            self.logger.warning("No data found for session %s", session.id)
            return None

        df["time"] = pd.to_datetime(df["time"], utc=True)
        df["station_id"] = session.id

        if "lo" in df:
            df[LatitudeColumn], df[LongitudeColumn] = (
                [r[1] for r in df["lo"].values],
                [r[0] for r in df["lo"].values],
            )
            del df["lo"]

        living_labs = {
            living_lab
            for pattern, living_lab in self.author_regex_to_living_lab.items()
            if pattern.match(session.author)
        }
        if len(living_labs) == 0:
            living_lab = "unknown"
        elif len(living_labs) == 1:
            living_lab = living_labs.pop()
        else:
            raise ValueError(
                f"Multiple living labs matched for {session.author = } {living_labs = }"
            )

        df["living_lab"] = living_lab
        df["author"] = session.author

        return df
