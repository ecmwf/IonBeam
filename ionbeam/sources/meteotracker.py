import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Dict, List, NewType, Optional
from uuid import uuid4

import httpx
import pandas as pd
from pydantic import BaseModel

from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.storage.arrow_store import ArrowStore
from ionbeam.utilities.arrow_tools import dataframes_to_record_batches, schema_from_ingestion_map
from ionbeam.utilities.cache import cached

from ..core.constants import LatitudeColumn, LongitudeColumn
from ..core.handler import BaseHandler
from ..models.models import (
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


class MeteoTrackerConfig(BaseModel):
    base_url: str = "https://app.meteotracker.com/api/"
    token_endpoint: str = "https://app.meteotracker.com/auth/login/api"
    refresh_endpoint: str = "https://app.meteotracker.com/auth/refreshtoken"
    timeout: int = 30
    max_retries: int = 3
    headers: Optional[Dict[str, str]] = None
    max_queries: int = 500
    username: Optional[str] = None
    password: Optional[str] = None
    cache_enabled: bool = True
    author_patterns: Dict[str, List[str]] = {
        "Bologna": ["bologna_living_lab_.+"],
        "Barcelona": ["barcelona_living_lab_.+", "Barcelona_living_lab_.+"],
        "Genoa": ["CIMA I-Change", "genova_living_lab_.+"],
        "Amsterdam": ["Amsterdam_living_lab_ICHANGE", "Gert-Jan Steeneveld"],
        "Ouagadougou": ["llwa_living_lab_.+"],
        "Dublin": ["Dublin LL"],
        "Jerusalem": ["jerusalem_living_lab_.+"],
    }


## Meteotracker helper classes
SessionId = NewType("SessionId", str)
Location = NewType("Location", str)
Username = NewType("Username", str)


@dataclass
class MT_Session:
    "Represents a single MeteoTracker trip"

    id: SessionId
    n_points: int
    offset_tz: str
    start_time: datetime
    author: str
    raw_json: dict = field(repr=False)
    end_time: datetime | None
    columns: List[str]
    living_lab: str | None = None

    def __init__(self, **d):
        self.id = SessionId(d["_id"])
        self.n_points = int(d["nPoints"])
        self.offset_tz = d["offsetTZ"]
        self.start_time = datetime.fromisoformat(d["startTime"])
        self.end_time = datetime.fromisoformat(d["endTime"]) if "endTime" in d else None
        self.columns = [k for k in d if isinstance(d[k], dict) and "avgVal" in d[k]]  # filter for nonzero data
        self.author = d["by"]
        self.raw_json = d


class MeteoTrackerSource(BaseHandler[StartSourceCommand, Optional[IngestDataCommand]]):
    def __init__(self, config: MeteoTrackerConfig, metrics: IonbeamMetricsProtocol, arrow_store: ArrowStore):
        super().__init__("MeteoTrackerSource", metrics)
        self.config = config
        self.arrow_store = arrow_store
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self.metadata: IngestionMetadata = IngestionMetadata(
            dataset=DatasetMetadata(
                name="meteotracker",
                description="raw IoT data collected from MeteoTracker",
                subject_to_change_window=timedelta(days=1),
                aggregation_span=timedelta(days=1),
                source_links=[],
                keywords=["meteotracker", "iot", "data"],
            ),
            ingestion_map=DataIngestionMap(
                datetime=TimeAxis(from_col="time"),
                lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
                lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
                canonical_variables=[
                    CanonicalVariable(column="a", standard_name="altitude", cf_unit="m"),
                    CanonicalVariable(column="P", standard_name="air_pressure", cf_unit="mbar"),
                    CanonicalVariable(column="T0", standard_name="air_temperature", cf_unit="degC"),
                    CanonicalVariable(column="H", standard_name="relative_humidity", cf_unit="1"),
                    CanonicalVariable(column="tp", standard_name="air_potential_temperature", cf_unit="K"),
                    CanonicalVariable(column="td", standard_name="dew_point_temperature", cf_unit="degC"),
                    CanonicalVariable(column="HDX", standard_name="humidity_index", cf_unit="degC"),  # Not a valid cf standard
                    CanonicalVariable(
                        column="i", standard_name="air_temperature_lapse_rate", cf_unit="degC/100m"
                    ),  # TODO - CF standard is only deg_c m-1 - should the source convert?
                    CanonicalVariable(column="s", standard_name="wind_speed", cf_unit="km h-1"),
                    CanonicalVariable(column="L", standard_name="solar_radiation_index", cf_unit="1"),  # Not a valid cf standard
                    CanonicalVariable(column="bt", standard_name="bluetooth_RSSI", cf_unit="dBm"),  # Not a valid cf standard
                    CanonicalVariable(column="CO2", standard_name="mole_fraction_of_carbon_dioxide_in_air", cf_unit="1e-6"),  # ppm
                    CanonicalVariable(column="CO", standard_name="mole_fraction_of_carbon_monoxide_in_air", cf_unit="1e-6"),  # ppm
                    CanonicalVariable(column="SO2", standard_name="mole_fraction_of_sulphur_dioxide_in_air", cf_unit="1e-6"),  # ppm
                    CanonicalVariable(column="m1", standard_name="mass_concentration_of_pm1_ambient_aerosol_in_air", cf_unit="ug m-3"),
                    CanonicalVariable(column="m2", standard_name="mass_concentration_of_pm2p5_ambient_aerosol_in_air", cf_unit="ug m-3"),
                    CanonicalVariable(
                        column="m4", standard_name="mass_concentration_of_pm4_ambient_aerosol_in_air", cf_unit="ug m-3"
                    ),  # pm4 not a cf standard
                    CanonicalVariable(column="m10", standard_name="mass_concentration_of_pm10_ambient_aerosol_in_air", cf_unit="ug m-3"),
                    CanonicalVariable(column="n0", standard_name="particulate_matter_particle_number_0_5", cf_unit="cm-3"),  # Not a valid cf standard
                    CanonicalVariable(column="n1", standard_name="particulate_matter_particle_number_1", cf_unit="cm-3"),  # Not a valid cf standard
                    CanonicalVariable(column="n2", standard_name="particulate_matter_particle_number_2_5", cf_unit="cm-3"),  # Not a valid cf standard
                    CanonicalVariable(column="n4", standard_name="particulate_matter_particle_number_4", cf_unit="cm-3"),  # Not a valid cf standard
                    CanonicalVariable(column="n10", standard_name="particulate_matter_particle_number_10", cf_unit="cm-3"),  # Not a valid cf standard
                    CanonicalVariable(column="tps", standard_name="typical_particle_size", cf_unit="um"),  # Not a valid cf standard
                    CanonicalVariable(column="EAQ", standard_name="epa_air_quality", cf_unit="1"),  # Not a valid cf standard
                    CanonicalVariable(column="FAQ", standard_name="fast_air_quality", cf_unit="1"),  # Not a valid cf standard
                    CanonicalVariable(column="O3", standard_name="ozone", cf_unit="1e-9"),  # ppb - Also not a valid cf standard
                ],
                metadata_variables=[
                    MetadataVariable(column="station_id"),
                    MetadataVariable(column="living_lab"),
                    MetadataVariable(column="author"),
                ],
            ),
        )
        self._arrow_schema = schema_from_ingestion_map(self.metadata.ingestion_map)

        self.author_regex_to_living_lab = {
            re.compile(pattern): living_lab for living_lab, patterns in self.config.author_patterns.items() for pattern in patterns
        }

    async def _authenticate(self) -> bool:
        """
        Authenticate with MeteoTracker OAuth2 API

        Returns:
            True if authentication successful, False otherwise
        """
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
                self.logger.error(f"Authentication failed: HTTP {e.response.status_code}")
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
                self.logger.error(f"Token refresh failed: HTTP {e.response.status_code}")
                self.logger.debug(f"Response: {e.response.text}")
                return False
            except Exception as e:
                self.logger.error(f"Token refresh error: {e}")
                return False

    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        require_auth: bool = True,
    ) -> Optional[httpx.Response]:
        if require_auth and not self._access_token:
            if not await self._authenticate():
                self.logger.error("Authentication failed, cannot make authenticated request")
                return None

        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        # Prepare headers with authentication if available
        request_headers = dict(self.config.headers) if self.config.headers else {}
        if require_auth and self._access_token:
            request_headers["Authorization"] = f"Bearer {self._access_token}"

        async with httpx.AsyncClient(timeout=self.config.timeout, headers=request_headers, follow_redirects=True) as client:
            for attempt in range(self.config.max_retries + 1):
                try:
                    self.logger.debug(f"Making {method} request to {url} (attempt {attempt + 1})")
                    self.logger.debug(f"Params: {params}")
                    self.logger.debug(f"JSON data: {json_data}")

                    if method.upper() == "GET":
                        response = await client.get(url, params=params)
                    elif method.upper() == "POST":
                        response = await client.post(url, params=params, json=json_data)
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")

                    response.raise_for_status()
                    self.logger.debug(f"Successfully fetched {url} - Status: {response.status_code}")
                    self.logger.debug(f"Response headers: {dict(response.headers)}")
                    return response

                except httpx.HTTPStatusError as e:
                    self.logger.warning(f"HTTP error for {url}: {e.response.status_code}")
                    self.logger.debug(f"Response content: {e.response.text}")

                    # Handle 401 Unauthorized - try to refresh token
                    if e.response.status_code == 401 and require_auth and self._refresh_token:
                        self.logger.info("Received 401, attempting to refresh token")
                        if await self._refresh_access_token():
                            # Update headers with new token and retry this attempt
                            request_headers["Authorization"] = f"Bearer {self._access_token}"
                            client.headers.update(request_headers)
                            continue

                        self.logger.error("Token refresh failed, re-authenticating")
                        if await self._authenticate():
                            request_headers["Authorization"] = f"Bearer {self._access_token}"
                            client.headers.update(request_headers)
                            continue

                    if attempt == self.config.max_retries:
                        self.logger.error(f"Failed to fetch {url} after {self.config.max_retries + 1} attempts")
                        return None

                except httpx.RequestError as e:
                    self.logger.warning(f"Request error for {url}: {e}")
                    if attempt == self.config.max_retries:
                        self.logger.error(f"Failed to fetch {url} after {self.config.max_retries + 1} attempts")
                        return None

                except Exception as e:
                    self.logger.error(f"Unexpected error fetching {url}: {e}")
                    return None

                # Exponential backoff
                wait_time = 2**attempt
                self.logger.debug(f"Waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)

        return None

    async def _fetch_session_metadata(self, event: StartSourceCommand) -> List[MT_Session]:
        t1, t2 = (int(t.timestamp()) for t in [event.start_time, event.end_time])
        params = {
            "startTime": (f'{{"$gte":{t1},"$lte":{t2}}}',),
            "dataType": "all",
            "items": 1000,
        }

        @cached(lambda x: f"sessions_{json.dumps(x['startTime'])}_{x.get('page', 0)}", event_id=event.id, cache_enabled=self.config.cache_enabled)
        async def _fetch(params):
            response = await self._make_request("/sessions", params=params)
            if response is None:
                return None
            return response.json()

        sessions_metadata = []
        consecutive_failures = 0
        max_consecutive_failures = 3

        for i in range(self.config.max_queries):
            params["page"] = i
            payload = await _fetch(params)

            if payload is None:
                consecutive_failures += 1
                self.logger.warning("Failed to fetch page %s (consecutive failures: %s)", i, consecutive_failures)

                if consecutive_failures >= max_consecutive_failures:
                    self.logger.error("Multiple consecutive request failures detected. Failing fast.")
                    raise Exception(f"Unable to connect to MeteoTracker after {consecutive_failures} consecutive failures")

                continue

            consecutive_failures = 0

            sessions_metadata.append(payload)
            if len(payload) < params["items"]:
                break

        out = [s for session in sessions_metadata for s in session]
        return [MT_Session(**j) for j in out]

    async def _fetch_session_data(self, session: MT_Session, event: StartSourceCommand):
        self.logger.debug("Session data fetching")
        variables = session.columns + ["time", "lo"]
        params = dict(id=session.id, data=" ".join(variables))

        @cached(lambda x: f"session_points_{x['id']}", event_id=event.id, cache_enabled=self.config.cache_enabled)
        async def _fetch(params):
            response = await self._make_request("/points/session", params=params)
            if response is None:
                return None
            return response.json()

        payload = await _fetch(params)

        if payload is None:
            self.logger.error("failed to fetch session data for %s ", session.id)
            return None

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

        living_labs = {living_lab for pattern, living_lab in self.author_regex_to_living_lab.items() if pattern.match(session.author)}
        if len(living_labs) == 0:
            living_lab = "unknown"
        elif len(living_labs) == 1:
            living_lab = living_labs.pop()
        else:
            raise ValueError(f"Multiple living labs matched for {session.author = } {living_labs = }")

        df["living_lab"] = living_lab
        df["author"] = session.author

        return df

    async def _handle(self, event: StartSourceCommand) -> Optional[IngestDataCommand]:
        dataset_name = self.metadata.dataset.name
        request_start = perf_counter()
        total_rows = 0

        try:
            sessions_metadata = await self._fetch_session_metadata(event)
            self.logger.debug(sessions_metadata)

            if not sessions_metadata:
                request_duration = perf_counter() - request_start
                self.metrics.sources.observe_fetch_duration(dataset_name, request_duration)
                self.metrics.sources.observe_request_rows(dataset_name, 0)
                self.metrics.sources.record_ingestion_request(dataset_name, "empty")
                self.logger.warning("No session metadata available")
                return None

            start_s = event.start_time.strftime("%Y%m%dT%H%M%SZ")
            end_s = event.end_time.strftime("%Y%m%dT%H%M%SZ")
            now_s = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            key = f"raw/{self.metadata.dataset.name}/{start_s}-{end_s}_{now_s}"

            async def dataframe_stream():
                for session in sessions_metadata:
                    df = await self._fetch_session_data(session, event)
                    if df is not None and not df.empty:
                        yield df

            batch_stream = dataframes_to_record_batches(
                dataframe_stream(),
                schema=self._arrow_schema,
                preserve_index=False,
            )

            total_rows = await self.arrow_store.write_record_batches(
                key,
                batch_stream,
                schema=self._arrow_schema,
            )

            request_duration = perf_counter() - request_start
            self.metrics.sources.observe_fetch_duration(dataset_name, request_duration)
            self.metrics.sources.observe_request_rows(dataset_name, int(total_rows))

            if total_rows == 0:
                self.metrics.sources.record_ingestion_request(dataset_name, "empty")
                self.logger.warning("No data collected for MeteoTracker")
                return None

            self.metrics.sources.record_ingestion_request(dataset_name, "success")
            self.logger.info("Saved MeteoTracker data to store", rows=total_rows, key=key)

            return IngestDataCommand(
                id=uuid4(),
                metadata=self.metadata,
                payload_location=key,
                start_time=event.start_time,
                end_time=event.end_time,
            )

        except Exception as e:
            request_duration = perf_counter() - request_start
            self.metrics.sources.observe_fetch_duration(dataset_name, request_duration)
            self.metrics.sources.observe_request_rows(dataset_name, int(total_rows))
            self.metrics.sources.record_ingestion_request(dataset_name, "error")
            self.logger.exception("MeteoTracker ingestion failed", error=str(e))
            raise
