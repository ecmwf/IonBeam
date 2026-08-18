# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import re
from datetime import datetime
from typing import Any, AsyncIterator, Optional
from uuid import UUID

import httpx
from httpx_retries import Retry, RetryTransport
import pandas as pd
import structlog
from ionbeam_client import IonbeamClient
from ionbeam_client.canonical_stream import canonical_record_batches
from ionbeam_client.alignment import drop_undeclared_columns
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

from .models import MeteoTrackerConfig, MT_Session

# MeteoTracker device channel -> the canonical column it lands in.
DEVICE_CHANNELS = {
    "a": "altitude",
    "P": "air_pressure",
    "T0": "air_temperature",
    "H": "relative_humidity",
    "tp": "air_potential_temperature",
    "td": "dew_point_temperature",
    "s": "wind_speed",
    "CO2": "mole_fraction_of_carbon_dioxide_in_air",
    "CO": "mole_fraction_of_carbon_monoxide_in_air",
    "SO2": "mole_fraction_of_sulphur_dioxide_in_air",
    "m1": "mass_concentration_of_pm1_ambient_aerosol_in_air",
    "m2": "mass_concentration_of_pm2p5_ambient_aerosol_in_air",
    "m10": "mass_concentration_of_pm10_ambient_aerosol_in_air",
    "O3": "ozone",
    "HDX": "humidity_index",
    "i": "air_temperature_lapse_rate",
    "L": "solar_radiation_index",
    "bt": "bluetooth_rssi",
    "m4": "mass_concentration_of_pm4_ambient_aerosol_in_air",
    "n0": "particulate_matter_particle_number_0_5",
    "n1": "particulate_matter_particle_number_1",
    "n2": "particulate_matter_particle_number_2_5",
    "n4": "particulate_matter_particle_number_4",
    "n10": "particulate_matter_particle_number_10",
    "tps": "typical_particle_size",
    "EAQ": "epa_air_quality",
    "FAQ": "fast_air_quality",
}


class MeteoTrackerSource:
    def __init__(self, config: MeteoTrackerConfig):
        self.config = config
        self.logger = structlog.get_logger(__name__)
        self._access_token: str | None = None
        self._http = httpx.AsyncClient(
            timeout=config.timeout,
            headers=config.headers or {},
            follow_redirects=True,
            transport=RetryTransport(
                retry=Retry(total=config.max_retries, backoff_factor=0.5)
            ),
        )
        self.metadata: IngestionMetadata = IngestionMetadata(
            # Altitude is the trajectory's z coordinate (it locates the
            # observation); only genuine CF standard names claim CF governance —
            # the device's own channels (bluetooth_rssi, air-quality indices,
            # particle counts, pm4) are ungoverned named columns.
            version=3,
            name="meteotracker",
            dataset_schema=DatasetSchema(
                time=TimeCoordinate(),
                coordinates=geographic_point_coordinates(altitude=True),
                variables=[
                    cf("air_pressure", "hPa"),
                    cf("air_temperature", "degC"),
                    cf("relative_humidity", "%"),
                    cf("air_potential_temperature", "K"),
                    cf("dew_point_temperature", "degC"),
                    cf("wind_speed", "km h-1"),
                    cf("mole_fraction_of_carbon_dioxide_in_air", "1e-6"),
                    cf("mole_fraction_of_carbon_monoxide_in_air", "1e-6"),
                    cf("mole_fraction_of_sulphur_dioxide_in_air", "1e-6"),
                    cf("mass_concentration_of_pm1_ambient_aerosol_in_air", "ug m-3"),
                    cf("mass_concentration_of_pm2p5_ambient_aerosol_in_air", "ug m-3"),
                    cf("mass_concentration_of_pm10_ambient_aerosol_in_air", "ug m-3"),
                    Variable(
                        name="ozone",
                        semantics=CfSemantics(standard_name="mole_fraction_of_ozone_in_air"),
                        unit="1e-9",
                    ),
                    Variable(name="humidity_index", unit="degC"),
                    Variable(name="air_temperature_lapse_rate", unit="K hm-1"),
                    Variable(name="solar_radiation_index", unit="1"),
                    Variable(name="bluetooth_rssi", unit="dBm"),
                    # CF names pm1/pm2p5/pm10 exist; there is no pm4 standard name
                    Variable(name="mass_concentration_of_pm4_ambient_aerosol_in_air",
                             unit="ug m-3"),
                    Variable(name="particulate_matter_particle_number_0_5", unit="cm-3"),
                    Variable(name="particulate_matter_particle_number_1", unit="cm-3"),
                    Variable(name="particulate_matter_particle_number_2_5", unit="cm-3"),
                    Variable(name="particulate_matter_particle_number_4", unit="cm-3"),
                    Variable(name="particulate_matter_particle_number_10", unit="cm-3"),
                    Variable(name="typical_particle_size", unit="um"),
                    Variable(name="epa_air_quality", unit="1"),
                    Variable(name="fast_air_quality", unit="1"),
                ],
                tags=[
                    Tag(name="station_id"),
                    Tag(name="living_lab"),
                    Tag(name="author"),
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
        ingestion_id: Optional[UUID] = None,
    ) -> None:
        sessions_metadata = await self._fetch_session_metadata(start_time, end_time)
        self.logger.debug("Fetched session metadata", count=len(sessions_metadata))

        if not sessions_metadata:
            self.logger.warning("No session metadata available")
            return

        async def dataframe_stream() -> AsyncIterator[pd.DataFrame]:
            for session in sessions_metadata:
                df = await self._fetch_session_data(session)
                yield df

        batch_stream = canonical_record_batches(dataframe_stream(), self.metadata)

        await client.ingest(
            batch_stream=batch_stream,
            metadata=self.metadata,
            start_time=start_time,
            end_time=end_time,
            ingestion_id=ingestion_id,
        )

        self.logger.info(
            "MeteoTracker ingestion completed",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

    async def _authenticate(self) -> None:
        if not self.config.username or not self.config.password:
            raise RuntimeError("username and password required for authentication")

        response = await self._http.post(
            self.config.token_endpoint,
            json={"email": self.config.username, "password": self.config.password},
        )
        response.raise_for_status()
        token = response.json().get("accessToken")
        if not token:
            raise RuntimeError("authentication response missing access token")
        self._access_token = token

    async def _get(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        """One authenticated GET; transient failures retry in the transport.
        A 401 re-authenticates once — a second 401 raises, and any other
        failure raises so the trigger redelivers the whole fetch."""
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        if not self._access_token:
            await self._authenticate()
        response = await self._http.get(url, params=params, headers=self._auth)
        if response.status_code == 401:
            await self._authenticate()
            response = await self._http.get(url, params=params, headers=self._auth)
        response.raise_for_status()
        return response

    @property
    def _auth(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_token}"}

    async def _fetch_session_metadata(
        self, start_time: datetime, end_time: datetime
    ) -> list[MT_Session]:
        t1, t2 = (int(t.timestamp()) for t in [start_time, end_time])
        params = {
            "startTime": f'{{"$gte":{t1},"$lte":{t2}}}',
            "dataType": "all",
            "items": 1000,
        }

        sessions_metadata = []

        for i in range(self.config.max_queries):
            params["page"] = i
            response = await self._get("/sessions", params=params)
            payload = response.json()

            sessions_metadata.append(payload)
            if len(payload) < params["items"]:
                break
        else:
            # Stopping here would ingest a truncated sweep and still claim the
            # whole range as checked; fail so the trigger redelivers.
            raise RuntimeError(
                f"session metadata exceeded max_queries={self.config.max_queries} "
                f"pages; range not fully swept"
            )

        out = [s for session in sessions_metadata for s in session]
        return [MT_Session(**j) for j in out]

    async def _fetch_session_data(self, session: MT_Session):
        variables = session.columns + ["time", "lo"]
        params = dict(id=session.id, data=" ".join(variables))

        response = await self._get("/points/session", params=params)
        payload = response.json()

        df = pd.DataFrame.from_records(payload)
        if df.empty:
            self.logger.warning("No data found for session %s", session.id)
            return None

        df["time"] = pd.to_datetime(df["time"], utc=True)
        df["station_id"] = session.id

        if "lo" in df:
            unlocated = df["lo"].isna()
            if unlocated.any():
                self.logger.warning(
                    "Dropping %s points without location in session %s",
                    int(unlocated.sum()),
                    session.id,
                )
                df = df[~unlocated].reset_index(drop=True)
                if df.empty:
                    return None
            df["lat"], df["lon"] = (
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

        df = df.rename(columns=DEVICE_CHANNELS)
        return drop_undeclared_columns(df, self.metadata.dataset_schema)
