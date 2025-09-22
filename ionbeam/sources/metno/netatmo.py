import asyncio
import logging
import pathlib
from datetime import datetime, time, timedelta, timezone
from typing import AsyncIterator, List, Optional, Tuple
from uuid import uuid4

import httpx
import numpy as np
import pandas as pd
import pyarrow as pa
from httpx_retries import Retry, RetryTransport
from pydantic import BaseModel
from shapely import Polygon

from ionbeam.utilities.dataframe_tools import coerce_types

from ...core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ...core.handler import BaseHandler
from ...models.models import (
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
from ...utilities.parquet_tools import stream_dataframes_to_parquet


class NetAtmoConfig(BaseModel):
    base_url: str = "https://observations.meteogate.eu"
    username: str
    password: str
    timeout_seconds: int = 60
    concurrency: int = 8
    data_path: pathlib.Path
    trigger_queue: str = "ionbeam.source.netatmo.start"
    ingestion_queue: str = "ionbeam.ingestion.ingestV1"


retry_transport = RetryTransport(retry=Retry(total=5, backoff_factor=0.5))

netatmo_metadata: IngestionMetadata = IngestionMetadata(
            dataset=DatasetMetadata(
                name="netatmo",
                aggregation_span=timedelta(hours=1),
                subject_to_change_window=timedelta(hours=2),
                description="IoT NetAtmo data collected from Met No",
                source_links=[],
                keywords=["netatmo", "iot", "data"],
            ),
            ingestion_map=DataIngestionMap(
                datetime=TimeAxis(),
                lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
                lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
                canonical_variables=[
                    CanonicalVariable(
                        column="air_temperature:2.0:point:PT0S",
                        standard_name="air_temperature",
                        cf_unit="degC",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    ),
                    CanonicalVariable(
                        column="relative_humidity:2.0:point:PT0S",
                        standard_name="relative_humidity",
                        cf_unit="1",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    ),
                    CanonicalVariable(
                        column="surface_air_pressure:2.0:point:PT0S",
                        standard_name="surface_air_pressure",
                        cf_unit="hPa",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    ),
                    CanonicalVariable(
                        column="wind_from_direction:2.0:mean:PT5M",
                        standard_name="wind_from_direction",
                        cf_unit="deg",
                        level=2.0,
                        method="mean",
                        period="PT5M",
                    ),
                    CanonicalVariable(
                        column="wind_speed:2.0:mean:PT5M",
                        standard_name="wind_speed",
                        cf_unit="m s-1",
                        level=2.0,
                        method="mean",
                        period="PT5M",
                    ),
                    CanonicalVariable(
                        column="wind_speed_of_gust:2.0:mean:PT5M",
                        standard_name="wind_speed_of_gust",
                        cf_unit="m/s",
                        level=2.0,
                        method="mean",
                        period="PT5M",
                    )
                ],
                metadata_variables=[
                    MetadataVariable(column="station_id"),
                ],
            ),
            version=1,
        )

class NetAtmoSource(BaseHandler[StartSourceCommand, Optional[IngestDataCommand]]):
    def __init__(self, config: NetAtmoConfig):
        super().__init__("NetAtmoSource")
        self._config = config
        self.metadata: IngestionMetadata = netatmo_metadata


    def split_bbox(self, bbox_dict, scale_factor=9):
        """
        Split a bbox (given as dict) into smaller bboxes using a square grid.
        bbox_dict: {'min_lon': float, 'min_lat': float, 'max_lon': float, 'max_lat': float}
        scale_factor: must be a perfect square (e.g., 4 -> 2x2)
        Returns: list of dicts with same keys
        """
        minx = bbox_dict['min_lon']
        miny = bbox_dict['min_lat']
        maxx = bbox_dict['max_lon']
        maxy = bbox_dict['max_lat']

        n = int(scale_factor ** 0.5)
        if n * n != scale_factor:
            raise ValueError("scale_factor must be a perfect square")

        dx = (maxx - minx) / n
        dy = (maxy - miny) / n

        tiles = []
        for j in range(n):
            for i in range(n):
                tiles.append({
                    'min_lon': minx + i * dx,
                    'min_lat': miny + j * dy,
                    'max_lon': minx + (i + 1) * dx,
                    'max_lat': miny + (j + 1) * dy
                })
        return tiles

    def _transform_station_data_to_df(self, station_coverage: dict) -> pd.DataFrame:
        # Depending on the actual endpoint we'll use the dict could either be a payload contains a single Coverage or a CoverageCollection
        if "coverages" in station_coverage:
            coverages = station_coverage["coverages"]
        else:
            coverages = [station_coverage]

        df_list = []
        for coverage in coverages:
            domain = coverage.get("domain", {})
            ranges = coverage.get("ranges", {})
            # parameters = coverage.get("parameters", {})
            
            station_id = coverage.get('metocean:wigosId', 'unknown')

            # Extract axes
            axes = domain.get("axes", {})
            timestamps = axes.get("t", {}).get("values", [])
            xs = axes.get("x", {}).get("values", [])
            ys = axes.get("y", {}).get("values", [])

            # Prepare a dictionary to hold all parameter values indexed by flat index
            param_values = {}
            for param_name, param_data in ranges.items():
                values = param_data.get("values", [])
                param_values[param_name] = values

            # Create rows by combining timestamp, x, y, and all parameter values
            rows = []
            for t_index, timestamp in enumerate(timestamps):
                for y_index, y in enumerate(ys):
                    for x_index, x in enumerate(xs):
                        flat_index = (t_index * len(ys) * len(xs)) + (y_index * len(xs)) + x_index
                        row = {ObservationTimestampColumn: timestamp, LongitudeColumn: x, LatitudeColumn: y, 'station_id': station_id}

                        for param_name, values in param_values.items():
                            row[param_name] = values[flat_index]
                        rows.append(row)
            df = pd.DataFrame(rows)
            df_list.append(df)
        df = pd.concat(df_list, ignore_index=True)
        # self.logger.info(f"After first DF {df.head()}")
        return df

    async def crawl_netatmo_by_area(self, start_time: datetime, end_time: datetime, cache_only) -> AsyncIterator[pd.DataFrame]:
        self.logger.info(cache_only)
        assert (end_time - start_time).total_seconds() <= 86400, "NetAtmo only supports 24h windows"
        datetime_range = f"{start_time.strftime('%Y-%m-%dT%H:%MZ')}/{end_time.strftime('%Y-%m-%dT%H:%MZ')}"
        
        url = f"{self._config.base_url}/collections/observations/area"
        # TODO - currently /area endpoint does not expose any filtering for naming authority - so we are not guarateeed the data caputured here is actually NetAtmo data

        auth = httpx.BasicAuth(self._config.username, self._config.password)

        async with httpx.AsyncClient(timeout=self._config.timeout_seconds, transport=retry_transport, auth=auth) as client:
            countries = dict(
                ch=dict(min_lon=5.96, min_lat=45.82, max_lon=10.49, max_lat=47.81),  # http://bboxfinder.com/#45.82,5.96,47.81,10.49
                cz=dict(min_lon=12.09, min_lat=48.55, max_lon=18.86, max_lat=51.06),  # http://bboxfinder.com/#48.55,12.09,51.06,18.86
                de=dict(min_lon=5.87, min_lat=47.27, max_lon=15.04, max_lat=55.06),  # http://bboxfinder.com/#47.27,5.87,55.06,15.04
                dk=dict(min_lon=8.09, min_lat=54.56, max_lon=12.69, max_lat=57.75),  # http://bboxfinder.com/#54.56,8.09,57.75,12.69
                fi=dict(min_lon=19.08, min_lat=59.81, max_lon=31.59, max_lat=70.09),  # http://bboxfinder.com/#59.81,19.08,70.09,31.59
                fr=dict(min_lon=-5.14, min_lat=41.30, max_lon=9.56, max_lat=51.12),  # http://bboxfinder.com/#41.30,-5.14,51.12,9.56
                gb=dict(min_lon=-8.62, min_lat=49.86, max_lon=1.77, max_lat=60.85),  # http://bboxfinder.com/#49.86,-8.62,60.85,1.77
                ie=dict(min_lon=-10.48, min_lat=51.42, max_lon=-5.34, max_lat=55.43),  # http://bboxfinder.com/#51.42,-10.48,55.43,-5.34
                it=dict(min_lon=6.62, min_lat=36.65, max_lon=18.51, max_lat=47.10),  # http://bboxfinder.com/#36.65,6.62,47.10,18.51
                lu=dict(min_lon=5.74, min_lat=49.45, max_lon=6.53, max_lat=50.18),  # http://bboxfinder.com/#49.45,5.74,50.18,6.53
                nl=dict(min_lon=3.36, min_lat=50.75, max_lon=7.22, max_lat=53.53),  # http://bboxfinder.com/#50.75,3.36,53.53,7.22
                no=dict(min_lon=4.99, min_lat=57.98, max_lon=31.07, max_lat=71.18),  # http://bboxfinder.com/#57.98,4.99,71.18,31.07
                se=dict(min_lon=11.03, min_lat=55.34, max_lon=23.67, max_lat=69.06)   # http://bboxfinder.com/#55.34,11.03,69.06,23.67
            )

            for country, bbox in countries.items():
                bboxes = self.split_bbox(bbox)
                for i, bbox in enumerate(bboxes):
                    # convert to WKT polygon
                    coords = [
                        (bbox["min_lon"], bbox["min_lat"]),
                        (bbox["max_lon"], bbox["min_lat"]),
                        (bbox["max_lon"], bbox["max_lat"]),
                        (bbox["min_lon"], bbox["max_lat"]),
                        (bbox["min_lon"], bbox["min_lat"]) 
                    ]
                    # out = Polygon(coords).wkt
                    # self.logger.info(out)
                    params = {
                        "coords": Polygon(coords).wkt,
                        "datetime": datetime_range,
                        "f": "CoverageJSON",
                    }
                    headers = {
                        "Accept": "application/prs.coverage+json",
                        "Accept-Encoding": "gzip, deflate, br, zstd"
                    }
                    
                    cache_key = f"area_{country}_{i}_{start_time.timestamp()}_{end_time.timestamp()}"
                    logger = self.logger
                    # @cached(cache_key, cache_only)
                    async def fetch_station_data_by_area():
                        response = await client.get(url, params=params, headers=headers)
                        # response.raise_for_status()
                        logger.info("-----------")
                        logger.info(response.status_code)
                        if(response.status_code != 200):
                            return dict()
                        return response.json()
                    
                    station_coverage = await fetch_station_data_by_area()
                    result = self._transform_station_data_to_df(station_coverage)
                    if not result.empty:
                        result = coerce_types(result, self.metadata.ingestion_map)
                        yield result
                    else:
                        self.logger.warning("Not data found for %s", cache_key)

    async def _handle(self, event: StartSourceCommand) -> Optional[IngestDataCommand]:
        self._config.data_path.mkdir(parents=True, exist_ok=True)
        path = self._config.data_path / f"{self.metadata.dataset.name}_{event.start_time}-{event.end_time}_{datetime.now(timezone.utc)}.parquet"
        
        async def dataframe_stream():
                async for chunk in self.crawl_netatmo_by_area(event.start_time, event.end_time, event.use_cache):
                    if chunk is not None:
                        yield chunk

        # Build schema from ingestion_map
        schema_fields: List[Tuple[str, pa.DataType]] = [
            (self.metadata.ingestion_map.datetime.from_col or ObservationTimestampColumn, pa.timestamp('ns', tz='UTC')),
            (self.metadata.ingestion_map.lat.from_col or LatitudeColumn, pa.float64()),
            (self.metadata.ingestion_map.lon.from_col or LongitudeColumn, pa.float64()),
        ]

        for var in self.metadata.ingestion_map.canonical_variables + self.metadata.ingestion_map.metadata_variables:
            pa_type = pa.string() if var.dtype == "string" or var.dtype == "object" else pa.from_numpy_dtype(np.dtype(var.dtype))
            schema_fields.append((var.column, pa_type))

        # Stream data directly to parquet file using helper
        total_rows = await stream_dataframes_to_parquet(
            dataframe_stream(),
            path,
            schema_fields
        )

        if total_rows == 0:
            self.logger.warning("No data collected")
            return None

        self.logger.info(f"Saved {total_rows} rows to {path}")
        
        # TODO - we need to gzip our cache directory and store it somewhere else

        return IngestDataCommand(
            id=uuid4(),
            metadata=self.metadata,
            payload_location=path,
            start_time=event.start_time,
            end_time=event.end_time,
        )


async def main():
    config = NetAtmoConfig(
        base_url='',
        data_path=pathlib.Path("./data-raw"),
        username="",
        password="")
    source = NetAtmoSource(config)

    now = datetime.now(timezone.utc)
    start = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)
    end = datetime.combine(now.date(), time.max, tzinfo=timezone.utc)

    command = StartSourceCommand(
        id=uuid4(),
        source_name="netatmo",
        start_time=start,
        end_time=end,
        use_cache=False,
    )
    result = await source.handle(command)
    print(result)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        pass
