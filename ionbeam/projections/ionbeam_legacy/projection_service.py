import pathlib
import uuid
from dataclasses import dataclass
from typing import AsyncIterator, Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel
from shapely import wkt as wkt_parser
from shapely.geometry import MultiPoint, box

from ionbeam.core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ionbeam.core.handler import BaseHandler
from ionbeam.models.models import DataSetAvailableEvent
from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.storage.arrow_store import ArrowStore


@dataclass
class PlatformConfig:
    """Configuration for platform-specific transformations."""
    schema: pa.Schema
    column_map: dict[str, str]
    station_name_builder: Callable[[pd.DataFrame, str], pd.Series]
    extra_fields: dict[str, Callable[[pd.DataFrame], pd.Series | None]]


class IonbeamLegacyConfig(BaseModel):
    output_path: pathlib.Path


class IonbeamLegacyProjectionService(BaseHandler[DataSetAvailableEvent, None]):
    """
    Transforms and persists materialized view of raw station data + metadata for the legacy API.

    Outputs:
      - Raw data: ionbeam-legacy/data/platform={platform}/{start}_{end}.parquet
      - Station metadata: ionbeam-legacy/metadata/platform={platform}/metadata.parquet
    """

    RAW_DIR = "ionbeam-legacy"
    DATA_SUBDIR = "data"
    METADATA_SUBDIR = "metadata"

    # Common fields shared across all platforms
    # NOTE: 'platform' is NOT included here because it exists as a Hive partition
    # and will be automatically added by DuckDB when reading with hive_partitioning=true
    LEGACY_COMMON_FIELDS = [
        pa.field("datetime", pa.string()),
        pa.field("author", pa.string()),
        pa.field("station_id", pa.string()),
        pa.field("external_station_id", pa.string()),
        pa.field("station_name", pa.string()),
        pa.field("aggregation_type", pa.string()),
        pa.field("chunk_date", pa.string()),
        pa.field("chunk_time", pa.string()),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
    ]

    LEGACY_METEOTRACKER_SCHEMA = pa.schema(LEGACY_COMMON_FIELDS + [
        pa.field("relative_humidity_near_surface", pa.float64()),
        pa.field("solar_radiation_index", pa.float64()),
        pa.field("air_pressure_near_surface", pa.float64()),
        pa.field("altitude", pa.float64()),
        pa.field("vertical_temperature_gradient", pa.float64()),
        pa.field("air_temperature_near_surface", pa.float64()),
        pa.field("bluetooth_RSSI", pa.float64()),
        pa.field("dew_point_temperature", pa.float64()),
        pa.field("potential_temperature", pa.float64()),
        pa.field("humidity_index", pa.float64()),
        pa.field("living_lab", pa.string()),
    ])

    LEGACY_ACRONET_SCHEMA = pa.schema(LEGACY_COMMON_FIELDS + [
        pa.field("rainfall", pa.float64()),
        pa.field("air_temperature_near_surface", pa.float64()),
        pa.field("relative_humidity_near_surface", pa.float64()),
        pa.field("wind_direction_near_surface", pa.float64()),
        pa.field("wind_speed_near_surface", pa.float64()),
        pa.field("air_pressure_near_surface", pa.float64()),
        pa.field("solar_radiation", pa.float64()),
        pa.field("battery_level", pa.float64()),
        pa.field("internal_temperature", pa.float64()),
        pa.field("wind_gust_direction", pa.float64()),
        pa.field("wind_gust", pa.float64()),
        pa.field("thermometer_min", pa.float64()),
        pa.field("thermometer_max", pa.float64()),
        pa.field("signal_strength", pa.float64()),
    ])

    METADATA_SCHEMA = pa.schema([
        pa.field("external_id", pa.string()),
        pa.field("internal_id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("description", pa.string()),
        pa.field("aggregation_type", pa.string()),
        pa.field("time_span_start", pa.timestamp("ms", tz="UTC")),
        pa.field("time_span_end", pa.timestamp("ms", tz="UTC")),
        pa.field("location_lat", pa.float64()),
        pa.field("location_lon", pa.float64()),
        pa.field("author", pa.string()),
        pa.field("geometry_wkt", pa.string()),
    ])

    METEOTRACKER_COLUMN_MAP = {
        "relative_humidity__1__0.0__point__PT0S": "relative_humidity_near_surface",
        "solar_radiation_index__1__0.0__point__PT0S": "solar_radiation_index",
        "air_pressure__mbar__0.0__point__PT0S": "air_pressure_near_surface",
        "altitude__m__0.0__point__PT0S": "altitude",
        "air_temperature_lapse_rate__degC/100m__0.0__point__PT0S": "vertical_temperature_gradient",
        "air_temperature__degC__0.0__point__PT0S": "air_temperature_near_surface",
        "bluetooth_RSSI__dBm__0.0__point__PT0S": "bluetooth_RSSI",
        "dew_point_temperature__degC__0.0__point__PT0S": "dew_point_temperature",
        "air_potential_temperature__K__0.0__point__PT0S": "potential_temperature",
        "humidity_index__degC__0.0__point__PT0S": "humidity_index",
    }

    ACRONET_COLUMN_MAP = {
        "precipitation_amount__mm__0.0__point__PT0S": "rainfall",
        "air_temperature__degC__0.0__point__PT0S": "air_temperature_near_surface",
        "relative_humidity__%__0.0__point__PT0S": "relative_humidity_near_surface",
        "wind_from_direction__degree__0.0__point__PT0S": "wind_direction_near_surface",
        "wind_speed__m s-1__0.0__point__PT0S": "wind_speed_near_surface",
        "air_pressure__hPa__0.0__point__PT0S": "air_pressure_near_surface",
        "surface_downwelling_shortwave_flux_in_air__W m-2__0.0__point__PT0S": "solar_radiation",
        "battery_level__V__0.0__point__PT0S": "battery_level",
        "indoor_air_temperature__degC__0.0__point__PT0S": "internal_temperature",
        "wind_from_direction_of_gust__degree__0.0__point__PT0S": "wind_gust_direction",
        "wind_speed_of_gust__m s-1__0.0__point__PT0S": "wind_gust",
        "minimum_air_temperature__degC__0.0__point__PT0S": "thermometer_min",
        "maximum_air_temperature__degC__0.0__point__PT0S": "thermometer_max",
        "signal_strength__CSQ__0.0__point__PT0S": "signal_strength",
    }

    def __init__(
        self,
        config: IonbeamLegacyConfig,
        metrics: IonbeamMetricsProtocol,
        arrow_store: ArrowStore,
    ):
        super().__init__("IonbeamLegacyProjectionService", metrics)
        self.config = config
        self.arrow_store = arrow_store

        # Platform-specific configurations
        self._platform_configs: dict[str, PlatformConfig] = {
            "meteotracker": PlatformConfig(
                schema=self.LEGACY_METEOTRACKER_SCHEMA,
                column_map=self.METEOTRACKER_COLUMN_MAP,
                station_name_builder=lambda df, p: "meteotracker: " + df["station_id"].astype(str),
                extra_fields={"living_lab": lambda df: df.get("living_lab", None)}
            ),
            "acronet": PlatformConfig(
                schema=self.LEGACY_ACRONET_SCHEMA,
                column_map=self.ACRONET_COLUMN_MAP,
                station_name_builder=lambda df, p: df.get("station_name", df["station_id"]),
                extra_fields={}
            ),
        }

        self._station_name_resolver: dict[str, Callable[[str, pd.DataFrame, str], str]] = {
            "meteotracker": lambda sid, grp, platform: f"{platform}: {sid}",
            "acronet": lambda sid, grp, platform: grp["station_name"].iloc[-1] if "station_name" in grp.columns else sid,
        }

    def _calculate_trajectory_wkt(self, group_df: pd.DataFrame) -> str:
        coords = group_df[[LongitudeColumn, LatitudeColumn]].values
        trajectory = MultiPoint(coords)
        return trajectory.wkt
    
    def _calculate_centroid_from_trajectory(self, trajectory_wkt: str) -> tuple[float, float, str]:
        trajectory = wkt_parser.loads(trajectory_wkt)
        bbox = box(*trajectory.bounds)
        centroid = bbox.centroid
        return centroid.y, centroid.x, bbox.wkt

    def _build_common_fields(
        self,
        df: pd.DataFrame,
        platform: str,
        dt_series: pd.Series
    ) -> pd.DataFrame:
        legacy_df = pd.DataFrame()
        
        legacy_df["datetime"] = dt_series.apply(
            lambda x: x.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        )
        
        legacy_df["author"] = df.get("author", None)
        legacy_df["station_id"] = df["station_id"]
        legacy_df["external_station_id"] = df["station_id"]
        legacy_df["aggregation_type"] = "by_time"
        
        floored = dt_series.dt.floor('H')
        legacy_df["chunk_date"] = floored.dt.strftime("%Y%m%d")
        legacy_df["chunk_time"] = floored.dt.strftime("%H%M")
        
        legacy_df["lat"] = df[LatitudeColumn]
        legacy_df["lon"] = df[LongitudeColumn]
        
        return legacy_df

    def _transform_raw_data(
        self,
        table: pa.Table,
        platform: str,
        config: PlatformConfig
    ) -> pa.Table:
        df = table.to_pandas()
        
        if df.empty:
            return pa.table({}, schema=config.schema)

        dt_series = pd.to_datetime(df[ObservationTimestampColumn], unit="ms", utc=True)
        
        legacy_df = self._build_common_fields(df, platform, dt_series)
        
        legacy_df["station_name"] = config.station_name_builder(df, platform)
        
        for canonical_name, legacy_name in config.column_map.items():
            legacy_df[legacy_name] = df.get(canonical_name, None)
        
        for field_name, extractor in config.extra_fields.items():
            result = extractor(df)
            if result is not None:
                legacy_df[field_name] = result
            else:
                legacy_df[field_name] = None
        
        return pa.Table.from_pandas(legacy_df, schema=config.schema)

    def _build_metadata_base(
        self,
        df: pd.DataFrame,
        platform: str,
        name_resolver: Callable[[str, pd.DataFrame, str], str]
    ) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        
        results = []
        for station_id, group in df.groupby("station_id"):
            trajectory_wkt = self._calculate_trajectory_wkt(group)
            lat, lon, bbox_wkt = self._calculate_centroid_from_trajectory(trajectory_wkt)
            
            station_id_str = str(station_id)
            
            result = {
                "station_id": station_id_str,
                "time_span_start": group[ObservationTimestampColumn].min(),
                "time_span_end": group[ObservationTimestampColumn].max(),
                "location_lat": lat,
                "location_lon": lon,
                "geometry_wkt": trajectory_wkt,
                "author": group["author"].iloc[-1] if "author" in group.columns else None,
                "resolved_name": name_resolver(station_id_str, group, platform)
            }
            results.append(result)
        
        agg = pd.DataFrame(results)
        
        agg["external_id"] = agg["station_id"]
        agg["internal_id"] = platform + "_" + agg["station_id"]
        agg["name"] = agg["resolved_name"]
        agg["description"] = None
        agg["aggregation_type"] = "by_time"
        
        agg["time_span_start"] = pd.to_datetime(agg["time_span_start"], utc=True)
        agg["time_span_end"] = pd.to_datetime(agg["time_span_end"], utc=True)
        
        return agg[[
            "external_id", "internal_id", "name", "description",
            "aggregation_type", "time_span_start", "time_span_end",
            "location_lat", "location_lon", "author", "geometry_wkt"
        ]]

    def _aggregate_metadata_unified(
        self,
        table: pa.Table,
        platform: str,
        name_resolver: Callable[[str, pd.DataFrame, str], str]
    ) -> pa.Table:
        """Unified metadata aggregation."""
        df = table.to_pandas()
        
        if df.empty:
            return pa.table({}, schema=self.METADATA_SCHEMA)
        
        result_df = self._build_metadata_base(df, platform, name_resolver)
        return pa.Table.from_pandas(result_df, schema=self.METADATA_SCHEMA)

    def _aggregate_station_metadata(self, table: pa.Table, platform: str) -> pa.Table:
        """Route to platform-specific metadata aggregation."""
        name_resolver = self._station_name_resolver.get(
            platform,
            lambda sid, grp, p: f"{p}: {sid}"
        )
        return self._aggregate_metadata_unified(table, platform, name_resolver)

    def _merge_metadata(self, new_table: pa.Table, platform: str) -> None:
        """Merge new metadata with existing and write to disk."""
        out_dir = self.config.output_path / self.RAW_DIR / self.METADATA_SUBDIR
        platform_dir = out_dir / f"platform={platform}"
        platform_dir.mkdir(parents=True, exist_ok=True)
        
        metadata_file = platform_dir / "metadata.parquet"
        new_df = new_table.to_pandas()
        
        if metadata_file.exists():
            try:
                existing_df = pq.read_table(str(metadata_file)).to_pandas()
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            except Exception as e:
                self.logger.warning("Failed to read existing metadata, overwriting", error=str(e))
                combined_df = new_df
        else:
            combined_df = new_df
        
        results = []
        for internal_id, group in combined_df.groupby("internal_id"):
            all_points = []
            for wkt in group["geometry_wkt"].dropna():
                geom = wkt_parser.loads(wkt)
                if geom.geom_type == 'MultiPoint':
                    for point in geom.geoms:  # type: ignore
                        all_points.append((point.x, point.y))
            
            if all_points:
                unique_points = list(set(all_points))
                merged_trajectory = MultiPoint(unique_points)
                bbox = box(*merged_trajectory.bounds)
                centroid = bbox.centroid
                location_lat = centroid.y
                location_lon = centroid.x
                geometry_wkt = merged_trajectory.wkt
            else:
                location_lat = group["location_lat"].iloc[-1]
                location_lon = group["location_lon"].iloc[-1]
                geometry_wkt = group["geometry_wkt"].iloc[-1]
            
            result = {
                "internal_id": internal_id,
                "external_id": group["external_id"].iloc[0],
                "name": group["name"].iloc[0],
                "description": group["description"].iloc[0],
                "aggregation_type": group["aggregation_type"].iloc[0],
                "time_span_start": group["time_span_start"].min(),
                "time_span_end": group["time_span_end"].max(),
                "location_lat": location_lat,
                "location_lon": location_lon,
                "author": group["author"].iloc[-1],
                "geometry_wkt": geometry_wkt,
            }
            results.append(result)
        
        agg = pd.DataFrame(results)
        
        agg = agg.drop_duplicates(subset=['internal_id'], keep='last')
        
        merged_table = pa.Table.from_pandas(agg, schema=self.METADATA_SCHEMA)
        
        temp_path = metadata_file.parent / f"{metadata_file.name}.tmp-{uuid.uuid4().hex}"
        
        try:
            pq.write_table(
                merged_table,
                str(temp_path),
                compression="snappy",
            )
            temp_path.replace(metadata_file)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise
        
        self.logger.info(
            "Station metadata written",
            platform=platform,
            stations=merged_table.num_rows,
            path=str(metadata_file),
        )

    async def _persist_batches(
        self,
        batch_stream: AsyncIterator[pa.RecordBatch],
        event: DataSetAvailableEvent,
    ) -> None:
        """Persist raw data and generate metadata projection."""
        batches = []
        
        async for batch in batch_stream:
            if batch.num_rows == 0:
                continue
            batches.append(batch)
        
        if not batches:
            self.logger.warning("No data to persist", dataset=event.metadata.name)
            return
        
        platform = event.metadata.name
        table = pa.Table.from_batches(batches)

        platform_config = self._platform_configs.get(platform)
        if platform_config is None:
            self.logger.warning("No platform configuration found; skipping projection", platform=platform)
            return
        legacy_table = self._transform_raw_data(table, platform, platform_config)
        
        if legacy_table.num_rows == 0:
            self.logger.warning("No rows after transformation", dataset=event.metadata.name)
            return
        
        df = legacy_table.to_pandas()
        df['_observation_datetime'] = pd.to_datetime(df['datetime'])
        df['_observation_date'] = df['_observation_datetime'].dt.date
        df['_observation_hour'] = df['_observation_datetime'].dt.hour
        
        raw_dir = self.config.output_path / self.RAW_DIR / self.DATA_SUBDIR
        total_rows_written = 0
        files_written = []
        
        for (observation_date, observation_hour), hour_group in df.groupby(['_observation_date', '_observation_hour']):
            year = observation_date.year
            month = observation_date.month
            day = observation_date.day
            
            partition_path = (
                raw_dir
                / f"platform={platform}"
                / f"year={year}"
                / f"month={month}"
                / f"day={day}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)
            
            hour_group_clean = hour_group.drop(columns=['_observation_datetime', '_observation_date', '_observation_hour'])
            hour_table = pa.Table.from_pandas(hour_group_clean, schema=legacy_table.schema)
            hour_str = f"{year:04d}{month:02d}{day:02d}_{observation_hour:02d}"
            raw_file = partition_path / f"{hour_str}.parquet"
            
            temp_path = raw_file.parent / f"{raw_file.name}.tmp-{uuid.uuid4().hex}"
            
            try:
                pq.write_table(
                    hour_table,
                    str(temp_path),
                    compression="snappy",
                )
                temp_path.replace(raw_file)
                
                self.logger.debug(
                    "Wrote hourly file",
                    schema=hour_table.schema.names,
                    rows=hour_table.num_rows,
                    path=str(raw_file),
                )
            except Exception:
                if temp_path.exists():
                    temp_path.unlink()
                raise
            
            total_rows_written += hour_table.num_rows
            files_written.append(str(raw_file.relative_to(raw_dir)))
        
        self.logger.info(
            "Raw data written to date/hour partitions",
            total_rows=total_rows_written,
            files_written=files_written,
            file_count=len(files_written),
        )
        
        metadata_table = self._aggregate_station_metadata(table, platform)
        if metadata_table.num_rows > 0:
            self._merge_metadata(metadata_table, platform)

    async def _handle(self, event: DataSetAvailableEvent) -> None:
        try:
            batch_stream = self.arrow_store.read_record_batches(event.dataset_location)
        except Exception as e:
            self.logger.error(
                "Failed to read dataset from object store",
                key=event.dataset_location,
                error=str(e),
            )
            raise
        
        await self._persist_batches(batch_stream, event)
        
        self.logger.info(
            "Dataset projection complete",
            dataset=event.metadata.name,
        )
