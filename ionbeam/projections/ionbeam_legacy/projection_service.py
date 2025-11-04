import pathlib
import uuid
from dataclasses import dataclass
from typing import AsyncIterator, Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel
from shapely import wkt as wkt_parser
from shapely.geometry import box

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
        floored = dt_series.dt.floor('h')
        
        # Build common fields inline
        legacy_df = pd.DataFrame({
            "datetime": dt_series.apply(lambda x: x.isoformat(timespec='milliseconds').replace('+00:00', 'Z')),
            "author": df.get("author", None),
            "station_id": df["station_id"],
            "external_station_id": df["station_id"],
            "station_name": config.station_name_builder(df, platform),
            "aggregation_type": "by_time",
            "chunk_date": floored.dt.strftime("%Y%m%d"),
            "chunk_time": floored.dt.strftime("%H%M"),
            "lat": df[LatitudeColumn],
            "lon": df[LongitudeColumn],
        })
        
        # Add platform-specific columns
        for canonical_name, legacy_name in config.column_map.items():
            legacy_df[legacy_name] = df.get(canonical_name, None)
        
        # Add extra fields
        for field_name, extractor in config.extra_fields.items():
            legacy_df[field_name] = extractor(df) if (result := extractor(df)) is not None else None
        
        return pa.Table.from_pandas(legacy_df, schema=config.schema)

    def _aggregate_station_metadata(self, table: pa.Table, platform: str) -> pa.Table:
        """Aggregate station metadata from raw observation data."""
        df = table.to_pandas()
        
        if df.empty:
            return pa.table({}, schema=self.METADATA_SCHEMA)
        
        # Get platform-specific name resolver
        name_resolver = self._station_name_resolver.get(
            platform,
            lambda sid, grp, p: f"{p}: {sid}"
        )
        
        # Build metadata for each station
        results = []
        for station_id, group in df.groupby("station_id"):
            # Create bounding box from coordinates
            coords = group[[LongitudeColumn, LatitudeColumn]].values
            lons = coords[:, 0]
            lats = coords[:, 1]
            
            # Create bbox and calculate centroid inline
            bbox = box(min(lons), min(lats), max(lons), max(lats))
            centroid = bbox.centroid
            lat, lon = centroid.y, centroid.x
            bbox_wkt = bbox.wkt
            
            station_id_str = str(station_id)
            
            results.append({
                "external_id": station_id_str,
                "internal_id": f"{platform}_{station_id_str}",
                "name": name_resolver(station_id_str, group, platform),
                "description": None,
                "aggregation_type": "by_time",
                "time_span_start": pd.to_datetime(group[ObservationTimestampColumn].min(), utc=True),
                "time_span_end": pd.to_datetime(group[ObservationTimestampColumn].max(), utc=True),
                "location_lat": lat,
                "location_lon": lon,
                "author": group["author"].iloc[-1] if "author" in group.columns else None,
                "geometry_wkt": bbox_wkt,
            })
        
        return pa.Table.from_pandas(pd.DataFrame(results), schema=self.METADATA_SCHEMA)

    def _merge_metadata(self, new_table: pa.Table, platform: str) -> None:
        """Merge new metadata with existing and write to disk."""
        metadata_file = (
            self.config.output_path / self.METADATA_SUBDIR
            / f"platform={platform}" / "metadata.parquet"
        )
        metadata_file.parent.mkdir(parents=True, exist_ok=True)
        
        new_df = new_table.to_pandas()
        
        if not metadata_file.exists():
            # No existing metadata, just write new data
            self._write_parquet_atomically(metadata_file, pa.Table.from_pandas(new_df, schema=self.METADATA_SCHEMA))
            self.logger.info(
                "Station metadata written (new file)",
                platform=platform,
                stations=len(new_df),
                path=str(metadata_file),
            )
            return
        
        # Read existing metadata and identify conflicts
        existing_df = pq.read_table(str(metadata_file)).to_pandas()
        new_ids = set(new_df["internal_id"])
        existing_ids = set(existing_df["internal_id"])
        conflicting_ids = new_ids & existing_ids
        
        # Build result efficiently
        results = []
        
        # Add non-conflicting stations AS-IS
        non_conflicting_existing = existing_ids - new_ids
        non_conflicting_new = new_ids - existing_ids
        
        if non_conflicting_existing:
            results.extend(existing_df[existing_df["internal_id"].isin(non_conflicting_existing)].to_dict('records'))
        if non_conflicting_new:
            results.extend(new_df[new_df["internal_id"].isin(non_conflicting_new)].to_dict('records'))

        for internal_id in conflicting_ids:
            existing_records = existing_df[existing_df["internal_id"] == internal_id]
            new_records = new_df[new_df["internal_id"] == internal_id]
            
            # Merge bounding boxes by collecting all bbox bounds and creating a new encompassing bbox
            all_bounds: list[tuple[float, float, float, float]] = []
            for wkt_str in pd.concat([existing_records["geometry_wkt"], new_records["geometry_wkt"]]).dropna():
                geom = wkt_parser.loads(str(wkt_str))
                all_bounds.append(geom.bounds)

            if all_bounds:
                minx, miny, maxx, maxy = (func(col) for func, col in zip(
                    (min, min, max, max),
                    zip(*all_bounds)
                ))
                merged_bbox = box(minx, miny, maxx, maxy)
                c = merged_bbox.centroid
                location_lat, location_lon = c.y, c.x
                geometry_wkt = merged_bbox.wkt
            else:
                # Fallback to new record values
                location_lat = new_records["location_lat"].iloc[-1]
                location_lon = new_records["location_lon"].iloc[-1]
                geometry_wkt = new_records["geometry_wkt"].iloc[-1]
            
            # Combine time spans and use latest values for other fields
            all_records = pd.concat([existing_records, new_records])
            results.append({
                "internal_id": internal_id,
                "external_id": new_records["external_id"].iloc[0],
                "name": new_records["name"].iloc[0],
                "description": new_records["description"].iloc[0],
                "aggregation_type": new_records["aggregation_type"].iloc[0],
                "time_span_start": all_records["time_span_start"].min(),
                "time_span_end": all_records["time_span_end"].max(),
                "location_lat": location_lat,
                "location_lon": location_lon,
                "author": new_records["author"].iloc[-1],
                "geometry_wkt": geometry_wkt,
            })
        
        # Write merged metadata
        self._write_parquet_atomically(metadata_file, pa.Table.from_pandas(pd.DataFrame(results), schema=self.METADATA_SCHEMA))
        self.logger.info(
            "Station metadata written (optimized)",
            platform=platform,
            total_stations=len(results),
            conflicting_stations=len(conflicting_ids),
            new_stations=len(non_conflicting_new),
            existing_kept=len(non_conflicting_existing),
            path=str(metadata_file),
        )
    
    def _write_parquet_atomically(self, file_path: pathlib.Path, table: pa.Table) -> None:
        """Write a Parquet table to disk atomically using a temporary file."""
        temp_path = file_path.parent / f"{file_path.name}.tmp-{uuid.uuid4().hex}"
        
        try:
            pq.write_table(table, str(temp_path), compression="snappy")
            temp_path.replace(file_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

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

        if (platform_config := self._platform_configs.get(platform)) is None:
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
        
        raw_dir = self.config.output_path / self.DATA_SUBDIR
        total_rows_written = 0
        files_written = []
        
        for (observation_date, observation_hour), hour_group in df.groupby(['_observation_date', '_observation_hour']):
            partition_path = (
                raw_dir / f"platform={platform}"
                / f"year={observation_date.year}"
                / f"month={observation_date.month}"
                / f"day={observation_date.day}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)
            
            hour_group_clean = hour_group.drop(columns=['_observation_datetime', '_observation_date', '_observation_hour'])
            hour_table = pa.Table.from_pandas(hour_group_clean, schema=legacy_table.schema)
            
            hour_str = f"{observation_date.year:04d}{observation_date.month:02d}{observation_date.day:02d}_{observation_hour:02d}"
            raw_file = partition_path / f"{hour_str}.parquet"
            
            self._write_parquet_atomically(raw_file, hour_table)
            self.logger.debug(
                "Wrote hourly file",
                schema=hour_table.schema.names,
                rows=hour_table.num_rows,
                path=str(raw_file),
            )
            
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
