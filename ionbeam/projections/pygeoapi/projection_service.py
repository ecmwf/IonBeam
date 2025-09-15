import pathlib

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import yaml
from pydantic import BaseModel
from shapely.geometry import Point

from ionbeam.core.handler import BaseHandler

from ...models.models import DataSetAvailableEvent, DatasetMetadata
from .models import (
    Extents,
    Provider,
    ProviderData,
    Resource,
    SpatialExtent,
    TemporalExtent,
)


class PyGeoApiConfig(BaseModel):
    input_path: pathlib.Path
    output_path: pathlib.Path
    config_path: pathlib.Path # PyGeoAPI config - data sources are declared here


class PyGeoApiProjectionService(BaseHandler[DataSetAvailableEvent, None]):
    """
    Read data available event, maintains geo-parquet folders for each source, that can be queried by PyGeoAPI (as an example)
    also creates a pygeoapi config in line with the available data, tempo/spatial extents etc..
    """

    def __init__(self, config: PyGeoApiConfig):
        super().__init__("PyGeoApiProjectionService")
        self.config = config

    def _create_resource_config(self, metadata: DatasetMetadata, event: DataSetAvailableEvent):
        """Create resource config with single provider pointing to dataset directory"""
        dataset_dir = self.config.output_path / metadata.name
        
        # Get existing temporal extent from config if it exists
        existing_begin = None
        existing_end = None
        
        if self.config.config_path.exists():
            with open(self.config.config_path, "r") as f:
                existing_config = yaml.safe_load(f) or {}
                
            if "resources" in existing_config and metadata.name in existing_config["resources"]:
                existing_resource = existing_config["resources"][metadata.name]
                if "extents" in existing_resource and "temporal" in existing_resource["extents"]:
                    temporal = existing_resource["extents"]["temporal"]
                    if "begin" in temporal:
                        existing_begin = temporal["begin"]
                    if "end" in temporal:
                        existing_end = temporal["end"]
        
        # Determine final temporal extent
        final_begin = event.start_time
        final_end = event.end_time
        
        if existing_begin is not None:
            final_begin = min(existing_begin, event.start_time)
        if existing_end is not None:
            final_end = max(existing_end, event.end_time)
        
        # Format timestamps properly for RFC3339 with Z suffix
        extents = Extents(
            spatial=SpatialExtent(  # TODO - calculate this from actual data
                bbox=[-180, -90, 180, 90],
                crs="http://www.opengis.net/def/crs/OGC/1.3/CRS84",
            ),
            temporal=TemporalExtent(
                begin=final_begin,
                end=final_end,
                trs="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
            ),
        )

        links = [
            {
                "type": link.mime_type,
                "rel": "canonical",
                "title": link.title,
                "href": link.href,
                "hreflang": "en-US",
            }
            for link in metadata.source_links
        ]

        # Single provider pointing to dataset directory
        provider = Provider(
            type="feature",
            name="Parquet",
            data=ProviderData(source=str(dataset_dir)),
            id_field="id",
            time_field="datetime",
            x_field="lon",
            y_field="lat",
        )

        resource = Resource(
            type="collection",
            visibility="default",
            title=metadata.name,
            description=metadata.description,
            keywords=metadata.keywords,
            links=links,
            extents=extents,
            providers=[provider],
        )

        return resource

    def _get_max_id_from_dataset(self, dataset_name: str, chunk_size: int = 10000) -> int:
        """Get the maximum ID across all files in the dataset directory using chunked reading"""
        dataset_dir = self.config.output_path / dataset_name
        if not dataset_dir.exists():
            return -1
        
        max_id = -1
        for file_path in dataset_dir.glob("*.parquet"):
            try:
                # Read file in chunks to avoid memory issues
                parquet_file = pq.ParquetFile(file_path)
                
                for batch in parquet_file.iter_batches(batch_size=chunk_size, columns=['id']):
                    batch_df = batch.to_pandas()
                    if not batch_df.empty:
                        batch_max = batch_df['id'].max()
                        max_id = max(max_id, batch_max)
                        
            except Exception as e:
                self.logger.warning(f"Could not read IDs from {file_path}: {e}")
        
        return max_id

    async def _persist_data(self, incoming_df: pd.DataFrame, event: DataSetAvailableEvent):
        """Create time-partitioned parquet file in dataset directory"""
        # Create dataset directory
        dataset_dir = self.config.output_path / event.metadata.name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Create filename with time range
        start_str = event.start_time.strftime("%Y%m%d_%H%M")
        end_str = event.end_time.strftime("%Y%m%d_%H%M")
        file_path = dataset_dir / f"{start_str}_{end_str}.parquet"
        
        geometry = [Point(xy) for xy in zip(incoming_df["lon"], incoming_df["lat"])]
        incoming_gdf = gpd.GeoDataFrame(incoming_df, geometry=geometry, crs="EPSG:4326")
        
        # Sort by timestamp for consistent ordering within file
        if 'datetime' in incoming_gdf.columns:
            incoming_gdf = incoming_gdf.sort_values('datetime').reset_index(drop=True)
        
        # Get next available ID range starting from max existing ID
        start_id = self._get_max_id_from_dataset(event.metadata.name) + 1
        incoming_gdf["id"] = range(start_id, start_id + len(incoming_gdf))
        
        # Always replace the entire file (handles updates)
        incoming_gdf.to_parquet(file_path, compression='snappy')
        self.logger.info(f"Created/updated geoparquet file with {len(incoming_gdf)} rows: {file_path}")
        
        return file_path


    async def _handle(self, event: DataSetAvailableEvent):
        """
        Create time-partitioned parquet file in dataset directory and update PyGeoAPI config
        """
        dataset_path = pathlib.Path(event.dataset_location)
        if not dataset_path.exists():
            self.logger.error(f"Dataset file not found: {dataset_path}")
            return

        # Read and persist the data as a time-partitioned file
        df = pd.read_parquet(dataset_path)
        _ = await self._persist_data(df, event)

        # Create resource config with single provider pointing to dataset directory
        resource_section = self._create_resource_config(event.metadata, event)

        # Load existing config
        if self.config.config_path.exists():
            with open(self.config.config_path, "r") as f:
                config = yaml.safe_load(f) or {}
        else:
            raise Exception("Expected config - not found")

        # Update the resource
        config["resources"][event.metadata.name] = resource_section.model_dump()

        # Write back to the same file
        with open(self.config.config_path, "w") as f:
            yaml.dump(config, f, sort_keys=False)
        
        dataset_dir = self.config.output_path / event.metadata.name
        file_count = len(list(dataset_dir.glob("*.parquet"))) if dataset_dir.exists() else 0
        self.logger.info(f"Updated PyGeoAPI config for {event.metadata.name} with directory containing {file_count} file(s)")
