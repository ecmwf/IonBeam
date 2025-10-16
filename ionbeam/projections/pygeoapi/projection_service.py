import pathlib
import uuid
from typing import AsyncIterator

import geopandas as gpd
import pyarrow as pa
import yaml
from pydantic import BaseModel

from ionbeam.core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ionbeam.core.handler import BaseHandler
from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.storage.arrow_store import ArrowStore

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
    output_path: pathlib.Path
    config_path: pathlib.Path  # PyGeoAPI config - data sources are declared here


class PyGeoApiProjectionService(BaseHandler[DataSetAvailableEvent, None]):
    """
    Read data available event, maintains geo-parquet folders for each source, that can be queried by PyGeoAPI (as an example)
    also creates a pygeoapi config in line with the available data, tempo/spatial extents etc..
    """

    def __init__(self, config: PyGeoApiConfig, metrics: IonbeamMetricsProtocol, arrow_store: ArrowStore):
        super().__init__("PyGeoApiProjectionService", metrics)
        self.config = config
        self.arrow_store = arrow_store

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
            time_field=ObservationTimestampColumn,
            x_field=LongitudeColumn,
            y_field=LatitudeColumn,
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

    async def _persist_batches(
        self,
        batch_stream: AsyncIterator[pa.RecordBatch],
        event: DataSetAvailableEvent,
    ) -> pathlib.Path:
        """Process Arrow batches and write to geo-parquet file."""
        dataset_dir = self.config.output_path / event.metadata.name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        start_str = event.start_time.strftime("%Y%m%d_%H%M")
        end_str = event.end_time.strftime("%Y%m%d_%H%M")
        file_path = dataset_dir / f"{start_str}_{end_str}.parquet"
        tmp_path = dataset_dir / f"{file_path.name}.tmp"

        if tmp_path.exists():
            tmp_path.unlink()

        batches: list[pa.RecordBatch] = []
        total_rows = 0

        # TODO - writing geoparquet from stream
        async for batch in batch_stream:
            if batch.num_rows == 0:
                self.logger.warning(
                    "Empty batch received during projection",
                    dataset=event.metadata.name,
                    event=event.id,
                )
                continue

            batches.append(batch)
            total_rows += batch.num_rows

        if batches:
            table = pa.Table.from_batches(batches)
            df = table.to_pandas()
            df = df.drop(columns=["geometry", "id"], errors="ignore")

            geometry = gpd.points_from_xy(df[LongitudeColumn], df[LatitudeColumn], crs="EPSG:4326")

            gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            gdf["id"] = [str(uuid.uuid4()) for _ in range(len(gdf))]
        else:
            self.logger.warning(
                "No data available for projection; writing empty GeoParquet",
                dataset=event.metadata.name,
                event=event.id,
            )
            gdf = gpd.GeoDataFrame(
                {"geometry": gpd.GeoSeries([], crs="EPSG:4326")},
                geometry="geometry",
                crs="EPSG:4326",
            )
            gdf["id"] = []

        gdf.to_parquet(tmp_path, index=False, compression="snappy")
        tmp_path.replace(file_path)

        if total_rows > 0:
            self.logger.info("GeoParquet written", rows=total_rows, path=str(file_path))
        else:
            self.logger.warning(
                "GeoParquet written with no rows",
                dataset=event.metadata.name,
                event=event.id,
                path=str(file_path),
            )

        return file_path

    async def _handle(self, event: DataSetAvailableEvent):
        """
        Create time-partitioned parquet file in dataset directory and update PyGeoAPI config
        """
        # Read Arrow batches from object store
        try:
            batch_stream = self.arrow_store.read_record_batches(event.dataset_location)
        except Exception as e:
            self.logger.error("Failed to read dataset from object store", key=event.dataset_location, error=str(e))
            return

        # Persist the data as a time-partitioned file
        _ = await self._persist_batches(batch_stream, event)

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
        self.logger.info("Updated PyGeoAPI config", dataset=event.metadata.name, file_count=file_count)
