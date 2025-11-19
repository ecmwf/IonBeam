import pathlib
import uuid
from typing import AsyncIterator, Optional

import geopandas as gpd
import pyarrow as pa
import structlog
import yaml
from pydantic import BaseModel

from ionbeam_client.constants import (
    LatitudeColumn,
    LongitudeColumn,
    ObservationTimestampColumn,
)
from ionbeam_client.models import DataSetAvailableEvent

from .models import (
    Extents,
    Provider,
    ProviderData,
    Resource,
    SpatialExtent,
    TemporalExtent,
)


logger = structlog.get_logger(__name__)


class PyGeoApiExporterConfig(BaseModel):
    output_path: pathlib.Path
    config_path: pathlib.Path


class PyGeoApiExporter:
    def __init__(self, config: PyGeoApiExporterConfig):
        self.config = config
        self.logger = logger.bind(exporter="pygeoapi")

    def _create_resource_config(self, event: DataSetAvailableEvent) -> Resource:
        dataset_dir = self.config.output_path / event.metadata.name

        existing_begin = None
        existing_end = None

        if self.config.config_path.exists():
            with open(self.config.config_path, "r") as f:
                existing_config = yaml.safe_load(f) or {}

            if (
                "resources" in existing_config
                and event.metadata.name in existing_config["resources"]
            ):
                existing_resource = existing_config["resources"][event.metadata.name]
                if (
                    "extents" in existing_resource
                    and "temporal" in existing_resource["extents"]
                ):
                    temporal = existing_resource["extents"]["temporal"]
                    if "begin" in temporal:
                        existing_begin = temporal["begin"]
                    if "end" in temporal:
                        existing_end = temporal["end"]

        final_begin = event.start_time
        final_end = event.end_time

        if existing_begin is not None:
            final_begin = min(existing_begin, event.start_time)
        if existing_end is not None:
            final_end = max(existing_end, event.end_time)

        extents = Extents(
            spatial=SpatialExtent(
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
            for link in event.metadata.source_links
        ]

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
            title=event.metadata.name,
            description=event.metadata.description,
            keywords=event.metadata.keywords,
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

        async for batch in batch_stream:
            if batch.num_rows == 0:
                self.logger.warning(
                    "Empty batch received during projection",
                    dataset=event.metadata.name,
                    event=str(event.id),
                )
                continue

            batches.append(batch)
            total_rows += batch.num_rows

        if batches:
            table = pa.Table.from_batches(batches)
            df = table.to_pandas()
            df = df.drop(columns=["geometry", "id"], errors="ignore")

            geometry = gpd.points_from_xy(
                df[LongitudeColumn], df[LatitudeColumn], crs="EPSG:4326"
            )

            gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            gdf["id"] = [str(uuid.uuid4()) for _ in range(len(gdf))]
        else:
            self.logger.warning(
                "No data available for projection; writing empty GeoParquet",
                dataset=event.metadata.name,
                event=str(event.id),
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
                event=str(event.id),
                path=str(file_path),
            )

        return file_path

    async def export_handler(
        self, event: DataSetAvailableEvent, batch_stream: AsyncIterator[pa.RecordBatch]
    ) -> None:
        self.logger.info(
            "Starting PyGeoAPI export",
            dataset=event.metadata.name,
            event_id=str(event.id),
            start=event.start_time.isoformat(),
            end=event.end_time.isoformat(),
        )

        _ = await self._persist_batches(batch_stream, event)

        resource_section = self._create_resource_config(event)

        if self.config.config_path.exists():
            with open(self.config.config_path, "r") as f:
                config = yaml.safe_load(f) or {}
        else:
            raise Exception("Expected PyGeoAPI config - not found")

        config["resources"][event.metadata.name] = resource_section.model_dump()

        with open(self.config.config_path, "w") as f:
            yaml.dump(config, f, sort_keys=False)

        dataset_dir = self.config.output_path / event.metadata.name
        file_count = (
            len(list(dataset_dir.glob("*.parquet"))) if dataset_dir.exists() else 0
        )

        self.logger.info(
            "PyGeoAPI export completed",
            dataset=event.metadata.name,
            file_count=file_count,
        )
