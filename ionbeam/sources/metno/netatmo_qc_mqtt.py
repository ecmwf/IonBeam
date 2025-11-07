import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import aio_pika
import structlog

from ionbeam.models.models import (
    CanonicalVariable,
    DataIngestionMap,
    DatasetMetadata,
    IngestDataCommand,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    TimeAxis,
)
from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.storage.arrow_store import ArrowStore
from ionbeam.utilities.arrow_tools import schema_from_ingestion_map

from .batched_mqtt import MessageBatch, subscribe_with_batching
from .config import NetAtmoMQTTConfig
from .netatmo_processing import netatmo_record_batch_stream

netatmo_qc_metadata: IngestionMetadata = IngestionMetadata(
    dataset=DatasetMetadata(
        name="netatmo_qc",
        aggregation_span=timedelta(hours=1),
        subject_to_change_window=timedelta(hours=0),
        description="Quality-controlled NetAtmo data from FMI",
        source_links=[],
        keywords=["netatmo", "iot", "qc", "fmi"],
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
            )
        ],
        metadata_variables=[
            MetadataVariable(column="air_temperature:2.0:point:PT0S_qc"),
            MetadataVariable(column="station_id"),
        ],
    ),
    version=1,
)

class NetAtmoQCMQTTSource:
    def __init__(
        self,
        config: NetAtmoMQTTConfig,
        metrics: IonbeamMetricsProtocol,
        arrow_store: ArrowStore,
    ):
        self.config = config
        self.metrics = metrics
        self.arrow_store = arrow_store
        self.metadata: IngestionMetadata = netatmo_qc_metadata
        self._arrow_schema = schema_from_ingestion_map(self.metadata.ingestion_map)
        self._subscription = None
        self.logger = structlog.get_logger("ionbeam.sources.metno.netatmo_qc_mqtt")

    async def start(self):
        """Start the MQTT subscription with internal connection management"""
        self._subscription = subscribe_with_batching(
            hostname=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            client_id=self.config.client_id,
            topic="qc-obs/fmi/+/#",
            qos=1,
            batch_handler=self._handle_batch,
            flush_interval_seconds=self.config.flush_interval_seconds,
            flush_max_records=self.config.flush_max_records,
            max_buffer_size=self.config.max_buffer_size,
            logger_name="ionbeam.sources.metno.netatmo_qc_mqtt",
            keepalive=self.config.keepalive,
            use_tls=self.config.use_tls,
        )
        await self._subscription.__aenter__()

    async def stop(self):
        """Stop the MQTT subscription"""
        if self._subscription:
            await self._subscription.__aexit__(None, None, None)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    @staticmethod
    def _ts_for_path(dt: datetime) -> str:
        """Format datetime for object key path"""
        import pandas as pd
        
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y%m%dT%H%M%SZ")

    async def _handle_batch(self, batch: MessageBatch) -> None:
        dataset_name = self.metadata.dataset.name
        
        # Generate object key
        start_s = self._ts_for_path(batch.start_time)
        end_s = self._ts_for_path(batch.end_time)
        now_s = self._ts_for_path(datetime.now(timezone.utc))
        object_key = f"raw/{dataset_name}/{start_s}-{end_s}_{now_s}"

        try:
            batch_stream = netatmo_record_batch_stream(
                batch.messages,
                batch_size=self.config.flush_max_records,
                logger=self.logger,
                schema=self._arrow_schema,
            )

            rows = await self.arrow_store.write_record_batches(
                object_key,
                batch_stream,
                schema=self._arrow_schema,
            )

            if rows == 0:
                self.logger.warning("No rows written to object store", key=object_key)
                self.metrics.sources.record_ingestion_request(dataset_name, "empty")
                return

            # Record metrics
            now_utc = datetime.now(timezone.utc)
            lag_seconds = max(0.0, (now_utc - batch.start_time).total_seconds())
            self.metrics.sources.observe_data_lag(dataset_name, lag_seconds)
            self.metrics.sources.observe_request_rows(dataset_name, int(rows))
            self.metrics.sources.record_ingestion_request(dataset_name, "success")
            
            self.logger.info(
                "Wrote QC data to object store",
                rows=rows,
                key=object_key,
                start=batch.start_time.isoformat(),
                end=batch.end_time.isoformat(),
                dataset=dataset_name,
            )

            # Create ingestion command
            ingestion_command = IngestDataCommand(
                id=uuid4(),
                metadata=self.metadata,
                payload_location=object_key,
                start_time=batch.start_time,
                end_time=batch.end_time,
            )

            # Publish to AMQP
            await self._publish_ingestion_command(ingestion_command)

        except Exception as e:
            self.metrics.sources.record_ingestion_request(dataset_name, "error")
            self.logger.error(
                "Failed to process QC batch",
                error=str(e),
                key=object_key,
                dataset=dataset_name,
            )
            raise

    async def _publish_ingestion_command(self, command: IngestDataCommand) -> None:
        connection = await aio_pika.connect_robust(self.config.url)
        try:
            async with connection:
                channel = await connection.channel()
                
                for attempt in range(3):
                    try:
                        await channel.default_exchange.publish(
                            aio_pika.Message(body=command.model_dump_json().encode()),
                            routing_key=self.config.routing_key,
                        )
                        return
                    except Exception as e:
                        if attempt < 2:
                            self.logger.warning(
                                "Failed to publish QC command",
                                error=str(e),
                                attempt=attempt + 1,
                                key=command.payload_location,
                            )
                            await asyncio.sleep(1)
                        else:
                            self.logger.error(
                                "AMQP publish error after retries",
                                error=str(e),
                                key=command.payload_location,
                            )
                            raise
        finally:
            await connection.close()