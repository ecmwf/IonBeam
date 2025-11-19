import asyncio
from uuid import UUID, uuid4

import click
from ionbeam_client.models import IngestDataCommand, IngestionMetadata

from ..amqp_utils import publish_message
from ..utils import load_json_model, parse_datetime


@click.command(name="reingest")
@click.argument("parquet_uri")
@click.argument("metadata_file", type=click.Path(exists=True))
@click.argument("start_time")
@click.argument("end_time")
@click.option("--id", "command_id", help="Optional UUID for the command")
@click.pass_context
def reingest(
    ctx,
    parquet_uri: str,
    metadata_file: str,
    start_time: str,
    end_time: str,
    command_id: str | None,
):
    """Reprocess existing parquet file through the ingestion pipeline.
    
    PARQUET_URI: Object store URI of the parquet file (e.g., raw/sensor_community/...)
    
    METADATA_FILE: Path to JSON file containing IngestionMetadata
    
    START_TIME: Start datetime in ISO format (e.g., 2024-01-01T00:00:00Z)
    
    END_TIME: End datetime in ISO format (e.g., 2024-01-01T01:00:00Z)
    """
    config = ctx.obj["config"]
    
    start_dt = parse_datetime(start_time)
    end_dt = parse_datetime(end_time)
    
    if start_dt >= end_dt:
        raise click.ClickException("Start time must be before end time")
    
    metadata = load_json_model(metadata_file, IngestionMetadata)
    
    cmd_uuid = None
    if command_id:
        try:
            cmd_uuid = UUID(command_id)
        except ValueError:
            raise click.ClickException(f"Invalid UUID format: {command_id}")
    else:
        cmd_uuid = uuid4()
    
    command = IngestDataCommand(
        id=cmd_uuid,
        metadata=metadata,
        payload_location=parquet_uri,
        start_time=start_dt,
        end_time=end_dt,
    )
    
    async def execute():
        await publish_message(
            config=config,
            message=command,
            exchange_name="ionbeam.ingestion",
            routing_key="ingestV1",
        )
        click.echo("✓ Reingest command published")
        click.echo(f"  Command ID: {cmd_uuid}")
        click.echo(f"  Dataset: {metadata.dataset.name}")
        click.echo(f"  Payload: {parquet_uri}")
        click.echo(f"  Time range: {start_dt.isoformat()} to {end_dt.isoformat()}")
    
    asyncio.run(execute())