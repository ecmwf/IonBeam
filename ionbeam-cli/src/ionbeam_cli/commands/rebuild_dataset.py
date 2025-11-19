import asyncio
from datetime import timedelta
from uuid import uuid4

import click
from ionbeam_client.models import DataAvailableEvent, IngestionMetadata

from ..amqp_utils import publish_message
from ..utils import load_json_model, parse_datetime


@click.command(name="rebuild-dataset")
@click.argument("dataset_name")
@click.argument("window_timestamp")
@click.argument("metadata_file", type=click.Path(exists=True))
@click.pass_context
def rebuild_dataset(
    ctx,
    dataset_name: str,
    window_timestamp: str,
    metadata_file: str,
):
    """Trigger dataset window rebuild by publishing DataAvailableEvent.
    
    This publishes a minimal 1-second DataAvailableEvent that forces the coordinator
    to schedule a rebuild of the dataset window via hash comparison.
    
    DATASET_NAME: Name of the dataset (e.g., sensor_community)
    
    WINDOW_TIMESTAMP: ISO timestamp for the window start (e.g., 2024-01-01T12:00:00Z)
    
    METADATA_FILE: Path to JSON file containing IngestionMetadata
    """
    config = ctx.obj["config"]
    
    window_ts = parse_datetime(window_timestamp)
    
    metadata = load_json_model(metadata_file, IngestionMetadata)
    
    if metadata.dataset.name != dataset_name:
        raise click.ClickException(
            f"Dataset name mismatch: provided '{dataset_name}' but metadata has '{metadata.dataset.name}'"
        )
    
    event = DataAvailableEvent(
        id=uuid4(),
        metadata=metadata,
        start_time=window_ts,
        end_time=window_ts + timedelta(seconds=1),
    )
    
    async def execute():
        await publish_message(
            config=config,
            message=event,
            exchange_name="ionbeam.data.available",
        )
        click.echo("✓ Rebuild request published")
        click.echo(f"  Dataset: {dataset_name}")
        click.echo(f"  Window timestamp: {window_ts.isoformat()}")
        click.echo(f"  Event ID: {event.id}")
        click.echo(f"  Event window: {event.start_time.isoformat()} to {event.end_time.isoformat()}")
    
    asyncio.run(execute())