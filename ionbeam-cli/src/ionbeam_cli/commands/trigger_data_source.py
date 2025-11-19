import asyncio
from uuid import UUID, uuid4

import click
from ionbeam_client.models import StartSourceCommand

from ..amqp_utils import publish_message
from ..utils import parse_datetime, sanitize_name


@click.command(name="trigger-data-source")
@click.argument("source_name")
@click.argument("start_time")
@click.argument("end_time")
@click.option("--id", "command_id", help="Optional UUID for the command")
@click.pass_context
def trigger_data_source(ctx, source_name: str, start_time: str, end_time: str, command_id: str | None):
    """Trigger a data source to fetch data for a specific time range.
    
    SOURCE_NAME: Name of the data source (e.g., sensor_community, meteotracker)
    
    START_TIME: Start datetime in ISO format (e.g., 2024-01-01T00:00:00Z)
    
    END_TIME: End datetime in ISO format (e.g., 2024-01-01T01:00:00Z)
    """
    config = ctx.obj["config"]
    
    start_dt = parse_datetime(start_time)
    end_dt = parse_datetime(end_time)
    
    if start_dt >= end_dt:
        raise click.ClickException("Start time must be before end time")
    
    cmd_uuid = None
    if command_id:
        try:
            cmd_uuid = UUID(command_id)
        except ValueError:
            raise click.ClickException(f"Invalid UUID format: {command_id}")
    else:
        cmd_uuid = uuid4()
    
    command = StartSourceCommand(
        id=cmd_uuid,
        source_name=source_name,
        start_time=start_dt,
        end_time=end_dt,
    )
    
    sanitized_name = sanitize_name(source_name)
    routing_key = f"ionbeam.source.{sanitized_name}.start"
    
    async def execute():
        await publish_message(
            config=config,
            message=command,
            routing_key=routing_key,
        )
        click.echo(f"✓ Triggered {source_name}")
        click.echo(f"  Command ID: {cmd_uuid}")
        click.echo(f"  Time range: {start_dt.isoformat()} to {end_dt.isoformat()}")
        click.echo(f"  Routing key: {routing_key}")
    
    asyncio.run(execute())