import asyncio
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

import click
from dependency_injector.wiring import Provide, inject

from ..core.containers import IonbeamContainer
from ..models.models import StartSourceCommand
from ..scheduler.source_scheduler import SourceScheduler
from ..sources.ioncannon import IonCannonSource
from ..sources.meteotracker import MeteoTrackerSource
from ..sources.metno.netatmo import NetAtmoSource
from ..sources.sensor_community import SensorCommunitySource


def parse_datetime(date_str: str) -> datetime:
    """Parse ISO date string to UTC datetime."""
    try:
        return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise ValueError(f"Invalid date format '{date_str}'. Use ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS") from e


@inject
async def trigger_source_backfill(
    source_name: str, 
    start_time: datetime, 
    end_time: datetime,
    scheduler: SourceScheduler = Provide[IonbeamContainer.source_scheduler],
):
    result = await scheduler.trigger_source(source_name, start_time, end_time)
    if not result:
        raise RuntimeError(f"Failed to trigger source {source_name}")
    return result


@inject
async def run_source_directly(
    source_name: str,
    start_time: datetime,
    end_time: datetime,
    netatmo_source: NetAtmoSource = Provide[IonbeamContainer.netatmo_source],
    sensor_community_source: SensorCommunitySource = Provide[IonbeamContainer.sensor_community_source],
    meteotracker_source: MeteoTrackerSource = Provide[IonbeamContainer.meteotracker_source],
    ioncannon_source: IonCannonSource = Provide[IonbeamContainer.ion_cannon_source],
):
    sources = {
        "netatmo": netatmo_source,
        "sensor_community": sensor_community_source,
        "meteotracker": meteotracker_source,
        "ioncannon": ioncannon_source,
    }
    
    if source_name not in sources:
        raise ValueError(f"Unknown source: {source_name}. Available: {list(sources.keys())}")
    
    source = sources[source_name]
    command = StartSourceCommand(
        id=uuid4(),
        source_name=source_name,
        start_time=start_time,
        end_time=end_time,
    )
    
    click.echo(f"Running {source_name} source directly...")
    result = await source.handle(command)
    
    if result:
        click.echo("✓ Source completed successfully")
        click.echo(f"  Output: {result.payload_location}")
        click.echo(f"  Time range: {result.start_time} to {result.end_time}")
        click.echo(f"  Dataset: {result.metadata.dataset.name}")
    else:
        click.echo("✗ Source failed or returned no data")
    
    return result


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
@click.pass_context
def cli(ctx, verbose, config):
    """Ionbeam CLI - Data ingestion and processing tools"""
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    os.environ["IONBEAM_CONFIG_PATH"] = config
    
    # Store config in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['verbose'] = verbose


@cli.command()
@click.argument("source_name")
@click.argument("start_date")
@click.argument("end_date")
@click.pass_context
def trigger(ctx, source_name, start_date, end_date):
    """Trigger source via AMQP messaging (requires RabbitMQ).

    SOURCE_NAME: Source to trigger (netatmo, sensor_community, meteotracker, ioncannon)

    START_DATE: Start date (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)

    END_DATE: End date (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    """
    try:
        start_time = parse_datetime(start_date)
        end_time = parse_datetime(end_date)
    except ValueError as e:
        raise click.ClickException(str(e))
    
    async def execute():
        container = IonbeamContainer()
        container.wire(modules=[__name__])
        
        try:
            init = container.init_resources()
            if init is not None:
                await init
            await trigger_source_backfill(source_name, start_time, end_time)
            click.echo(f"✓ Trigger command published for {source_name}: {start_time} to {end_time}")
        finally:
            shutdown = container.shutdown_resources()
            if shutdown is not None:
                await shutdown
    
    asyncio.run(execute())


@cli.command()
@click.argument("source_name")
@click.argument("start_date")
@click.argument("end_date")
@click.pass_context
def run(ctx, source_name, start_date, end_date):
    """Run source directly.

    SOURCE_NAME: Source to run (netatmo, sensor_community, meteotracker, ioncannon)

    START_DATE: Start date (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)

    END_DATE: End date (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    """
    try:
        start_time = parse_datetime(start_date)
        end_time = parse_datetime(end_date)
    except ValueError as e:
        raise click.ClickException(str(e))
    
    async def execute():
        container = IonbeamContainer()
        container.wire(modules=[__name__])
        
        try:
            init = container.init_resources()
            if init is not None:
                await init
            await run_source_directly(source_name, start_time, end_time)
        finally:
            shutdown = container.shutdown_resources()
            if shutdown is not None:
                await shutdown
    
    asyncio.run(execute())


if __name__ == "__main__":
    cli()
