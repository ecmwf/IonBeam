# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import asyncio
from uuid import uuid4

import click
from ionbeam_client.models import DataSetAvailableEvent, DatasetMetadata

from ..amqp_utils import publish_message
from ..utils import load_json_model, parse_datetime


@click.command(name="trigger-exporters")
@click.argument("dataset_uri")
@click.argument("metadata_file", type=click.Path(exists=True))
@click.argument("start_time")
@click.argument("end_time")
@click.pass_context
def trigger_exporters(
    ctx,
    dataset_uri: str,
    metadata_file: str,
    start_time: str,
    end_time: str,
):
    """Republish DataSetAvailableEvent to trigger exporters for existing dataset.
    
    This allows reprocessing of an existing dataset through all exporters,
    useful for redriving failed exports or updating export formats.
    
    DATASET_URI: Dataset location in object store (e.g., sensor_community/20240101T120000_PT1H_a3f2bc9d)
    
    METADATA_FILE: Path to JSON file containing DatasetMetadata
    
    START_TIME: Window start datetime in ISO format (e.g., 2024-01-01T12:00:00Z)
    
    END_TIME: Window end datetime in ISO format (e.g., 2024-01-01T13:00:00Z)
    """
    config = ctx.obj["config"]
    
    start_dt = parse_datetime(start_time)
    end_dt = parse_datetime(end_time)
    
    if start_dt >= end_dt:
        raise click.ClickException("Start time must be before end time")
    
    metadata = load_json_model(metadata_file, DatasetMetadata)
    
    event = DataSetAvailableEvent(
        id=uuid4(),
        metadata=metadata,
        dataset_location=dataset_uri,
        start_time=start_dt,
        end_time=end_dt,
    )
    
    async def execute():
        await publish_message(
            config=config,
            message=event,
            exchange_name="ionbeam.dataset.available",
        )
        click.echo("✓ Export trigger published")
        click.echo(f"  Event ID: {event.id}")
        click.echo(f"  Dataset: {metadata.name}")
        click.echo(f"  Location: {dataset_uri}")
        click.echo(f"  Time window: {start_dt.isoformat()} to {end_dt.isoformat()}")
    
    asyncio.run(execute())