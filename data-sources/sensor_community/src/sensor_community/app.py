# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import os
from contextlib import asynccontextmanager

import click
import structlog
import yaml
from ionbeam_client import IonbeamClient, run_source

from .client import SensorCommunitySource
from .models import SensorCommunityConfig

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def live_poll(
    source: SensorCommunitySource, client: IonbeamClient, shutdown: asyncio.Event
):
    poll_task = asyncio.create_task(source.poll_live(client, shutdown))
    # a dead poll loop takes the pod down instead of degrading to triggers-only
    poll_task.add_done_callback(lambda _: shutdown.set())
    try:
        yield
    finally:
        await poll_task


async def run_app():
    config_path = os.getenv("SENSOR_COMMUNITY_CONFIG_PATH", "config.yaml")
    logger.info("Loading configuration", config_path=config_path)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    source = SensorCommunitySource(
        SensorCommunityConfig(**config.get("sensor_community", {}))
    )

    def setup(client: IonbeamClient, shutdown: asyncio.Event):
        async def handle_time_window(start_time, end_time, trigger_id) -> None:
            logger.info(
                "Handling archive time window",
                start=start_time.isoformat(),
                end=end_time.isoformat(),
            )
            await source.fetch(start_time, end_time, client, ingestion_id=trigger_id)

        client.register_trigger_handler(
            config.get("source_name", "sensor.community"), handle_time_window
        )
        return live_poll(source, client, shutdown)

    await run_source("sensor.community", config, setup)


@click.command()
@click.option("--config", "-c", envvar="SENSOR_COMMUNITY_CONFIG_PATH", default="config.yaml", help="Path to config file")
def main(config):
    """Sensor.community data source - Fetch data from Sensor.community air quality sensors."""
    os.environ["SENSOR_COMMUNITY_CONFIG_PATH"] = config
    asyncio.run(run_app())


if __name__ == "__main__":
    main()
