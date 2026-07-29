# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import os

import click
import structlog
import yaml
from ionbeam_client import IonbeamClient, run_source

from .client import AcronetSource
from .models import AcronetConfig

logger = structlog.get_logger(__name__)


async def run_app():
    config_path = os.getenv("ACRONET_CONFIG_PATH", "config.yaml")
    logger.info("Loading configuration", config_path=config_path)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    source = AcronetSource(AcronetConfig(**config.get("acronet", {})))

    def setup(client: IonbeamClient, shutdown: asyncio.Event) -> None:
        async def handle_time_window(start_time, end_time, trigger_id) -> None:
            logger.info(
                "Handling time window",
                start=start_time.isoformat(),
                end=end_time.isoformat(),
            )
            await source.fetch(start_time, end_time, client, ingestion_id=trigger_id)

        client.register_trigger_handler(
            config.get("source_name", "acronet"), handle_time_window
        )

    await run_source("acronet", config, setup)


@click.command()
@click.option("--config", "-c", envvar="ACRONET_CONFIG_PATH", default="config.yaml", help="Path to config file")
def main(config):
    """Acronet data source - Fetch data from Acronet weather stations."""
    os.environ["ACRONET_CONFIG_PATH"] = config
    asyncio.run(run_app())
