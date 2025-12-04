# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import asyncio
import os
import signal

import click
import structlog
import yaml

from ionbeam_client import IonbeamClient, IonbeamClientConfig

from .client import MeteoTrackerSource
from .models import MeteoTrackerConfig


logger = structlog.get_logger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


async def run_app():
    config_path = os.getenv("METEOTRACKER_CONFIG_PATH", "config.yaml")
    logger.info("Loading configuration", config_path=config_path)

    config_dict = load_config(config_path)

    ionbeam_config = IonbeamClientConfig(**config_dict.get("ionbeam", {}))
    mt_config = MeteoTrackerConfig(**config_dict.get("meteotracker", {}))

    source = MeteoTrackerSource(mt_config)
    ionbeam_client = IonbeamClient(ionbeam_config)

    async def handle_time_window(start_time, end_time) -> None:
        logger.info(
            "Handling time window",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )
        await source.fetch(start_time, end_time, ionbeam_client)

    source_name = config_dict.get("source_name", "meteotracker")
    ionbeam_client.register_trigger_handler(source_name, handle_time_window)

    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting MeteoTracker data source", source_name=source_name)

    async with ionbeam_client:
        logger.info("MeteoTracker data source running and listening for triggers")
        await shutdown_event.wait()
        logger.info("Shutting down")

    logger.info("MeteoTracker data source stopped")


@click.command()
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
def main(config):
    """MeteoTracker data source - Fetch data from MeteoTracker weather stations."""
    os.environ["METEOTRACKER_CONFIG_PATH"] = config
    asyncio.run(run_app())


if __name__ == "__main__":
    main()
