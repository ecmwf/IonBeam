import asyncio
import os
import signal

import click
import structlog
import yaml

from ionbeam_client import IonbeamClient, IonbeamClientConfig

from .client import IonCannonSource
from .models import IonCannonConfig


logger = structlog.get_logger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


async def run_app():
    config_path = os.getenv("IONCANNON_CONFIG_PATH", "config.yaml")
    logger.info("Loading configuration", config_path=config_path)

    config_dict = load_config(config_path)

    ionbeam_config = IonbeamClientConfig(**config_dict.get("ionbeam", {}))
    ioncannon_config = IonCannonConfig(**config_dict.get("ioncannon", {}))

    source = IonCannonSource(ioncannon_config)
    ionbeam_client = IonbeamClient(ionbeam_config)

    async def handle_time_window(start_time, end_time) -> None:
        logger.info(
            "Handling time window",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )
        await source.fetch(start_time, end_time, ionbeam_client)

    source_name = config_dict.get("source_name", "ioncannon")
    ionbeam_client.register_trigger_handler(source_name, handle_time_window)

    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting IonCannon data source", source_name=source_name)

    async with ionbeam_client:
        logger.info("IonCannon data source running and listening for triggers")
        await shutdown_event.wait()
        logger.info("Shutting down")

    logger.info("IonCannon data source stopped")


@click.command()
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
def main(config):
    """IonCannon data source - Generate synthetic data for load testing."""
    os.environ["IONCANNON_CONFIG_PATH"] = config
    asyncio.run(run_app())


if __name__ == "__main__":
    main()
