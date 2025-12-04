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

from .client import NetAtmoMQTTSource
from .models import NetAtmoMQTTConfig
from .netatmo_metadata import netatmo_metadata
from .netatmo_qc_metadata import netatmo_qc_metadata


logger = structlog.get_logger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


async def run_app():
    config_path = os.getenv("EUMETNET_CONFIG_PATH", "config.yaml")
    logger.info("Loading configuration", config_path=config_path)

    config_dict = load_config(config_path)

    ionbeam_config = IonbeamClientConfig(**config_dict.get("ionbeam", {}))
    mqtt_config = NetAtmoMQTTConfig(**config_dict.get("netatmo_mqtt", {}))

    enable_qc = config_dict.get("enable_netatmo_qc", False)
    enable_regular = config_dict.get("enable_netatmo", True)

    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info(
        "Starting Eumetnet Netatmo MQTT data source(s)",
        enable_regular=enable_regular,
        enable_qc=enable_qc,
    )

    sources = []

    if enable_regular:
        ionbeam_client_regular = IonbeamClient(ionbeam_config)
        regular_source = NetAtmoMQTTSource(
            config=mqtt_config,
            client=ionbeam_client_regular,
            metadata=netatmo_metadata,
            topic="raw-obs/+/netatmo/#",
            client_id_suffix="",
        )
        sources.append(regular_source)
        logger.info("Enabled regular Netatmo source", topic="raw-obs/+/netatmo/#")

    if enable_qc:
        ionbeam_client_qc = IonbeamClient(ionbeam_config)
        qc_source = NetAtmoMQTTSource(
            config=mqtt_config,
            client=ionbeam_client_qc,
            metadata=netatmo_qc_metadata,
            topic="qc-obs/fmi/+/#",
            client_id_suffix="_qc",
        )
        sources.append(qc_source)
        logger.info("Enabled Netatmo QC source", topic="qc-obs/fmi/+/#")

    if not sources:
        logger.error(
            "No sources enabled! Enable at least one of: enable_netatmo, enable_netatmo_qc"
        )
        return

    async def run_sources():
        async with asyncio.TaskGroup() as tg:
            for source in sources:
                tg.create_task(source.__aenter__())

        try:
            logger.info("Eumetnet Netatmo MQTT data source(s) running")
            await shutdown_event.wait()
        finally:
            for source in sources:
                await source.__aexit__(None, None, None)

    await run_sources()
    logger.info("Eumetnet Netatmo MQTT data source(s) stopped")


@click.command()
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
def main(config):
    """Eumetnet data source - Fetch data from Eumetnet Netatmo MQTT stream."""
    os.environ["EUMETNET_CONFIG_PATH"] = config
    asyncio.run(run_app())


if __name__ == "__main__":
    main()
