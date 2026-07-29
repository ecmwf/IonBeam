# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import os

import click
import structlog
import yaml
from ionbeam_client import IonbeamClient, run_source

from .client import NetAtmoMQTTSource
from .models import NetAtmoMQTTConfig
from .netatmo_metadata import netatmo_metadata

logger = structlog.get_logger(__name__)

# The one EUMETNET E-SOH dataset draws from both the raw observation feed and
# FMI's quality-controlled feed; both land in `netatmo_metadata`.
NETATMO_TOPICS = ["raw-obs/+/netatmo/#", "qc-obs/+/netatmo/#"]


async def run_app():
    config_path = os.getenv("EUMETNET_CONFIG_PATH", "config.yaml")
    logger.info("Loading configuration", config_path=config_path)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    def setup(client: IonbeamClient, shutdown: asyncio.Event) -> NetAtmoMQTTSource:
        # Flush tuning comes from the config file; broker connection + credentials
        # come from MQTT_* env vars (the model reads them).
        return NetAtmoMQTTSource(
            config=NetAtmoMQTTConfig(**config.get("netatmo_mqtt", {})),
            client=client,
            metadata=netatmo_metadata,
            topics=NETATMO_TOPICS,
        )

    await run_source("eumetnet", config, setup)


@click.command()
@click.option("--config", "-c", envvar="EUMETNET_CONFIG_PATH", default="config.yaml", help="Path to config file")
def main(config):
    """Eumetnet data source - Fetch data from Eumetnet Netatmo MQTT stream."""
    os.environ["EUMETNET_CONFIG_PATH"] = config
    asyncio.run(run_app())


if __name__ == "__main__":
    main()
