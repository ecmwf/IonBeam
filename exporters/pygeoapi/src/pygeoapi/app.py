# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import asyncio
import os
import pathlib
import signal

import click
import structlog
import yaml

from ionbeam_client import IonbeamClient, IonbeamClientConfig

from .exporter import PyGeoApiExporter, PyGeoApiExporterConfig


logger = structlog.get_logger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


async def run_app():
    config_path = os.getenv("PYGEOAPI_CONFIG_PATH", "config.yaml")
    logger.info("Loading configuration", config_path=config_path)

    config_dict = load_config(config_path)

    ionbeam_config = IonbeamClientConfig(**config_dict.get("ionbeam", {}))

    pygeoapi_config_dict = config_dict.get("pygeoapi_exporter", {})

    pygeoapi_config = PyGeoApiExporterConfig(
        output_path=pathlib.Path(
            pygeoapi_config_dict.get("output_path", "./data/pygeoapi")
        ),
        config_path=pathlib.Path(
            pygeoapi_config_dict.get("config_path", "./pygeoapi.yaml")
        ),
    )

    exporter = PyGeoApiExporter(pygeoapi_config)

    ionbeam_client = IonbeamClient(ionbeam_config)

    exporter_name = config_dict.get("exporter_name", "pygeoapi")
    dataset_filter = set(config_dict.get("dataset_filter", []))

    ionbeam_client.register_export_handler(
        exporter_name=exporter_name,
        handler=exporter.export_handler,
        dataset_filter=dataset_filter if dataset_filter else None,
    )

    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info(
        "Starting PyGeoAPI exporter",
        exporter=exporter_name,
        dataset_filter=sorted(dataset_filter) if dataset_filter else None,
    )

    async with ionbeam_client:
        logger.info("PyGeoAPI exporter running and listening for dataset events")

        await shutdown_event.wait()

        logger.info("Shutting down")

    logger.info("PyGeoAPI exporter stopped")


@click.command()
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
def main(config):
    os.environ["PYGEOAPI_CONFIG_PATH"] = config
    asyncio.run(run_app())


if __name__ == "__main__":
    main()
