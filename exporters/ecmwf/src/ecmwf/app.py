# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import signal

import click
import structlog
import yaml

from ionbeam_client import IonbeamClient, IonbeamClientConfig

from .exporter import ODBExporter, ODBExporterConfig


logger = structlog.get_logger(__name__)


async def run_app(config_path: str):
    logger.info("Loading configuration", config_path=config_path)

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    ionbeam_config = IonbeamClientConfig(**config_dict.get("ionbeam", {}))

    odb_config_dict = config_dict.get("odb_exporter", {})

    odb_config = ODBExporterConfig(
        **{
            "output_path": "./data/odb",
            **{k: v for k, v in odb_config_dict.items() if v is not None},
        }
    )

    exporter = ODBExporter(odb_config)
    ionbeam_client = IonbeamClient(ionbeam_config)

    exporter_name = config_dict.get("exporter_name", "odb")
    dataset_filter = set(config_dict.get("dataset_filter", []))

    ionbeam_client.register_export_handler(
        exporter_name=exporter_name,
        handler=exporter.export_handler,
        dataset_filter=dataset_filter if dataset_filter else None,
    )

    shutdown_event = asyncio.Event()

    def request_shutdown(signum: int) -> None:
        logger.info("Received shutdown signal", signal=signum)
        shutdown_event.set()

    # loop-aware handlers: a raw signal.signal handler sets the event without
    # waking the selector, stalling shutdown until the next dataset event
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_shutdown, sig)

    logger.info(
        "Starting ECMWF ODB exporter",
        exporter=exporter_name,
        dataset_filter=sorted(dataset_filter) if dataset_filter else None,
    )

    async with ionbeam_client:
        logger.info("ODB exporter running and listening for dataset events")

        await shutdown_event.wait()

        logger.info("Shutting down")

    logger.info("ODB exporter stopped")


@click.command()
@click.option(
    "--config",
    "-c",
    envvar="ECMWF_CONFIG_PATH",
    default="config.yaml",
    help="Path to config file",
)
def main(config):
    """ECMWF ODB exporter - Export Ionbeam datasets to ECMWF ODB format."""
    asyncio.run(run_app(config))


if __name__ == "__main__":
    main()
