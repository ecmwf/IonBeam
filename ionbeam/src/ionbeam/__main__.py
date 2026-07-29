# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import logging
import os

import click


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option(
    "--config", "-c", default=None,
    help="Path to config file (default: $IONBEAM_CONFIG_PATH or config.yaml)",
)
@click.pass_context
def cli(ctx, verbose, config):
    """Ionbeam - the public Arrow Flight endpoint for observation ingestion and datasets."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if config is not None:
        os.environ["IONBEAM_CONFIG_PATH"] = config

    ctx.ensure_object(dict)
    ctx.obj["config"] = os.environ.get("IONBEAM_CONFIG_PATH", "config.yaml")
    ctx.obj["verbose"] = verbose


@cli.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8815, type=int, help="Port to bind to")
@click.pass_context
def start(ctx, host, port):
    """Start the ionbeam Flight service."""
    click.echo(f"Starting ionbeam Flight service on {host}:{port}")

    # Deferred so the container reads IONBEAM_CONFIG_PATH set by the group.
    from ionbeam.application.factory import run

    run(host=host, port=port)


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
