# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import asyncio
import logging
import os

import click

from ionbeam.application.factory import factory


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
@click.pass_context
def cli(ctx, verbose, config):
    """Ionbeam Core - IoT data ingestion and aggregation platform."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    os.environ["IONBEAM_CONFIG_PATH"] = config

    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["verbose"] = verbose


@cli.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8000, type=int, help="Port to bind to")
@click.option("--with-builder", is_flag=True, help="Enable dataset builder service")
@click.pass_context
def start(ctx, host, port, with_builder):
    """Start the ionbeam service.

    This runs the core ingestion and coordination handlers.
    Use --with-builder to also run the dataset builder.
    """
    click.echo(f"Starting ionbeam service on {host}:{port}")
    if with_builder:
        click.echo("Dataset builder service enabled")

    async def _run():
        app = await factory(with_builder=with_builder)
        await app.run(run_extra_options=dict(host=host, port=port))

    asyncio.run(_run())


@cli.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8001, type=int, help="Port to bind to")
@click.pass_context
def start_builder(ctx, host, port):
    """Start only the dataset builder service.

    This runs the builder as a standalone service.
    """
    click.echo(f"Starting dataset builder service on {host}:{port}")

    async def _run():
        app = await factory(with_builder=True)
        await app.run(run_extra_options=dict(host=host, port=port))

    asyncio.run(_run())


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
