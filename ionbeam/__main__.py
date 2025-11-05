import asyncio
import os
from pathlib import Path

import click
import uvicorn
import yaml

from ionbeam.apps.faststream import factory
from ionbeam.cli.cli import cli as ionbeam_cli
from ionbeam.legacy_api import LegacyApiConfig, create_legacy_application


@click.group(help="Ionbeam entrypoint. Use 'faststream' to run the FastStream app, 'legacy-api' to run the legacy API, or 'cli' to access Ionbeam CLI commands.")
def main():
    pass


@main.command("faststream")
def faststream_cmd():
    """Run ionbeam using faststream framework for AMQP orchestration."""
    async def _run():
        app = await factory()
        await app.run(run_extra_options=dict(host="0.0.0.0"))

    asyncio.run(_run())


@main.command("legacy-api")
def legacy_api_cmd():
    """Run the legacy API server."""
    config_path = Path(os.getenv("IONBEAM_CONFIG_PATH", "config.yaml"))
    
    if config_path.exists():
        with open(config_path) as f:
            yaml_config = yaml.safe_load(f)

        legacy_config_dict = yaml_config.get("legacy_api", {})
        legacy_api_config = LegacyApiConfig(**legacy_config_dict)
    else:
        legacy_api_config = LegacyApiConfig()
    
    app = create_legacy_application(config=legacy_api_config)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
    )


@main.command(
    "cli",
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    add_help_option=False,
)
@click.pass_context
def cli_forward(ctx):
    """
    Forward to ionbeam.cli commands.
    """
    args = list(ctx.args)
    # Delegate to the real CLI group
    try:
        ionbeam_cli(args=args, prog_name=f"{ctx.info_name} cli", standalone_mode=False)
    except SystemExit as e:
        # Propagate click's exit codes cleanly
        raise e


if __name__ == "__main__":
    main()
