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
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8080, type=int, help="Port to bind to")
@click.option("--log-level", default="info", help="Log level (debug, info, warning, error)")
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
def legacy_api_cmd(host, port, log_level, config):
    # Set config path environment variable for consistency
    os.environ["IONBEAM_CONFIG_PATH"] = config
    
    # Load configuration from YAML file
    config_path = Path(config)
    if config_path.exists():
        with open(config_path) as f:
            yaml_config = yaml.safe_load(f)
        
        # Extract legacy API config if it exists in the YAML
        legacy_config_dict = yaml_config.get("legacy_api", {})
        legacy_api_config = LegacyApiConfig(**legacy_config_dict)
    else:
        # Use default config if file doesn't exist
        legacy_api_config = LegacyApiConfig()
    
    app = create_legacy_application(config=legacy_api_config)
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=log_level,
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
