import os

import click
import yaml

from .commands import rebuild_dataset, reingest, trigger_data_source, trigger_exporters
from .config import CLIConfig


@click.group()
@click.version_option(version="0.1.0")
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
@click.pass_context
def cli(ctx, config: str):
    ctx.ensure_object(dict)
    
    config_path = os.path.expanduser(config)
    if os.path.exists(config_path):
        with open(config_path) as f:
            config_dict = yaml.safe_load(f) or {}
    else:
        config_dict = {}
    
    ctx.obj["config"] = CLIConfig(**config_dict)


cli.add_command(trigger_data_source)
cli.add_command(reingest)
cli.add_command(rebuild_dataset)
cli.add_command(trigger_exporters)


def main():
    cli()


if __name__ == "__main__":
    main()
