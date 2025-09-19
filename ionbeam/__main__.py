import asyncio

import click

from ionbeam.apps.faststream import factory
from ionbeam.cli.cli import cli as ionbeam_cli


@click.group(help="Ionbeam entrypoint. Use 'faststream' to run the FastStream app or 'cli' to access Ionbeam CLI commands.")
def main():
    pass


@main.command("faststream")
def faststream_cmd():
    """Run ionbeam using faststream framework for AMQP orchestration."""
    async def _run():
        app = await factory()
        await app.run()

    try:
        asyncio.run(_run())
    except asyncio.CancelledError:
        pass


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
