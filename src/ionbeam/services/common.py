import argparse
from pathlib import Path
from dataclasses import dataclass
import pika
from ..core.bases import Action


def arg_parser(name):
    parser = argparse.ArgumentParser(
        prog=f"IonBeam {name} Service",
        description="",
        epilog="See https://github.com/ecmwf-projects/ionbeam for more info.",
    )
    parser.add_argument(
        "--config_file",
        default="/etc/ionbeam/config.yaml",
        help="Path to a yaml config file for this service.",
        type=Path,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Set the logging level, default is warnings only, -v and -vv give increasing verbosity",
    )
    return parser


def get_rabbitMQ_connection():
    credentials = pika.PlainCredentials("guest", "guest")
    parameters = pika.ConnectionParameters(host="rabbitMQ", port=5673, virtual_host="/", credentials=credentials)

    connection = pika.BlockingConnection(parameters)

    # Open the channel
    channel = connection.channel()

    # Declare the queues
    channel.queue_declare(
        queue="Stateless",
        durable=True,
        exclusive=False,
        auto_delete=False,
    )

    # # Turn on delivery confirmations
    channel.confirm_delivery()

    return channel, connection


if __name__ == "__main__":
    print("This file should not be invoked as a script!")
