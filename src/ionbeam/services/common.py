import argparse
from pathlib import Path
from dataclasses import dataclass
import pika
from ..core.bases import Action
import logging

# from rich.logging import RichHandler


def setup_logging(name, verbosity=2):
    logging.basicConfig(
        level=[logging.WARNING, logging.INFO, logging.DEBUG][min(2, verbosity)],
        # format="%(message)s",
        # datefmt="[%X]",
        # handlers=[RichHandler(markup=True, rich_tracebacks=True)],
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


def arg_parser(name):
    parser = argparse.ArgumentParser(
        prog=f"IonBeam {name} Service",
        description="",
        epilog="See https://github.com/ecmwf-projects/ionbeam for more info.",
    )
    parser.add_argument(
        "--config_file",
        default="/etc/ionbeam/config.pickle",
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

    channel.queue_declare(
        queue="Done",
        durable=True,
        exclusive=False,
        auto_delete=False,
    )

    # # Turn on delivery confirmations
    channel.confirm_delivery()

    # Tells rabbitMQ to only give us one message at a time
    channel.basic_qos(prefetch_count=1)

    return channel, connection


def send_to_queue(channel, queue, body):
    channel.basic_publish(
        "",
        queue,
        body,
        properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
    )


def aggregator_queue_name(A):
    return f"{A.__class__.__name__}({', '.join(m.source for m in A.match)})"


def declare_aggregator_queue(channel, name):
    channel.queue_declare(queue=name, durable=True, exclusive=False, auto_delete=False)


def listen(connection, channel, logger, queue_name: str, on_message):
    channel.basic_consume(queue_name, on_message)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info(f"Caught interrupt, exiting gracefully...")
        channel.stop_consuming()
    connection.close()


if __name__ == "__main__":
    print("This file should not be invoked as a script!")
