import os
import pika
from pathlib import Path
from typing import Iterable
import pickle
import logging
import argparse
import sys

from ..core.config_parser import parse_config
from ..core.bases import Message, FinishMessage

from .common import arg_parser, get_rabbitMQ_connection

logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)

parser = arg_parser("Source")
parser.add_argument(
    "--redirect-to-terminal",
    action="store_true",
    help="Just print the messages directly to the terminal instead of sending them to rabbitMQ",
)
args = parser.parse_args()

# Set the log level, default is warnings, -v gives info, -vv for debug
logging.basicConfig(
    level=[logging.WARNING, logging.INFO, logging.DEBUG][min(2, args.verbose)],
)

with open(args.config_file, "rb") as f:
    config = pickle.load(f)

producer = config["action"]

if args.redirect_to_terminal:
    print("Just printing messages directly to the terminal")
    print(producer.metadata)
    for i, message in enumerate(producer.generate()):
        print(message)
    sys.exit()

channel, connection = get_rabbitMQ_connection()

# Send a message
for i, message in enumerate(producer.generate()):
    body = pickle.dumps(message)
    try:
        channel.basic_publish(
            exchange="",
            routing_key="Stateless",
            body=body,
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
        )
        print(f"Sent {str(message)}")
    except pika.exceptions.UnroutableError:
        print("Message could not be confirmed")

body = pickle.dumps(FinishMessage(reason="Done"))
channel.basic_publish(
    exchange="",
    routing_key="Stateless",
    body=body,
    properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
)

# Close the channel and the connection
channel.close()
connection.close()
