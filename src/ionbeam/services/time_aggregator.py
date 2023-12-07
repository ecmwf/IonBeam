import pika
from pathlib import Path
from typing import Iterable
import pickle
import logging
from ..core.bases import Message
from .common import (
    arg_parser,
    get_rabbitMQ_connection,
    aggregator_queue_name,
    listen,
    declare_aggregator_queue,
    setup_logging,
    send_to_queue,
)
from ..core.singleprocess_pipeline import log_message_transmission

parser = arg_parser("Source")
args = parser.parse_args()
config_file = Path("/ionbeam/config/config.pickle")

# Get the aggregator instance from the config
with open(args.config_file, "rb") as f:
    config = pickle.load(f)
    aggregator = config["action"]

listen_queue = aggregator_queue_name(aggregator)
publish_queue = "Stateless"

logger = setup_logging(name=listen_queue, verbosity=args.verbose)

# Connect to RabbitMQ
logger.info("Connecting to rabbitMQ...")
channel, connection = get_rabbitMQ_connection()

# Declare this Aggregator queue (the others are declared in get_rabbitMQ_connection)
declare_aggregator_queue(channel, listen_queue)


def on_message(channel, method_frame, header_frame, body):
    logger.info(f"Got message, tag: {method_frame.delivery_tag}")

    message = pickle.loads(body)
    output_messages = aggregator.process(message)

    for message in output_messages:
        body = pickle.dumps(message)
        log_message_transmission(logger, message, publish_queue)
        send_to_queue(channel, publish_queue, body)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


logger.info(f"Aggregator listening on {listen_queue} queue")
listen(connection, channel, logger, queue_name=listen_queue, on_message=on_message)
