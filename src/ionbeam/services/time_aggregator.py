import pika
from pathlib import Path
from typing import Iterable
import pickle
import logging
import os

from ..core.config_parser import parse_config
from ..core.bases import Message


def aggregator_queue_name(A):
    return f"{A.__class__.__name__}({', '.join(m.source for m in A.match)})"


logger = logging.getLogger()
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info("Loading config file...")

config_file = Path("/ionbeam/config/config.yaml")
config = parse_config(config_file)

pipeline_dict = {name: getattr(config, name) for name in config.pipeline}
this_aggregator = pipeline_dict["aggregators"][int(os.environ["TIME_AGGREGATOR_INDEX"])]
aggregator_queue_name = aggregator_queue_name(this_aggregator)

logger.info(f"queue name: {aggregator_queue_name}")

logger.info("Connecting to rabbitMQ...")
credentials = pika.PlainCredentials("guest", "guest")
parameters = pika.ConnectionParameters(
    host="rabbitMQ", port=5673, virtual_host="/", credentials=credentials
)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(
    queue="Stateless",
    durable=True,
    exclusive=False,
    auto_delete=False,
)

channel.queue_declare(
    queue=aggregator_queue_name, durable=True, exclusive=False, auto_delete=False
)


def on_message(channel, method_frame, header_frame, body):
    logger.info(f"Got message, tag: {method_frame.delivery_tag}")

    message = pickle.loads(body)
    logger.info(f"Unpickled message: {message}")
    output_messages = this_aggregator.process(message)

    for output_message in output_messages:
        logger.info("Sending message to output.")
        channel.basic_publish(
            "",
            "Stateless",
            body=pickle.dumps(output_message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


# Tells rabbitMQ to only give us one message at a time
channel.basic_qos(prefetch_count=1)

print(f"{aggregator_queue_name} alive, listening on {aggregator_queue_name}")
channel.basic_consume(aggregator_queue_name, on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
