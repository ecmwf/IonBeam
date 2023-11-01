import os
import pika
from pathlib import Path
from typing import Iterable
import pickle
import logging

from ..core.config_parser import parse_config
from ..core.bases import Message, FinishMessage

logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)

config_file = Path("/ionbeam/config/config.yaml")
config = parse_config(config_file)

pipeline = [getattr(config, name) for name in config.pipeline]
producer = pipeline[0][int(os.environ["SOURCE_INDEX"])]

credentials = pika.PlainCredentials("guest", "guest")
parameters = pika.ConnectionParameters(
    host="rabbitMQ", port=5673, virtual_host="/", credentials=credentials
)

connection = pika.BlockingConnection(parameters)

# Open the channel
channel = connection.channel()

# Declare the queue
channel.queue_declare(
    queue="Stateless",
    durable=True,
    exclusive=False,
    auto_delete=False,
)

# # Turn on delivery confirmations
channel.confirm_delivery()

# Send a message
for i, message in enumerate(producer.generate()):
    body = pickle.dumps(message)
    try:
        channel.basic_publish(
            exchange="",
            routing_key="Stateless",
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
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
