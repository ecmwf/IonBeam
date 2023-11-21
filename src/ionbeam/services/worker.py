import pika
from pathlib import Path
from typing import Iterable
import pickle
import logging

from ..core.config_parser import parse_config
from ..core.bases import Message, FinishMessage


def aggregator_queue_name(A):
    return f"{A.__class__.__name__}({', '.join(m.source for m in A.match)})"


logger = logging.getLogger()
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logger.info("Loading config file...")

config_file = Path("/ionbeam/config/config.yaml")
config = parse_config(config_file)

# TODO: fix the consumers to do nothing when the config is loaded, only when run
pipeline = [getattr(config, name) for name in config.pipeline]
pipeline_dict = {name: getattr(config, name) for name in config.pipeline}
consumers = [consumer for stage in pipeline[1:-2] for consumer in stage]
aggregator_queues = {aggregator_queue_name(A): A for A in pipeline_dict["aggregators"]}


def process_message(msg: Message) -> Iterable[Message]:
    logger.info(f"Processing message: {msg}")
    for dest in consumers:
        logger.info(f"Trying {dest}")
        if dest.matches(msg):
            logger.info(f"Giving {msg} to {dest.__class__.__name__}")
            return False, dest.process(msg)
    return True, [msg]


def do_all_work(message):
    input_messages = [message]
    finished_messages = []
    while input_messages:
        msg = input_messages.pop()
        is_done, msgs = process_message(msg)
        if is_done:
            finished_messages.extend(msgs)
        else:
            input_messages.extend(msgs)

    logger.info(f"Finshed messages: {finished_messages}")
    return finished_messages


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
    queue="Done",
    durable=True,
    exclusive=False,
    auto_delete=False,
)

# Declare the aggregator queues
for queue_name, aggregator in aggregator_queues.items():
    channel.queue_declare(
        queue=queue_name, durable=True, exclusive=False, auto_delete=False
    )


def on_message(channel, method_frame, header_frame, body):
    logger.info(f"Got message, tag: {method_frame.delivery_tag}")

    message = pickle.loads(body)
    logger.info(f"Unpickled message: {message}")

    if isinstance(message, FinishMessage):
        logger.info(f"FINISH MESSAGE")
        for queue_name, aggregator in aggregator_queues.items():
            channel.basic_publish(
                "",
                queue_name,
                body=pickle.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ),
            )

    output_messages = do_all_work(message)

    for output_message in output_messages:
        matched = False
        for queue_name, aggregator in aggregator_queues.items():
            print(queue_name, aggregator)
            if aggregator.matches(output_message):
                matched = True
                channel.basic_publish(
                    "",
                    queue_name,
                    body=pickle.dumps(output_message),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                    ),
                )
        if not matched:
            channel.basic_publish(
                "",
                "Done",
                body=pickle.dumps(output_message),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ),
            )

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


# Tells rabbitMQ to only give us one message at a time
channel.basic_qos(prefetch_count=1)

print("Worker alive, listening on Stateless queue")
channel.basic_consume("Stateless", on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
