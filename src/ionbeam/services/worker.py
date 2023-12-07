import pika
from pathlib import Path
from typing import Iterable
import pickle

from ..core.bases import Message

from .common import arg_parser, get_rabbitMQ_connection, aggregator_queue_name, listen, setup_logging, send_to_queue
from ..core.singleprocess_pipeline import log_message_transmission

parser = arg_parser("Source")
args = parser.parse_args()
config_file = Path("/ionbeam/config/config.pickle")


with open(args.config_file, "rb") as f:
    config = pickle.load(f)
    actions = config["actions"]
    aggregators = config["aggregators"]

listen_queue = "Stateless"
logger = setup_logging("worker", verbosity=args.verbose)


def process_message(msg: Message) -> tuple[bool, Iterable[Message]]:
    "Attempt to process a message, return true, [message] if it cannot be processed any more."
    logger.debug(f"Processing message: {msg}")
    for dest in actions:
        if dest.matches(msg):
            log_message_transmission(logger, msg, dest)
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

    return finished_messages


logger.info("Connecting to rabbitMQ...")
channel, connection = get_rabbitMQ_connection()

# # Declare the aggregator queues
aggregator_queues = {aggregator_queue_name(a): a for a in aggregators}
for k in aggregator_queues.keys():
    channel.queue_declare(queue=k, durable=True, exclusive=False, auto_delete=False)


def on_message(channel, method_frame, header_frame, body):
    logger.debug(f"Got message, tag: {method_frame.delivery_tag}")

    message = pickle.loads(body)

    # if isinstance(message, FinishMessage):
    #     logger.info(f"FINISH MESSAGE")
    #     for queue_name, aggregator in aggregator_queues.items():
    #         send_to_queue(channel, queue_name, pickle.dumps(message))

    output_messages = do_all_work(message)

    for output_message in output_messages:
        matched = False
        body = pickle.dumps(output_message)
        for queue_name, aggregator in aggregator_queues.items():
            if aggregator.matches(output_message):
                matched = True
                log_message_transmission(logger, output_message, queue_name)
                send_to_queue(channel, queue_name, body)
        if not matched:
            log_message_transmission(logger, output_message, "Done")
            send_to_queue(channel, "Done", body)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


logger.info(f"Worker node listening on {listen_queue} queue")
listen(connection, channel, logger, queue_name=listen_queue, on_message=on_message)
