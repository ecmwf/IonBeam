import pika
import pickle
import sys

from .common import arg_parser, get_rabbitMQ_connection, setup_logging, send_to_queue
from ..core.singleprocess_pipeline import log_message_transmission

parser = arg_parser("Source")

parser.add_argument(
    "--redirect-to-terminal",
    action="store_true",
    help="Just print the messages directly to the terminal instead of sending them to rabbitMQ",
)
args = parser.parse_args()

with open(args.config_file, "rb") as f:
    config = pickle.load(f)

producer = config["action"]
name = f"{producer.source}_source"
logger = setup_logging(name, verbosity=args.verbose)

if args.redirect_to_terminal:
    print("Just printing messages directly to the terminal")
    print(producer.metadata)
    for i, message in enumerate(producer.generate()):
        print(message)
    sys.exit()

publish_queue = "Stateless"

channel, connection = get_rabbitMQ_connection()

# Send a message
try:
    for i, message in enumerate(producer.generate()):
        body = pickle.dumps(message)
        try:
            log_message_transmission(logger, message, publish_queue)
            send_to_queue(channel, publish_queue, body)

        except pika.exceptions.UnroutableError:
            print("Message could not be confirmed")
except KeyboardInterrupt:
    logger.info(f"Caught interrupt, exiting gracefully...")
finally:
    channel.close()
    connection.close()
