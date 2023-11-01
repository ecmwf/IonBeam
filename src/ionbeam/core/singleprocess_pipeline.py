# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import logging
from typing import Iterable, Sequence

from itertools import cycle, islice, chain

from .bases import FinishMessage, MessageStream, Processor

from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm import tqdm


logger = logging.getLogger()


def roundrobin(iterables: Sequence[MessageStream]) -> MessageStream:
    # Recipe credited to George Sakkis
    pending = len(iterables)
    nexts = cycle(iter(it).__next__ for it in iterables)
    while pending:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            pending -= 1
            nexts = cycle(islice(nexts, pending))


def pipeline_connection(
    destination_stage: str, source: MessageStream, destinations: Iterable[Processor]
) -> MessageStream:
    """
    This function is the 'pipe' in between factory functions of the pipeline
    It is responsible for grabbing incoming messages and giving them to any destination in the output that matches

    source: iterable of messages coming in
    destinations: iterable of possible destinations, send the messages their if destination.matches(message)
    returns: Iterable of messages
    """
    # at the end of the message stream, add a FinishMessage which tells the consumers
    # to finish up and yield any last data they want to before stopping
    msg_stream = chain(source, [FinishMessage("Pipeline Stage Exhausted")])

    for input_message in msg_stream:
        atleast_one_match = False
        for dest in destinations:
            if isinstance(input_message, FinishMessage) or dest.matches(input_message):
                atleast_one_match = True
                logger.debug(f"Giving {input_message} to {dest}")
                for output_message in dest.process(input_message):
                    yield output_message

        if not atleast_one_match:
            assert not isinstance(input_message, FinishMessage)
            logger.warning(
                f"Message {input_message} {input_message.metadata} did not match with anything in {destination_stage}!"
            )


def connect_up(names: Iterable[str], pipeline):
    # Take all the sources and aggregate their streams in a roundrobin fashion
    incoming_datastream = roundrobin(pipeline[0])

    # Connect each step of the pipeline
    for dest_name, connection in zip(names[1:], pipeline[1:]):
        incoming_datastream = pipeline_connection(
            dest_name, incoming_datastream, connection
        )

    return incoming_datastream


def singleprocess_pipeline(config, pipeline):
    with logging_redirect_tqdm():
        logger.info("Starting single threaded execution...")

        outgoing_datastream = connect_up(config.pipeline, pipeline)

        for msg in tqdm(outgoing_datastream):
            logger.debug(f"Encoder emitted {msg}")
