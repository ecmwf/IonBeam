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

from .bases import FinishMessage, MessageStream, Processor, Action, DataMessage


logger = logging.getLogger(__name__)


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
    source_stage: str, destination_stage: str, source: MessageStream, destinations: Iterable[Processor]
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
    msg_stream = chain(source, [FinishMessage(f"{source_stage} Stage Exhausted")])

    for input_message in msg_stream:
        atleast_one_match = False
        for dest in destinations:
            if isinstance(input_message, FinishMessage) or dest.matches(input_message):
                atleast_one_match = True

                if getattr(input_message, "history", []):
                    prev_action = f"{input_message.history[-1].action.name} --->"
                else:
                    prev_action = ""

                logger.info(f"{prev_action} {str(input_message)} --> {dest.__class__.__name__}")
                for output_message in dest.process(input_message):
                    yield output_message

        if not atleast_one_match:
            assert isinstance(input_message, DataMessage)
            logger.warning(
                f"Message {input_message} {input_message.metadata} did not match with anything in {destination_stage}!"
            )


def connect_up(names: list[str], pipeline):
    # Take all the sources and aggregate their streams in a roundrobin fashion
    incoming_datastream = roundrobin(pipeline[0])

    # Connect each step of the pipeline
    for src_name, dest_name, connection in zip(names, names[1:], pipeline[1:]):
        incoming_datastream = pipeline_connection(src_name, dest_name, incoming_datastream, connection)

    return incoming_datastream


def singleprocess_pipeline(pipeline_names: list[str], pipeline: list[list[Action]]):
    logger.info("Starting single threaded execution...")

    outgoing_datastream = connect_up(pipeline_names, pipeline)

    from rich.progress import track

    for msg in track(outgoing_datastream, description="Output messages:", total=None):
        logger.debug(f"{pipeline_names[-1]} emitted {msg}")
