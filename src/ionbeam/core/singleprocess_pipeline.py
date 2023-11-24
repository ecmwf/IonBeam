# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import logging, sys
from typing import Iterable, Sequence

from itertools import cycle, islice, chain

from .bases import FinishMessage, MessageStream, Processor, Action, DataMessage, Message, Aggregator
from typing import Tuple


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


def fully_process_message(msg: Message, actions) -> list[Message]:
    input_messages = [msg]
    finished_messages = []
    while input_messages:
        msg = input_messages.pop()
        matched = False
        for dest in actions:
            if dest.matches(msg):
                log_message_transmission(msg, dest)
                input_messages.extend(dest.process(msg))
                matched = True
        if not matched:
            finished_messages.append(msg)
    return finished_messages


def empty_aggregators(actions) -> Iterable[Message]:
    """
    Iterate over all the actions and send a finish message to the aggregators
    yield any messages that result.
    """
    for a in actions:
        if isinstance(a, Aggregator):
            for m in a.process(FinishMessage(f"Sources Exhausted")):
                yield m


def log_message_transmission(msg, dest):
    "Print a nicely formatted message showing where a message came from and where it got sent."
    prev_action = f"{msg.history[-1].action.name} --->" if getattr(msg, "history", []) else ""
    logger.info(f"{prev_action} {str(msg)} --> {dest.__class__.__name__}")


from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn


def customProgress():
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        # MofNCompleteColumn(),
        TextColumn("{task.completed}"),
    )


def singleprocess_pipeline(sources, actions, emit_partial: bool):
    logger.info("Starting single threaded execution...")

    source = roundrobin(sources)
    source = chain(source, [FinishMessage(f"Sources Exhausted")])

    input_messages = []
    finished_messages = []
    sources_done = False
    aggregators_emptied = False

    # The first time we catch ctrl_c we stop the sources
    # The second time we hard exit
    keyboard_interrupts_caught = 0

    total_input_messages = 0
    processed_messages = 0

    with customProgress() as progress:
        message_queue_bar = progress.add_task("[red]Input Queue", total=None)
        processed_messages_bar = progress.add_task("[yellow]Actions Run", total=None)
        finished_message_bar = progress.add_task("[green]Finished Messages", total=None)

        while True:
            progress.update(processed_messages_bar, completed=processed_messages, total=processed_messages)
            progress.update(message_queue_bar, completed=len(input_messages), total=total_input_messages)
            progress.update(finished_message_bar, completed=len(finished_messages), total=total_input_messages)
            try:  # Use this try except block to catch CTRL-C and use it to
                # Grab a message from the source if there are any.
                if not sources_done and len(input_messages) < 500:
                    try:
                        input_messages.append(next(source))
                        total_input_messages += 1
                    except StopIteration:
                        sources_done = True

                # If we've processed every message we can, empty the aggregators.
                if not input_messages and not aggregators_emptied:
                    progress.update(message_queue_bar, description="[red]Aggregated Messages Queue")

                    # By default (i.e emit_partial == false) we just throw half filled message buckets away
                    if emit_partial:
                        logger.warning(f"Initial messages consumed, emptying aggregators.")
                        input_messages.extend(empty_aggregators(actions))
                    aggregators_emptied = True

                # If we've processed every message and emptied the aggregators, finish.
                if not input_messages and aggregators_emptied:
                    break

                # Pop a message off the queue and process it
                msg = input_messages.pop()
                processed_messages += 1

                # We don't want to send a FinshMessage to the aggregators before all messages have had a chance to get there.
                if isinstance(msg, FinishMessage):
                    continue

                # fully_process_message(msg)
                # If the message doesn't match any actions, we move it to the finished queue.
                matched = False
                for dest in actions:
                    if dest.matches(msg):
                        log_message_transmission(msg, dest)
                        input_messages.extend(dest.process(msg))
                        matched = True
                if not matched:
                    logger.info(f"{str(msg)} --> Done")
                    finished_messages.append(msg)

            # logger.info(f"Finshed messages: {finished_messages}")
            except KeyboardInterrupt:
                keyboard_interrupts_caught += 1
                if keyboard_interrupts_caught == 1:
                    logger.warning("Caught ctrl-c, shutting down the sources and trying to exit gracefully...")
                    sources_done = False

                elif keyboard_interrupts_caught == 2:
                    logger.warning("Okay okay, hard exiting...")
                    sys.exit()

    return finished_messages
