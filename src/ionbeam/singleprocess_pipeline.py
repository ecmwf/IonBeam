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
import sys
from itertools import chain, cycle, islice
from time import time
from typing import Iterable, Sequence

from .core.bases import Aggregator, FinishMessage, Message, MessageStream

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
        for dest in msg.find_matches(actions):
            if dest.matches(msg):
                t0 = time()
                input_messages.extend(dest.process(msg))
                log_message_transmission(logger, msg, dest, time() - t0)
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
            for m in a.process(FinishMessage("Sources Exhausted")):
                yield m

def fmt_time(t):
    units = ["s", "ms", "us", "ns"]
    for unit in units:
        if t > 1:
            return f"{t:.2f} {unit}"
        t *= 1000
    return f"{t:.0f} ns"

def log_message_transmission(logger, msg, dest, time):
    "Print a nicely formatted message showing where a message came from and where it got sent."
    prev_action = f"{msg.history[-1].action.name} --->" if getattr(msg, "history", []) else ""
    if not isinstance(dest, str):
        dest = dest.__class__.__name__
    logger.debug(f"{prev_action} {str(msg)} --> {dest} {fmt_time(time)}")


from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn


def customProgress():
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        # MofNCompleteColumn(),
        TextColumn("{task.completed}"),
    )

class DummyProgress():
    "A dummy version of the RICH progress object allowing it to be turned off"
    def __enter__(self, *args, **kwargs): return self
    def __exit__(self, *args, **kwargs): pass
    def add_task(self, *args, **kwargs): pass
    def update(self, *args, **kwargs): pass


def singleprocess_pipeline(globals, sources, actions, emit_partial: bool, 
                           simple_output : bool = False, 
                           die_on_error: bool = False) -> list[Message]:
    logger.debug("Starting single threaded execution...")

    source = roundrobin(sources)
    source = chain(source, [FinishMessage("Sources Exhausted")])

    input_messages = []
    finished_messages = []
    sources_done = False
    aggregators_emptied = False

    # The first time we catch ctrl_c we stop the sources
    # The second time we hard exit
    keyboard_interrupts_caught = 0

    total_input_messages = 0
    processed_messages = 0

    ProgressClass = DummyProgress if simple_output else customProgress 

    with ProgressClass() as progress:
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
                        s = next(source)
                        input_messages.append(s)
                        total_input_messages += 1
                    except StopIteration:
                        sources_done = True
                    except Exception as e:
                        logger.error(f"Source failed with error: {e}")
                        if die_on_error: 
                            raise e
                        else:
                            continue

                # If we've processed every message we can, empty the aggregators.
                if not input_messages and not aggregators_emptied:
                    progress.update(message_queue_bar, description="[red]Aggregated Messages Queue")

                    # By default (i.e emit_partial == false) we just throw half filled message buckets away
                    if emit_partial:
                        logger.warning("Initial messages consumed, emptying aggregators.")
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
                matches = msg.find_matches(globals.actions_by_id)

                for action in matches:                        
                    try:
                        t0 = time()
                        input_messages.extend(action.process(msg))
                        log_message_transmission(logger, msg, action, time() - t0)
                    except Exception as e:
                        logger.error(f"Failed to process {msg} with {action} with exception {e}")
                        if die_on_error:
                            raise e
                if not matches:
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
