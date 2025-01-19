# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import dataclasses
import logging
import pickle
import sys
from copy import deepcopy
from datetime import UTC, datetime
from itertools import cycle, islice
from time import time
from typing import Iterable, Sequence

import dill

from .bases import DataMessage, Globals, Message, MessageStream, Processor

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class SavedError:
    exception: Exception
    globals: dict
    message: DataMessage


def save_message_error(msg : DataMessage, e : Exception, globals : Globals):
    "Save a message to the error queue"
    dt = datetime.now(UTC).isoformat()
    p = globals.data_path / "errors" / f"error_{dt}.pickle"
    if not p.parent.exists():
        p.parent.mkdir(parents=True)
    with p.open("wb") as f:
        globals_dict = {field.name : val for field in dataclasses.fields(Globals)
        if dill.pickles(val := getattr(globals, field.name))}
        
        pickle.dump(SavedError(
            message = msg,
            exception = e, 
            globals = globals_dict
            ), f)

def load_most_recent_error(globals : Globals):
    "Load the most recent error from the error queue"
    files = sorted(globals.data_path.glob("errors/error_*.pickle"))
    if not files:
        return None
    with files[-1].open("rb") as f:
        return pickle.load(f)


def roundrobin(iterables: Sequence[MessageStream], finish_after : int | None = None) -> MessageStream:
    if finish_after is not None:
        iterables = [islice(it, finish_after) for it in iterables]

    pending = len(iterables)
    nexts = cycle(iter(it).__next__ for it in iterables)
    while pending:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            pending -= 1
            nexts = cycle(islice(nexts, pending))

def fully_process_message(msg: DataMessage, globals: Globals) -> tuple[list[Message], int]:
    """
    Take a message and keep working on its children until they're all finished.
    """
    number_processed = 0
    input_messages = [msg]
    finished_messages = []
    while input_messages:
        msg = input_messages.pop()
        matches = msg.find_matches(globals.actions_by_id)

        for i, action in enumerate(matches):
            assert isinstance(action, Processor), f"Expected a Processor Action, got {action}"

            # The first action to process the message gets the original message, the rest get a copy.
            # This speeds up the common case where a message is consumed by only one action.
            if i > 0: 
                msg = deepcopy(msg)         

            try:
                t0 = time()
                output_messages = process_message(msg, action)

                if globals.finish_after is not None:
                    output_messages = islice(output_messages, globals.finish_after)

                input_messages.extend(output_messages)
                log_message_transmission(logger, msg, action, time() - t0)
                number_processed += 1

            except Exception as e:
                logger.warning(f"Failed to process {msg} with {action} with exception {e}")
                save_message_error(msg, e, globals)
                if globals.die_on_error:
                    raise e
        if not matches:
            logger.info(f"{str(msg)} --> Done")
            finished_messages.append(msg)

    return finished_messages, number_processed

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

def process_message(msg: DataMessage, action: Processor) -> Iterable[DataMessage]:
    for out_msg in action.process(msg):
        # Updates the history metadata of the output message
        assert isinstance(out_msg, DataMessage), f"Expected a DataMessage, got {out_msg}"
        action.tag_message(out_msg, msg)
        yield out_msg


def singleprocess_pipeline(config, sources, actions, emit_partial: bool, 
                           simple_output : bool = False, 
                           die_on_error: bool = False) -> list[Message]:
    logger.info("Starting single threaded execution...")
    globals = config.globals

    if globals.finish_after is not None:
        logger.debug(f"Stopping after each action has emitted {globals.finish_after} messages")

    source = roundrobin(sources, globals.finish_after)

    input_messages = []
    finished_messages = []
    sources_done = False

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
                        input_messages.append(next(source))
                        total_input_messages += 1
                    except StopIteration:
                        sources_done = True


                # If we've processed every message, finish.
                if not input_messages:
                    break

                # Pop a message off the queue and process it
                msg = input_messages.pop()

                # Take this message and keep working on the output until they're all fully processed
                # Doing it in this order helps to catch errors in the processing chain early
                finished_messages, number_processed = fully_process_message(msg, globals)
                finished_messages.extend(finished_messages)
                processed_messages += number_processed



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
