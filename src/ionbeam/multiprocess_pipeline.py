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
import multiprocessing
import sys
from itertools import chain
from pathlib import Path
from queue import Empty, Queue
from time import time
from typing import Iterable

from .core.bases import FinishMessage, Message, Source
from .core.config_parser import Config, load_action_from_paths

logger = logging.getLogger(__name__)


def source_worker(config_path : Path, source_path: Path, source_class : str, out_queue : Queue):
    logger = logging.getLogger(__name__)
    config, source = load_action_from_paths(config_path, source_path, source_class)

    try:
        for msg in source.generate():
            out_queue.put(msg)
    except KeyboardInterrupt:
        pass


def multi_process_sources(config: Config, sources: list[Source]) -> Iterable[Message]:
    """
    Launch each source in its own subprocess, read from them in a non-blocking
    manner, and yield messages as they arrive.
    """
    processes = []
    queues = []

    # 1. Create a process & queue for each source
    for src in sources:
        q = multiprocessing.Queue()
        args = (config.globals.source_path, src.definition_path, src.__class__.__name__, q)
        p = multiprocessing.Process(target=source_worker, args=args)
        p.start()
        processes.append(p)
        queues.append(q)

    # 2. Poll the queues in a loop
    while queues:
        finished_queues = []
        for q in queues:
            # Attempt to get everything currently in the queue (non-blocking)
            while True:
                try:
                    msg = q.get(timeout = 0.01)
                except Empty:
                    break

                # If we see a FinishMessage, that means one source is done
                if isinstance(msg, FinishMessage):
                    logger.info(f"Source {msg} exhausted.")
                    finished_queues.append(q)
                    break
                else:
                    # Yield normal messages
                    yield msg

        for fq in finished_queues:
            if fq in queues:
                queues.remove(fq)

    # 3. Clean up processes
    for p in processes:
        p.join()


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


def multiprocess_pipeline(config, sources, actions, emit_partial: bool, 
                           simple_output : bool = False, 
                           die_on_error: bool = False) -> list[Message]:
    logger.info("Starting multiprocess execution...")

    source = multi_process_sources(config, sources)
    source = chain(source, [FinishMessage("Sources Exhausted")])

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
                        logger.warning(f"Failed to process {msg} with {action} with exception {e}")
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
