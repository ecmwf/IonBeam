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
from typing import Any
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import dataclasses

from concurrent.futures import ProcessPoolExecutor, CancelledError
from multiprocessing import Manager, Pipe
from multiprocessing.connection import PipeConnection
from queue import Empty, Queue

from .config_parser import Source
from ..core.bases import Processor, Message


# prime the pump by setting all the sources up
# and pumping them into the first queue
def pump(
    producer: Source | Processor,
    queue: Queue[Any] | None,
    message: None | Message = None,
):
    """
    producer: any of Source, Preprocessor, Encoder
    queue: the Queue to put the output into
    message: optional, the input data
    """
    if message is not None:
        assert isinstance(producer, Processor)
        stream = producer.process(message)
    else:
        assert isinstance(producer, Source)
        stream = producer.generate()

    for item in stream:
        if queue is not None:
            queue.put(item, block=True)


@dataclasses.dataclass
class ConsumerWorker:
    consumer: Processor
    output_queue: Queue[Any] | None
    send_pipe: PipeConnection | None = None
    receive_pipe: PipeConnection | None = None

    def __post_init__(self):
        self.receive_pipe, self.send_pipe = Pipe()

    def runner(self):
        assert isinstance(self.receive_pipe, PipeConnection)
        while True:
            incoming_message = self.receive_pipe.get(block=True)
            for outgoing_message in self.consumer.process(incoming_message):
                if self.output_queue is not None:
                    self.output_queue.put(outgoing_message, block=True)


@dataclasses.dataclass
class SourceWorker:
    source: Source
    output_queue: Queue

    def runner(self):
        for item in self.source.generate():
            self.output_queue.put(item, block=True)


def mulitprocess_pipeline(pipeline, parallel_workers=None, queue_size=10):
    # Manager will keep a lock around the queues so we can access them safely from both the main and worker processes
    logger = logging.getLogger()

    max_workers = sum(len(p) for p in pipeline)
    logger.info(f"Starting parallel execution with {max_workers} workers...")

    with Manager() as manager, ProcessPoolExecutor(max_workers) as executor, logging_redirect_tqdm() as _:
        # Between each stage there is a single managed output queue
        # on which Sources and Consumers put their output
        queues = [manager.Queue(maxsize=queue_size) for _ in pipeline[:-1]]

        # The main process is responsible for getting messages off output queues
        # and sending them to the input pipes of the appropriate workers

        # set to True if we err out
        killed = False

        # Start a task for every part of the pipeline
        pipeline_workers = [[] for stage in pipeline]

        pipeline_workers[0] = [SourceWorker(src, queues[0]) for src in pipeline[0]]

        pipeline_workers[1:-1] = [
            [ConsumerWorker(consumer, queue) for consumer in stage] for stage, queue in zip(pipeline[1:-1], queues[1:])
        ]

        pipeline_workers[-1] = [ConsumerWorker(consumer, None) for consumer in pipeline[-1]]

        pipeline_futures = [[executor.submit(worker.runner) for worker in stage] for stage in pipeline_workers]

        # we're done when all the sources are done and all the queues have emptied.
        def done():
            if killed:
                return True  # i.e we exit even if the queues are still full
            tasks_done = all(s.done() for stage in pipeline_futures for s in stage)
            queues_empty = all(q.empty() for q in queues)
            return tasks_done and queues_empty

        names = ["Sources", "Preprocesors", "Aggregators"]
        assert len(names) == len(pipeline) - 1
        print("Output Queue Size")
        progress_bars = [
            tqdm(total=queue_size, colour="green", desc=f"{name:<15}", bar_format="{desc}: {n}/{total} |{bar}{r_bar}")
            for name in names
        ]

        # Now that the first queue is filling up in the background
        # we can go through each queue in turn, pull one message and
        # pass that to every consumer that matches with it
        # for every match we get we'll submit a new task to the task executor
        while not done():
            # print([q.empty() for q in queues])
            for stage, queue, progress_bar in zip(pipeline_workers[1:], queues, progress_bars):
                # print(q, consumers, next_queue)
                # print(f"worker done: {src_workers[0].done()}, q empty: {q.empty()}")
                try:
                    message = queue.get(block=False)
                    progress_bar.update(queue.qsize() - progress_bar.n)
                    # logger.debug(f"Pulled {message} from a queue.")

                    atleast_one_match = False
                    for consumerworker in stage:
                        if consumerworker.consumer.matches(message):
                            atleast_one_match = True
                            consumerworker.send_pipe.send(message)

                    if not atleast_one_match:
                        logger.warning(f"Message {message} did not match with any pipelines!")
                        logger.warning("Shutting down...")
                        executor.shutdown(wait=True, cancel_futures=True)
                        killed = True

                    queue.task_done()

                except Empty:
                    # logger.debug(f"Queue for {type(consumers[0])} was empty")
                    continue

            for stage in pipeline_futures:
                for future in stage:
                    try:
                        e = future.exception(timeout=0)
                        if e is not None:
                            logger.warning(f"Task died: {future}, exception: {e}")
                            logger.warning("Shutting down...")
                            executor.shutdown(wait=True, cancel_futures=True)
                            killed = True
                    except CancelledError:
                        logger.warning(f"Future {future} cancelled before exception could be read")
                    except TimeoutError:
                        pass

        for bar in progress_bars:
            bar.close()
        logger.info("Execution exited gracefully!")
