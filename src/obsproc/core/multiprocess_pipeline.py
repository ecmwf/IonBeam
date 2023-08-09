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
from tqdm.contrib.logging import logging_redirect_tqdm

import dataclasses

from concurrent.futures import ProcessPoolExecutor, CancelledError
from multiprocessing import Manager
from queue import Empty, Queue

from .config_parser import Source
from ..core.bases import Processor, FinishMessage

logger = logging.getLogger()


@dataclasses.dataclass
class ConsumerWorker:
    consumer: Processor
    input_queue: Queue[Any]
    output_queue: Queue[Any] | None

    def runner(self):
        logger.debug(f"{self.consumer} started")
        while True:
            incoming_message = self.input_queue.get(block=True)

            if isinstance(incoming_message, FinishMessage):
                logger.debug(f"{self.consumer} got finish signal")
                break

            try:
                for outgoing_message in self.consumer.process(incoming_message):
                    if self.output_queue is not None:
                        self.output_queue.put(outgoing_message, block=True)
            except Exception as e:
                logger.debug(f"{self.consumer} died with {e}")
                return


@dataclasses.dataclass
class SourceWorker:
    source: Source
    output_queue: Queue

    def runner(self):
        logger.debug(f"{self.source} started")
        try:
            for item in self.source.generate():
                self.output_queue.put(item, block=True)
        except Exception as e:
            logger.debug(f"{self.source} died with {e}")
            return

        logger.debug(f"{self.source} finished")


def mulitprocess_pipeline(config, pipeline, queue_size=100):
    # Manager will keep a lock around the queues so we can access them safely from both the main and worker processes

    logger.info("Starting parallel execution...")

    with Manager() as manager, ProcessPoolExecutor() as executor, logging_redirect_tqdm() as _:
        # This holds the output of all the workers
        message_queue = manager.Queue(maxsize=queue_size)

        # The main process is responsible for getting messages from the message queue and putting
        # them into the input queues of the appropriate workers

        # set to True if we err out
        killed = False

        # Start a task for every part of the pipeline
        workers: list[SourceWorker | ConsumerWorker] = [SourceWorker(src, message_queue) for src in pipeline[0]]

        workers.extend(
            [
                ConsumerWorker(consumer, input_queue=manager.Queue(), output_queue=message_queue)
                for stage in pipeline[1:]
                for consumer in stage
            ]
        )

        futures = [executor.submit(worker.runner) for worker in workers]

        # we're done when all the sources are done and all the queues have emptied.
        def done():
            if killed:
                return True  # i.e we exit even if the queues are still full
            return all(f.done() for f in futures)

        while not done():
            try:
                message = message_queue.get(block=False)
                logger.debug(f"Pulled {message} from a queue.")

                atleast_one_match = False
                for worker in workers:
                    if isinstance(worker, ConsumerWorker):
                        if worker.consumer.matches(message):
                            atleast_one_match = True
                            worker.input_queue.put(message)

                if not atleast_one_match:
                    logger.warning(f"Message {message} did not match with any pipelines!")
                    logger.warning("Shutting down...")
                    executor.shutdown(wait=True, cancel_futures=True)
                    killed = True

                message_queue.task_done()

            except Empty:
                continue

            for future in futures:
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

        logger.info("Execution exited gracefully!")
