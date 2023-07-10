#!/usr/bin/env python3

# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

# Note: I've put some imports after the argparse code to make the cmdline usage feel snappier
import logging
import argparse
from pathlib import Path
import sys

if __name__ == "__main__":
    # Stages:
    # 1. Raw data from sources (--> location && label)
    # 2. Parsed raw data (into Pandas Dataframes)
    # 3. Annotated raw data --> expand based on decoded values and on ID
    #      --> Self-described messages!!!!
    # 4. Regroup the data
    # 5. Encode output data

    # n.b. Design these such that they can be driven from a configuration database (i.e.
    #      the pre-processing configuration can change dynamically.
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(
        prog="ECMWF IOT Observation Processor",
        description="Put IOT data into the FDB",
        epilog="See https://github.com/ecmwf-projects for more info.",
    )
    parser.add_argument(
        "config_file",
        default="examples/example.yaml",
        help="Path to a yaml config file.",
        type=Path,
    )

    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Just parse the config and do nothing else.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Set the logging level, default is warnings only, -v and -vv give increasing verbosity",
    )

    parser.add_argument(
        "-j",
        "--parallel",
        dest="parallel_workers",
        metavar="NUMBER",
        type=int,
        nargs="?",
        default=argparse.SUPPRESS,
        const=None,
        help="Engage parallel mode. Optionally specify how many parallel workers to use, \
        defaults to the number of CPUs + the number of srcs in the pipeline",
    )

    parser.add_argument(
        "--finish-after",
        metavar="NUMBER",
        type=int,
        nargs="?",
        default=argparse.SUPPRESS,
        const=1,
        help="If present, limit the number of processed messages to 1 or the given integer",
    )

    args = parser.parse_args()

    # Set the log level, default is warnings, -v gives info, -vv for debug
    logging.basicConfig(
        level=[logging.WARNING, logging.INFO, logging.DEBUG][min(2, args.verbose)],
    )

    from obsproc.core.config_parser import parse_config

    config = parse_config(args.config_file)

    # The pipeline has sources, a bunch of processors and then a set of sinks
    # between each there is a queue holding messages ready to pass to the next stage
    # the main task iterates through each queue and 'pumps' the pipeline
    # pipeline = [config.sources, config.preprocessors, config.aggregators, config.encoders]
    pipeline = [getattr(config, name) for name in config.pipeline]
    for name, stage in zip(config.pipeline, pipeline):
        logger.info(f" {name.capitalize()}: " + " ".join(s.__class__.__name__ for s in stage))

    if args.validate_config:
        sys.exit()

    # If --finish-after N is set then tell all the sources to finish after N messages
    if "finish_after" in args:
        logger.warning(f"Telling all sources to finish after emitting {args.finish_after} messages")
        for source in pipeline[0]:
            source.finish_after = args.finish_after

    if "parallel_workers" not in args:
        from obsproc.core.singleprocess_pipeline import singleprocess_pipeline

        singleprocess_pipeline(config, pipeline)
    else:
        from obsproc.core.multiprocess_pipeline import mulitprocess_pipeline

        mulitprocess_pipeline(config, pipeline, parallel_workers=args.parallel_workers)
