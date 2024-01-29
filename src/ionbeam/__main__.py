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
import argparse
from pathlib import Path
import sys

import logging
from rich.logging import RichHandler
import dataclasses
import yaml

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

    parser = argparse.ArgumentParser(
        prog="ECMWF IOT Observation Processor",
        description="Put IOT data into the FDB",
        epilog="See https://github.com/ecmwf-projects for more info.",
    )
    parser.add_argument(
        "config_folder",
        help="Path to the config folder.",
        type=Path,
    )

    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Just parse the config and do nothing else.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Run in offline mode.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If specified then overwrite data even if it already exists in the database.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Set the logging level, default is warnings only, -v and -vv give increasing verbosity",
    )
    parser.add_argument(
        "--emit-partial",
        action="store_true",
        help="If set, tells the time aggregators to emit messages containing partial information when the program is terminated. By default these are thrown away.",
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
    parser.add_argument(
        "--logfile",
        type=Path,
        help="The path to a file to send the logs to.",
    )

    args = parser.parse_args()

    handlers = [RichHandler(markup=True, rich_tracebacks=True)]
    if args.logfile:
        handlers.append(logging.FileHandler(args.logfile))
        

    # Set the log level, default is warnings, -v gives info, -vv for debug
    logging.basicConfig(
        level=[logging.WARNING, logging.INFO, logging.DEBUG][min(2, args.verbose)],
        format="%(message)s",
        datefmt="[%X]",
        handlers=handlers,
    )
    logger = logging.getLogger("CMDLINE")

    from .core.config_parser import parse_config
    from .core.bases import Source, Aggregator

    globals, actions = parse_config(args.config_folder, offline=args.offline, overwrite=args.overwrite)

    sources, downstream_actions = [], []
    for action in actions:
        if isinstance(action, Source):
            sources.append(action)
        else:
            downstream_actions.append(action)

    logger.info(f"Globals:")
    logger.info(f"    Data Path: {globals.globals.data_path}")
    logger.info(f"    Config Path: {globals.globals.config_path}")
    logger.info(f"    Data Path: {globals.globals.data_path}")
    logger.info(f"    Offline: {globals.globals.offline}")
    logger.info(f"    Overwrite: {globals.globals.overwrite}")
    logger.info(f"    Ingestion Time Constants:")
    logger.info(f"         Query Timespan: {tuple(d.isoformat() for d in globals.globals.ingestion_time_constants.query_timespan)}")
    logger.info(f"         Emit After (Hours): {globals.globals.ingestion_time_constants.emit_after_hours}")
    logger.info(f"         Granularity: {globals.globals.ingestion_time_constants.granularity}")


    logger.info("Sources")
    for i, a in enumerate(sources):
        logger.info(f"    {i} {str(a)}")

    if args.validate_config:
        sys.exit()

    if "finish_after" in args:
        logger.warning(f"Telling all sources to finish after emitting {args.finish_after} messages")
        for source in sources:
            source.finish_after = args.finish_after

    from .core.singleprocess_pipeline import singleprocess_pipeline

    try:
        singleprocess_pipeline(sources, downstream_actions, emit_partial=args.emit_partial)
    except Exception:
        logger.exception("Exception during run:")
