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
import logging
import os
import pdb
import shutil
import sys
import traceback
from pathlib import Path

os.environ["ODC_ENABLE_WRITING_LONG_STRING_CODEC"] = "1"

from rich.logging import RichHandler

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
        prog="IonBeam",
        description="Beam IoT data around",
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
        "--overwrite-fdb",
        action="store_true",
        help="If specified then overwrite data even if it already exists in the database.",
    )
    parser.add_argument(
        "--overwrite-cache",
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

    parser.add_argument(
        "--simple-output",
        action="store_true",
        help="If set, turns richely formatted text output off. Use this if using breakpoint() in the code.",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="If set, drops into a debugger on error.",
    )

    parser.add_argument(
        "--init-db",
        action="store_true",
        help="(Re)initialise all the databases.",
    )

    parser.add_argument(
        "--environment",
        "-e",
        help="Which environment to use, local, dev or test",
    )

    parser.add_argument(
    "--sources",
    type=str,
    nargs='+',
    help="Which sources to use, e.g. --sources meteotracker acronet smartcitizenkit"
)

    args = parser.parse_args()

    handlers = []

    if args.simple_output:
        handlers.append(logging.StreamHandler())
        prompt = input
    else:
        from rich.prompt import Prompt
        from rich.traceback import install

        install(show_locals=False)

        handlers.append(RichHandler(markup=True, rich_tracebacks=True))

        # override the input builtin when using fancy output
        prompt = Prompt.ask

    if args.logfile:
        file_handler = logging.FileHandler(args.logfile)
        file_handler.setLevel(logging.WARNING)
        # file_format = logging.Formatter(
        #     "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        #     datefmt="[%Y-%m-%d %H:%M:%S]",
        # )
        # file_handler.setFormatter(file_format)
        handlers.append(file_handler)



    # Set the log level, default is warnings, -v gives info, -vv for debug
    logging.basicConfig(
        level=[logging.WARNING, logging.INFO, logging.DEBUG][min(2, args.verbose)],
        format="%(message)s",
        datefmt="[%X]",
        handlers=handlers,
    )
    logger = logging.getLogger("CMDLINE")

    from .core.bases import Source
    from .core.config_parser.config_parser import parse_config

    config, actions = parse_config(
        args.config_folder,
        offline = args.offline,
        overwrite_fdb = args.overwrite_fdb,
        overwrite_cache = args.overwrite_cache,
        environment = args.environment,
        sources = args.sources,
    )

    sources, downstream_actions = [], []
    for action in actions:
        if isinstance(action, Source):
            sources.append(action)
        else:
            downstream_actions.append(action)

    logger.info("Globals:")
    logger.info(f"    Environment: {config.globals.environment}")
    logger.info(f"    Data Path: {config.globals.data_path}")
    logger.info(f"    Config Path: {config.globals.config_path}")
    logger.info(f"    Data Path: {config.globals.data_path}")
    logger.info(f"    Offline: {config.globals.offline}")
    logger.info(f"    Overwrite FDB: {config.globals.overwrite_fdb}")
    logger.info(f"    Overwrite Cache: {config.globals.overwrite_cache}")

    logger.info("Sources")
    for i, a in enumerate(sources):
        logger.info(f"    {i} {str(a)}")

    if args.validate_config:
        sys.exit()

    if args.init_db:
        logger.warning(f"Wiping the postgres, fdb and cache databases for env = {config.globals.environment}!")
        if (
            config.globals.environment != "local"
            and prompt(
                f"Are you sure you want to wipe the database for env = {config.globals.environment}? y/n: "
            )
            != "y"
        ):
            sys.exit()


        paths = [
            ["Data", config.globals.data_path],
            ["FDB Root", config.globals.fdb_root],
            ["Cache", config.globals.cache_path],
        ]
        for name, path in paths:
            logger.warning(f"Deleting {name} at {path}")
            shutil.rmtree(path, ignore_errors=True)
        
        for name, path in paths:
            logger.warning(f"Recreating {name} at {path}")
            path.mkdir(parents=True, exist_ok=True)


        logger.warning("Wiping Postgres db")
        from ionbeam.metadata.db import init_db
        init_db(config.globals)
        logger.warning("SQL Database wiped and reinitialised.")

    if "finish_after" in args:
        logger.warning(
            f"Telling all sources to finish after emitting {args.finish_after} messages"
        )
        for source in sources:
            source.finish_after = args.finish_after

    from .core.singleprocess_pipeline import singleprocess_pipeline

    try:
        singleprocess_pipeline(
            sources,
            downstream_actions,
            emit_partial=args.emit_partial,
            simple_output=args.simple_output or args.debug,
        )
    except Exception as e:
        if args.debug:
            extype, value, tb = sys.exc_info()
            traceback.print_exc()
            pdb.post_mortem(tb)
        else:
            raise e
