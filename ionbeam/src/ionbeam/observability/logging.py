import logging
import logging.handlers
from pathlib import Path

import structlog
from structlog import dev as structlog_dev
from structlog.contextvars import merge_contextvars
from structlog.processors import TimeStamper
from structlog.stdlib import LoggerFactory, ProcessorFormatter


def setup_logging(
    level: int = logging.DEBUG,
    log_dir: Path = Path("./logs"),
    log_name: str = "ionbeam.log",
) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamper = TimeStamper(fmt="iso", utc=True)
    foreign_pre_chain = [
        merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.ExtraAdder(),
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(
        ProcessorFormatter(
            processors=[
                ProcessorFormatter.remove_processors_meta,
                structlog_dev.ConsoleRenderer(colors=True),
            ],
            foreign_pre_chain=foreign_pre_chain,
        )
    )

    file_handler = logging.handlers.WatchedFileHandler(str(log_dir / log_name))
    file_handler.setLevel(level)
    file_handler.setFormatter(
        ProcessorFormatter(
            processors=[
                ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
            foreign_pre_chain=foreign_pre_chain,
        )
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(console)
    root.addHandler(file_handler)

    structlog.configure(
        processors=[
            merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.PositionalArgumentsFormatter(),
            timestamper,
            structlog.processors.StackInfoRenderer(),
            ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
