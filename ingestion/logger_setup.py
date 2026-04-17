"""
Logging configuration for the CommercePulse ingestion pipeline.

Sets up both a console handler and a rotating file handler.
Import and call `setup_logging()` once at the entry point.
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path


def setup_logging(
    log_level: str = "INFO",
    log_dir: str | Path = "logs",
    log_file: str = "ingestion.log",
) -> None:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-35s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    # Rotating file handler — keeps last 5 × 10 MB files
    file_handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    logging.getLogger("duckdb").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
