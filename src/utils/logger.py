"""Logging configuration for the ETL pipeline."""

import sys
from pathlib import Path

from loguru import logger


def setup_logger(log_file: str = "logs/pipeline.log", level: str = "INFO"):
    """
    Configure structured logging with both console and file output.

    Args:
        log_file: Path to the log file
        level: Logging level (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured logger instance
    """
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger.remove()

    # Console handler with color
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=level,
        colorize=True,
    )

    # File handler with rotation
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation="10 MB",
        retention="30 days",
        level=level,
    )

    logger.info(f"Logger initialized - Level: {level}, File: {log_file}")
    return logger
