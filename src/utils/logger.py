"""
Structured logging configuration using loguru.

Provides JSON-formatted logs for production and human-readable logs for development.
Log output goes to both console and rotating file.
"""

import sys

from loguru import logger

from src.config.settings import get_settings


def setup_logging() -> None:
    """Configure loguru with structured logging sinks."""
    settings = get_settings()

    # Remove default handler
    logger.remove()

    # ── Console sink (human-readable) ─────────────────────────────────
    logger.add(
        sys.stderr,
        level=settings.LOG_LEVEL,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # ── File sink (JSON for log aggregation) ──────────────────────────
    logger.add(
        settings.LOG_FILE,
        level=settings.LOG_LEVEL,
        format="{message}",
        rotation=settings.LOG_ROTATION,
        retention=settings.LOG_RETENTION,
        serialize=True,  # JSON output
        enqueue=True,  # thread-safe
    )

    logger.info(
        "Logging initialized",
        log_level=settings.LOG_LEVEL,
        log_file=settings.LOG_FILE,
    )


def get_logger(name: str = __name__) -> logger.__class__:
    """Return a logger instance bound with the given module name."""
    return logger.bind(module=name)
