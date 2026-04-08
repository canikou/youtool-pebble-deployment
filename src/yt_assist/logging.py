"""Application logging setup."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler
from pathlib import Path
from queue import Queue
from time import time

from yt_assist.config import AppConfig

LOG_FORMAT = "%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s"
SECONDS_PER_DAY = 24 * 60 * 60


@dataclass(slots=True)
class LoggingGuards:
    listener: QueueListener

    def stop(self) -> None:
        self.listener.stop()


def init_logging(config: AppConfig) -> LoggingGuards:
    config.storage.log_dir.mkdir(parents=True, exist_ok=True)

    queue: Queue[logging.LogRecord] = Queue(-1)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(_parse_level(config.logging.level))
    root.addHandler(QueueHandler(queue))

    handlers: list[logging.Handler] = [_build_console_handler(), _build_file_handler(config)]
    if _should_enable_debug_file(config.logging.level):
        handlers.append(_build_debug_file_handler(config))

    listener = QueueListener(queue, *handlers, respect_handler_level=True)
    listener.start()
    return LoggingGuards(listener=listener)


def cleanup_old_log_files(log_dir: Path, retention_days: int) -> int:
    if retention_days <= 0 or not log_dir.exists():
        return 0

    cutoff = time() - (retention_days * SECONDS_PER_DAY)
    removed = 0
    for entry in log_dir.iterdir():
        if not entry.is_file():
            continue
        if not (
            entry.name.startswith("yt-assist.log.")
            or entry.name.startswith("yt-assist-debug.log.")
            or entry.name.startswith("yt-assist.log.")
        ):
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                entry.unlink()
                removed += 1
        except FileNotFoundError:
            continue
    return removed


def _build_console_handler() -> logging.Handler:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler


def _build_file_handler(config: AppConfig) -> logging.Handler:
    handler = TimedRotatingFileHandler(
        config.storage.log_dir / "yt-assist.log",
        when="midnight",
        backupCount=14,
        encoding="utf-8",
    )
    handler.setLevel(_parse_level(config.logging.level))
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler


def _build_debug_file_handler(config: AppConfig) -> logging.Handler:
    handler = TimedRotatingFileHandler(
        config.storage.log_dir / "yt-assist-debug.log",
        when="midnight",
        backupCount=14,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler


def _should_enable_debug_file(level: str) -> bool:
    return level.strip().lower() in {"debug", "trace"} or bool(os.getenv("YT_ASSIST_DEBUG_LOG"))


def _parse_level(level: str) -> int:
    normalized = level.strip().upper()
    if normalized == "TRACE":
        return logging.DEBUG
    return getattr(logging, normalized, logging.INFO)
