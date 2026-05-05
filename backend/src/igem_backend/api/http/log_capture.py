from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Iterator

from igem_backend.utils.logger import Logger


@contextmanager
def capture_logger_output(logger: Logger) -> Iterator[list[str]]:
    """
    Capture messages emitted through the IGEM logger during a request.

    Filters by thread id so concurrent requests do not contaminate each
    other's captured output when FastAPI runs sync handlers in a
    threadpool. Multiline `log.msg` values are split into individual
    entries so the separator lines emitted by `logger.footer` are
    preserved.
    """
    captured: list[str] = []
    my_thread = threading.get_ident()

    class _CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if record.thread != my_thread:
                return
            try:
                msg = record.getMessage()
            except Exception:
                return
            prefix = f"[{record.levelname}] "
            for line in msg.splitlines() or [""]:
                captured.append(prefix + line)

    handler = _CaptureHandler()
    handler.setLevel(logging.DEBUG)
    logger.logger.addHandler(handler)
    try:
        yield captured
    finally:
        logger.logger.removeHandler(handler)
