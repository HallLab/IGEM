from __future__ import annotations

import logging
import sys

try:
    from colorama import Fore, Style, init as _colorama_init
    _colorama_init(autoreset=True)
    _COLORS = True
except Exception:
    _COLORS = False
    Fore = Style = None  # type: ignore[assignment]


class Logger:
    """
    Console logger for the IGEM client.

    Mirrors the API of the backend logger (``log``, ``footer``,
    ``set_log_level``) so component code is portable, but writes only
    to stderr — pip-installed clients should not litter the user's
    working directory with log files.
    """

    _instance: "Logger | None" = None

    LOG_LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
        "SUCCESS": logging.INFO,
    }

    def __new__(cls, log_level: str = "INFO") -> "Logger":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(log_level)
        return cls._instance

    def _initialize(self, log_level: str) -> None:
        if getattr(self, "_configured", False):
            return

        self._raw = logging.getLogger("IgemClient")
        self._raw.setLevel(self.LOG_LEVELS.get(log_level.upper(), logging.INFO))
        self._raw.propagate = False

        if not self._raw.handlers:
            handler = logging.StreamHandler(stream=sys.stderr)
            handler.setFormatter(self._ColoredFormatter())
            self._raw.addHandler(handler)

        self._configured = True

    @property
    def logger(self) -> logging.Logger:
        return self._raw

    def log(self, message: str, level: str = "INFO") -> None:
        self._raw.log(self.LOG_LEVELS.get(level.upper(), logging.INFO), message)

    def footer(self, message: str, level: str = "SUCCESS") -> None:
        self.log(message, level)
        self.log("=" * 44, "INFO")

    def set_log_level(self, log_level: str) -> None:
        self._raw.setLevel(self.LOG_LEVELS.get(log_level.upper(), logging.INFO))

    class _ColoredFormatter(logging.Formatter):
        if _COLORS:
            COLORS = {
                logging.DEBUG: Fore.CYAN,
                logging.INFO: Fore.GREEN,
                logging.WARNING: Fore.YELLOW,
                logging.ERROR: Fore.RED,
                logging.CRITICAL: Fore.RED + Style.BRIGHT,
            }
        else:
            COLORS = {}

        def format(self, record: logging.LogRecord) -> str:
            if _COLORS:
                color = self.COLORS.get(record.levelno, Fore.WHITE)
                return f"{color}[{record.levelname}] {record.msg}{Style.RESET_ALL}"
            return f"[{record.levelname}] {record.msg}"
