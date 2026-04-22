import logging
import os

from colorama import Fore, Style, init


class Logger:
    """Singleton logger with colored console output and file handler."""

    _instance = None

    LOG_LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
        "SUCCESS": logging.INFO,
    }

    def __new__(cls, log_file="igem.log", log_level="INFO"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(log_file, log_level)
        return cls._instance

    def _initialize(self, log_file, log_level):
        if getattr(self, "_configured", False):
            return

        init(autoreset=True)

        self.logger = logging.getLogger("IgemLogger")
        self.logger.setLevel(self.LOG_LEVELS.get(log_level.upper(), logging.INFO))

        log_path = os.path.abspath(log_file)
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.ColoredFormatter())

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self._configured = True

    def log(self, message, level="INFO"):
        log_level = self.LOG_LEVELS.get(level.upper(), logging.INFO)
        self.logger.log(log_level, message)

    def set_log_level(self, log_level):
        level = self.LOG_LEVELS.get(log_level.upper(), logging.INFO)
        self.logger.setLevel(level)

    class ColoredFormatter(logging.Formatter):
        COLORS = {
            logging.DEBUG: Fore.CYAN,
            logging.INFO: Fore.GREEN,
            logging.WARNING: Fore.YELLOW,
            logging.ERROR: Fore.RED,
            logging.CRITICAL: Fore.RED + Style.BRIGHT,
        }

        def format(self, record):
            color = self.COLORS.get(record.levelno, Fore.WHITE)
            return f"{color}[{record.levelname}] {record.msg}{Style.RESET_ALL}"
