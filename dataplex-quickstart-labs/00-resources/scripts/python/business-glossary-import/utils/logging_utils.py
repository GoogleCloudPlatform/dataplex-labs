"""Custom logger for the Business Glossary import tool.
"""
import logging
import sys
import os
from datetime import datetime
from constants import LOGS_DIRECTORY

# Global instance of logger, instantiated only once. The instance is accessed by
# using the public method get_logger()
_logger = None

class ConsoleLogFilter(logging.Filter):
    """
    This filter is for the console. It allows only INFO and WARNING levels through
    to the stdout console handler. DEBUG is blocked and ERROR+ will be routed to stderr.
    """
    def filter(self, record):
        # Allow only INFO and WARNING to stdout handler.
        return record.levelno >= logging.INFO and record.levelno < logging.ERROR

def get_logger() -> logging.Logger:
    """
    Returns a singleton logger instance.
    The logger is ALWAYS configured at the DEBUG level.
    Handlers control what is actually displayed.
    """
    global _logger
    if _logger is not None:
        return _logger

    # 1. Create the logger and set its level to the lowest possible.
    _logger = logging.getLogger("glossary_tool")
    _logger.setLevel(logging.DEBUG)

    # 2. Create the default console handler (shows INFO and WARNING only)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.set_name("default_console")  # Give it a name to find it later
    stdout_handler.setLevel(logging.INFO)  # Only process INFO and higher
    stdout_handler.setFormatter(_LogFormatter())

    # 3. Create the error handler for the console (ERROR and above)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.set_name("default_err_console")
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(_LogFormatter())

    # Attach handlers
    _logger.addHandler(stdout_handler)
    _logger.addHandler(stderr_handler)

    # Prevent messages from propagating to the root logger (avoid duplicates)
    _logger.propagate = False

    return _logger

def setup_file_logging():
    """
    Reconfigures the logging system for debugging mode.
    - Creates `logs/` directory relative to current working directory if missing.
    - Adds a file handler to log all DEBUG messages. If a file handler already exists
      it will be removed and replaced (so the content is rewritten).
    - Adds a filter to the console handler to hide DEBUG messages and to exclude ERRORs from stdout.
    """
    logger = get_logger()

    # Ensure logs directory exists (relative to current working directory)
    logs_dir = os.path.join(os.getcwd(), LOGS_DIRECTORY)
    os.makedirs(logs_dir, exist_ok=True)

    # Build filename and path
    log_filename = f"logs_{datetime.now().strftime('%b-%d-%Y_%I-%M-%S%p')}.txt"
    log_path = os.path.join(logs_dir, log_filename)

    # Create new file handler (mode='w' will overwrite any existing file with same name)
    file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    # Remove existing FileHandler(s) to avoid duplicates and replace with new one
    # (iterate on a copy to avoid modifying list during iteration)
    existing_handlers = list(logger.handlers)
    for h in existing_handlers:
        if isinstance(h, logging.FileHandler):
            logger.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

    # Attach the new file handler
    logger.addHandler(file_handler)

    # Add the ConsoleLogFilter to the stdout handler so stdout shows only INFO/WARNING.
    for handler in logger.handlers:
        if getattr(handler, "name", None) == "default_console":
            handler.addFilter(ConsoleLogFilter())
            break
    
    logger.info("Debug logging is active. Logs will be saved in %s", log_filename)


class _LogFormatter(logging.Formatter):
    """Format configuration for each logging level."""
    purple = "\x1b[35m"
    green = "\x1b[32m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    reset = "\x1b[0m"
    header = "%(asctime)s : %(levelname)s : "
    debug_header = "%(funcName)s : %(levelname)s : "
    message_format = "%(message)s"

    FORMATS = {
        logging.DEBUG: purple + debug_header + reset + message_format,
        logging.INFO: green + header + reset + message_format,
        logging.WARNING: yellow + header + reset + message_format,
        logging.ERROR: red + header + reset + message_format,
    }

    def format(self, record):
        log_format = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_format, "%H:%M:%S")
        return formatter.format(record)
