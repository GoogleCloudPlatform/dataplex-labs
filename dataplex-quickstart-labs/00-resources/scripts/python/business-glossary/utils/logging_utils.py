"""Custom logger for the Business Glossary import tool.
"""
import logging
import sys
from datetime import datetime

# Global instance of logger, instantiated only once. The instance is accessed by
# using the public method get_logger()
_logger = None

class ConsoleLogFilter(logging.Filter):
    """
    This filter is for the console. It blocks any message
    that is DEBUG level.
    """
    def filter(self, record):
        return record.levelno > logging.DEBUG

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

  # 2. Create the default console handler (shows INFO and up)
  stdout_handler = logging.StreamHandler(sys.stdout)
  stdout_handler.set_name("default_console") # Give it a name to find it later
  stdout_handler.setLevel(logging.INFO) # Only process INFO and higher
  stdout_handler.setFormatter(_LogFormatter())

  # 3. Create the error handler for the console
  stderr_handler = logging.StreamHandler(sys.stderr)
  stderr_handler.setLevel(logging.ERROR)
  stderr_handler.setFormatter(_LogFormatter())

  _logger.addHandler(stdout_handler)
  _logger.addHandler(stderr_handler)

  return _logger

def setup_file_logging():
    """
    Reconfigures the logging system for debugging mode.
    - Adds a file handler to log all DEBUG messages.
    - Adds a filter to the console handler to hide DEBUG messages.
    """
    logger = get_logger()

    # This handler will write all DEBUG and higher messages to the file.
    log_filename = f"logs_{datetime.now().strftime('%Y-%b-%d_%I-%M%p')}.txt"
    file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    # This is a fallback in case the level on the handler was not enough
    for handler in logger.handlers:
        if handler.get_name() == "default_console":
            handler.addFilter(ConsoleLogFilter())
            break
    
    logger.info(f"Debug logging to {log_filename} is enabled.")

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