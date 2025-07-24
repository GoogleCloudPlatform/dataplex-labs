"""Custom logger for the Business Glossary import tool.
"""
import logging
import sys


# Global instance of logger, instantiated only once. The instance is accessed by
# using the public method get_logger()
_logger = None


def get_logger(level: ... = logging.INFO) -> logging.Logger:
  """Returns an instance of a logger.

  DEBUG, INFO and WARNING messages are output to stdout.
  ERROR messages are output to stderr.
  Args:
    level: Logging level.

  Returns:
    logging.Logger.
  """
  global _logger
  if _logger is not None:
    return _logger

  _logger = logging.getLogger()

  stdout_handler = logging.StreamHandler(sys.stdout)
  stderr_handler = logging.StreamHandler(sys.stderr)

  stdout_handler.addFilter(_LogFilter(logging.WARNING))

  stdout_handler.setLevel(logging.DEBUG)
  stderr_handler.setLevel(logging.ERROR)

  stdout_handler.setFormatter(_LogFormatter())
  stderr_handler.setFormatter(_LogFormatter())

  _logger.addHandler(stdout_handler)
  _logger.addHandler(stderr_handler)

  _logger.setLevel(level)

  return _logger


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


class _LogFilter(logging.Filter):
  """Filter used to have stdout not handle ERROR messages."""

  def __init__(self, level):
    super().__init__()
    self.level = level

  def filter(self, record):
    return record.levelno <= self.level
