"""Centralized retry utilities with exponential backoff.

This module provides a generic retry mechanism that can be used across
all API calls (Dataplex, Sheets, GCS, etc.) with customizable retry predicates.
"""

import random
import time
from typing import Callable, Optional, Set, Type, TypeVar

import httplib2
from google.api_core import exceptions as gcs_exceptions
from googleapiclient.errors import HttpError

from utils import logging_utils
from utils.constants import (
    INITIAL_BACKOFF_SECONDS,
    MAX_BACKOFF_SECONDS,
    MAX_RETRY_DURATION_SECONDS,
)

logger = logging_utils.get_logger()

T = TypeVar('T')

# HTTP status codes indicating transient/retryable errors
RETRYABLE_HTTP_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# OSError covers the full network exception hierarchy:
# TimeoutError, ConnectionError, BrokenPipeError
NETWORK_EXCEPTION_TYPES: tuple = (
    OSError,
    httplib2.ServerNotFoundError,
    httplib2.RelativeURIError,
)

def is_network_error(error: Exception) -> bool:
    """Check if error is a network/connectivity issue based on exception type."""
    return isinstance(error, NETWORK_EXCEPTION_TYPES)


def is_retryable_http_error(error: Exception) -> bool:
    """Check if an HTTP error has a retryable status code (5xx or 429)."""
    if isinstance(error, HttpError):
        status = getattr(error.resp, 'status', 0) if hasattr(error, 'resp') else 0
        return status in RETRYABLE_HTTP_STATUS_CODES
    return False


def _get_error_status_code(error: Exception) -> int:
    """Extract HTTP status code from various Google API exception types."""
    # google.api_core exceptions (GCS, etc.)
    if isinstance(error, gcs_exceptions.GoogleAPICallError):
        return error.code
    # googleapiclient HttpError
    if isinstance(error, HttpError):
        return getattr(error.resp, 'status', 0) if hasattr(error, 'resp') else 0
    return 0


def is_retryable_google_api_error(error: Exception) -> bool:
    """Check if a Google API error is retryable (network errors or retryable status codes)."""
    if is_network_error(error):
        return True
    return _get_error_status_code(error) in RETRYABLE_HTTP_STATUS_CODES


def is_retryable_gcs_error(error: Exception) -> bool:
    """Check if a GCS error is transient and should be retried."""
    if is_network_error(error):
        return True
    return _get_error_status_code(error) in RETRYABLE_HTTP_STATUS_CODES


# -----------------------------------------------------------------------------
# Formatting Helpers
# -----------------------------------------------------------------------------

def format_duration(seconds: float) -> str:
    """Format seconds into human-readable string (e.g., '30s' or '2.5m')."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{seconds / 60:.1f}m"


def format_error_message(error: Exception) -> str:
    """Format an exception into a concise error message."""
    error_str = str(error)
    
    # Common network error patterns
    if "unable to find the server" in error_str.lower():
        return "Unable to reach server (DNS resolution failed)"
    if "connection refused" in error_str.lower():
        return "Connection refused by server"
    if "timed out" in error_str.lower():
        return "Connection timed out"
    if "name resolution" in error_str.lower():
        return "DNS lookup failed"
    
    # Truncate long messages
    return error_str[:200] if len(error_str) > 200 else error_str


# -----------------------------------------------------------------------------
# Core Retry Function
# -----------------------------------------------------------------------------

def execute_with_retry(
    operation: Callable[[], T],
    operation_name: str = "operation",
    is_retryable: Callable[[Exception], bool] = is_retryable_google_api_error,
    max_duration_seconds: float = MAX_RETRY_DURATION_SECONDS,
    initial_backoff: float = INITIAL_BACKOFF_SECONDS,
    max_backoff: float = MAX_BACKOFF_SECONDS,
) -> T:
    """Execute an operation with exponential backoff retry for transient errors.
    
    Args:
        operation: Callable that performs the operation. Should take no arguments.
        operation_name: Human-readable name for logging purposes.
        is_retryable: Predicate function that returns True if the error should be retried.
        max_duration_seconds: Maximum total time to spend retrying.
        initial_backoff: Initial backoff delay in seconds.
        max_backoff: Maximum backoff delay in seconds.
    
    Returns:
        The result of the operation.
    
    Raises:
        Exception: The last exception if all retries are exhausted or error is not retryable.
    """
    start_time = time.time()
    backoff = initial_backoff
    attempt = 0
    
    while True:
        attempt += 1
        elapsed = time.time() - start_time
        remaining = max_duration_seconds - elapsed
        
        try:
            return operation()
        except Exception as error:
            elapsed = time.time() - start_time
            remaining = max_duration_seconds - elapsed
            
            # Check if we should retry
            if not is_retryable(error) or remaining <= 0:
                if remaining <= 0 and is_retryable(error):
                    logger.error(
                        "Network connectivity issue persists after retrying for %s. "
                        "Please check your internet connection.",
                        format_duration(elapsed)
                    )
                raise
            
            # Log and retry with backoff
            logger.warning(
                "%s failed (attempt %d): %s. Retrying in %s... (will retry for up to %s more)",
                operation_name,
                attempt,
                format_error_message(error),
                format_duration(backoff),
                format_duration(remaining)
            )
            
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, max_backoff)
