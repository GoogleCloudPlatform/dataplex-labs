"""Centralized retry utilities with exponential backoff.

This module provides a generic retry mechanism that can be used across
all API calls (Dataplex, Sheets, GCS, etc.) with customizable retry predicates.
"""

import random
import socket
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

# -----------------------------------------------------------------------------
# Network/Transient Error Indicators
# -----------------------------------------------------------------------------

NETWORK_ERROR_INDICATORS = frozenset([
    "unable to find the server",
    "network is unreachable",
    "connection refused",
    "connection reset",
    "timed out",
    "name resolution",
    "no route to host",
    "temporary failure",
    "service unavailable",
    "internal server error",
])

NETWORK_EXCEPTION_TYPES: tuple = (
    socket.timeout,
    socket.error,
    TimeoutError,
    OSError,
    ConnectionError,
    BrokenPipeError,
    httplib2.ServerNotFoundError,
    httplib2.RelativeURIError,
)

GCS_TRANSIENT_EXCEPTIONS: tuple = (
    gcs_exceptions.ServiceUnavailable,
    gcs_exceptions.InternalServerError,
    gcs_exceptions.TooManyRequests,
    ConnectionError,
    TimeoutError,
)


# -----------------------------------------------------------------------------
# Retry Predicates
# -----------------------------------------------------------------------------

def is_network_error(error: Exception) -> bool:
    """Check if error is a network/connectivity issue."""
    if isinstance(error, NETWORK_EXCEPTION_TYPES):
        return True
    
    error_str = str(error).lower()
    return any(indicator in error_str for indicator in NETWORK_ERROR_INDICATORS)


def is_retryable_http_error(error: Exception) -> bool:
    """Check if an HTTP error is retryable (5xx or 429)."""
    if isinstance(error, HttpError):
        status = getattr(error.resp, 'status', 0) if hasattr(error, 'resp') else 0
        return status >= 500 or status == 429
    return False


def is_retryable_google_api_error(error: Exception) -> bool:
    """Check if a Google API error is retryable (network errors or 5xx/429)."""
    return is_network_error(error) or is_retryable_http_error(error)


def is_retryable_gcs_error(error: Exception) -> bool:
    """Check if a GCS error is transient and should be retried."""
    return isinstance(error, GCS_TRANSIENT_EXCEPTIONS)


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
    
    Example:
        >>> result = execute_with_retry(
        ...     lambda: api_client.list_items(),
        ...     "list items",
        ...     is_retryable=is_retryable_google_api_error
        ... )
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
