"""Utils for API calls."""

import logging
from typing import Any, Callable, Dict
import time
import random
import requests
from requests.exceptions import (
    ConnectionError,
    Timeout,
    ChunkedEncodingError,
)
from urllib3.exceptions import (
    NewConnectionError,
    MaxRetryError,
    ProtocolError,
)
from utils import logging_utils
from utils.constants import (
    MAX_BACKOFF_SECONDS,
    INITIAL_BACKOFF_SECONDS,
    MAX_RETRY_DURATION_SECONDS,
)
import google.auth
from google.auth.transport.requests import Request
from google.auth.exceptions import TransportError, RefreshError


logging.getLogger('urllib3').setLevel(logging.WARNING)
logger = logging_utils.get_logger()


# Cache
cached_creds = None
cached_token = None
last_refresh_time = 0

REFRESH_INTERVAL_SECONDS = 55 * 60  # 55 minutes

# Error patterns that indicate transient network issues (safe to retry)
TRANSIENT_ERROR_PATTERNS = [
    "Network is unreachable",
    "Connection refused",
    "Connection reset",
    "Connection timed out",
    "Temporary failure in name resolution",
    "Name or service not known",
    "No route to host",
    "Connection aborted",
    "Remote end closed connection",
    "Read timed out",
    "Failed to establish a new connection",
    "Max retries exceeded",
]


def is_transient_error(error: Exception) -> bool:
    """Check if an exception represents a transient network error that's safe to retry.
    
    Args:
        error: The exception to check.
    
    Returns:
        True if the error is transient and can be retried, False otherwise.
    """
    # Check exception types known to be transient
    if isinstance(error, (
        ConnectionError,
        Timeout,
        ChunkedEncodingError,
        NewConnectionError,
        MaxRetryError,
        ProtocolError,
        TransportError,
        OSError,  # Includes network-related OS errors like ENETUNREACH
    )):
        return True
    
    # Also check error message for known transient patterns
    error_str = str(error).lower()
    for pattern in TRANSIENT_ERROR_PATTERNS:
        if pattern.lower() in error_str:
            return True
    
    return False


def _format_retry_wait_time(seconds: float) -> str:
    """Format seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    return f"{minutes:.1f}m"


def _refresh_adc_token():
    """Refresh ADC credentials and update cache with retry logic for network errors.
    
    Retries transient network errors with exponential backoff for up to 
    MAX_RETRY_DURATION_SECONDS (10 minutes).
    
    Raises:
        TransportError: If token refresh fails after all retries.
    """
    global cached_creds, cached_token, last_refresh_time
    
    start_time = time.time()
    backoff = INITIAL_BACKOFF_SECONDS
    attempt = 0
    
    while True:
        attempt += 1
        elapsed = time.time() - start_time
        
        try:
            creds, _ = google.auth.default()
            creds.refresh(Request())
            cached_creds = creds
            cached_token = creds.token
            last_refresh_time = time.time()
            if attempt > 1:
                logger.info("ADC token refreshed successfully after %d attempts", attempt)
            else:
                logger.debug("ADC token refreshed successfully.")
            return
            
        except (TransportError, RefreshError, OSError) as e:
            elapsed = time.time() - start_time
            remaining = MAX_RETRY_DURATION_SECONDS - elapsed
            
            if not is_transient_error(e) or remaining <= 0:
                # Non-transient error or timeout exceeded
                if remaining <= 0:
                    logger.error(
                        "Network connectivity issue persists after retrying for %s. "
                        "Please check your internet connection.",
                        _format_retry_wait_time(elapsed)
                    )
                raise
            
            # Log and retry
            logger.warning(
                "Network error during authentication (attempt %d): %s. "
                "Retrying in %s... (will retry for up to %s more)",
                attempt,
                _get_friendly_error_message(e),
                _format_retry_wait_time(backoff),
                _format_retry_wait_time(remaining)
            )
            
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)


def _get_friendly_error_message(error: Exception) -> str:
    """Extract a user-friendly error message from an exception.
    
    Args:
        error: The exception to extract message from.
    
    Returns:
        A concise, user-friendly error message.
    """
    error_str = str(error)
    
    # Check for common network error patterns and return friendly messages
    if "Network is unreachable" in error_str:
        return "Network is unreachable"
    if "Connection refused" in error_str:
        return "Connection refused"
    if "Connection timed out" in error_str or "timed out" in error_str.lower():
        return "Connection timed out"
    if "Name or service not known" in error_str or "name resolution" in error_str.lower():
        return "DNS resolution failed"
    if "No route to host" in error_str:
        return "No route to host"
    if "Connection reset" in error_str:
        return "Connection reset by server"
    
    # For other errors, try to extract the core message
    # Often the actual message is after the last colon
    if ": " in error_str:
        parts = error_str.split(": ")
        # Return the last meaningful part
        for part in reversed(parts):
            part = part.strip()
            if part and len(part) > 5 and not part.startswith("<"):
                return part
    
    # Truncate very long messages
    if len(error_str) > 100:
        return error_str[:100] + "..."
    
    return error_str


def _get_header(project_id: str) -> Dict[str, str]:
    """Return headers using ADC, refreshing token every 30 minutes."""
    global last_refresh_time, cached_token

    is_token_expired = (time.time() - last_refresh_time) > REFRESH_INTERVAL_SECONDS
    if not cached_token or is_token_expired:
        logger.debug("Refreshing ADC token (interval reached)...")
        _refresh_adc_token()

    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cached_token}",
        "X-Goog-User-Project": project_id,
    }

def extract_error_details(
  response_err: requests.exceptions.RequestException,
) -> list[Any]:
  """Extract error details from response error data.

  Args:
    response_err: RequestException containing response error data.

  Returns:
    List of error details dictionaries.
  """
  if response_err.response is None:
    return []

  try:
    data = response_err.response.json()
    return data.get('error', dict()).get('details', [])
  except requests.exceptions.JSONDecodeError:
    return []


def extract_error_code(
  response_err: requests.exceptions.RequestException,
) -> str | None:
  """Extract error code from response error data.

  Return None if no error code found.

  Args:
    response_err: RequestException containing response error data.

  Returns:
    String representing error code or None if no error code found.
  """
  for detail in extract_error_details(response_err):
    if (
      detail.get('@type') == 'type.googleapis.com/google.rpc.ErrorInfo'
      and detail.get('metadata') is not None
      and detail.get('metadata').get('code') is not None
      and not str(detail.get('metadata').get('code')).isspace()
    ):
      return str(detail.get('metadata').get('code'))

  return None


def extract_debug_info_detail(
  response_err: requests.exceptions.RequestException,
) -> str | None:
  """Extract debug info details from response error data.

  Return None if no debug info detail found.

  Args:
    response_err: RequestException containing response error data.

  Returns:
    String representing debug infor detail or None if no debug info detail
    found.
  """
  for detail in extract_error_details(response_err):
    if (
      detail.get('@type') == 'type.googleapis.com/google.rpc.DebugInfo'
      and detail.get('detail') is not None
      and not str(detail.get('detail')).isspace()
    ):
      return str(detail.get('detail'))
  return None


def create_error_message(
  method_name: str,
  url: str,
  response_err: requests.exceptions.RequestException,
) -> str:
  """Create an error message.

  Args:
    method_name: String containing a http method name.
    url: String containing targeted url.
    response_err: RequestException containing response error data.

  Returns:
    String containing user friendly error message.
  """
  base_err_description = str(response_err)
  err_description = (
    extract_debug_info_detail(response_err)
    or extract_error_code(response_err)
    or base_err_description
  )
  return f'{method_name} call to {url} returned: {err_description}'


def fetch_api_response(
  method: Callable[..., Any],
  url: str,
  project_id: str,
  request_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
  """REST API call helper with exponential backoff retry mechanism.
  
  Handles transient network errors and server errors (5xx, 429) with
  exponential backoff retry for up to MAX_RETRY_DURATION_SECONDS (10 minutes).

  Args:
    method: HTTP method name.
    url: URL of the resource.
    project_id: Google Cloud Project id.
    request_body: Optional body of request.

  Returns:
    Dictionary with response and error if any.
  """
  method_name = 'GET' if method == requests.get else 'POST'
  context = f'[{method_name} {url}]'

  logger.debug(f'{context} Starting API call with project_id={project_id} and request_body={request_body}')

  start_time = time.time()
  backoff = INITIAL_BACKOFF_SECONDS
  attempt = 0

  while True:
    attempt += 1
    elapsed = time.time() - start_time
    remaining = MAX_RETRY_DURATION_SECONDS - elapsed
    
    try:
      res = method(url, headers=_get_header(project_id), json=request_body)
      logger.debug(f'{context} Response status: {res.status_code}, Response text: {res.text}')

      try:
        data = res.json()
      except requests.exceptions.JSONDecodeError:
        error_msg = 'Call returned non-valid JSON.'
        logger.debug(f'{context} JSON decode error: {error_msg}')
        return {'json': None, 'error_msg': error_msg}

      if res.ok:
        logger.debug(f'{context} Successful response JSON: {data}')
        return {'json': data, 'error_msg': None}

      # If response is error, capture error message
      error_msg = data.get('error', {}).get('message') or f'Call returned HTTP {res.status_code}.'
      logger.debug(f'{context} Bad response: {error_msg}')

      # Retry only for infra-related HTTP errors (e.g., 500s, 429 rate limit)
      if res.status_code >= 500 or res.status_code == 429:
        if remaining <= 0:
          logger.error(f"{context} Max retry duration exceeded. Last error: {error_msg}")
          return {'json': data, 'error_msg': error_msg}
        
        logger.warning(
          f"Server error (HTTP {res.status_code}), attempt {attempt}. "
          f"Retrying in {_format_retry_wait_time(backoff)}... "
          f"(will retry for up to {_format_retry_wait_time(remaining)} more)"
        )
        time.sleep(backoff + random.uniform(0, 0.5))  # Add jitter
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
        continue

      # For client errors (e.g., 400, 401, 403, 404), do not retry
      return {'json': data, 'error_msg': error_msg}

    except requests.exceptions.RequestException as err:
      elapsed = time.time() - start_time
      remaining = MAX_RETRY_DURATION_SECONDS - elapsed
      
      # Only retry transient network errors
      if not is_transient_error(err) or remaining <= 0:
        if remaining <= 0:
          logger.error(
            "Network connectivity issue persists after retrying for %s. "
            "Please check your internet connection.",
            _format_retry_wait_time(elapsed)
          )
        error_msg = create_error_message(method_name, url, err)
        return {'json': None, 'error_msg': error_msg}
      
      # Log and retry for transient errors
      logger.warning(
        "Network error (attempt %d): %s. Retrying in %s... "
        "(will retry for up to %s more)",
        attempt,
        _get_friendly_error_message(err),
        _format_retry_wait_time(backoff),
        _format_retry_wait_time(remaining)
      )
      
      time.sleep(backoff + random.uniform(0, 0.5))  # Add jitter
      backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
      continue
