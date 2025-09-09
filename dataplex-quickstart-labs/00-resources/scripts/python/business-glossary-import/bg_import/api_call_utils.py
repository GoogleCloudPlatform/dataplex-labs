"""Utils for API calls."""

import logging
import os
from typing import Any, Callable

import time
import random
import requests
import logging_utils
from constants import MAX_BACKOFF_SECONDS, INITIAL_BACKOFF_SECONDS

logging.getLogger('urllib3').setLevel(logging.WARNING)
logger = logging_utils.get_logger()


def _get_header(project_id: str) -> dict[str, str]:
  return {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {os.environ.get("GCLOUD_ACCESS_TOKEN")}',
    'X-Goog-User-Project': project_id,
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

  backoff = INITIAL_BACKOFF_SECONDS

  while True:
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
        logger.info(f"{context} Retrying in {backoff} seconds due to transient error...")
        time.sleep(backoff + random.uniform(0, 0.5))  # Add jitter
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
        continue

      # Retry if connection is lost (e.g., requests.ConnectionError)
      if res.status_code == 0:
        logger.info(f"{context} Retrying in {backoff} seconds due to connection lost (status 0)...")
        time.sleep(backoff + random.uniform(0, 0.5))
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
        continue

      # For client errors (e.g., 400, 401), do not retry
      return {'json': data, 'error_msg': error_msg}

    except requests.exceptions.RequestException as err:
      error_msg = create_error_message(method_name, url, err)
      logger.debug(f'{context} Exception occurred: {error_msg}. Retrying in {backoff} seconds...')

      time.sleep(backoff + random.uniform(0, 0.5))  # Add jitter
      backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
      continue
