"""Utils for API calls."""

import logging
import os
from typing import Any, Callable

from . import logging_utils
import requests

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
  """REST api call helper.

  Args:
    method: HTTP method name.
    url: URL of the resource.
    project_id: Google Cloud Project id.
    request_body: Optional body of request.

  Returns:
    Dictionary with response and error if any.
  """
  data, error_msg = None, None
  method_name = 'GET' if method == requests.get else 'POST'
  try:
    res = method(url, headers=_get_header(project_id), json=request_body)
    try:
      data = res.json()
    except requests.exceptions.JSONDecodeError:
      error_msg = f'{method_name} call to {url} returned non valid JSON.'
      return {
          'json': None,
          'error_msg': error_msg
      }
    if not res.ok:
      # If the response is an error, capture the error message from the JSON
      error_msg = data.get('error', {}).get('message') or f'{method_name} call to {url} returned HTTP {res.status_code}.'
      return {
          'json': data,
          'error_msg': error_msg
      }
    return {
        'json': data,
        'error_msg': None
    }
  except requests.exceptions.RequestException as err:
    error_msg = create_error_message(method_name, url, err)
    return {
        'json': None,
        'error_msg': error_msg
    }
