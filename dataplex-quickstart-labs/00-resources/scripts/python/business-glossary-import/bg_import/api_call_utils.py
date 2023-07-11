"""Utils for API calls."""

import logging
import os
from typing import Any, Callable

import logging_utils
import requests


logging.getLogger('urllib3').setLevel(logging.WARNING)
logger = logging_utils.get_logger()


def _get_header(project_id: str) -> dict[str, str]:
  return {
      'Content-Type': 'application/json',
      'Authorization': f'Bearer {os.environ.get("GCLOUD_ACCESS_TOKEN")}',
      'X-Goog-User-Project': project_id,
  }


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
    res.raise_for_status()
  except requests.exceptions.RequestException as err:
    error_msg = f'{method_name} call to {url} returned: {err}'
    return {
        'json': data,
        'error_msg': error_msg
    }

  try:
    data = res.json()
  except requests.exceptions.JSONDecodeError:
    error_msg = f'{method_name} call to {url} returned non valid JSON.'
  return {
      'json': data,
      'error_msg': error_msg
  }
