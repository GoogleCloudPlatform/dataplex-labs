import time
import random
import json
import logging
from googleapiclient.errors import HttpError
import requests
import socket
from constants import MAX_ATTEMPTS, INITIAL_BACKOFF_SECONDS

logger = logging.getLogger(__name__)

TRANSIENT_EXCEPTIONS = (
        requests.exceptions.RequestException,
        ConnectionError,
        TimeoutError,
        socket.timeout,
    )
def handle_transient_error(generated_job_id, backoff, attempt, error_detail):
    sleep_time = backoff + random.uniform(0, 0.5)
    logger.info(f"Transient error creating metadata job '{generated_job_id}' (attempt {attempt}/{MAX_ATTEMPTS}): {error_detail}. "
                f"Retrying in {sleep_time:.1f}s...")
    time.sleep(sleep_time)
    backoff *= 2
    return backoff

def is_transient_http_error(http_error: HttpError) -> bool:
    """Return True if the HttpError is a transient error we should retry (5xx or 429)."""
    try:
        # googleapiclient.errors.HttpError often exposes `.resp.status` (or .status_code)
        if hasattr(http_error, "resp") and getattr(http_error.resp, "status", None) is not None:
            status = int(http_error.resp.status)
        elif hasattr(http_error, "status_code"):
            status = int(http_error.status_code)
        else:
            return False
        return status >= 500 or status == 429
    except Exception:
        return False

def extract_error_detail(error: HttpError) -> str:
    try:
        # error.content is a byte string containing the HTTP response body
        error_json = json.loads(error.content.decode('utf-8'))
        details = error_json.get("error", {}).get("details", [])
        for detail in details:
            if "detail" in detail:
                return detail["detail"]

        return error_json.get("error", {}).get("message", "Unknown error")

    except Exception:
        return f"HttpError {error}"