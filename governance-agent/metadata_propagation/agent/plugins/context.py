import contextvars
import logging
import threading

import google.auth
import google.oauth2.credentials

logger = logging.getLogger(__name__)

_oauth_token = contextvars.ContextVar("oauth_token", default=None)
_cached_adc_creds = None
_adc_lock = threading.Lock()


def set_oauth_token(token: str | None):
    """Sets the OAuth token for the current context."""
    _oauth_token.set(token)


def get_oauth_token() -> str | None:
    """Gets the OAuth token from the current context."""
    return _oauth_token.get()


def get_credentials(quota_project_id: str):
    """
    Returns Google Credentials object created from the stored OAuth token,
    falling back to cached Application Default Credentials (ADC) if token is None.
    """
    token = get_oauth_token()
    if token:
        return google.oauth2.credentials.Credentials(
            token, quota_project_id=quota_project_id
        )

    global _cached_adc_creds
    if not _cached_adc_creds:
        with _adc_lock:
            if not _cached_adc_creds:
                logger.info(
                    "Pre-loading system Application Default Credentials (ADC) to avoid concurrent thread forks..."
                )
                try:
                    _cached_adc_creds, _ = google.auth.default()
                except Exception as e:
                    logger.error(
                        f"Failed to load Application Default Credentials: {e}"
                    )
                    _cached_adc_creds = None

    return _cached_adc_creds
