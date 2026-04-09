# Standard library imports
import re
import time
import uuid

# Third-party imports
import google.auth
import google_auth_httplib2
import httplib2
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError

# Local imports
from utils import logging_utils
from utils.constants import INITIAL_BACKOFF_SECONDS, MAX_ATTEMPTS, MAX_POLLS, POLL_INTERVAL_MINUTES
from utils.error_utils import TRANSIENT_EXCEPTIONS, extract_error_detail, handle_transient_error, is_transient_http_error

logger = logging_utils.get_logger()

def get_dataplex_service() -> Resource:
    """Returns an authenticated Dataplex service client."""
    logger.debug("Initializing Dataplex service client.")
    credentials, _ = google.auth.default()
    http_client = httplib2.Http(timeout=300)
    authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http_client)
    return build('dataplex', 'v1', http=authorized_http, cache_discovery=False)

def is_job_succeeded(state: str) -> bool:
    return state in ("SUCCEEDED", "SUCCEEDED_WITH_ERRORS")

def is_job_failed(state: str) -> bool:
    return state == "FAILED"

def is_job_queued(state: str) -> bool:
    return state == "QUEUED"

def log_job_failure(job: dict, job_id: str):
    error_msg = job.get("status", {}).get("message", "No error message provided.")
    logger.error(f"Job '{job_id}' FAILED. Reason: {error_msg}")

def normalize_job_id(job_prefix: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", job_prefix.lower()).strip("-")[:50]

def generate_job_id(job_prefix: str) -> str:
    normalized_job_id = normalize_job_id(job_prefix)
    return f"{normalized_job_id}-{uuid.uuid4().hex[:8]}"

def validate_create_job_params(dataplex_service: Resource, project_id, location, payload, generated_job_id) -> bool:
    if not dataplex_service or not project_id or not location or not payload or not generated_job_id:
        logger.debug(
            f"create_metadata_job input: service={dataplex_service}, project_id={project_id}, "
            f"location={location}, payload={payload}, job_id={generated_job_id} | output: False (Missing parameters)"
        )
        logger.error("Missing required parameters for metadata job creation.")
        return False
    return True

def log_metadata_job_submission(dataplex_service: Resource, project_id, location, payload, generated_job_id, response):
    logger.debug(
                f"create_metadata_job input: service={dataplex_service}, project_id={project_id}, "
                f"location={location}, payload={payload}, job_id={generated_job_id} | output: {response}"
            )
    logger.info(f"Job '{generated_job_id}' submitted successfully.")

def _extract_entry_groups_from_payload(payload: dict) -> str:
    """Extract entry groups from payload for error messages."""
    try:
        entry_groups = payload.get('import_spec', {}).get('scope', {}).get('entry_groups', [])
        return ', '.join(entry_groups) if entry_groups else 'unknown'
    except Exception:
        return 'unknown'


def create_metadata_job(dataplex_service: Resource, project_id: str, location: str, payload: dict, job_prefix: str, fake_job: bool = False) -> str:
    """
    Generates a unique job ID and creates a metadata job with exponential backoff retry.

    Retries on transient server errors (5xx), rate-limit (429) and network/transport exceptions
    until TOTAL_RETRY_TIMEOUT_SECONDS elapses. Returns the generated job id on success;
    on failure returns '' (or error detail string when fake_job=True).
    """
    generated_job_id = generate_job_id(job_prefix)
    parent = f"projects/{project_id}/locations/{location}"

    if not validate_create_job_params(dataplex_service, project_id, location, payload, generated_job_id):
        return ""

    backoff = INITIAL_BACKOFF_SECONDS
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = dataplex_service.projects().locations().metadataJobs().create(
                parent=parent, metadataJobId=generated_job_id, body=payload
            ).execute()
            log_metadata_job_submission(dataplex_service, project_id, location, payload, generated_job_id, response)
            return generated_job_id
        except HttpError as error:
            error_detail = extract_error_detail(error)
            # Retry only if transient http error (5xx or 429) and attempts remain
            if is_transient_http_error(error) and attempt < MAX_ATTEMPTS:
                backoff = handle_transient_error(generated_job_id, backoff, attempt, error_detail)
                continue
            if fake_job:
                return error_detail
            logger.error(f"Failed to create metadata job '{generated_job_id}' with error: {error_detail}")
            return ""
        except Exception as exception:
            error_message = str(exception)
            # Retry only for transient exceptions
            if isinstance(exception, TRANSIENT_EXCEPTIONS) and attempt < MAX_ATTEMPTS:
                backoff = handle_transient_error(generated_job_id, backoff, attempt, error_message)
                continue
            if fake_job:
                return error_message
            logger.error(f"Unexpected error during metadata job creation for job id {generated_job_id} - {error_message}")
            return ""
    return ""

def create_and_monitor_job(dataplex_service: Resource, project_id: str, location: str, payload: dict, job_prefix: str) -> bool:
    """Creates a metadata job and monitors it until completion. Returns True only if job creation and monitoring succeed."""
    try:
        job_id = create_metadata_job(dataplex_service, project_id, location, payload, job_prefix)
        if job_id:
            return poll_metadata_job(dataplex_service, project_id, location, job_id)
        return False
    except Exception as e:
        logger.error(f"Failed to create or monitor job '{job_prefix}': {e}")
        logger.debug(f"create_and_monitor_job input: service={dataplex_service}, project_id={project_id}, location={location}, payload={payload}, job_id={job_prefix} | output: {e}")
        return False


def poll_metadata_job(dataplex_service: Resource, project_id: str, location: str, job_id: str) -> bool:
    """Polls a metadata job until completion or failure."""
    logger.info(f"Polling status for job '{job_id}' every {POLL_INTERVAL_MINUTES} minutes...")
    poll_interval_seconds = POLL_INTERVAL_MINUTES * 60
    max_polls = MAX_POLLS
    job_name = f"projects/{project_id}/locations/{location}/metadataJobs/{job_id}"

    for i in range(max_polls):
        # Sleep in smaller chunks (10s) to allow faster response to KeyboardInterrupt
        for _ in range(poll_interval_seconds // 10):
            time.sleep(10)
        job, state = get_job_and_state(dataplex_service, job_name)
        if job is None:
            return False
        if is_job_succeeded(state):
            logger.info(f"Job '{job_id}' SUCCEEDED.")
            return True
        if is_job_failed(state):
            log_job_failure(job, job_id)
            return False
        logger.info(f"Job '{job_id}' is {state}. Continuing to wait... (check {i+1}/{max_polls})")
    logger.warning(f"Polling timed out for job '{job_id}'.")
    return False

def get_job_and_state(dataplex_service: Resource, job_name: str) -> tuple:
    """Fetches a metadata job by its full resource name and returns (job, state)."""
    try:
        job = dataplex_service.projects().locations().metadataJobs().get(name=job_name).execute()
        state = job.get("status", {}).get("state")
        logger.debug(f"Job '{job_name}' state={state}, full response: {job}")
        return job, state
    except HttpError as err:
        logger.error(f"Error polling job '{job_name}'")
        logger.debug(f"get_job_and_state input: job_name={job_name} | output: {err}")
        return None, None