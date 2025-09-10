import re
import uuid
import time
from typing import Tuple, Optional
from googleapiclient.errors import HttpError
import httplib2
import google_auth_httplib2
import google.auth
from googleapiclient.discovery import build
import logging_utils
from constants import POLL_INTERVAL_MINUTES, MAX_POLLS, QUEUED_TIMEOUT_MINUTES

logger = logging_utils.get_logger()

def get_dataplex_service():
    """Returns an authenticated Dataplex service client."""
    logger.debug("Initializing Dataplex service client.")
    credentials, _ = google.auth.default()
    http_client = httplib2.Http(timeout=300)
    authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http_client)
    return build('dataplex', 'v1', http=authorized_http, cache_discovery=False)

def is_job_succeeded(state: str) -> bool:
    return state in ("SUCCEEDED", "SUCCEEDED_WITH_ERRORS") #Check with Saurabh

def is_job_failed(state: str) -> bool:
    return state == "FAILED"

def is_job_queued(state: str) -> bool:
    return state == "QUEUED"

def log_job_failure(job: dict, job_id: str):
    error_msg = job.get("status", {}).get("message", "No error message provided.")
    logger.error(f"Job '{job_id}' FAILED. Reason: {error_msg}")


def create_metadata_job(service, project_id: str, location: str, payload: dict, job_prefix: str) -> str:
    """Generates a unique job ID and creates a metadata job with error handling."""
    normalized_job_id = re.sub(r'[^a-z0-9-]', '-', job_prefix.lower()).strip('-')[:50]
    generated_job_id = f"{normalized_job_id}-{uuid.uuid4().hex[:8]}"
    parent = f"projects/{project_id}/locations/{location}"

    # Validate required parameters
    if not service or not project_id or not location or not payload or not generated_job_id:
        logger.debug(f"create_metadata_job input: service={service}, project_id={project_id}, location={location}, payload={payload}, job_id={generated_job_id} | output: False (Missing parameters)")
        logger.error("Missing required parameters for metadata job creation.")
        return ""

    try:
        response = service.projects().locations().metadataJobs().create(
            parent=parent, metadataJobId=generated_job_id, body=payload
        ).execute()
        logger.debug(f"create_metadata_job input: service={service}, project_id={project_id}, location={location}, payload={payload}, job_id={generated_job_id} | output: {response}")
        logger.info(f"Job '{generated_job_id}' submitted successfully.")
        return generated_job_id
    except HttpError as error:
        logger.debug(f"create_metadata_job input: service={service}, project_id={project_id}, location={location}, payload={payload}, job_id={generated_job_id} | output: {error}")
        logger.error(f"Failed to create metadata job '{generated_job_id}'")
        return ""
    except Exception as error:
        logger.debug(f"create_metadata_job input: service={service}, project_id={project_id}, location={location}, payload={payload}, job_id={generated_job_id} | output: {error}")
        logger.error(f"Unexpected error during metadata job creation for job id {generated_job_id} - {error}")
        return ""

def create_and_monitor_job(service, project_id: str, location: str, payload: dict, job_prefix: str) -> bool:
    """Creates a metadata job and monitors it until completion."""
    try:
        job_id = create_metadata_job(service, project_id, location, payload, job_prefix)
        if job_id:
            return poll_metadata_job(service, project_id, location, job_id)
    except Exception as e:
        logger.error(f"Failed to create or monitor job '{job_prefix}': {e}")
        logger.debug(f"create_and_monitor_job input: service={service}, project_id={project_id}, location={location}, payload={payload}, job_id={job_prefix} | output: {e}")
        return False


def poll_metadata_job(service, project_id: str, location: str, job_id: str) -> bool:
    """Polls a metadata job until completion or failure."""
    logger.info(f"Polling status for job '{job_id}' every {POLL_INTERVAL_MINUTES} minutes...")
    poll_interval = POLL_INTERVAL_MINUTES * 60
    max_polls = MAX_POLLS
    job_path = f"projects/{project_id}/locations/{location}/metadataJobs/{job_id}"

    for i in range(max_polls):
        time.sleep(poll_interval)
        job, state = get_job_and_state(service, job_path, job_id)
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

def get_job_and_state(service, job_path: str, job_id: str):
    try:
        job = service.projects().locations().metadataJobs().get(name=job_path).execute()
        state = job.get("status", {}).get("state")
        logger.debug(f"Job '{job_id}' and entire job: {job}")
        return job, state
    except HttpError as err:
        logger.error(f"Error polling job '{job_id}'")
        logger.debug(f"input: service={service}, job_path={job_path}, job_id={job_id} | output: {err}")
        return None, "huhaaa"

