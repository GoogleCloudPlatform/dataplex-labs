

import random
import time
from google.cloud import storage
from google.api_core import exceptions as gcs_exceptions
from utils import logging_utils, dataplex_dao
from utils.constants import INITIAL_BACKOFF_SECONDS, MAX_ATTEMPTS, MAX_BACKOFF_SECONDS

logger = logging_utils.get_logger()

# GCS transient exceptions that warrant retry
GCS_TRANSIENT_EXCEPTIONS = (
    gcs_exceptions.ServiceUnavailable,
    gcs_exceptions.InternalServerError,
    gcs_exceptions.TooManyRequests,
    ConnectionError,
    TimeoutError,
)


def _is_transient_gcs_error(error: Exception) -> bool:
    """Check if a GCS error is transient and should be retried."""
    return isinstance(error, GCS_TRANSIENT_EXCEPTIONS)


def _execute_with_retry(operation, operation_name: str):
    """Execute a GCS operation with exponential backoff retry."""
    backoff = INITIAL_BACKOFF_SECONDS
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return operation()
        except Exception as error:
            if _is_transient_gcs_error(error) and attempt < MAX_ATTEMPTS:
                sleep_time = backoff + random.uniform(0, 0.5)
                logger.info(f"Transient GCS error during {operation_name} (attempt {attempt}/{MAX_ATTEMPTS}): {error}. "
                           f"Retrying in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
                continue
            raise
    return None


def prepare_gcs_bucket(gcs_bucket: str, file_path: str, filename: str) -> bool:
    """Prepares GCS bucket by clearing it and uploading the file. Returns True only if both operations succeed."""
    if not clear_bucket(gcs_bucket):
        logger.error(f"Failed to clear bucket '{gcs_bucket}'. Skipping upload.")
        return False
    
    if not upload_to_gcs(gcs_bucket, file_path, filename):
        logger.error(f"Failed to upload '{filename}' to bucket '{gcs_bucket}'.")
        return False
    
    return True


def upload_to_gcs(bucket_name: str, file_path: str, file_name: str) -> bool:
    """Upload a file to GCS with retry logic."""
    logger.debug(f"[GCS UPLOAD] Request: bucket={bucket_name}, file={file_name}, source={file_path}")
    
    def _upload():
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_path)
        return True
    
    try:
        result = _execute_with_retry(_upload, f"upload to gs://{bucket_name}/{file_name}")
        logger.debug(f"[GCS UPLOAD] Response: success, uploaded {file_path} -> gs://{bucket_name}/{file_name}")
        return result
    except Exception as error:
        logger.error(f"[GCS UPLOAD] Response: failed - {error}")
        return False


def clear_bucket(bucket_name: str) -> bool:
    """Deletes all objects in a bucket with retry logic. Returns True on success, False on failure."""
    logger.debug(f"[GCS CLEAR] Request: bucket={bucket_name}")
    
    def _clear():
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs())
        if not blobs:
            return 0
        bucket.delete_blobs(blobs)
        return len(blobs)
    
    try:
        deleted_count = _execute_with_retry(_clear, f"clear bucket '{bucket_name}'")
        if deleted_count == 0:
            logger.debug(f"[GCS CLEAR] Response: bucket already empty")
        else:
            logger.debug(f"[GCS CLEAR] Response: deleted {deleted_count} objects")
        return True
    except Exception as error:
        logger.error(f"[GCS CLEAR] Response: failed - {error}")
        return False

def build_dummy_payload(bucket_name):
    return {
        "type": "IMPORT",
        "import_spec": {
            "log_level": "DEBUG",
            "source_storage_uri": f"gs://{bucket_name}/",
            "entry_sync_mode": "FULL",
            "aspect_sync_mode": "INCREMENTAL",
            "scope": {
                "glossaries": [f"projects/dummy-project-id/locations/global/glossaries/dummy-glossary"]
            }
        }
    }
    
def check_metadata_job_creation_for_bucket(service, project_id: str, bucket_name: str) -> bool:
    """
    Tries to create a dummy metadata job using the specific GCS bucket to check if the Dataplex service account has permissions.
    Returns True if the permission check passes, False if permission is denied for that bucket.
    """
    logger.debug(f"[GCS PERMISSION CHECK] Request: project={project_id}, bucket={bucket_name}")
    dummy_payload = build_dummy_payload(bucket_name)
    job_prefix = "permission-check"
    location = "global"
    result = dataplex_dao.create_metadata_job(service, project_id, location, dummy_payload, job_prefix, fake_job=True)

    if "does not have sufficient permission" in result:
        logger.debug(f"[GCS PERMISSION CHECK] Response: permission denied")
        logger.error(result)
        return False
    logger.debug(f"[GCS PERMISSION CHECK] Response: permission granted")
    return True


def check_all_buckets_permissions(buckets: list[str], project_number: str) -> bool:
    """Checks if the Dataplex service account associated with the project number has permissions on all specified GCS buckets."""
    service = dataplex_dao.get_dataplex_service()
    for bucket in buckets:
        if not check_metadata_job_creation_for_bucket(service, project_number, bucket):
            return False
    return True
