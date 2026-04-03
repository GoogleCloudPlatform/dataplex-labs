

from google.cloud import storage
from utils import logging_utils, dataplex_dao
from utils.retry_utils import execute_with_retry, is_retryable_gcs_error

logger = logging_utils.get_logger()


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
        result = execute_with_retry(_upload, f"upload to gs://{bucket_name}/{file_name}", is_retryable=is_retryable_gcs_error)
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
        deleted_count = execute_with_retry(_clear, f"clear bucket '{bucket_name}'", is_retryable=is_retryable_gcs_error)
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
        logger.debug(f"[GCS PERMISSION CHECK] Response: permission denied - {result}")
        return False
    logger.debug(f"[GCS PERMISSION CHECK] Response: permission granted")
    return True


def check_all_buckets_permissions(buckets: list[str], project_number: str) -> list[str]:
    """Checks bucket permissions and returns list of failed buckets (empty if all pass)."""
    service = dataplex_dao.get_dataplex_service()
    failed_buckets = []
    for bucket in buckets:
        if not check_metadata_job_creation_for_bucket(service, project_number, bucket):
            failed_buckets.append(bucket)
    return failed_buckets
