from google.cloud import storage

import httplib2
import google_auth_httplib2
import logging_utils
from migration_utils import *
from constants import *
from dataplex_dao import *
logger = logging_utils.get_logger()


def create_folders(bucket_name: str, folder_name: str) -> bool:
    """Creates a zero-byte object to represent a folder prefix when missing."""
    normalized_folder = folder_name.strip("/")
    prefix = f"{normalized_folder}/"
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        existing = list(bucket.list_blobs(prefix=prefix, max_results=1))
        if existing:
            return True
        blob = bucket.blob(prefix)
        blob.upload_from_string("")
        logger.debug(f"Created folder placeholder gs://{bucket_name}/{prefix}")
        return True
    except Exception as error:
        logger.error("Failed to ensure folder '%s' in bucket '%s': %s", prefix, bucket_name, error)
        return False


def ensure_folders_exist(bucket_name: str, folder_names: list[str]) -> bool:
    """Ensures every folder prefix exists before uploads; returns False on first failure."""
    logger.info("Creating necessary folders in bucket '%s'...", bucket_name)
    for folder_name in folder_names:
        if not create_folders(bucket_name, folder_name):
            return False
    return True


def prepare_gcs_bucket(gcs_bucket: str, folder_name: str, file_path: str, filename: str) -> bool:
    destination_path = f"{folder_name.strip('/')}/{filename}"
    if not create_folders(gcs_bucket, folder_name):
        return False
    if not clear_gcs_path(gcs_bucket, folder_name):
        return False
    return upload_to_gcs(gcs_bucket, file_path, destination_path)


def upload_to_gcs(bucket_name: str, file_path: str, destination_blob_name: str) -> bool:
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        logger.debug(f"Uploaded {file_path} -> gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as error:
        logger.error("Failed to upload '%s' to bucket '%s' with error '%s'", file_path, bucket_name, error)
        return False


def clear_gcs_path(bucket_name: str, folder_prefix: str = "") -> bool:
    """Deletes all objects in a bucket. Returns True on success, False on failure."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        prefix = folder_prefix.strip("/") + "/" if folder_prefix else ""
        blobs = list(bucket.list_blobs(prefix=prefix)) if prefix else list(bucket.list_blobs())
        if not blobs:
            logger.debug(f"Bucket '{bucket_name}' is already empty.")
            return True
        bucket.delete_blobs(blobs)
        logger.debug(f"Deleted {len(blobs)} objects from bucket '{bucket_name}'.")
        return True
    except Exception as error:
        logger.error("Failed to clear GCS bucket '%s' with error as '%s'", bucket_name, error)
        return False

def build_dummy_payload(bucket_name):
    folder_name = "permission-check"
    return {
        "type": "IMPORT",
        "import_spec": {
            "log_level": "DEBUG",
            "source_storage_uri": f"gs://{bucket_name}/{folder_name}/",
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
    dummy_payload = build_dummy_payload(bucket_name)

    job_prefix = "permission-check"
    location = "global"
    result = create_metadata_job(service, project_id, location, dummy_payload, job_prefix, fake_job=True)

    if "does not have sufficient permission" in result:
        logger.error(result)
        return False
    return True


def check_all_buckets_permissions(buckets: list[str], project_number: str) -> bool:
    """Checks if the Dataplex service account associated with the project number has permissions on all specified GCS buckets."""
    service = get_dataplex_service()
    for bucket in buckets:
        if not check_metadata_job_creation_for_bucket(service, project_number, bucket):
            return False
    return True
