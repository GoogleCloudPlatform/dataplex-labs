
from google.cloud import storage
from gcs_dao import *

import httplib2
import google_auth_httplib2
from google.cloud import storage
import logging_utils
from migration_utils import *
from constants import *
logger = logging_utils.get_logger()


def prepare_gcs_bucket(gcs_bucket: str, file_path: str, filename: str) -> bool:
    clear_bucket(gcs_bucket)
    upload_to_gcs(gcs_bucket, file_path, filename)
    return True


def upload_to_gcs(bucket_name: str, file_path: str, file_name: str) -> bool:
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_path)
        logger.debug(f"Uploaded {file_path} -> gs://{bucket_name}/{file_name}")
        return True
    except Exception as error:
        logger.error("Failed to upload '%s' to bucket '%s' with error '%s'", file_path, bucket_name, error)
        return False


def clear_bucket(bucket_name: str) -> bool:
    """Deletes all objects in a bucket. Returns True on success, False on failure."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs())
        if not blobs:
            logger.debug(f"Bucket '{bucket_name}' is already empty.")
            return True
        bucket.delete_blobs(blobs)
        logger.debug(f"Deleted {len(blobs)} objects from bucket '{bucket_name}'.")
        return True
    except Exception as error:
        logger.error("Failed to clear GCS bucket '%s' withoooiii error as '%s'", bucket_name, error)
        return False


def check_gcs_permissions(bucket_name: str, project_number: str) -> bool:
    """
    Checks if the current credentials have permission to upload and delete objects in the specified GCS bucket.
    Attempts to upload and delete a small test file.
    """
    test_blob_name = f"permission_check_{uuid.uuid4().hex}.txt"
    test_content = b"permission check"
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(test_blob_name)
        blob.upload_from_string(test_content)
        blob.delete()
        logger.debug(f"Permission check succeeded for bucket '{bucket_name}'.")
        return True
    except Exception as error:
        logger.error(f"Permission check failed for bucket '{bucket_name}' with error: {error}")
        logger.error(
            f"Permission check failed for bucket '{bucket_name}'. Grant the Storage Admin IAM role to the service account running the migration: "
            f"service-{project_number}@gcp-sa-dataplex.iam.gserviceaccount.com"
        )
        return False

def check_all_buckets_permissions(buckets: list[str], project_number: str) -> bool:
    """
    Checks GCS permissions for all buckets. Returns True if all checks pass.
    """
    for bucket in buckets:
        if not check_gcs_permissions(bucket, project_number):
            return False
    return True
