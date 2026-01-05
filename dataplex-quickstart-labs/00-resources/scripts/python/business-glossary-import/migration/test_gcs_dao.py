import pytest
from unittest.mock import MagicMock
from gcs_dao import *

@pytest.fixture
def mock_storage_client(monkeypatch):
    mock_client = MagicMock()
    monkeypatch.setattr("gcs_dao.storage.Client", MagicMock(return_value=mock_client))
    return mock_client

@pytest.fixture
def mock_logger(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr("gcs_dao.logger", mock_logger)
    return mock_logger

def test_upload_success(monkeypatch, mock_storage_client, mock_logger):
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    result = upload_to_gcs("test-bucket", "/tmp/test.txt", "test.txt")
    mock_blob.upload_from_filename.assert_called_once_with("/tmp/test.txt")
    mock_logger.debug.assert_called_once()
    assert result is True

def test_upload_failure(monkeypatch, mock_storage_client, mock_logger):
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.upload_from_filename.side_effect = Exception("Upload failed")

    result = upload_to_gcs("test-bucket", "/tmp/test.txt", "test.txt")
    mock_logger.error.assert_called_once()
    assert result is False

def test_clear_bucket_empty(monkeypatch, mock_storage_client, mock_logger):
    mock_bucket = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = []
    result = clear_bucket("empty-bucket")
    mock_logger.debug.assert_called_once_with("Bucket 'empty-bucket' is already empty.")
    assert result is True

def test_clear_bucket_with_blobs(monkeypatch, mock_storage_client, mock_logger):
    mock_bucket = MagicMock()
    mock_blob1 = MagicMock()
    mock_blob2 = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
    result = clear_bucket("non-empty-bucket")
    mock_bucket.delete_blobs.assert_called_once_with([mock_blob1, mock_blob2])
    mock_logger.debug.assert_called_with("Deleted 2 objects from bucket 'non-empty-bucket'.")
    assert result is True

def test_clear_bucket_exception(monkeypatch, mock_storage_client, mock_logger):
    mock_bucket = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.side_effect = Exception("List failed")
    result = clear_bucket("fail-bucket")
    mock_logger.error.assert_called_once()
    assert result is False

def test_upload_to_gcs_success(monkeypatch, mock_storage_client, mock_logger):
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    result = upload_to_gcs("my-bucket", "/path/to/file.txt", "file.txt")
    mock_blob.upload_from_filename.assert_called_once_with("/path/to/file.txt")
    mock_logger.debug.assert_called_once_with("Uploaded /path/to/file.txt -> gs://my-bucket/file.txt")
    assert result is True

def test_upload_to_gcs_failure(monkeypatch, mock_storage_client, mock_logger):
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.upload_from_filename.side_effect = Exception("Upload error")

    result = upload_to_gcs("my-bucket", "/path/to/file.txt", "file.txt")
    mock_logger.error.assert_called_once()
    assert result is False

def test_prepare_gcs_bucket_success(monkeypatch):
    """Test prepare_gcs_bucket with successful folder creation and upload."""
    mock_ensure_folder = MagicMock(return_value=True)
    mock_upload = MagicMock(return_value=True)

    monkeypatch.setattr("gcs_dao.ensure_folder_exists", mock_ensure_folder)
    monkeypatch.setattr("gcs_dao.upload_to_gcs", mock_upload)

    result = prepare_gcs_bucket("bucket1", "migration_folder_1", "/tmp/file.txt", "file.txt")
    assert result is True
    assert mock_ensure_folder.call_count == 1
    assert mock_upload.call_count == 1
    # Check that destination path includes the folder name
    assert mock_upload.call_args[0][2] == "migration_folder_1/file.txt"

def test_prepare_gcs_bucket_folder_creation_fails(monkeypatch):
    """Test prepare_gcs_bucket when folder creation fails."""
    def mock_ensure_folder_exists(bucket, folder):
        return False

    def mock_upload_to_gcs(bucket, file_path, destination_path):
        return True

    monkeypatch.setattr("gcs_dao.ensure_folder_exists", mock_ensure_folder_exists)
    monkeypatch.setattr("gcs_dao.upload_to_gcs", mock_upload_to_gcs)

    result = prepare_gcs_bucket("bucket2", "migration_folder_1", "/tmp/file2.txt", "file2.txt")
    assert result is False

def test_prepare_gcs_bucket_upload_fails(monkeypatch):
    """Test prepare_gcs_bucket when upload fails."""
    def mock_ensure_folder_exists(bucket, folder):
        return True

    def mock_upload_to_gcs(bucket, file_path, destination_path):
        return False

    monkeypatch.setattr("gcs_dao.ensure_folder_exists", mock_ensure_folder_exists)
    monkeypatch.setattr("gcs_dao.upload_to_gcs", mock_upload_to_gcs)

    result = prepare_gcs_bucket("bucket3", "migration_folder_1", "/tmp/file3.txt", "file3.txt")
    assert result is False

def test_ensure_folder_exists_success(monkeypatch, mock_storage_client, mock_logger):
    """Test ensure_folder_exists when folder doesn't exist."""
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = []  # Folder doesn't exist
    mock_bucket.blob.return_value = mock_blob

    result = ensure_folder_exists("test-bucket", "my-folder")
    mock_bucket.blob.assert_called_once_with("my-folder/")
    mock_blob.upload_from_string.assert_called_once_with("")
    mock_logger.debug.assert_called_once()
    assert result is True

def test_ensure_folder_exists_already_exists(monkeypatch, mock_storage_client, mock_logger):
    """Test ensure_folder_exists when folder already exists."""
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = [mock_blob]  # Folder exists

    result = ensure_folder_exists("test-bucket", "existing-folder")
    mock_bucket.blob.assert_not_called()
    assert result is True

def test_ensure_folder_exists_with_trailing_slash(monkeypatch, mock_storage_client, mock_logger):
    """Test ensure_folder_exists normalizes folder names."""
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = []

    result = ensure_folder_exists("test-bucket", "my-folder/")
    mock_bucket.blob.assert_called_once_with("my-folder/")
    assert result is True

def test_ensure_folder_exists_exception(monkeypatch, mock_storage_client, mock_logger):
    """Test ensure_folder_exists handles exceptions."""
    mock_bucket = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.side_effect = Exception("List failed")

    result = ensure_folder_exists("test-bucket", "fail-folder")
    mock_logger.error.assert_called_once()
    assert result is False

def test_ensure_folders_exist_all_success(monkeypatch, mock_storage_client, mock_logger):
    """Test ensure_folders_exist when all folders are created successfully."""
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = []
    mock_bucket.blob.return_value = mock_blob

    folder_names = ["migration_folder_1", "migration_folder_2", "migration_folder_3"]
    result = ensure_folders_exist("test-bucket", folder_names)
    assert result is True
    assert mock_bucket.blob.call_count == 3

def test_ensure_folders_exist_one_fails(monkeypatch, mock_storage_client, mock_logger):
    """Test ensure_folders_exist stops on first failure."""
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    
    # First succeeds, second fails
    call_count = [0]
    def side_effect(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 2:
            raise Exception("Creation failed")
        return []
    
    mock_bucket.list_blobs.side_effect = side_effect
    mock_bucket.blob.return_value = mock_blob

    folder_names = ["folder1", "folder2", "folder3"]
    result = ensure_folders_exist("test-bucket", folder_names)
    assert result is False

def test_ensure_folders_exist_empty_list(mock_storage_client, mock_logger):
    """Test ensure_folders_exist with empty list."""
    result = ensure_folders_exist("test-bucket", [])
    assert result is True

def test_check_all_buckets_permissions_empty_list_single(monkeypatch):
    # Should return True for empty list
    result = check_all_buckets_permissions([], "123456789")
    assert result is True
    

@pytest.fixture
def mock_get_dataplex_service(monkeypatch):
    mock_service = MagicMock()
    monkeypatch.setattr("gcs_dao.get_dataplex_service", MagicMock(return_value=mock_service))
    return mock_service

def test_check_all_buckets_permissions_all_success(monkeypatch, mock_get_dataplex_service):
    buckets = ["bucket1", "bucket2", "bucket3"]
    project_number = "123456789"
    called = []

    def mock_check_metadata_job_creation_for_bucket(service, project_id, bucket):
        called.append((service, project_id, bucket))
        return True

    monkeypatch.setattr("gcs_dao.check_metadata_job_creation_for_bucket", mock_check_metadata_job_creation_for_bucket)
    result = check_all_buckets_permissions(buckets, project_number)
    assert result is True
    assert called == [(mock_get_dataplex_service, project_number, b) for b in buckets]

def test_check_all_buckets_permissions_one_failure(monkeypatch, mock_get_dataplex_service):
    buckets = ["bucket1", "bucket2", "bucket3"]
    project_number = "123456789"

    def mock_check_metadata_job_creation_for_bucket(service, project_id, bucket):
        return bucket != "bucket2"

    monkeypatch.setattr("gcs_dao.check_metadata_job_creation_for_bucket", mock_check_metadata_job_creation_for_bucket)
    result = check_all_buckets_permissions(buckets, project_number)
    assert result is False

def test_check_all_buckets_permissions_empty_list(monkeypatch, mock_get_dataplex_service):
    buckets = []
    project_number = "123456789"

    # Should not call check_metadata_job_creation_for_bucket at all
    monkeypatch.setattr("gcs_dao.check_metadata_job_creation_for_bucket", MagicMock())
    result = check_all_buckets_permissions(buckets, project_number)
    assert result is True

@pytest.fixture
def mock_build_dummy_payload(monkeypatch):
    monkeypatch.setattr("gcs_dao.build_dummy_payload", MagicMock(return_value={"dummy": "payload"}))

def test_check_metadata_job_creation_for_bucket_permission_granted(monkeypatch, mock_logger, mock_build_dummy_payload):
    mock_service = MagicMock()
    mock_create_metadata_job = MagicMock(return_value="Job created successfully")
    monkeypatch.setattr("gcs_dao.create_metadata_job", mock_create_metadata_job)
    result = check_metadata_job_creation_for_bucket(mock_service, "project-id", "bucket-name")
    assert result is True
    mock_create_metadata_job.assert_called_once_with(
        mock_service,
        "project-id",
        "global",
        {"dummy": "payload"},
        "permission-check",
        fake_job=True
    )
    mock_logger.error.assert_not_called()

def test_check_metadata_job_creation_for_bucket_permission_denied(monkeypatch, mock_logger, mock_build_dummy_payload):
    mock_service = MagicMock()
    monkeypatch.setattr("gcs_dao.create_metadata_job", MagicMock(return_value="does not have sufficient permission"))
    result = check_metadata_job_creation_for_bucket(mock_service, "project-id", "bucket-name")
    assert result is False
    mock_logger.error.assert_called_once_with("does not have sufficient permission")

def test_check_metadata_job_creation_for_bucket_other_error(monkeypatch, mock_logger, mock_build_dummy_payload):
    mock_service = MagicMock()
    monkeypatch.setattr("gcs_dao.create_metadata_job", MagicMock(return_value="Some other error"))
    result = check_metadata_job_creation_for_bucket(mock_service, "project-id", "bucket-name")
    assert result is True
    mock_logger.error.assert_not_called()
    
def test_build_dummy_payload_basic():
    bucket_name = "my-bucket"
    payload = build_dummy_payload(bucket_name)
    assert isinstance(payload, dict)
    assert payload["type"] == "IMPORT"
    assert "import_spec" in payload
    import_spec = payload["import_spec"]
    assert import_spec["log_level"] == "DEBUG"
    assert import_spec["source_storage_uri"] == f"gs://{bucket_name}/permission-check/"
    assert import_spec["entry_sync_mode"] == "FULL"
    assert import_spec["aspect_sync_mode"] == "INCREMENTAL"
    assert "scope" in import_spec
    assert "glossaries" in import_spec["scope"]
    assert import_spec["scope"]["glossaries"] == [
        "projects/dummy-project-id/locations/global/glossaries/dummy-glossary"
    ]

def test_build_dummy_payload_with_different_bucket_names():
    for bucket_name in ["bucket1", "bucket-2", "bucket_3"]:
        payload = build_dummy_payload(bucket_name)
        assert payload["import_spec"]["source_storage_uri"] == f"gs://{bucket_name}/permission-check/"

def test_build_dummy_payload_structure():
    bucket_name = "test-bucket"
    payload = build_dummy_payload(bucket_name)
    # Check top-level keys
    assert set(payload.keys()) == {"type", "import_spec"}
    # Check import_spec keys
    expected_keys = {
        "log_level",
        "source_storage_uri",
        "entry_sync_mode",
        "aspect_sync_mode",
        "scope"
    }
    assert set(payload["import_spec"].keys()) == expected_keys
    # Check scope structure
    scope = payload["import_spec"]["scope"]
    assert isinstance(scope, dict)
    assert "glossaries" in scope
    assert isinstance(scope["glossaries"], list)
    assert len(scope["glossaries"]) == 1
    assert scope["glossaries"][0].startswith("projects/dummy-project-id/locations/global/glossaries/")
