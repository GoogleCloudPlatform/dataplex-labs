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
    clear_bucket_called = []
    upload_to_gcs_called = []

    def mock_clear_bucket(bucket):
        clear_bucket_called.append(bucket)
        return True

    def mock_upload_to_gcs(bucket, file_path, filename):
        upload_to_gcs_called.append((bucket, file_path, filename))
        return True

    monkeypatch.setattr("gcs_dao.clear_bucket", mock_clear_bucket)
    monkeypatch.setattr("gcs_dao.upload_to_gcs", mock_upload_to_gcs)

    result = prepare_gcs_bucket("bucket1", "/tmp/file.txt", "file.txt")
    assert result is True
    assert clear_bucket_called == ["bucket1"]
    assert upload_to_gcs_called == [("bucket1", "/tmp/file.txt", "file.txt")]

def test_prepare_gcs_bucket_clear_bucket_fails(monkeypatch):
    def mock_clear_bucket(bucket):
        return False

    def mock_upload_to_gcs(bucket, file_path, filename):
        return True

    monkeypatch.setattr("gcs_dao.clear_bucket", mock_clear_bucket)
    monkeypatch.setattr("gcs_dao.upload_to_gcs", mock_upload_to_gcs)

    # prepare_gcs_bucket always returns True, even if clear_bucket fails
    result = prepare_gcs_bucket("bucket2", "/tmp/file2.txt", "file2.txt")
    assert result is True

def test_prepare_gcs_bucket_upload_to_gcs_fails(monkeypatch):
    def mock_clear_bucket(bucket):
        return True

    def mock_upload_to_gcs(bucket, file_path, filename):
        return False

    monkeypatch.setattr("gcs_dao.clear_bucket", mock_clear_bucket)
    monkeypatch.setattr("gcs_dao.upload_to_gcs", mock_upload_to_gcs)

    # prepare_gcs_bucket always returns True, even if upload_to_gcs fails
    result = prepare_gcs_bucket("bucket3", "/tmp/file3.txt", "file3.txt")
    assert result is True
def test_check_all_buckets_permissions_all_success(monkeypatch):
    called_buckets = []

    def mock_check_gcs_permissions(bucket):
        called_buckets.append(bucket)
        return True

    monkeypatch.setattr("gcs_dao.check_gcs_permissions", mock_check_gcs_permissions)
    buckets = ["bucket1", "bucket2", "bucket3"]
    result = check_all_buckets_permissions(buckets)
    assert result is True
    assert called_buckets == buckets

def test_check_all_buckets_permissions_one_failure(monkeypatch):
    def mock_check_gcs_permissions(bucket):
        return bucket != "bucket2"

    monkeypatch.setattr("gcs_dao.check_gcs_permissions", mock_check_gcs_permissions)
    buckets = ["bucket1", "bucket2", "bucket3"]
    result = check_all_buckets_permissions(buckets)
    assert result is False

def test_check_all_buckets_permissions_empty_list(monkeypatch):
    # Should return True for empty list
    result = check_all_buckets_permissions([])
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
def mock_logger(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr("gcs_dao.logger", mock_logger)
    return mock_logger

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
    assert import_spec["source_storage_uri"] == f"gs://{bucket_name}/"
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
        assert payload["import_spec"]["source_storage_uri"] == f"gs://{bucket_name}/"

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







