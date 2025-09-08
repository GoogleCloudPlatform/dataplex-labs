import pytest
from unittest.mock import MagicMock
from gcs_dao import upload_to_gcs, clear_bucket
from gcs_dao import prepare_gcs_bucket

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


