import pytest
from unittest.mock import MagicMock, patch
import dataplex_dao

@pytest.fixture
def mock_service():
    service = MagicMock()
    projects = service.projects.return_value
    locations = projects.locations.return_value
    metadataJobs = locations.metadataJobs.return_value
    create = metadataJobs.create.return_value
    create.execute.return_value = {"name": "test-job"}
    return service

def test_create_metadata_job_success(monkeypatch, mock_service):
    monkeypatch.setattr(dataplex_dao, "logger", MagicMock())
    job_id = dataplex_dao.create_metadata_job(
        mock_service, "test-project", "us-central1", {"foo": "bar"}, "TestJob"
    )
    assert job_id.startswith("testjob-")
    assert len(job_id) > 0

def test_create_metadata_job_missing_params(monkeypatch):
    monkeypatch.setattr(dataplex_dao, "logger", MagicMock())
    # Missing service
    job_id = dataplex_dao.create_metadata_job(
        None, "test-project", "us-central1", {"foo": "bar"}, "TestJob"
    )
    assert job_id == ""
    # Missing project_id
    job_id = dataplex_dao.create_metadata_job(
        MagicMock(), "", "us-central1", {"foo": "bar"}, "TestJob"
    )
    assert job_id == ""
    # Missing location
    job_id = dataplex_dao.create_metadata_job(
        MagicMock(), "test-project", "", {"foo": "bar"}, "TestJob"
    )
    assert job_id == ""
    # Missing payload
    job_id = dataplex_dao.create_metadata_job(
        MagicMock(), "test-project", "us-central1", {}, "TestJob"
    )
    assert job_id == ""

def test_create_metadata_job_http_error(monkeypatch):
    mock_service = MagicMock()
    error = dataplex_dao.HttpError(resp=MagicMock(), content=b"error")
    mock_create = mock_service.projects.return_value.locations.return_value.metadataJobs.return_value.create
    mock_create.return_value.execute.side_effect = error
    monkeypatch.setattr(dataplex_dao, "logger", MagicMock())
    job_id = dataplex_dao.create_metadata_job(
        mock_service, "test-project", "us-central1", {"foo": "bar"}, "TestJob"
    )
    assert job_id == ""

def test_create_metadata_job_generic_exception(monkeypatch):
    mock_service = MagicMock()
    mock_create = mock_service.projects.return_value.locations.return_value.metadataJobs.return_value.create
    mock_create.return_value.execute.side_effect = Exception("generic error")
    monkeypatch.setattr(dataplex_dao, "logger", MagicMock())
    job_id = dataplex_dao.create_metadata_job(
        mock_service, "test-project", "us-central1", {"foo": "bar"}, "TestJob"
    )
    assert job_id == ""

def test_is_job_succeeded_true_for_succeeded():
    assert dataplex_dao.is_job_succeeded("SUCCEEDED") is True

def test_is_job_succeeded_true_for_succeeded_with_errors():
    assert dataplex_dao.is_job_succeeded("SUCCEEDED_WITH_ERRORS") is True

def test_is_job_succeeded_false_for_failed():
    assert dataplex_dao.is_job_succeeded("FAILED") is False

def test_is_job_succeeded_false_for_queued():
    assert dataplex_dao.is_job_succeeded("QUEUED") is False

def test_is_job_succeeded_false_for_unknown_state():
    assert dataplex_dao.is_job_succeeded("UNKNOWN") is False

def test_is_job_succeeded_false_for_empty_string():
    assert dataplex_dao.is_job_succeeded("") is False

def test_is_job_succeeded_false_for_none():
    assert dataplex_dao.is_job_succeeded(None) is False

def test_get_dataplex_service_success(monkeypatch):
    mock_logger = MagicMock()
    mock_credentials = MagicMock()
    mock_http = MagicMock()
    mock_authorized_http = MagicMock()
    mock_build = MagicMock(return_value="dataplex_service_client")

    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    monkeypatch.setattr(dataplex_dao.google.auth, "default", MagicMock(return_value=(mock_credentials, None)))
    monkeypatch.setattr(dataplex_dao.httplib2, "Http", MagicMock(return_value=mock_http))
    monkeypatch.setattr(dataplex_dao.google_auth_httplib2, "AuthorizedHttp", MagicMock(return_value=mock_authorized_http))
    monkeypatch.setattr(dataplex_dao, "build", mock_build)

    service = dataplex_dao.get_dataplex_service()
    assert service == "dataplex_service_client"
    mock_logger.debug.assert_called_with("Initializing Dataplex service client.")
    mock_build.assert_called_with('dataplex', 'v1', http=mock_authorized_http, cache_discovery=False)

def test_get_dataplex_service_auth_error(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    monkeypatch.setattr(dataplex_dao.google.auth, "default", MagicMock(side_effect=Exception("auth error")))
    monkeypatch.setattr(dataplex_dao.httplib2, "Http", MagicMock())
    monkeypatch.setattr(dataplex_dao.google_auth_httplib2, "AuthorizedHttp", MagicMock())
    monkeypatch.setattr(dataplex_dao, "build", MagicMock())

    with pytest.raises(Exception) as excinfo:
        dataplex_dao.get_dataplex_service()
    assert "auth error" in str(excinfo.value)

def test_create_and_monitor_job_success(monkeypatch):
    mock_service = MagicMock()
    # Patch create_metadata_job to return a valid job_id
    monkeypatch.setattr(dataplex_dao, "create_metadata_job", MagicMock(return_value="testjob-1234"))
    # Patch poll_metadata_job to return True (job succeeded)
    monkeypatch.setattr(dataplex_dao, "poll_metadata_job", MagicMock(return_value=True))
    result = dataplex_dao.create_and_monitor_job(mock_service, "test-project", "us-central1", {"foo": "bar"}, "TestJob")
    assert result is True
    dataplex_dao.create_metadata_job.assert_called_once()
    dataplex_dao.poll_metadata_job.assert_called_once()

def test_create_and_monitor_job_failure_on_create(monkeypatch):
    mock_service = MagicMock()
    # Patch create_metadata_job to return empty string (simulate failure)
    monkeypatch.setattr(dataplex_dao, "create_metadata_job", MagicMock(return_value=""))
    # Patch poll_metadata_job to ensure it's not called
    monkeypatch.setattr(dataplex_dao, "poll_metadata_job", MagicMock())
    result = dataplex_dao.create_and_monitor_job(mock_service, "test-project", "us-central1", {"foo": "bar"}, "TestJob")
    assert result is None or result is False
    dataplex_dao.create_metadata_job.assert_called_once()
    dataplex_dao.poll_metadata_job.assert_not_called()

def test_create_and_monitor_job_failure_on_poll(monkeypatch):
    mock_service = MagicMock()
    # Patch create_metadata_job to return a valid job_id
    monkeypatch.setattr(dataplex_dao, "create_metadata_job", MagicMock(return_value="testjob-1234"))
    # Patch poll_metadata_job to return False (job failed)
    monkeypatch.setattr(dataplex_dao, "poll_metadata_job", MagicMock(return_value=False))
    result = dataplex_dao.create_and_monitor_job(mock_service, "test-project", "us-central1", {"foo": "bar"}, "TestJob")
    assert result is False
    dataplex_dao.create_metadata_job.assert_called_once()
    dataplex_dao.poll_metadata_job.assert_called_once()

def test_create_and_monitor_job_exception(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    # Patch create_metadata_job to raise an exception
    monkeypatch.setattr(dataplex_dao, "create_metadata_job", MagicMock(side_effect=Exception("unexpected error")))
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    result = dataplex_dao.create_and_monitor_job(mock_service, "test-project", "us-central1", {"foo": "bar"}, "TestJob")
    assert result is False
    mock_logger.error.assert_called()

def test_poll_metadata_job_succeeds_on_first_poll(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    # Patch get_job_and_state to return a succeeded job on first poll
    monkeypatch.setattr(dataplex_dao, "get_job_and_state", MagicMock(return_value=({"status": {"state": "SUCCEEDED"}}, "SUCCEEDED")))
    monkeypatch.setattr(dataplex_dao, "is_job_succeeded", MagicMock(return_value=True))
    monkeypatch.setattr(dataplex_dao, "is_job_failed", MagicMock(return_value=False))
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    monkeypatch.setattr(dataplex_dao.time, "sleep", MagicMock())
    result = dataplex_dao.poll_metadata_job(mock_service, "test-project", "us-central1", "testjob-1234")
    assert result is True
    mock_logger.info.assert_any_call("Job 'testjob-1234' SUCCEEDED.")

def test_poll_metadata_job_fails_on_first_poll(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    # Patch get_job_and_state to return a failed job on first poll
    monkeypatch.setattr(dataplex_dao, "get_job_and_state", MagicMock(return_value=({"status": {"state": "FAILED"}}, "FAILED")))
    monkeypatch.setattr(dataplex_dao, "is_job_succeeded", MagicMock(return_value=False))
    monkeypatch.setattr(dataplex_dao, "is_job_failed", MagicMock(return_value=True))
    monkeypatch.setattr(dataplex_dao, "log_job_failure", MagicMock())
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    monkeypatch.setattr(dataplex_dao.time, "sleep", MagicMock())
    result = dataplex_dao.poll_metadata_job(mock_service, "test-project", "us-central1", "testjob-1234")
    assert result is False
    dataplex_dao.log_job_failure.assert_called_once()

def test_poll_metadata_job_returns_false_if_job_none(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    # Patch get_job_and_state to return None for job
    monkeypatch.setattr(dataplex_dao, "get_job_and_state", MagicMock(return_value=(None, "FAILED")))
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    result = dataplex_dao.poll_metadata_job(mock_service, "test-project", "us-central1", "testjob-1234")
    assert result is False

def test_poll_metadata_job_times_out(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    # Patch get_job_and_state to always return a queued job
    monkeypatch.setattr(dataplex_dao, "get_job_and_state", MagicMock(return_value=({"status": {"state": "QUEUED"}}, "QUEUED")))
    monkeypatch.setattr(dataplex_dao, "is_job_succeeded", MagicMock(return_value=False))
    monkeypatch.setattr(dataplex_dao, "is_job_failed", MagicMock(return_value=False))
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    monkeypatch.setattr(dataplex_dao.time, "sleep", MagicMock())
    # Patch MAX_POLLS to 3 for faster test
    monkeypatch.setattr(dataplex_dao, "MAX_POLLS", 3)
    result = dataplex_dao.poll_metadata_job(mock_service, "test-project", "us-central1", "testjob-1234")
    assert result is False
    mock_logger.warning.assert_called_with("Polling timed out for job 'testjob-1234'.")

def test_poll_metadata_job_multiple_polls_then_succeeds(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    # Simulate job state changing from QUEUED to SUCCEEDED
    states = [
        ({"status": {"state": "QUEUED"}}, "QUEUED"),
        ({"status": {"state": "QUEUED"}}, "QUEUED"),
        ({"status": {"state": "SUCCEEDED"}}, "SUCCEEDED"),
    ]
    def get_job_and_state_side_effect(*args, **kwargs):
        return states.pop(0)
    monkeypatch.setattr(dataplex_dao, "get_job_and_state", MagicMock(side_effect=get_job_and_state_side_effect))
    monkeypatch.setattr(dataplex_dao, "is_job_succeeded", lambda state: state == "SUCCEEDED")
    monkeypatch.setattr(dataplex_dao, "is_job_failed", lambda state: state == "FAILED")
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    monkeypatch.setattr(dataplex_dao.time, "sleep", MagicMock())
    monkeypatch.setattr(dataplex_dao, "MAX_POLLS", 5)
    result = dataplex_dao.poll_metadata_job(mock_service, "test-project", "us-central1", "testjob-1234")
    assert result is True
    mock_logger.info.assert_any_call("Job 'testjob-1234' SUCCEEDED.")

def test_get_job_and_state_success(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    mock_job = {"status": {"state": "SUCCEEDED"}}
    mock_execute = MagicMock(return_value=mock_job)
    mock_get = mock_service.projects.return_value.locations.return_value.metadataJobs.return_value.get
    mock_get.return_value.execute = mock_execute

    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)

    job, state = dataplex_dao.get_job_and_state(mock_service, "job_path", "job_id")
    assert job == mock_job
    assert state == "SUCCEEDED"
    mock_logger.debug.assert_called_with("Job 'job_id' status: SUCCEEDED")

def test_get_job_and_state_http_error(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    error = dataplex_dao.HttpError(resp=MagicMock(), content=b"error")
    mock_get = mock_service.projects.return_value.locations.return_value.metadataJobs.return_value.get
    mock_get.return_value.execute.side_effect = error

    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)

    job, state = dataplex_dao.get_job_and_state(mock_service, "job_path", "job_id")
    assert job is None
    assert state == "huhaaa"
    mock_logger.error.assert_called_with("Error polling job 'job_id'")
    mock_logger.debug.assert_called()

def test_get_job_and_state_generic_exception(monkeypatch):
    mock_service = MagicMock()
    mock_logger = MagicMock()
    mock_get = mock_service.projects.return_value.locations.return_value.metadataJobs.return_value.get
    mock_get.return_value.execute.side_effect = Exception("unexpected error")

    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)

    # Should propagate exception, not catch it
    with pytest.raises(Exception) as excinfo:
        dataplex_dao.get_job_and_state(mock_service, "job_path", "job_id")
    assert "unexpected error" in str(excinfo.value)
    
def test_log_job_failure_with_message(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    job = {"status": {"message": "Something went wrong"}}
    job_id = "job-123"
    dataplex_dao.log_job_failure(job, job_id)
    mock_logger.error.assert_called_with("Job 'job-123' FAILED. Reason: Something went wrong")

def test_log_job_failure_without_message(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    job = {"status": {}}
    job_id = "job-456"
    dataplex_dao.log_job_failure(job, job_id)
    mock_logger.error.assert_called_with("Job 'job-456' FAILED. Reason: No error message provided.")

def test_log_job_failure_without_status(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr(dataplex_dao, "logger", mock_logger)
    job = {}
    job_id = "job-789"
    dataplex_dao.log_job_failure(job, job_id)
    mock_logger.error.assert_called_with("Job 'job-789' FAILED. Reason: No error message provided.")





