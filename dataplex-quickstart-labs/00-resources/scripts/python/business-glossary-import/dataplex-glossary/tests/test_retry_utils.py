"""
Unit tests for retry_utils.py

Test coverage:
- Retry predicates (is_network_error, is_retryable_http_error, is_retryable_google_api_error, is_retryable_gcs_error)
- Formatting helpers (format_duration, format_error_message)
- Core retry function (execute_with_retry)
"""

import socket
import time
from unittest.mock import MagicMock, patch
import pytest
from googleapiclient.errors import HttpError

from utils import retry_utils
from utils.retry_utils import (
    execute_with_retry,
    format_duration,
    format_error_message,
    is_network_error,
    is_retryable_google_api_error,
    is_retryable_gcs_error,
    is_retryable_http_error,
)


# ============================================================================
# RETRY PREDICATE TESTS
# ============================================================================

class TestIsRetryableGoogleApiError:
    """Test is_retryable_google_api_error function"""
    
    def test_rate_limit_error_is_retryable(self):
        """429 rate limit errors should be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 429
        error = HttpError(mock_resp, b'Rate limited')
        
        assert is_retryable_google_api_error(error) is True
    
    def test_server_error_is_retryable(self):
        """500 server errors should be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 500
        error = HttpError(mock_resp, b'Server error')
        
        assert is_retryable_google_api_error(error) is True
    
    def test_503_service_unavailable_is_retryable(self):
        """503 service unavailable should be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 503
        error = HttpError(mock_resp, b'Service unavailable')
        
        assert is_retryable_google_api_error(error) is True
    
    def test_400_bad_request_not_retryable(self):
        """400 bad request should not be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 400
        error = HttpError(mock_resp, b'Bad request')
        
        assert is_retryable_google_api_error(error) is False
    
    def test_404_not_found_not_retryable(self):
        """404 not found should not be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 404
        error = HttpError(mock_resp, b'Not found')
        
        assert is_retryable_google_api_error(error) is False
    
    def test_403_forbidden_not_retryable(self):
        """403 forbidden should not be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 403
        error = HttpError(mock_resp, b'Forbidden')
        
        assert is_retryable_google_api_error(error) is False
    
    def test_non_http_error_not_retryable(self):
        """Non-HTTP, non-network errors should not be retryable"""
        assert is_retryable_google_api_error(ValueError("Invalid value")) is False
    
    def test_socket_timeout_is_retryable(self):
        """Socket timeout should be retryable"""
        error = socket.timeout("Connection timed out")
        assert is_retryable_google_api_error(error) is True
    
    def test_timeout_message_is_retryable(self):
        """Error with 'timed out' message should be retryable"""
        error = Exception("Request timed out")
        assert is_retryable_google_api_error(error) is True


class TestIsNetworkError:
    """Test is_network_error function"""
    
    def test_connection_refused(self):
        """Connection refused should be detected as network error"""
        error = Exception("Connection refused")
        assert is_network_error(error) is True
    
    def test_dns_resolution_failure(self):
        """DNS resolution failure should be detected as network error"""
        error = Exception("name resolution failed")
        assert is_network_error(error) is True
    
    def test_timeout_error(self):
        """TimeoutError should be detected as network error"""
        error = TimeoutError("Connection timed out")
        assert is_network_error(error) is True
    
    def test_connection_error(self):
        """ConnectionError should be detected as network error"""
        error = ConnectionError("Connection reset")
        assert is_network_error(error) is True
    
    def test_regular_exception_not_network_error(self):
        """Regular exceptions should not be detected as network errors"""
        error = ValueError("Invalid value")
        assert is_network_error(error) is False


class TestIsRetryableHttpError:
    """Test is_retryable_http_error function"""
    
    def test_429_retryable(self):
        """429 Too Many Requests should be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 429
        error = HttpError(mock_resp, b'Rate limited')
        assert is_retryable_http_error(error) is True
    
    def test_500_retryable(self):
        """500 Internal Server Error should be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 500
        error = HttpError(mock_resp, b'Server error')
        assert is_retryable_http_error(error) is True
    
    def test_400_not_retryable(self):
        """400 Bad Request should not be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 400
        error = HttpError(mock_resp, b'Bad request')
        assert is_retryable_http_error(error) is False
    
    def test_non_http_error_not_retryable(self):
        """Non-HttpError should not be retryable"""
        assert is_retryable_http_error(Exception("Generic error")) is False


# ============================================================================
# FORMATTING HELPER TESTS
# ============================================================================

class TestFormatDuration:
    """Test format_duration function"""
    
    def test_seconds(self):
        """Format seconds correctly"""
        assert format_duration(30) == "30s"
        assert format_duration(5) == "5s"
    
    def test_minutes(self):
        """Format minutes correctly"""
        assert format_duration(120) == "2.0m"
        assert format_duration(90) == "1.5m"
    
    def test_boundary(self):
        """Values at boundary (60s = 1m)"""
        assert format_duration(59) == "59s"
        assert format_duration(60) == "1.0m"


class TestFormatErrorMessage:
    """Test format_error_message function"""
    
    def test_dns_error(self):
        """DNS errors should have friendly message"""
        error = Exception("unable to find the server at example.com")
        msg = format_error_message(error)
        assert "DNS" in msg or "server" in msg.lower()
    
    def test_connection_refused(self):
        """Connection refused should be detected"""
        error = Exception("Connection refused by server")
        msg = format_error_message(error)
        assert "refused" in msg.lower()
    
    def test_truncates_long_messages(self):
        """Long messages should be truncated"""
        long_message = "x" * 300
        error = Exception(long_message)
        msg = format_error_message(error)
        assert len(msg) <= 210  # 200 + some margin


# ============================================================================
# CORE RETRY FUNCTION TESTS
# ============================================================================

class TestExecuteWithRetry:
    """Test execute_with_retry function"""
    
    def test_success_on_first_attempt(self):
        """Successful execution returns result immediately"""
        operation = MagicMock(return_value="success")
        
        result = execute_with_retry(operation, "test op")
        
        assert result == "success"
        assert operation.call_count == 1
    
    def test_retries_on_transient_error(self, monkeypatch):
        """Should retry on transient errors"""
        # Make retry fast for testing
        monkeypatch.setattr(retry_utils, 'MAX_RETRY_DURATION_SECONDS', 5)
        monkeypatch.setattr(retry_utils, 'INITIAL_BACKOFF_SECONDS', 0.01)
        
        call_count = 0
        def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise socket.timeout("Timed out")
            return "success"
        
        result = execute_with_retry(flaky_operation, "flaky op")
        
        assert result == "success"
        assert call_count == 3
    
    def test_raises_on_non_retryable_error(self):
        """Should raise immediately on non-retryable errors"""
        operation = MagicMock(side_effect=ValueError("Bad value"))
        
        with pytest.raises(ValueError):
            execute_with_retry(operation, "test op")
        
        assert operation.call_count == 1
    
    def test_custom_predicate(self):
        """Should use custom predicate when provided"""
        call_count = 0
        def operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Retry me!")
            return "done"
        
        # Custom predicate that retries ValueError
        result = execute_with_retry(
            operation, 
            "test op", 
            is_retryable=lambda e: isinstance(e, ValueError)
        )
        
        assert result == "done"
        assert call_count == 2
    
    def test_raises_after_max_duration(self, monkeypatch):
        """Should raise after max duration exceeded"""
        monkeypatch.setattr(retry_utils.time, 'sleep', lambda s: None)
        
        def always_fail():
            raise socket.timeout("Always fails")
        
        with pytest.raises(socket.timeout):
            execute_with_retry(
                always_fail, "failing op",
                max_duration_seconds=0.1,
                initial_backoff=0.01
            )
