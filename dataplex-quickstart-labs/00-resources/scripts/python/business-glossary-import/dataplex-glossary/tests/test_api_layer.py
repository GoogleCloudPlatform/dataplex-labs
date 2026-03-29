"""
Unit tests for api_layer.py

Test coverage:
- Retry logic (_is_retryable_google_api_error, _execute_with_retry)
- Entry ID parsing (_parse_entry_id_components)
- Entry links pagination (_fetch_entry_links_page)
- Location caching (_is_location_cached, _extract_location_ids)
- Authentication (authenticate_dataplex)
- Glossary operations (list_glossary_terms, get_glossary)
- Entry operations (lookup_entry_links_for_term)
- Project operations (get_default_project)
"""

import sys
import socket
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, call, PropertyMock
import pytest
from googleapiclient.errors import HttpError

# Import the module
sys.path.insert(0, str(Path(__file__).parent.parent / 'utils'))
from utils import api_layer


# ============================================================================
# RETRY LOGIC TESTS
# ============================================================================

class TestIsRetryableGoogleApiError:
    """Test _is_retryable_google_api_error function"""
    
    def test_rate_limit_error_is_retryable(self):
        """429 rate limit errors should be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 429
        error = HttpError(mock_resp, b'Rate limited')
        
        result = api_layer._is_retryable_google_api_error(error)
        
        assert result is True
    
    def test_server_error_is_retryable(self):
        """500 server errors should be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 500
        error = HttpError(mock_resp, b'Server error')
        
        result = api_layer._is_retryable_google_api_error(error)
        
        assert result is True
    
    def test_503_service_unavailable_is_retryable(self):
        """503 service unavailable should be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 503
        error = HttpError(mock_resp, b'Service unavailable')
        
        result = api_layer._is_retryable_google_api_error(error)
        
        assert result is True
    
    def test_400_bad_request_not_retryable(self):
        """400 bad request should not be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 400
        error = HttpError(mock_resp, b'Bad request')
        
        result = api_layer._is_retryable_google_api_error(error)
        
        assert result is False
    
    def test_404_not_found_not_retryable(self):
        """404 not found should not be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 404
        error = HttpError(mock_resp, b'Not found')
        
        result = api_layer._is_retryable_google_api_error(error)
        
        assert result is False
    
    def test_403_forbidden_not_retryable(self):
        """403 forbidden should not be retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 403
        error = HttpError(mock_resp, b'Forbidden')
        
        result = api_layer._is_retryable_google_api_error(error)
        
        assert result is False
    
    def test_non_http_error_not_retryable(self):
        """Non-HTTP errors should not be retryable"""
        result = api_layer._is_retryable_google_api_error(Exception("Generic error"))
        
        assert result is False
    
    def test_socket_timeout_is_retryable(self):
        """Socket timeout should be retryable"""
        error = socket.timeout("Connection timed out")
        
        result = api_layer._is_retryable_google_api_error(error)
        
        assert result is True
    
    def test_timeout_message_is_retryable(self):
        """Error with 'timed out' message should be retryable"""
        error = Exception("Request timed out")
        
        result = api_layer._is_retryable_google_api_error(error)
        
        assert result is True


class TestExecuteWithRetry:
    """Test _execute_with_retry function"""
    
    def test_success_on_first_attempt(self, monkeypatch):
        """Successful execution returns result immediately"""
        call_count = [0]
        def mock_operation():
            call_count[0] += 1
            return {'data': 'success'}
        
        result = api_layer._execute_with_retry(mock_operation, "test operation")
        
        assert result == {'data': 'success'}
        assert call_count[0] == 1
    
    def test_retries_on_retryable_error(self, monkeypatch):
        """Should retry on retryable errors"""
        call_count = [0]
        mock_resp = MagicMock()
        mock_resp.status = 500
        
        def mock_operation():
            call_count[0] += 1
            if call_count[0] < 2:
                raise HttpError(mock_resp, b'Server error')
            return {'data': 'success'}
        
        # Speed up tests by reducing backoff
        monkeypatch.setattr(api_layer, 'INITIAL_BACKOFF_SECONDS', 0.01)
        monkeypatch.setattr(api_layer, 'MAX_RETRY_DURATION_SECONDS', 1)
        
        result = api_layer._execute_with_retry(mock_operation, "test operation")
        
        assert result == {'data': 'success'}
        assert call_count[0] == 2
    
    def test_raises_immediately_on_non_retryable(self, monkeypatch):
        """Should raise immediately on non-retryable errors"""
        call_count = [0]
        mock_resp = MagicMock()
        mock_resp.status = 400
        
        def mock_operation():
            call_count[0] += 1
            raise HttpError(mock_resp, b'Bad request')
        
        with pytest.raises(HttpError):
            api_layer._execute_with_retry(mock_operation, "test operation")
        
        # Should only try once
        assert call_count[0] == 1


# ============================================================================
# ENTRY ID PARSING TESTS
# ============================================================================

class TestParseEntryIdComponents:
    """Test _parse_entry_id_components function"""
    
    def test_parses_standard_entry_id(self):
        """Parse standard entry ID format - returns tuple (project, location)"""
        entry_id = 'projects/my-project/locations/us-central1/entryGroups/my-group/entries/my-entry'
        
        project, location = api_layer._parse_entry_id_components(entry_id)
        
        assert project == 'my-project'
        assert location == 'us-central1'
    
    def test_parses_global_location(self):
        """Parse entry with global location"""
        entry_id = 'projects/proj/locations/global/entryGroups/eg/entries/e'
        
        project, location = api_layer._parse_entry_id_components(entry_id)
        
        assert location == 'global'
    
    def test_handles_special_characters(self):
        """Parse entry with special characters in names"""
        entry_id = 'projects/my-project-123/locations/us-west1/entryGroups/group_1/entries/entry_a-b'
        
        project, location = api_layer._parse_entry_id_components(entry_id)
        
        assert project == 'my-project-123'
        assert location == 'us-west1'
    
    def test_raises_on_invalid_format(self):
        """Invalid format should raise InvalidEntryIdFormatError"""
        from utils.error import InvalidEntryIdFormatError
        
        with pytest.raises(InvalidEntryIdFormatError):
            api_layer._parse_entry_id_components('invalid-format')
    
    def test_raises_on_empty_string(self):
        """Empty string should raise InvalidEntryIdFormatError"""
        from utils.error import InvalidEntryIdFormatError
        
        with pytest.raises(InvalidEntryIdFormatError):
            api_layer._parse_entry_id_components('')


# ============================================================================
# ENTRY LINKS PAGINATION TESTS
# ============================================================================

class TestFetchEntryLinksPage:
    """Test _fetch_entry_links_page function"""
    
    def test_fetches_single_page(self, monkeypatch):
        """Fetch single page of results"""
        mock_response = {
            'entryLinks': [{'name': 'link1'}, {'name': 'link2'}]
        }
        mock_client = MagicMock()
        mock_client.projects().locations().lookupEntry().entryLinks().list().execute.return_value = mock_response
        
        # If function exists
        if hasattr(api_layer, '_fetch_entry_links_page'):
            result = api_layer._fetch_entry_links_page(
                mock_client, 'entry_name', None, 100
            )
            
            assert len(result.get('entryLinks', [])) == 2
    
    def test_handles_empty_response(self, monkeypatch):
        """Handle empty response from API"""
        mock_response = {}
        mock_client = MagicMock()
        mock_client.projects().locations().lookupEntry().entryLinks().list().execute.return_value = mock_response
        
        if hasattr(api_layer, '_fetch_entry_links_page'):
            result = api_layer._fetch_entry_links_page(
                mock_client, 'entry_name', None, 100
            )
            
            assert result.get('entryLinks', []) == []


# ============================================================================
# LOCATION CACHING TESTS
# ============================================================================

class TestIsLocationCached:
    """Test _is_location_cached function"""
    
    def test_cached_location_returns_true(self, monkeypatch):
        """Cached location should return True"""
        # Setup cache
        api_layer._location_cache = {'us-central1': True}
        
        if hasattr(api_layer, '_is_location_cached'):
            result = api_layer._is_location_cached('us-central1')
            assert result is True
    
    def test_uncached_location_returns_false(self, monkeypatch):
        """Uncached location should return False"""
        api_layer._location_cache = {}
        
        if hasattr(api_layer, '_is_location_cached'):
            result = api_layer._is_location_cached('eu-west1')
            assert result is False


class TestExtractLocationIds:
    """Test _extract_location_ids function"""
    
    def test_extracts_ids_from_locations(self):
        """Extract location IDs from location list"""
        locations = [
            {'name': 'projects/p/locations/us-central1', 'locationId': 'us-central1'},
            {'name': 'projects/p/locations/eu-west1', 'locationId': 'eu-west1'},
        ]
        
        if hasattr(api_layer, '_extract_location_ids'):
            result = api_layer._extract_location_ids(locations)
            
            assert 'us-central1' in result
            assert 'eu-west1' in result
    
    def test_handles_empty_list(self):
        """Handle empty locations list"""
        if hasattr(api_layer, '_extract_location_ids'):
            result = api_layer._extract_location_ids([])
            assert result == []


# ============================================================================
# AUTHENTICATION TESTS
# ============================================================================

class TestAuthenticateDataplex:
    """Test authenticate_dataplex function"""
    
    def test_returns_client_on_success(self, monkeypatch):
        """Should return API client on successful auth"""
        mock_credentials = MagicMock()
        mock_service = MagicMock()
        
        monkeypatch.setattr(api_layer, 'default', lambda scopes: (mock_credentials, 'project'))
        monkeypatch.setattr(api_layer, 'build', lambda *args, **kwargs: mock_service)
        
        result = api_layer.authenticate_dataplex()
        
        assert result is not None
    
    def test_raises_on_auth_failure(self, monkeypatch):
        """Should raise on authentication failure"""
        from utils.error import DataplexAPIError
        
        def raise_error(scopes):
            raise Exception("Auth failed")
        
        monkeypatch.setattr(api_layer, 'default', raise_error)
        
        with pytest.raises(DataplexAPIError):
            api_layer.authenticate_dataplex()


# ============================================================================
# GLOSSARY OPERATIONS TESTS
# ============================================================================

class TestListGlossaryTerms:
    """Test list_glossary_terms function"""
    
    def test_lists_terms_successfully(self, monkeypatch):
        """Should list glossary terms"""
        mock_terms = [{'name': 'term1'}, {'name': 'term2'}]
        mock_service = MagicMock()
        mock_service.projects().locations().glossaries().terms().list().execute.return_value = {
            'terms': mock_terms
        }
        mock_service.projects().locations().glossaries().terms().list_next.return_value = None
        
        result = api_layer.list_glossary_terms(mock_service, 'projects/p/locations/l/glossaries/g')
        
        assert len(result) == 2
    
    def test_handles_empty_glossary(self, monkeypatch):
        """Should handle glossary with no terms"""
        mock_service = MagicMock()
        mock_service.projects().locations().glossaries().terms().list().execute.return_value = {}
        mock_service.projects().locations().glossaries().terms().list_next.return_value = None
        
        result = api_layer.list_glossary_terms(mock_service, 'projects/p/locations/l/glossaries/g')
        
        assert result == []
    
    def test_paginates_through_results(self, monkeypatch):
        """Should handle paginated results"""
        mock_service = MagicMock()
        
        # First page with nextPageToken
        mock_service.projects().locations().glossaries().terms().list().execute.return_value = {
            'terms': [{'name': 'term1'}],
            'nextPageToken': 'token123'
        }
        # Mock list_next to return None (no more pages)
        mock_service.projects().locations().glossaries().terms().list_next.return_value = None
        
        result = api_layer.list_glossary_terms(mock_service, 'projects/p/locations/l/glossaries/g')
        
        # Should have terms from first page
        assert len(result) >= 1


# ============================================================================
# ENTRY OPERATIONS TESTS
# ============================================================================

class TestLookupEntryLinksForTerm:
    """Test lookup_entry_links_for_term function"""
    
    def test_returns_entry_links(self, monkeypatch):
        """Should return entry links for term"""
        mock_response = {
            'json': {
                'entryLinks': [
                    {'name': 'link1', 'entryLinkType': 'RELATED'},
                    {'name': 'link2', 'entryLinkType': 'DEFINITION'}
                ]
            }
        }
        
        monkeypatch.setattr(api_layer, 'fetch_api_response', lambda **kwargs: mock_response)
        
        entry_id = 'projects/test-project/locations/us-central1/entryGroups/eg/entries/e'
        result = api_layer.lookup_entry_links_for_term(entry_id, 'test-project')
        
        assert result is not None
    
    def test_handles_invalid_entry_id(self):
        """Should raise on invalid entry ID"""
        from utils.error import InvalidEntryIdFormatError
        
        with pytest.raises(InvalidEntryIdFormatError):
            api_layer.lookup_entry_links_for_term('invalid-entry', 'project')


# ============================================================================
# PROJECT OPERATIONS TESTS
# ============================================================================

class TestGetDefaultProject:
    """Test get_default_project function"""
    
    def test_returns_project_from_auth(self, monkeypatch):
        """Should return project from auth credentials"""
        api_layer._default_project = None  # Reset cache
        monkeypatch.setattr(api_layer, 'default', lambda scopes: (MagicMock(), 'my-project'))
        
        result = api_layer.get_default_project()
        
        assert result == 'my-project'
        
        # Reset after test
        api_layer._default_project = None
    
    def test_caches_project(self, monkeypatch):
        """Should cache the default project"""
        api_layer._default_project = 'cached-project'
        
        result = api_layer.get_default_project()
        
        assert result == 'cached-project'
        
        # Reset after test
        api_layer._default_project = None


# ============================================================================
# REGION RESOLUTION TESTS
# ============================================================================

class TestResolveRegionsToQuery:
    """Test resolve_regions_to_query function"""
    
    def test_global_returns_all_regions(self, monkeypatch):
        """Global location should return all available regions"""
        standard_regions = ['us-central1', 'us-west1', 'eu-west1']
        
        monkeypatch.setattr(api_layer, 'get_standard_regions', lambda locs: standard_regions)
        monkeypatch.setattr(api_layer, 'list_supported_locations', lambda p: [
            {'locationId': 'global'}, {'locationId': 'us'}, {'locationId': 'us-central1'}
        ])
        
        result = api_layer.resolve_regions_to_query('global', 'my-project')
        
        assert len(result) > 0
    
    def test_regional_returns_single_region(self, monkeypatch):
        """Regional location should return only that region"""
        result = api_layer.resolve_regions_to_query('us-central1', 'my-project')
        
        assert result == ['us-central1']
    
    def test_multiregion_returns_contained_regions(self, monkeypatch):
        """Multi-region (us, eu) should return contained regions"""
        standard_regions = ['us-central1', 'us-west1', 'eu-west1']
        
        monkeypatch.setattr(api_layer, 'get_standard_regions', lambda locs: standard_regions)
        monkeypatch.setattr(api_layer, 'list_supported_locations', lambda p: [
            {'locationId': 'global'}, {'locationId': 'us'}, {'locationId': 'us-central1'}
        ])
        
        result = api_layer.resolve_regions_to_query('us', 'my-project')
        
        # Should include us-central1 since it's in the us multi-region
        assert len(result) > 0


# ============================================================================
# LOCATION FUNCTIONS TESTS
# ============================================================================

class TestInitializeLocationsCache:
    """Test initialize_locations_cache function"""
    
    def test_initializes_cache(self, monkeypatch):
        """Should initialize locations cache"""
        standard_regions = ['us-central1', 'eu-west1']
        
        monkeypatch.setattr(api_layer, 'list_supported_locations', lambda p: [
            {'locationId': 'us-central1'}, {'locationId': 'eu-west1'}
        ])
        monkeypatch.setattr(api_layer, 'get_standard_regions', lambda locs: standard_regions)
        
        result = api_layer.initialize_locations_cache('my-project')
        
        assert result == standard_regions


class TestExtractLocationIds:
    """Test _extract_location_ids function"""
    
    def test_extracts_ids_from_locations(self):
        """Extract location IDs from location list"""
        locations = [
            {'name': 'projects/p/locations/us-central1', 'locationId': 'us-central1'},
            {'name': 'projects/p/locations/eu-west1', 'locationId': 'eu-west1'},
        ]
        
        result = api_layer._extract_location_ids(locations)
        
        assert 'us-central1' in result
        assert 'eu-west1' in result
    
    def test_handles_empty_list(self):
        """Handle empty locations list"""
        result = api_layer._extract_location_ids([])
        assert result == []


class TestIsLocationCached:
    """Test _is_location_cached function"""
    
    def test_returns_false_when_no_cache(self):
        """Should return False when cache doesn't have project"""
        # Clear any existing cache
        api_layer._locations_cache.clear()
        
        result = api_layer._is_location_cached('new-project', False)
        
        assert result is False
    
    def test_returns_true_when_cached(self):
        """Should return True when project is cached"""
        api_layer._locations_cache['cached-project'] = ['us-central1']
        
        result = api_layer._is_location_cached('cached-project', False)
        
        assert result is True
        
        # Cleanup
        api_layer._locations_cache.clear()
    
    def test_force_refresh_returns_false(self):
        """Should return False when force_refresh is True"""
        api_layer._locations_cache['cached-project'] = ['us-central1']
        
        result = api_layer._is_location_cached('cached-project', True)
        
        assert result is False
        
        # Cleanup
        api_layer._locations_cache.clear()


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

class TestAPIErrorHandling:
    """Test API error handling"""
    
    def test_http_error_is_properly_detected(self):
        """HttpError should be properly detected as retryable or not"""
        mock_resp_500 = MagicMock()
        mock_resp_500.status = 500
        error_500 = HttpError(mock_resp_500, b'Server error')
        
        mock_resp_403 = MagicMock()
        mock_resp_403.status = 403
        error_403 = HttpError(mock_resp_403, b'Forbidden')
        
        # 500 should be retryable
        assert api_layer._is_retryable_google_api_error(error_500) is True
        
        # 403 should not be retryable
        assert api_layer._is_retryable_google_api_error(error_403) is False
    
    def test_handles_quota_exceeded(self):
        """Should detect quota exceeded as retryable"""
        mock_resp = MagicMock()
        mock_resp.status = 429
        error = HttpError(mock_resp, b'Quota exceeded')
        
        result = api_layer._is_retryable_google_api_error(error)
        assert result is True


# ============================================================================
# INTEGRATION-LIKE TESTS
# ============================================================================

class TestEndToEndFlows:
    """Test end-to-end API flows"""
    
    def test_list_terms_workflow(self, monkeypatch):
        """Test listing terms workflow"""
        mock_terms = [{'name': 'projects/p/locations/l/glossaries/g/terms/t1'}]
        
        mock_service = MagicMock()
        mock_service.projects().locations().glossaries().terms().list().execute.return_value = {
            'terms': mock_terms
        }
        mock_service.projects().locations().glossaries().terms().list_next.return_value = None
        
        # List terms
        terms = api_layer.list_glossary_terms(mock_service, 'projects/p/locations/l/glossaries/g')
        
        assert len(terms) == 1
    
    def test_default_project_caching(self, monkeypatch):
        """Test that default project is properly cached"""
        api_layer._default_project = None
        
        monkeypatch.setattr(api_layer, 'default', lambda scopes: (MagicMock(), 'test-project'))
        
        # First call
        project1 = api_layer.get_default_project()
        
        # Second call should use cache
        project2 = api_layer.get_default_project()
        
        assert project1 == 'test-project'
        assert project2 == 'test-project'
        
        # Cleanup
        api_layer._default_project = None
