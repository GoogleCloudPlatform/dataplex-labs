"""
Unit tests for api_layer.py

Test coverage:
- Entry ID parsing (_parse_entry_id_components)
- Entry links pagination (_fetch_entry_links_page)
- Authentication (authenticate_dataplex)
- Location management (list_supported_locations, resolve_regions_to_query)
- Glossary operations (list_glossary_terms, get_glossary)
- Entry operations (lookup_entry_links_for_term)
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
# ENTRY ID PARSING TESTS
# ============================================================================

class TestParseEntryIdComponents:
    """Test _parse_entry_id_components function"""
    
    def test_parses_standard_entry_id(self):
        """Parse standard entry ID format - returns tuple (project, location, entry_group, entry_id)"""
        entry_id = 'projects/my-project/locations/us-central1/entryGroups/my-group/entries/my-entry'
        
        project, location, entry_group, entry_id_parsed = api_layer.parse_entry_name(entry_id)
        
        assert project == 'my-project'
        assert location == 'us-central1'
        assert entry_group == 'my-group'
        assert entry_id_parsed == 'my-entry'
    
    def test_parses_global_location(self):
        """Parse entry with global location"""
        entry_id = 'projects/proj/locations/global/entryGroups/eg/entries/e'
        
        project, location, entry_group, entry_id_parsed = api_layer.parse_entry_name(entry_id)
        
        assert location == 'global'
        assert entry_group == 'eg'
        assert entry_id_parsed == 'e'
    
    def test_handles_special_characters(self):
        """Parse entry with special characters in names"""
        entry_id = 'projects/my-project-123/locations/us-west1/entryGroups/group_1/entries/entry_a-b'
        
        project, location, entry_group, entry_id_parsed = api_layer.parse_entry_name(entry_id)
        
        assert project == 'my-project-123'
        assert location == 'us-west1'
        assert entry_group == 'group_1'
        assert entry_id_parsed == 'entry_a-b'
    
    def test_raises_on_invalid_format(self):
        """Invalid format should raise InvalidEntryIdFormatError"""
        from utils.error import InvalidEntryIdFormatError
        
        with pytest.raises(InvalidEntryIdFormatError):
            api_layer.parse_entry_name('invalid-format')
    
    def test_raises_on_empty_string(self):
        """Empty string should raise InvalidEntryIdFormatError"""
        from utils.error import InvalidEntryIdFormatError
        
        with pytest.raises(InvalidEntryIdFormatError):
            api_layer.parse_entry_name('')


# ============================================================================
# ENTRY LINKS PAGINATION TESTS
# ============================================================================

class TestFetchEntryLinksPage:
    """Test _fetch_entry_links_page function"""
    
    def test_fetches_single_page(self, monkeypatch):
        """Fetch single page of results"""
        mock_api_response = {
            'json': {
                'entryLinks': [{'name': 'link1'}, {'name': 'link2'}]
            },
            'error_msg': None
        }
        monkeypatch.setattr(api_layer, 'fetch_api_response', lambda **kwargs: mock_api_response)
        
        entry_links, next_token, error_msg = api_layer._fetch_entry_links_page(
            'projects/p/locations/us/entryGroups/eg/entries/e', 'p', 'us', 'billing-project'
        )
        
        assert len(entry_links) == 2
        assert error_msg is None
    
    def test_handles_empty_response(self, monkeypatch):
        """Handle empty response from API"""
        mock_api_response = {
            'json': {},
            'error_msg': None
        }
        monkeypatch.setattr(api_layer, 'fetch_api_response', lambda **kwargs: mock_api_response)
        
        entry_links, next_token, error_msg = api_layer._fetch_entry_links_page(
            'projects/p/locations/us/entryGroups/eg/entries/e', 'p', 'us', 'billing-project'
        )
        
        assert entry_links == []
        assert next_token is None
        assert error_msg is None


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
        """Should return None for invalid entry ID (exception is caught internally)"""
        result = api_layer.lookup_entry_links_for_term('invalid-entry', 'project')
        
        assert result is None


# ============================================================================
# REGION RESOLUTION TESTS
# ============================================================================

class TestResolveRegionsToQuery:
    """Test resolve_regions_to_query function"""
    
    def test_global_returns_all_locations(self, monkeypatch):
        """Global location should return all available locations"""
        all_locations = ['global', 'us', 'eu', 'us-central1', 'eu-west1']
        
        monkeypatch.setattr(api_layer, 'list_supported_locations', lambda p: all_locations)
        
        result = api_layer.resolve_regions_to_query('global', 'my-project')
        
        assert result == all_locations
    
    def test_regional_returns_single_region(self, monkeypatch):
        """Regional location should return only that region"""
        result = api_layer.resolve_regions_to_query('us-central1', 'my-project')
        
        assert result == ['us-central1']
    
    def test_multiregion_returns_single_location(self, monkeypatch):
        """Multi-region (us, eu) should return just that location"""
        result = api_layer.resolve_regions_to_query('us', 'my-project')
        
        assert result == ['us']


# ============================================================================
# LOCATION FUNCTIONS TESTS
# ============================================================================

class TestInitializeLocationsCache:
    """Test initialize_locations_cache function"""
    
    def test_initializes_cache(self, monkeypatch):
        """Should initialize locations cache with all locations"""
        all_locations = ['global', 'us', 'eu', 'us-central1', 'eu-west1']
        
        monkeypatch.setattr(api_layer, 'list_supported_locations', lambda p: all_locations)
        
        result = api_layer.initialize_locations_cache('my-project')
        
        assert result == all_locations


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
