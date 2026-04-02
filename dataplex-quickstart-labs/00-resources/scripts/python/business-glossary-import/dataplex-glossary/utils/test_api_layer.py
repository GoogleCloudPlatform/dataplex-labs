"""
Unit tests for api_layer.py
Tests all functions for Dataplex API interactions.
"""

from unittest.mock import MagicMock, Mock, call

import pytest

# Direct imports since test is in same directory as source
from . import api_layer
from .error import DataplexAPIError, InvalidEntryIdFormatError


class TestAuthentication:
    """Test cases for authentication functions"""
    
    def test_authenticate_dataplex_success(self, monkeypatch):
        """Test successful Dataplex authentication"""
        mock_creds = MagicMock()
        mock_default = MagicMock(return_value=(mock_creds, None))
        mock_service = MagicMock()
        mock_build = MagicMock(return_value=mock_service)
        
        monkeypatch.setattr('utils.api_layer.default', mock_default)
        monkeypatch.setattr('utils.api_layer.build', mock_build)
        
        result = api_layer.authenticate_dataplex()
        
        assert result == mock_service
        mock_default.assert_called_once()
        mock_build.assert_called_once_with('dataplex', 'v1', credentials=mock_creds, cache_discovery=False)
    
    def test_authenticate_dataplex_failure(self, monkeypatch):
        """Test failed Dataplex authentication"""
        mock_default = MagicMock(side_effect=Exception("Auth failed"))
        monkeypatch.setattr('utils.api_layer.default', mock_default)
        
        with pytest.raises(DataplexAPIError):
            api_layer.authenticate_dataplex()


class TestGlossaryOperations:
    """Test cases for glossary operations"""
    
    def test_list_glossary_terms_success(self):
        """Test successful listing of glossary terms"""
        mock_dataplex_service = MagicMock()
        glossary_name = 'projects/test-proj/locations/us/glossaries/test-glossary'
        mock_terms = [{'name': 'term1'}, {'name': 'term2'}]
        mock_response = {'terms': mock_terms}
        
        mock_dataplex_service.projects().locations().glossaries().terms().list().execute.return_value = mock_response
        mock_dataplex_service.projects().locations().glossaries().terms().list_next.return_value = None
        
        result = api_layer.list_glossary_terms(mock_dataplex_service, glossary_name)
        
        assert result == mock_terms
    
    def test_list_glossary_terms_with_pagination(self):
        """Test listing glossary terms with pagination"""
        mock_dataplex_service = MagicMock()
        glossary_name = 'projects/test-proj/locations/us/glossaries/test-glossary'
        mock_response1 = {'terms': [{'name': 'term1'}], 'nextPageToken': 'token1'}
        mock_response2 = {'terms': [{'name': 'term2'}]}
        
        # First request from list(), second from list_next()
        mock_first_request = MagicMock()
        mock_first_request.execute.return_value = mock_response1
        mock_second_request = MagicMock()
        mock_second_request.execute.return_value = mock_response2
        
        mock_dataplex_service.projects().locations().glossaries().terms().list.return_value = mock_first_request
        mock_dataplex_service.projects().locations().glossaries().terms().list_next.side_effect = [mock_second_request, None]
        
        result = api_layer.list_glossary_terms(mock_dataplex_service, glossary_name)
        
        assert len(result) == 2
    
    def test_list_glossary_terms_empty(self):
        """Test listing glossary terms when none exist"""
        mock_dataplex_service = MagicMock()
        glossary_name = 'projects/test-proj/locations/us/glossaries/test-glossary'
        mock_response = {}
        
        mock_dataplex_service.projects().locations().glossaries().terms().list().execute.return_value = mock_response
        mock_dataplex_service.projects().locations().glossaries().terms().list_next.return_value = None
        
        result = api_layer.list_glossary_terms(mock_dataplex_service, glossary_name)
        
        assert result == []


class TestEntryLookup:
    """Test cases for entry lookup functions"""
    
    def test_lookup_entry_links_success(self, monkeypatch):
        """Test successful entry links lookup"""
        entry_id = 'projects/test-proj/locations/us/entryGroups/@dataplex/entries/12345'
        mock_response = {
            'json': {
                'entryLinks': [{'name': 'link1'}]
            }
        }
        mock_fetch = MagicMock(return_value=mock_response)
        monkeypatch.setattr('utils.api_layer.fetch_api_response', mock_fetch)
        
        result = api_layer.lookup_entry_links_for_term(entry_id, 'test-proj')
        
        assert result == [{'name': 'link1'}]
    
    def test_lookup_entry_links_none_found(self, monkeypatch):
        """Test entry links lookup when none found"""
        entry_id = 'projects/test-proj/locations/us/entryGroups/@dataplex/entries/12345'
        mock_response = {'json': {}}
        mock_fetch = MagicMock(return_value=mock_response)
        monkeypatch.setattr('utils.api_layer.fetch_api_response', mock_fetch)
        
        result = api_layer.lookup_entry_links_for_term(entry_id, 'test-proj')
        
        assert result is None
    
    def test_build_entry_lookup_url(self):
        """Test building entry lookup URL"""
        entry_id = 'entry123'
        project_id = 'test-proj'
        location_id = 'us-central1'
        
        result = api_layer.build_entry_lookup_url(entry_id, project_id, location_id)
        
        assert project_id in result
        assert location_id in result
        assert entry_id in result
    
    def test_lookup_entry_success(self):
        """Test successful entry lookup"""
        mock_dataplex_service = MagicMock()
        entry_id = 'entry123'
        project_location = 'projects/test/locations/us'
        mock_response = {'name': entry_id}
        
        mock_dataplex_service.projects().locations().lookupEntry().execute.return_value = mock_response
        
        result = api_layer.lookup_entry(mock_dataplex_service, entry_id, project_location)
        
        assert result == mock_response


class TestProjectOperations:
    """Test cases for project operations"""
    
    def test_get_project_url(self):
        """Test building project URL"""
        project_id = 'test-project'
        
        result = api_layer._get_project_url(project_id)
        
        assert project_id in result
        assert 'cloudresourcemanager' in result
    
    def test_fetch_project_info_success(self, monkeypatch):
        """Test fetching project info"""
        project_id = 'test-project'
        mock_response = {'json': {'name': 'projects/123456789'}, 'error_msg': None}
        mock_fetch = MagicMock(return_value=mock_response)
        monkeypatch.setattr('utils.api_call_utils.fetch_api_response', mock_fetch)
        
        result = api_layer._fetch_project_info(project_id, project_id)
        
        assert result == {'name': 'projects/123456789'}
    
    def test_extract_project_number_from_info(self):
        """Test extracting project number"""
        project_info = {'name': 'projects/123456789'}
        
        result = api_layer._extract_project_number_from_info(project_info)
        
        assert result == '123456789'
    
    def test_get_project_number_success(self, monkeypatch):
        """Test getting project number"""
        project_id = 'test-project'
        project_info = {'name': 'projects/123456789'}
        
        mock_fetch = MagicMock(return_value=project_info)
        mock_extract = MagicMock(return_value='123456789')
        
        monkeypatch.setattr('utils.api_layer._fetch_project_info', mock_fetch)
        monkeypatch.setattr('utils.api_layer._extract_project_number_from_info', mock_extract)
        
        result = api_layer.get_project_number(project_id, project_id)
        
        assert result == '123456789'
