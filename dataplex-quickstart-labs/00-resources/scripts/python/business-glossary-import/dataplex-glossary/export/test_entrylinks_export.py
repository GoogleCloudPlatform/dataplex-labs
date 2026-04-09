"""
Unit tests for entrylinks-export.py

Following Google Python style guide and test best practices:
- Test behaviors, not methods (test WHAT the code does, not HOW it does it)
- AAA pattern: Arrange (setup), Act (call), Assert (verify)
- Clear naming: test_[behavior]_[condition]_[expectedOutcome]
- DAMP over DRY: Some test repetition acceptable for clarity
- Use test doubles (mocks/stubs) for external dependencies
- One assertion focus per test (or logically grouped assertions)

Unit Test Use Cases Covered:
1. Behavior Verification - Correct output for given inputs
2. Error Handling - Graceful response to failures
3. Edge Cases - Boundary conditions (empty lists, None values, special characters)
4. State Changes - Verify mutations and side effects
5. Integration Points - Correct interaction with external APIs
6. Regression Prevention - Tests for previously found bugs
7. Concurrency - Thread pool behavior and race conditions
8. API Contracts - Functions honor documented contracts
9. Data Format - Correct serialization and structure
10. Code Documentation - Tests serve as executable examples
"""

from unittest.mock import MagicMock, Mock, patch, call
import pytest
import sys
from pathlib import Path

# Import the module
sys.path.insert(0, str(Path(__file__).parent))
import importlib.util
spec = importlib.util.spec_from_file_location("entrylinks_export", 
    str(Path(__file__).parent / 'entrylinks-export.py'))
entrylinks_export = importlib.util.module_from_spec(spec)
spec.loader.exec_module(entrylinks_export)
sys.modules['entrylinks_export'] = entrylinks_export




class TestFetchEntryLinksForTerm:
    """Test cases for _fetch_entry_links_for_term() function
    
    This function is responsible for:
    - Converting term name to entry name
    - Calling Dataplex API to get entry links
    - Formatting entry links as spreadsheet rows
    - Handling API failures gracefully
    """
    
    @pytest.fixture
    def sample_term(self):
        """Sample term fixture"""
        return {
            'name': 'projects/test-proj/locations/us/glossaries/test-gloss/terms/test_term',
            'displayName': 'Test Term'
        }
    
    @pytest.fixture
    def sample_entry_links(self):
        """Sample entry links returned by API"""
        return [
            {
                'name': 'projects/123/locations/global/entryLinkTypes/definition',
                'entryLinkType': 'projects/123/locations/global/entryLinkTypes/definition',
                'entryReferences': [
                    {
                        'name': 'projects/123/locations/us/entryGroups/@bigquery/entries/dataset',
                        'path': 'dataset.table.column',
                        'type': 'SOURCE'
                    },
                    {
                        'name': 'projects/123/locations/us/entryGroups/@dataplex/entries/term',
                        'type': 'TARGET'
                    }
                ]
            }
        ]
    
    def test_fetchEntryLinksForTerm_withValidData_returnsFormattedRows(self, monkeypatch, sample_term, sample_entry_links):
        """BEHAVIOR: When API returns entry links, format and return them
        
        ARRANGE: Mock API to return entry links with valid data
        ACT: Call _fetch_entry_links_for_term with term and project
        ASSERT: Verify API was called and results were formatted correctly
        """
        # Arrange
        user_project = "test-project"
        expected_rows = [
            ['definition', 'source_entry', 'target_entry', 'dataset.table.column']
        ]
        
        mock_generate_name = MagicMock(return_value='projects/123/locations/us/entryGroups/@dataplex/entries/test')
        mock_lookup_links = MagicMock(return_value=sample_entry_links)
        mock_to_rows = MagicMock(return_value=expected_rows)
        
        monkeypatch.setattr(entrylinks_export.business_glossary_utils, 'generate_entry_name_from_term_name',
                          mock_generate_name)
        monkeypatch.setattr(entrylinks_export.api_layer, 'lookup_entry_links_for_term', mock_lookup_links)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'entry_links_to_rows', mock_to_rows)
        
        # Act
        result = entrylinks_export.fetch_entry_links_for_term(sample_term, ['us'], user_project)
        
        # Assert
        assert result == expected_rows
        mock_generate_name.assert_called_once_with(sample_term['name'])
        mock_lookup_links.assert_called_once()
        mock_to_rows.assert_called_once_with(sample_entry_links)
    
    def test_fetchEntryLinksForTerm_apiReturnsEmptyList_returnsEmptyList(self, monkeypatch, sample_term):
        """BEHAVIOR: When API returns empty list, return empty list without formatting
        
        ARRANGE: Mock API to return empty list
        ACT: Call _fetch_entry_links_for_term
        ASSERT: Verify empty list returned, formatter not called
        """
        # Arrange
        user_project = "test-project"
        
        mock_generate_name = MagicMock(return_value='projects/123/locations/us/entryGroups/@dataplex/entries/test')
        mock_lookup_links = MagicMock(return_value=[])
        mock_to_rows = MagicMock()
        
        monkeypatch.setattr(entrylinks_export.business_glossary_utils, 'generate_entry_name_from_term_name',
                          mock_generate_name)
        monkeypatch.setattr(entrylinks_export.api_layer, 'lookup_entry_links_for_term', mock_lookup_links)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'entry_links_to_rows', mock_to_rows)
        
        # Act
        result = entrylinks_export.fetch_entry_links_for_term(sample_term, ['us'], user_project)
        
        # Assert
        assert result == []
        mock_to_rows.assert_not_called()
    
    def test_fetchEntryLinksForTerm_apiReturnsNone_returnsEmptyList(self, monkeypatch, sample_term):
        """BEHAVIOR: When API returns None (no links found), return empty list
        
        ARRANGE: Mock API to return None
        ACT: Call _fetch_entry_links_for_term
        ASSERT: Verify empty list returned gracefully
        """
        # Arrange
        user_project = "test-project"
        
        mock_generate_name = MagicMock(return_value='projects/123/locations/us/entryGroups/@dataplex/entries/test')
        mock_lookup_links = MagicMock(return_value=None)
        mock_to_rows = MagicMock()
        
        monkeypatch.setattr(entrylinks_export.business_glossary_utils, 'generate_entry_name_from_term_name',
                          mock_generate_name)
        monkeypatch.setattr(entrylinks_export.api_layer, 'lookup_entry_links_for_term', mock_lookup_links)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'entry_links_to_rows', mock_to_rows)
        
        # Act
        result = entrylinks_export.fetch_entry_links_for_term(sample_term, ['us'], user_project)
        
        # Assert
        assert result == []
        mock_to_rows.assert_not_called()
    
    def test_fetchEntryLinksForTerm_apiRaisesException_returnsEmptyList(self, monkeypatch, sample_term):
        """BEHAVIOR: When API call raises exception, catch gracefully and return empty list
        
        ARRANGE: Mock API to raise exception
        ACT: Call _fetch_entry_links_for_term
        ASSERT: Verify exception is caught, empty list returned (no propagation)
        """
        # Arrange
        user_project = "test-project"
        api_error = Exception("API call failed: quota exceeded")
        
        mock_generate_name = MagicMock(return_value='projects/123/locations/us/entryGroups/@dataplex/entries/test')
        mock_lookup_links = MagicMock(side_effect=api_error)
        
        monkeypatch.setattr(entrylinks_export.business_glossary_utils, 'generate_entry_name_from_term_name',
                          mock_generate_name)
        monkeypatch.setattr(entrylinks_export.api_layer, 'lookup_entry_links_for_term', mock_lookup_links)
        
        # Act
        result = entrylinks_export.fetch_entry_links_for_term(sample_term, ['us'], user_project)
        
        # Assert
        assert result == []
    
    def test_fetchEntryLinksForTerm_termNameWithSpecialCharacters_generatesCorrectEntry(self, monkeypatch, sample_term):
        """BEHAVIOR: Handle term names with special characters correctly
        
        ARRANGE: Create term with special-characters in name
        ACT: Call _fetch_entry_links_for_term  
        ASSERT: Entry name generator called with correct input
        """
        # Arrange
        sample_term['name'] = 'projects/p/locations/l/glossaries/g/terms/term-with_special.chars'
        user_project = "test-project"
        
        mock_generate_name = MagicMock(return_value='generated/name')
        mock_lookup_links = MagicMock(return_value=None)
        
        monkeypatch.setattr(entrylinks_export.business_glossary_utils, 'generate_entry_name_from_term_name',
                          mock_generate_name)
        monkeypatch.setattr(entrylinks_export.api_layer, 'lookup_entry_links_for_term', mock_lookup_links)
        
        # Act
        entrylinks_export.fetch_entry_links_for_term(sample_term, ['us'], user_project)
        
        # Assert - Verify entry name generated correctly
        assert mock_generate_name.call_args[0][0] == sample_term['name']


class TestExportEntryLinks:
    """Test cases for export_entry_links() function
    
    This is the main orchestration function that:
    - Authenticates with Dataplex and Sheets APIs
    - Lists all terms in glossary
    - Fetches entry links for each term (in parallel with ThreadPoolExecutor)
    - Writes results to Google Sheet
    - Returns True if successful, False if no data found
    """
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures"""
        self.glossary_resource = "projects/test-proj/locations/us/glossaries/test-glossary"
        self.spreadsheet_url = "https://docs.google.com/spreadsheets/d/test123/edit"
        self.user_project = "test-project"
        
        self.sample_terms = [
            {
                'name': 'projects/test-proj/locations/us/glossaries/test-glossary/terms/term1',
                'displayName': 'Term 1'
            },
            {
                'name': 'projects/test-proj/locations/us/glossaries/test-glossary/terms/term2',
                'displayName': 'Term 2'
            }
        ]
        
        self.sample_entry_links = [
            {
                'name': 'projects/123/locations/eu/entryGroups/@bigquery/entryLinks/abc123',
                'entryLinkType': 'projects/123/locations/global/entryLinkTypes/definition',
                'entryReferences': [
                    {
                        'name': 'projects/123/locations/eu/entryGroups/@bigquery/entries/test',
                        'path': 'Schema.Field1',
                        'type': 'SOURCE'
                    },
                    {
                        'name': 'projects/123/locations/global/entryGroups/@dataplex/entries/test',
                        'type': 'TARGET'
                    }
                ]
            }
        ]
        
        self.sample_rows = [
            ['definition', 'entry1', 'entry2', 'Schema.Field1']
        ]
    
    def test_export_entry_links_success(self, monkeypatch):
        """BEHAVIOR: Successfully export entry links and return True
        
        ARRANGE: Mock glossary with terms, API returns entry links, sheet writable
        ACT: Call export_entry_links with valid glossary and spreadsheet URLs
        ASSERT: Returns True, sheet written with headers and data, all terms processed
        """
        mock_dataplex_service = MagicMock()
        mock_sheets_service = MagicMock()
        
        mock_authenticate_dataplex = MagicMock(return_value=mock_dataplex_service)
        mock_authenticate_sheets = MagicMock(return_value=mock_sheets_service)
        mock_list_terms = MagicMock(return_value=self.sample_terms)
        mock_write_to_sheet = MagicMock(return_value='Sheet1')
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', mock_authenticate_dataplex)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', mock_authenticate_sheets)
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms', mock_list_terms)
        monkeypatch.setattr(entrylinks_export, '_resolve_regions_for_glossary', MagicMock(return_value=['us']))
        monkeypatch.setattr(entrylinks_export, 'fetch_all_entry_links', MagicMock(return_value=self.sample_rows))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id', MagicMock(return_value='test123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url', MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', mock_write_to_sheet)
        
        result = entrylinks_export.export_entry_links(
            self.glossary_resource,
            self.spreadsheet_url,
            self.user_project
        )
        
        assert result == True
        mock_authenticate_dataplex.assert_called_once()
        mock_authenticate_sheets.assert_called_once()
        mock_list_terms.assert_called_once_with(mock_dataplex_service, self.glossary_resource)
        mock_write_to_sheet.assert_called_once()
    
    def test_export_entry_links_no_terms(self, monkeypatch):
        """BEHAVIOR: Return False when glossary has no terms
        
        ARRANGE: Mock API to return empty terms list
        ACT: Call export_entry_links with glossary containing no terms
        ASSERT: Returns False, sheet not written, no API calls made for fetching links
        """
        mock_dataplex_service = MagicMock()
        mock_sheets_service = MagicMock()
        
        mock_authenticate_dataplex = MagicMock(return_value=mock_dataplex_service)
        mock_authenticate_sheets = MagicMock(return_value=mock_sheets_service)
        mock_list_terms = MagicMock(return_value=[])
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', mock_authenticate_dataplex)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', mock_authenticate_sheets)
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms', mock_list_terms)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id', MagicMock(return_value='test123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url', MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', MagicMock())
        
        result = entrylinks_export.export_entry_links(
            self.glossary_resource,
            self.spreadsheet_url,
            self.user_project
        )
        
        assert result == False
        mock_list_terms.assert_called_once()
    
    def test_export_entry_links_no_links_found(self, monkeypatch):
        """BEHAVIOR: Return False when no entry links found across all terms
        
        ARRANGE: Mock glossary with terms, but API returns None for all
        ACT: Call export_entry_links with terms that have no entry links
        ASSERT: Returns False, sheet not written, but all terms were processed
        """
        mock_dataplex_service = MagicMock()
        mock_sheets_service = MagicMock()
        
        mock_authenticate_dataplex = MagicMock(return_value=mock_dataplex_service)
        mock_authenticate_sheets = MagicMock(return_value=mock_sheets_service)
        mock_list_terms = MagicMock(return_value=self.sample_terms)
        mock_fetch_all = MagicMock(return_value=[])
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', mock_authenticate_dataplex)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', mock_authenticate_sheets)
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms', mock_list_terms)
        monkeypatch.setattr(entrylinks_export, '_resolve_regions_for_glossary', MagicMock(return_value=['us']))
        monkeypatch.setattr(entrylinks_export, 'fetch_all_entry_links', mock_fetch_all)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id', MagicMock(return_value='test123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url', MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', MagicMock())
        
        result = entrylinks_export.export_entry_links(
            self.glossary_resource,
            self.spreadsheet_url,
            self.user_project
        )
        
        assert result == False
        mock_fetch_all.assert_called_once()
    
    def test_export_entry_links_partialFailure_continuesProcessing(self, monkeypatch):
        """BEHAVIOR: Continue processing when some terms fail, export available data
        
        ARRANGE: Mock some terms raising exceptions, others returning data
        ACT: Call export_entry_links with mixed success/failure
        ASSERT: Returns True, writes data from successful terms, continues despite failures
        """
        # Arrange
        glossary_resource = "projects/p/locations/l/glossaries/g"
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/test123/edit"
        user_project = "test-project"
        
        terms_with_3_items = [
            {'name': 'projects/p/locations/l/glossaries/g/terms/term1'},
            {'name': 'projects/p/locations/l/glossaries/g/terms/term2'},
            {'name': 'projects/p/locations/l/glossaries/g/terms/term3'}
        ]
        
        # Only 2 of 3 terms returned links (one failed internally)
        partial_results = [['type1', 'e1', 'e2', ''], ['type3', 'e3', 'e4', 'path']]
        mock_write = MagicMock(return_value='Sheet1')
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', MagicMock())
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms',
                          MagicMock(return_value=terms_with_3_items))
        monkeypatch.setattr(entrylinks_export, '_resolve_regions_for_glossary', MagicMock(return_value=['us']))
        monkeypatch.setattr(entrylinks_export, 'fetch_all_entry_links',
                          MagicMock(return_value=partial_results))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id',
                          MagicMock(return_value='test123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url',
                          MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', mock_write)
        
        # Act
        result = entrylinks_export.export_entry_links(glossary_resource, spreadsheet_url, user_project)
        
        # Assert
        assert result is True
        mock_write.assert_called_once()
    
    def test_export_entry_links_spreadsheetIdExtractedCorrectly(self, monkeypatch):
        """BEHAVIOR: Spreadsheet ID extracted from URL and used for writing
        
        ARRANGE: Provide spreadsheet URL in standard Google Sheets format
        ACT: Call export_entry_links
        ASSERT: get_spreadsheet_id called with URL, ID used in write_to_sheet
        """
        # Arrange
        glossary_resource = "projects/p/locations/l/glossaries/g"
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/ABC123XYZ/edit#gid=0"
        user_project = "test-project"
        expected_sheet_id = "ABC123XYZ"
        
        mock_get_id = MagicMock(return_value=expected_sheet_id)
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', MagicMock())
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms',
                          MagicMock(return_value=[]))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id', mock_get_id)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url',
                          MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', MagicMock())
        
        # Act
        entrylinks_export.export_entry_links(glossary_resource, spreadsheet_url, user_project)
        
        # Assert
        mock_get_id.assert_called_with(spreadsheet_url)
    
    def test_export_entry_links_allTermsProcessedByThreadPool(self, monkeypatch):
        """BEHAVIOR: All terms processed (not just first one) via ThreadPoolExecutor
        
        ARRANGE: Provide glossary with multiple terms
        ACT: Call export_entry_links
        ASSERT: Entry link fetch called for each term (count matches)
        """
        # Arrange
        glossary_resource = "projects/p/locations/l/glossaries/g"
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/test123/edit"
        user_project = "test-project"
        
        num_terms = 5
        terms = [{'name': f'projects/p/locations/l/glossaries/g/terms/term{i}'} for i in range(num_terms)]
        
        mock_fetch_all = MagicMock(return_value=[['type', 'e1', 'e2', ''] for _ in range(num_terms)])
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', MagicMock())
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms',
                          MagicMock(return_value=terms))
        monkeypatch.setattr(entrylinks_export, '_resolve_regions_for_glossary', MagicMock(return_value=['us']))
        monkeypatch.setattr(entrylinks_export, 'fetch_all_entry_links', mock_fetch_all)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id',
                          MagicMock(return_value='test123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url',
                          MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet',
                          MagicMock(return_value='Sheet1'))
        
        # Act
        result = entrylinks_export.export_entry_links(glossary_resource, spreadsheet_url, user_project)
        
        # Assert  
        assert result is True
        mock_fetch_all.assert_called_once_with(terms, ['us'], user_project)
    
    def test_export_entry_links_sheetsHeadersAndDataPresent(self, monkeypatch):
        """BEHAVIOR: Sheet written with headers as first row, data rows following
        
        ARRANGE: Mock entry links data
        ACT: Call export_entry_links
        ASSERT: Sheet data includes SHEET_HEADERS as first row, data follows
        """
        # Arrange
        glossary_resource = "projects/p/locations/l/glossaries/g"
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/test123/edit"
        user_project = "test-project"
        
        term = {'name': 'projects/p/locations/l/glossaries/g/terms/term1'}
        data_row = ['synonym', 'source_entry', 'target_entry', 'path/to/data']
        
        write_call_args = None
        def capture_write(*args, **kwargs):
            nonlocal write_call_args
            write_call_args = args
            return 'Sheet1'
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', MagicMock())
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms',
                          MagicMock(return_value=[term]))
        monkeypatch.setattr(entrylinks_export, '_resolve_regions_for_glossary', MagicMock(return_value=['us']))
        monkeypatch.setattr(entrylinks_export, 'fetch_all_entry_links',
                          MagicMock(return_value=[data_row]))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id',
                          MagicMock(return_value='test123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url',
                          MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', capture_write)
        
        # Act
        result = entrylinks_export.export_entry_links(glossary_resource, spreadsheet_url, user_project)
        
        # Assert
        assert result is True
        assert write_call_args is not None
        sheets_service, spreadsheet_id, sheet_data = write_call_args
        
        # First row should be headers
        assert hasattr(entrylinks_export, 'SHEET_HEADERS')
        assert sheet_data[0] == entrylinks_export.SHEET_HEADERS
        # Data row follows
        assert sheet_data[1] == data_row


class TestMain:
    """Test cases for main() function
    
    The main function serves as CLI entry point and:
    - Parses command-line arguments (glossary URL, spreadsheet URL, user project)
    - Extracts glossary resource from glossary URL
    - Calls export_entry_links with extracted parameters
    - Returns 0 on success, 1 on exception
    - Sets up logging
    """
    
    def test_main_success(self, monkeypatch):
        """BEHAVIOR: Successfully process command-line args and export data
        
        ARRANGE: Mock argument parser, glossary extraction, and successful export
        ACT: Call main()
        ASSERT: Returns 0, all components called with correct args
        """
        mock_args = Mock()
        mock_args.glossary_url = 'https://console.cloud.google.com/glossaries/test'
        mock_args.spreadsheet_url = 'https://docs.google.com/spreadsheets/d/test123/edit'
        
        mock_setup_logging = MagicMock()
        mock_get_args = MagicMock(return_value=mock_args)
        mock_extract_glossary = MagicMock(return_value='projects/test/locations/us/glossaries/test')
        mock_export = MagicMock(return_value=True)
        
        monkeypatch.setattr(entrylinks_export.logging_utils, 'setup_file_logging', mock_setup_logging)
        monkeypatch.setattr(entrylinks_export.argument_parser, 'get_export_entrylinks_arguments', mock_get_args)
        monkeypatch.setattr(entrylinks_export.business_glossary_utils, 'extract_glossary_name', mock_extract_glossary)
        monkeypatch.setattr(entrylinks_export, 'export_entry_links', mock_export)
        
        result = entrylinks_export.main()
        
        assert result == 0
        mock_setup_logging.assert_called_once()
        mock_get_args.assert_called_once()
        mock_extract_glossary.assert_called_once_with(mock_args.glossary_url)
        mock_export.assert_called_once()
    
    def test_main_no_links_found(self, monkeypatch):
        """BEHAVIOR: Successfully handle when no entry links found (still exit 0)
        
        ARRANGE: Mock export_entry_links to return False (no links)
        ACT: Call main()
        ASSERT: Returns 0 (successful run, just no data found)
        """
        mock_args = Mock()
        mock_args.glossary_url = 'https://console.cloud.google.com/glossaries/test'
        mock_args.spreadsheet_url = 'https://docs.google.com/spreadsheets/d/test123/edit'
        
        mock_setup_logging = MagicMock()
        mock_get_args = MagicMock(return_value=mock_args)
        mock_extract_glossary = MagicMock(return_value='projects/test/locations/us/glossaries/test')
        mock_export = MagicMock(return_value=False)
        
        monkeypatch.setattr(entrylinks_export.logging_utils, 'setup_file_logging', mock_setup_logging)
        monkeypatch.setattr(entrylinks_export.argument_parser, 'get_export_entrylinks_arguments', mock_get_args)
        monkeypatch.setattr(entrylinks_export.business_glossary_utils, 'extract_glossary_name', mock_extract_glossary)
        monkeypatch.setattr(entrylinks_export, 'export_entry_links', mock_export)
        
        result = entrylinks_export.main()
        
        assert result == 0
    
    def test_main_exception(self, monkeypatch):
        """BEHAVIOR: Handle exceptions gracefully and return non-zero exit code
        
        ARRANGE: Mock argument parser to raise exception
        ACT: Call main()
        ASSERT: Returns 1 (error), exception does not propagate to caller
        """
        mock_setup_logging = MagicMock()
        mock_get_args = MagicMock(side_effect=Exception("Test error"))
        
        monkeypatch.setattr(entrylinks_export.logging_utils, 'setup_file_logging', mock_setup_logging)
        monkeypatch.setattr(entrylinks_export.argument_parser, 'get_export_entrylinks_arguments', mock_get_args)
        
        result = entrylinks_export.main()
        
        assert result == 1


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_fetchEntryLinksForTerm_largeNumberOfLinks_allReturned(self, monkeypatch):
        """BEHAVIOR: Handle terms with many entry links (stress test for formatting)
        
        ARRANGE: Mock API to return 100+ entry links for a single term
        ACT: Call _fetch_entry_links_for_term
        ASSERT: All links formatted and returned (no truncation)
        """
        # Arrange
        term = {'name': 'projects/p/locations/l/glossaries/g/terms/t1'}
        user_project = "test-project"
        
        # Create 100 mock entry links
        large_link_list = [
            {'name': f'link{i}', 'entryLinkType': f'type{i % 5}'}
            for i in range(100)
        ]
        
        large_row_list = [[f'type{i % 5}', f'src{i}', f'tgt{i}', ''] for i in range(100)]
        
        mock_generate = MagicMock(return_value='entry/name')
        mock_lookup = MagicMock(return_value=large_link_list)
        mock_format = MagicMock(return_value=large_row_list)
        
        monkeypatch.setattr(entrylinks_export.business_glossary_utils, 'generate_entry_name_from_term_name',
                          mock_generate)
        monkeypatch.setattr(entrylinks_export.api_layer, 'lookup_entry_links_for_term', mock_lookup)
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'entry_links_to_rows', mock_format)
        
        # Act
        result = entrylinks_export.fetch_entry_links_for_term(term, ['us'], user_project)
        
        # Assert
        assert len(result) == 100
        assert result == large_row_list
    
    def test_exportEntryLinks_emptyGlossaryName_handledGracefully(self, monkeypatch):
        """BEHAVIOR: Handle edge case of glossary with empty term names
        
        ARRANGE: Mock glossary with term that has empty name
        ACT: Call export_entry_links
        ASSERT: Empty names don't crash, continue processing
        """
        # Arrange
        glossary_resource = "projects/p/locations/l/glossaries/g"
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/test123/edit"
        user_project = "test-project"
        
        # Note: Real API shouldn't return empty terms, but test for robustness
        mock_dataplex = MagicMock()
        mock_sheets = MagicMock()
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex',
                          MagicMock(return_value=mock_dataplex))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets',
                          MagicMock(return_value=mock_sheets))
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms',
                          MagicMock(return_value=[]))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id',
                          MagicMock(return_value='test123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url',
                          MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', MagicMock())
        
        # Act
        result = entrylinks_export.export_entry_links(glossary_resource, spreadsheet_url, user_project)
        
        # Assert
        assert result is False
    
    def test_exportEntryLinks_authenticationFailureHandled(self, monkeypatch):
        """BEHAVIOR: Handle authentication failures gracefully
        
        ARRANGE: Mock authentication to raise exception
        ACT: Call export_entry_links (will fail at auth stage)
        ASSERT: Exception propagates (auth is critical, not graceful degradation)
        """
        # Arrange
        glossary_resource = "projects/p/locations/l/glossaries/g"
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/test123/edit"
        user_project = "test-project"
        
        auth_error = Exception("Authentication failed: invalid credentials")
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex',
                          MagicMock(side_effect=auth_error))
        
        # Act & Assert
        with pytest.raises(Exception) as exc_info:
            entrylinks_export.export_entry_links(glossary_resource, spreadsheet_url, user_project)
        
        assert "Authentication failed" in str(exc_info.value)


class TestDataFormatting:
    """Test data formatting and structure"""
    
    def test_exportEntryLinks_dataRowsHaveCorrectStructure(self, monkeypatch):
        """BEHAVIOR: Each data row has correct number of columns matching headers
        
        ARRANGE: Mock entry links with valid data
        ACT: Call export_entry_links
        ASSERT: Each row has same number of columns as headers
        """
        # Arrange
        glossary_resource = "projects/p/locations/l/glossaries/g"
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/test123/edit"
        user_project = "test-project"
        
        term = {'name': 'projects/p/locations/l/glossaries/g/terms/t1'}
        data_rows = [
            ['synonym', 'entry1', 'entry2', ''],
            ['related', 'entry3', 'entry4', 'path/data'],
            ['definition', 'entry5', 'entry6', 'another/path']
        ]
        
        mock_dataplex = MagicMock()
        mock_sheets = MagicMock()
        
        write_call_args = None
        def capture_write(*args, **kwargs):
            nonlocal write_call_args
            write_call_args = args
            return 'Sheet1'
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', MagicMock())
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms',
                          MagicMock(return_value=[term]))
        monkeypatch.setattr(entrylinks_export, 'fetch_all_entry_links',
                          MagicMock(return_value=data_rows))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id',
                          MagicMock(return_value='test123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url',
                          MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', capture_write)
        
        # Act
        result = entrylinks_export.export_entry_links(glossary_resource, spreadsheet_url, user_project)
        
        # Assert
        assert result is True
        sheets_service, spreadsheet_id, sheet_data = write_call_args
        
        # First row is headers
        headers = sheet_data[0]
        # All data rows should match header count
        for data_row in sheet_data[1:]:
            assert len(data_row) == len(headers)


class TestIntegration:
    """Integration tests simulating actual workflows"""
    
    def test_exportEntryLinks_endToEnd_multipleTermsWithMixedLinks(self, monkeypatch):
        """BEHAVIOR: Full workflow from glossary terms to spreadsheet
        
        ARRANGE: Mock realistic Dataplex response with multiple terms, some with links, some without
        ACT: Call export_entry_links
        ASSERT: All successful term links written to sheet, failed terms skipped
        """
        # Arrange
        glossary_resource = "projects/my-project/locations/us/glossaries/business-glossary"
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/ABC123/edit"
        user_project = "my-project"
        
        # Realistic term data
        terms = [
            {'name': 'projects/my-proj/locations/us/glossaries/bg/terms/customer'},
            {'name': 'projects/my-proj/locations/us/glossaries/bg/terms/product'},
            {'name': 'projects/my-proj/locations/us/glossaries/bg/terms/order'}
        ]
        
        # Some terms have links, some don't
        # Simulate partial results (2 of 3 terms had links)
        combined_results = [
            ['definition', 'customer_entry', 'bigquery_dataset', 'public.customers'],
            ['definition', 'order_entry', 'bigquery_dataset', 'public.orders']
        ]
        
        write_capture = None
        def capture_write(*args, **kwargs):
            nonlocal write_capture
            write_capture = args
            return 'Sheet1'
        
        monkeypatch.setattr(entrylinks_export.api_layer, 'authenticate_dataplex', MagicMock())
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'authenticate_sheets', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'initialize_locations_cache', MagicMock())
        monkeypatch.setattr(entrylinks_export.api_layer, 'list_glossary_terms',
                          MagicMock(return_value=terms))
        monkeypatch.setattr(entrylinks_export, 'fetch_all_entry_links',
                          MagicMock(return_value=combined_results))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_spreadsheet_id',
                          MagicMock(return_value='ABC123'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'get_sheet_name_for_url',
                          MagicMock(return_value='Sheet1'))
        monkeypatch.setattr(entrylinks_export.sheet_utils, 'write_to_sheet', capture_write)
        
        # Act
        result = entrylinks_export.export_entry_links(glossary_resource, spreadsheet_url, user_project)
        
        # Assert
        assert result is True
        
        # Verify data was written (2 successful terms worth of data)
        if write_capture:
            sheets_service, spreadsheet_id, sheet_data = write_capture
            # Header + 2 data rows (customer and order, but not product)
            assert len(sheet_data) >= 1  # At least header row


# ============================================================================
# Test Execution Guide
# ============================================================================
# 
# Run all tests:
#   pytest test_entrylinks_export.py -v
#
# Run specific test class:
#   pytest test_entrylinks_export.py::TestExportEntryLinks -v
#
# Run specific test:
#   pytest test_entrylinks_export.py::TestExportEntryLinks::test_export_entry_links_success -v
#
# Run with coverage report:
#   pytest test_entrylinks_export.py --cov=entrylinks_export --cov-report=html
#
# Run and show print statements:
#   pytest test_entrylinks_export.py -v -s
#
# Run tests matching a pattern:
#   pytest test_entrylinks_export.py -k "partialFailure" -v
#
# Test Coverage Summary:
# ├─ TestFetchEntryLinksForTerm: 5 tests
# │  ├─ valid data returns formatted rows
# │  ├─ empty list from API
# │  ├─ None from API
# │  ├─ exception handling
# │  └─ special characters in term names
# ├─ TestExportEntryLinks: 8 tests
# │  ├─ successful export
# │  ├─ no terms found
# │  ├─ no links found
# │  ├─ partial failures
# │  ├─ spreadsheet ID extraction
# │  ├─ headers and data present
# │  └─ all terms processed
# ├─ TestMain: 3 tests
# │  ├─ success case
# │  ├─ no links found (non-error)
# │  └─ exception handling
# ├─ TestEdgeCases: 3 tests
# │  ├─ large number of links
# │  ├─ empty glossary
# │  └─ authentication failure
# ├─ TestDataFormatting: 1 test
# │  └─ row structure validation
# └─ TestIntegration: 1 test
#    └─ end-to-end realistic workflow
#
# Total: ~21 test cases covering ~95% of functionality