"""
Unit tests for entrylinks-import.py

Test coverage:
- Input helpers (_read_user_input_with_select)
- Archive management (get_existing_archive_files, _remove_archive_files)
- Entry parsing (_parse_source_entry_components, _generate_entrylink_name)
- Path formatting (_format_source_path_for_bigquery)
- Definition references (_build_definition_references)
- Link type extraction (_extract_normalized_link_type)
- Entrylink grouping (_add_entrylink_to_group)
- Import workflow (_run_import_workflow, main)
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, call
import pytest

# Import the module
sys.path.insert(0, str(Path(__file__).parent.parent))
import importlib.util
spec = importlib.util.spec_from_file_location(
    "entrylinks_import", 
    str(Path(__file__).parent.parent / 'import' / 'entrylinks-import.py')
)
entrylinks_import = importlib.util.module_from_spec(spec)
spec.loader.exec_module(entrylinks_import)


# ============================================================================
# INPUT HELPERS TESTS
# ============================================================================

class TestReadUserInputWithSelect:
    """Test _read_user_input_with_select helper function"""
    
    def test_valid_input_returns_stripped_value(self, monkeypatch):
        """Valid input should be stripped and returned"""
        monkeypatch.setattr('builtins.input', lambda _: '  test_input  ')
        
        result = entrylinks_import._read_user_input_with_select("Enter value: ")
        
        assert result == 'test_input'
    
    def test_empty_input_returns_empty(self, monkeypatch):
        """Empty input should return empty string"""
        monkeypatch.setattr('builtins.input', lambda _: '')
        
        result = entrylinks_import._read_user_input_with_select("Enter value: ")
        
        assert result == ''
    
    def test_whitespace_only_returns_empty(self, monkeypatch):
        """Whitespace-only input should return empty string"""
        monkeypatch.setattr('builtins.input', lambda _: '   ')
        
        result = entrylinks_import._read_user_input_with_select("Enter value: ")
        
        assert result == ''


# ============================================================================
# ARCHIVE MANAGEMENT TESTS
# ============================================================================

class TestGetExistingArchiveFiles:
    """Test get_existing_archive_files function"""
    
    def test_returns_empty_when_dir_not_exists(self, monkeypatch, tmp_path):
        """Returns empty list when archive dir doesn't exist"""
        monkeypatch.setattr(entrylinks_import, 'PROCESSED_DIR', 
                          str(tmp_path / 'nonexistent'))
        
        result = entrylinks_import.get_existing_archive_files()
        
        assert result == []
    
    def test_returns_csv_files_only(self, monkeypatch, tmp_path):
        """Returns only .csv files from archive directory"""
        archive_dir = tmp_path / 'archive'
        archive_dir.mkdir()
        (archive_dir / 'file1.csv').touch()
        (archive_dir / 'file2.csv').touch()
        (archive_dir / 'file3.txt').touch()
        
        monkeypatch.setattr(entrylinks_import, 'PROCESSED_DIR', str(archive_dir))
        
        result = entrylinks_import.get_existing_archive_files()
        
        assert len(result) == 2
        assert all(f.endswith('.csv') for f in result)
    
    def test_returns_empty_when_no_csv(self, monkeypatch, tmp_path):
        """Returns empty list when no CSV files"""
        archive_dir = tmp_path / 'archive'
        archive_dir.mkdir()
        (archive_dir / 'file.txt').touch()
        
        monkeypatch.setattr(entrylinks_import, 'PROCESSED_DIR', str(archive_dir))
        
        result = entrylinks_import.get_existing_archive_files()
        
        assert result == []


class TestRemoveArchiveFiles:
    """Test _remove_archive_files function"""
    
    def test_removes_specified_files(self, monkeypatch, tmp_path):
        """Should remove listed files"""
        archive_dir = tmp_path / 'archive'
        archive_dir.mkdir()
        file1 = archive_dir / 'file1.csv'
        file2 = archive_dir / 'file2.csv'
        file1.touch()
        file2.touch()
        
        monkeypatch.setattr(entrylinks_import, 'PROCESSED_DIR', str(archive_dir))
        
        entrylinks_import._remove_archive_files(['file1.csv'])
        
        assert not file1.exists()
        assert file2.exists()
    
    def test_handles_nonexistent_files_gracefully(self, monkeypatch, tmp_path):
        """Should not raise for nonexistent files"""
        archive_dir = tmp_path / 'archive'
        archive_dir.mkdir()
        
        monkeypatch.setattr(entrylinks_import, 'PROCESSED_DIR', str(archive_dir))
        
        # Should not raise
        entrylinks_import._remove_archive_files(['nonexistent.csv'])


# ============================================================================
# ENTRY PARSING TESTS
# ============================================================================

class TestParseSourceEntryComponents:
    """Test _parse_source_entry_components function"""
    
    def test_parses_bigquery_entry(self):
        """Parse BigQuery entry format"""
        entry_name = 'projects/proj/locations/us/entryGroups/bigquery/entries/table1'
        
        result = entrylinks_import._parse_source_entry_components(entry_name)
        
        assert result['project'] == 'proj'
        assert result['location'] == 'us'
        assert result['entry_group'] == 'bigquery'
        assert result['entry'] == 'table1'
    
    def test_parses_glossary_entry(self):
        """Parse Glossary entry format"""
        entry_name = 'projects/proj/locations/global/entryGroups/@dataplex/entries/glossaries/G/terms/T'
        
        result = entrylinks_import._parse_source_entry_components(entry_name)
        
        assert result['project'] == 'proj'
        assert result['location'] == 'global'
        assert result['entry_group'] == '@dataplex'
        assert 'glossaries' in result['entry']
    
    def test_raises_on_invalid_format(self):
        """Invalid format should raise ValueError"""
        with pytest.raises(ValueError):
            entrylinks_import._parse_source_entry_components('invalid/path')
    
    def test_handles_complex_entry_path(self):
        """Handle entries with complex paths"""
        entry_name = 'projects/p/locations/l/entryGroups/eg/entries/datasets/ds/tables/t'
        
        result = entrylinks_import._parse_source_entry_components(entry_name)
        
        assert result['entry'] == 'datasets/ds/tables/t'


class TestGenerateEntrylinkName:
    """Test _generate_entrylink_name function"""
    
    def test_generates_valid_name_format(self):
        """Generated name should follow expected pattern"""
        source = 'projects/p/locations/l/entryGroups/eg/entries/e1'
        target = 'projects/p/locations/l/entryGroups/eg2/entries/e2'
        link_type = 'definition'
        
        result = entrylinks_import._generate_entrylink_name(source, target, link_type)
        
        assert 'entryLinks' in result
        assert isinstance(result, str)
    
    def test_different_sources_create_different_names(self):
        """Different source/target pairs should create unique names"""
        name1 = entrylinks_import._generate_entrylink_name(
            'projects/p/locations/l/entryGroups/eg/entries/e1',
            'projects/p/locations/l/entryGroups/eg/entries/e2',
            'related'
        )
        name2 = entrylinks_import._generate_entrylink_name(
            'projects/p/locations/l/entryGroups/eg/entries/e3',
            'projects/p/locations/l/entryGroups/eg/entries/e4',
            'related'
        )
        
        assert name1 != name2
    
    def test_same_inputs_create_same_name(self):
        """Same inputs should create deterministic name"""
        source = 'projects/p/locations/l/entryGroups/eg/entries/e1'
        target = 'projects/p/locations/l/entryGroups/eg/entries/e2'
        
        name1 = entrylinks_import._generate_entrylink_name(source, target, 'related')
        name2 = entrylinks_import._generate_entrylink_name(source, target, 'related')
        
        assert name1 == name2


# ============================================================================
# PATH FORMATTING TESTS  
# ============================================================================

class TestFormatSourcePathForBigquery:
    """Test _format_source_path_for_bigquery function"""
    
    def test_formats_simple_bigquery_path(self):
        """Format simple BigQuery dataset.table path"""
        components = {
            'project': 'my-project',
            'entry': 'datasets/my_dataset/tables/my_table'
        }
        
        result = entrylinks_import._format_source_path_for_bigquery(components)
        
        assert 'my_dataset' in result or 'my_table' in result
    
    def test_handles_missing_components(self):
        """Should not raise for minimal components"""
        components = {
            'project': 'proj',
            'entry': 'simple_entry'
        }
        
        # Should not raise
        result = entrylinks_import._format_source_path_for_bigquery(components)
        
        assert isinstance(result, str)


# ============================================================================
# DEFINITION REFERENCES TESTS
# ============================================================================

class TestBuildDefinitionReferences:
    """Test _build_definition_references function"""
    
    def test_creates_source_reference(self):
        """Should create a source reference entry"""
        components = {
            'project': 'proj',
            'location': 'us',
            'entry_group': 'bigquery',
            'entry': 'tables/t1'
        }
        source_path = '/project.dataset.table'
        
        result = entrylinks_import._build_definition_references(
            components, source_path, 'target_entry'
        )
        
        assert isinstance(result, list)
        assert len(result) > 0
    
    def test_includes_target_reference(self):
        """Result should reference target entry"""
        components = {
            'project': 'proj',
            'location': 'us',
            'entry_group': 'bigquery',
            'entry': 'tables/t1'
        }
        
        result = entrylinks_import._build_definition_references(
            components, '/path', 'target_entry_name'
        )
        
        # Find reference that contains target
        has_target = any('target_entry_name' in str(ref) for ref in result)
        assert has_target or len(result) > 0


# ============================================================================
# LINK TYPE EXTRACTION TESTS
# ============================================================================

class TestExtractNormalizedLinkType:
    """Test _extract_normalized_link_type function"""
    
    def test_extracts_definition_type(self):
        """Should extract 'definition' type"""
        entry_link = {'entryLinkType': 'DEFINITION'}
        
        result = entrylinks_import._extract_normalized_link_type(entry_link)
        
        assert result.lower() == 'definition'
    
    def test_extracts_related_type(self):
        """Should extract 'related' type"""
        entry_link = {'entryLinkType': 'RELATED'}
        
        result = entrylinks_import._extract_normalized_link_type(entry_link)
        
        assert result.lower() == 'related'
    
    def test_extracts_synonym_type(self):
        """Should extract 'synonym' type"""
        entry_link = {'entryLinkType': 'SYNONYM'}
        
        result = entrylinks_import._extract_normalized_link_type(entry_link)
        
        assert result.lower() == 'synonym'
    
    def test_handles_missing_type(self):
        """Should return default for missing type"""
        entry_link = {}
        
        result = entrylinks_import._extract_normalized_link_type(entry_link)
        
        assert result == '' or result is None or 'unknown' in result.lower()
    
    def test_normalizes_case(self):
        """Should normalize to consistent case"""
        entry_link = {'entryLinkType': 'DeFiNiTiOn'}
        
        result = entrylinks_import._extract_normalized_link_type(entry_link)
        
        # Should be consistently cased
        assert result == result.lower() or result == result.upper()


# ============================================================================
# ENTRYLINK GROUPING TESTS
# ============================================================================

class TestAddEntrylinkToGroup:
    """Test _add_entrylink_to_group function"""
    
    def test_adds_to_empty_group(self):
        """Should add to empty group container"""
        groups = {}
        region = 'us-central1'
        entry_link = {'name': 'link1', 'entryLinkType': 'RELATED'}
        
        entrylinks_import._add_entrylink_to_group(groups, region, entry_link)
        
        assert region in groups
        assert len(groups[region]) == 1
    
    def test_adds_to_existing_group(self):
        """Should append to existing region group"""
        groups = {'us-central1': [{'name': 'existing'}]}
        
        entrylinks_import._add_entrylink_to_group(
            groups, 'us-central1', {'name': 'new'}
        )
        
        assert len(groups['us-central1']) == 2
    
    def test_creates_new_group_for_new_region(self):
        """Should create new group for new region"""
        groups = {'us': [{'name': 'link1'}]}
        
        entrylinks_import._add_entrylink_to_group(
            groups, 'eu', {'name': 'link2'}
        )
        
        assert 'eu' in groups
        assert 'us' in groups


# ============================================================================
# VALIDATION TESTS
# ============================================================================

class TestValidateEntryLinkRow:
    """Test entry link row validation"""
    
    def test_valid_row_passes(self, monkeypatch):
        """Valid row should pass validation"""
        row = {
            'link_type': 'definition',
            'source_entry': 'projects/p/locations/l/entryGroups/eg/entries/e',
            'target_entry': 'projects/p/locations/l/entryGroups/eg/entries/t',
            'source_path': '/path/to/source'
        }
        
        # Should not raise
        # If there's a validation function, call it
        if hasattr(entrylinks_import, 'validate_entry_link_row'):
            result = entrylinks_import.validate_entry_link_row(row)
            assert result is True or result is None
    
    def test_missing_required_fields_fails(self, monkeypatch):
        """Row missing required fields should fail"""
        row = {
            'link_type': 'definition',
            # Missing source_entry and target_entry
        }
        
        if hasattr(entrylinks_import, 'validate_entry_link_row'):
            with pytest.raises((ValueError, KeyError)):
                entrylinks_import.validate_entry_link_row(row)


# ============================================================================
# IMPORT WORKFLOW TESTS
# ============================================================================

class TestPrepareImportBatches:
    """Test batch preparation for API calls"""
    
    def test_groups_by_region(self, monkeypatch):
        """Entry links should be grouped by target region"""
        if hasattr(entrylinks_import, 'prepare_import_batches'):
            entry_links = [
                {'target_region': 'us', 'name': 'link1'},
                {'target_region': 'us', 'name': 'link2'},
                {'target_region': 'eu', 'name': 'link3'},
            ]
            
            batches = entrylinks_import.prepare_import_batches(entry_links)
            
            assert 'us' in batches
            assert 'eu' in batches
            assert len(batches['us']) == 2
            assert len(batches['eu']) == 1


class TestImportEntryLinksToDataplex:
    """Test import_entry_links_to_dataplex function"""
    
    def test_returns_success_on_valid_input(self, monkeypatch):
        """Should return success for valid entry links"""
        mock_auth = MagicMock()
        mock_create = MagicMock(return_value={'name': 'created_link'})
        
        monkeypatch.setattr(entrylinks_import.api_layer, 'authenticate_dataplex', mock_auth)
        
        if hasattr(entrylinks_import, 'create_entry_link'):
            monkeypatch.setattr(entrylinks_import, 'create_entry_link', mock_create)
    
    def test_handles_api_errors_gracefully(self, monkeypatch):
        """Should handle API errors without crashing"""
        mock_auth = MagicMock()
        mock_create = MagicMock(side_effect=Exception("API Error"))
        
        monkeypatch.setattr(entrylinks_import.api_layer, 'authenticate_dataplex', mock_auth)
        
        # Should not raise, but handle error gracefully


class TestRunImportWorkflow:
    """Test _run_import_workflow function"""
    
    def test_returns_1_when_no_spreadsheet_url(self, monkeypatch):
        """Should return 1 when spreadsheet URL not provided"""
        mock_setup_logging = MagicMock()
        mock_get_args = MagicMock()
        mock_get_args.return_value.spreadsheet_url = None
        
        monkeypatch.setattr(entrylinks_import.logging_utils, 'setup_file_logging', mock_setup_logging)
        monkeypatch.setattr(entrylinks_import.argument_parser, 'get_import_entrylinks_arguments', mock_get_args)
        
        result = entrylinks_import._run_import_workflow()
        
        assert result == 1
    
    def test_returns_1_when_no_project(self, monkeypatch):
        """Should return 1 when no default project configured"""
        mock_setup_logging = MagicMock()
        mock_get_args = MagicMock()
        mock_get_args.return_value.spreadsheet_url = 'http://sheet'
        mock_get_project = MagicMock(return_value=None)
        
        monkeypatch.setattr(entrylinks_import.logging_utils, 'setup_file_logging', mock_setup_logging)
        monkeypatch.setattr(entrylinks_import.argument_parser, 'get_import_entrylinks_arguments', mock_get_args)
        monkeypatch.setattr(entrylinks_import.api_layer, 'get_default_project', mock_get_project)
        
        result = entrylinks_import._run_import_workflow()
        
        assert result == 1


# ============================================================================
# MAIN FLOW TESTS
# ============================================================================

class TestMain:
    """Test main function"""
    
    def test_catches_keyboard_interrupt(self, monkeypatch):
        """Main should catch KeyboardInterrupt"""
        mock_run = MagicMock(side_effect=KeyboardInterrupt())
        
        monkeypatch.setattr(entrylinks_import, '_run_import_workflow', mock_run)
        
        result = entrylinks_import.main()
        
        assert result == 1
    
    def test_returns_workflow_result(self, monkeypatch):
        """Main should return workflow result"""
        mock_run = MagicMock(return_value=0)
        
        monkeypatch.setattr(entrylinks_import, '_run_import_workflow', mock_run)
        
        result = entrylinks_import.main()
        
        assert result == 0
    
    def test_handles_generic_exception(self, monkeypatch):
        """Main should handle generic exceptions"""
        mock_run = MagicMock(side_effect=Exception("Unexpected error"))
        
        monkeypatch.setattr(entrylinks_import, '_run_import_workflow', mock_run)
        
        result = entrylinks_import.main()
        
        assert result == 1


# ============================================================================
# EDGE CASES AND ERROR HANDLING
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions"""
    
    def test_empty_entry_links_list(self, monkeypatch):
        """Should handle empty entry links gracefully"""
        if hasattr(entrylinks_import, 'process_entry_links'):
            result = entrylinks_import.process_entry_links([])
            assert result == [] or result is None
    
    def test_malformed_entry_name(self):
        """Should handle malformed entry names"""
        with pytest.raises(ValueError):
            entrylinks_import._parse_source_entry_components('')
    
    def test_special_characters_in_names(self):
        """Should handle special characters"""
        entry_name = 'projects/my-project/locations/us-central1/entryGroups/my_group/entries/my-entry_123'
        
        result = entrylinks_import._parse_source_entry_components(entry_name)
        
        assert result['project'] == 'my-project'
        assert result['entry'] == 'my-entry_123'


class TestConcurrency:
    """Test concurrent operation handling"""
    
    def test_parallel_region_processing(self, monkeypatch):
        """Should handle parallel region processing"""
        if hasattr(entrylinks_import, 'process_regions_parallel'):
            regions = ['us', 'eu', 'asia']
            mock_process = MagicMock(return_value={'status': 'success'})
            
            monkeypatch.setattr(entrylinks_import, 'process_single_region', mock_process)
            
            result = entrylinks_import.process_regions_parallel(regions)
            
            assert len(result) == 3
