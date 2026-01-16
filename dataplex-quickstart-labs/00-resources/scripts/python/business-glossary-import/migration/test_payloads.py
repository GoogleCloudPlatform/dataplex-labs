import pytest
from unittest.mock import MagicMock
import payloads

class TestBuildImportSpecBase:
    def test_build_import_spec_base_success(self):
        """Test basic payload building with folder name."""
        gcs_bucket = "my-bucket"
        folder_name = "migration_folder_1"
        
        result = payloads.build_import_spec_base(gcs_bucket, folder_name)
        
        assert result["log_level"] == "DEBUG"
        assert result["source_storage_uri"] == "gs://my-bucket/migration_folder_1/"
        assert result["entry_sync_mode"] == "FULL"
        assert result["aspect_sync_mode"] == "INCREMENTAL"

    def test_build_import_spec_base_strips_trailing_slash(self):
        """Test that trailing slashes are stripped from folder names."""
        gcs_bucket = "my-bucket"
        folder_name = "migration_folder_1/"
        
        result = payloads.build_import_spec_base(gcs_bucket, folder_name)
        
        assert result["source_storage_uri"] == "gs://my-bucket/migration_folder_1/"

    def test_build_import_spec_base_complex_bucket_names(self):
        """Test with various bucket name formats."""
        test_cases = [
            ("bucket-1", "folder1"),
            ("bucket_2", "folder-2"),
            ("my.bucket", "folder_3"),
        ]
        
        for bucket, folder in test_cases:
            result = payloads.build_import_spec_base(bucket, folder)
            assert result["source_storage_uri"] == f"gs://{bucket}/{folder}/"

class TestExtractJobLocationFromEntryGroup:
    def test_extract_location_success(self):
        """Test extracting location from entry group."""
        entry_group = "projects/123/locations/us-central1/entryGroups/@dataplex"
        result = payloads.extract_job_location_from_entry_group(entry_group)
        assert result == "us-central1"

    def test_extract_location_global(self):
        """Test extraction with global location."""
        entry_group = "projects/123/locations/global/entryGroups/@dataplex"
        result = payloads.extract_job_location_from_entry_group(entry_group)
        assert result == "global"

    def test_extract_location_default_on_none(self):
        """Test that default 'global' is returned for None input."""
        result = payloads.extract_job_location_from_entry_group(None)
        assert result == "global"

    def test_extract_location_default_on_empty_string(self):
        """Test that default 'global' is returned for empty string."""
        result = payloads.extract_job_location_from_entry_group("")
        assert result == "global"

    def test_extract_location_no_match(self):
        """Test that default 'global' is returned when pattern doesn't match."""
        entry_group = "invalid/format"
        result = payloads.extract_job_location_from_entry_group(entry_group)
        assert result == "global"

class TestExtractScopes:
    def test_extract_scopes_from_entry_references_single_scope(self):
        """Test extracting project scope from entry references."""
        data = {
            "entryLink": {
                "entryReferences": [
                    {"name": "projects/75037423216/locations/global/glossaries/test/terms/term1"}
                ]
            }
        }
        result = payloads.extract_scopes_from_entry_references(data)
        assert "projects/75037423216" in result
        assert len(result) == 1

    def test_extract_scopes_from_entry_references_multiple_references(self):
        """Test extracting scope from first reference only."""
        data = {
            "entryLink": {
                "entryReferences": [
                    {"name": "projects/111/locations/global/glossaries/test/terms/term1"},
                    {"name": "projects/222/locations/global/glossaries/test/terms/term2"}
                ]
            }
        }
        result = payloads.extract_scopes_from_entry_references(data)
        # Should only extract from first reference
        assert "projects/111" in result
        assert len(result) == 1

    def test_extract_scopes_empty_references(self):
        """Test with no entry references."""
        data = {
            "entryLink": {
                "entryReferences": []
            }
        }
        result = payloads.extract_scopes_from_entry_references(data)
        assert len(result) == 0

    def test_extract_scopes_no_entry_link(self):
        """Test when entryLink is missing."""
        data = {}
        result = payloads.extract_scopes_from_entry_references(data)
        assert len(result) == 0

class TestBuildPayload:
    def test_build_glossary_payload(self, monkeypatch):
        """Test building payload for glossary files."""
        filename = "glossary_my-glossary.json"
        project_id = "test-project"
        gcs_bucket = "test-bucket"
        folder_name = "migration_folder_1"
        
        job_id, payload, location = payloads.build_payload(f"/path/{filename}", project_id, gcs_bucket, folder_name)
        
        assert job_id == "glossary-my-glossary"
        assert payload["type"] == "IMPORT"
        assert payload["import_spec"]["source_storage_uri"] == "gs://test-bucket/migration_folder_1/"
        assert location == "global"
        assert f"projects/{project_id}/locations/global/glossaries/my-glossary" in str(payload)

    def test_build_entrylink_payload_definition(self, monkeypatch):
        """Test building payload for definition entrylinks."""
        # Mock get_link_type to return definition
        monkeypatch.setattr(payloads, "get_link_type", lambda x: "definition")
        monkeypatch.setattr(payloads, "get_entry_group", lambda x: "projects/test/locations/us-central1/entryGroups/test")
        monkeypatch.setattr(payloads, "build_defintion_referenced_entry_scopes", lambda x, y: ["projects/test"])
        
        filename = "entrylinks_definition.json"
        project_id = "test-project"
        gcs_bucket = "test-bucket"
        folder_name = "migration_folder_1"
        
        job_id, payload, location = payloads.build_payload(f"/path/{filename}", project_id, gcs_bucket, folder_name)
        
        assert payload["type"] == "IMPORT"
        assert "entry_groups" in payload["import_spec"]["scope"]
        assert "entry_link_types" in payload["import_spec"]["scope"]

    def test_build_unknown_file_type(self, monkeypatch):
        """Test that unknown file types return None."""
        filename = "unknown_file.json"
        project_id = "test-project"
        gcs_bucket = "test-bucket"
        folder_name = "migration_folder_1"
        
        monkeypatch.setattr(payloads, "logger", MagicMock())
        
        job_id, payload, location = payloads.build_payload(f"/path/{filename}", project_id, gcs_bucket, folder_name)
        
        assert job_id is None
        assert payload is None
        assert location is None

class TestBuildGlossaryPayload:
    def test_build_glossary_payload_success(self):
        """Test building glossary payload."""
        filename = "glossary_my-test-glossary.json"
        project_id = "my-project"
        import_spec_base = payloads.build_import_spec_base("bucket", "folder")
        
        job_id, payload, location = payloads.build_glossary_payload(filename, project_id, import_spec_base)
        
        assert job_id == "glossary-my-test-glossary"
        assert location == "global"
        assert payload["type"] == "IMPORT"
        assert "scope" in payload["import_spec"]
        assert f"projects/{project_id}/locations/global/glossaries/my-test-glossary" in payload["import_spec"]["scope"]["glossaries"][0]

    def test_build_glossary_payload_id_conversion(self):
        """Test that underscores are converted to hyphens in glossary IDs."""
        filename = "glossary_my_test_glossary.json"
        project_id = "project"
        import_spec_base = {}
        
        job_id, payload, location = payloads.build_glossary_payload(filename, project_id, import_spec_base)
        
        assert job_id == "glossary-my-test-glossary"

class TestBuildDefinitionEntryLinkPayload:
    def test_build_definition_payload(self, monkeypatch):
        """Test building definition entrylink payload."""
        file_path = "/path/entrylinks_definition.json"
        project_id = "test-project"
        import_spec_base = payloads.build_import_spec_base("bucket", "folder")
        
        # Mock required functions
        monkeypatch.setattr(payloads, "get_entry_group", lambda x: "projects/test/locations/us-central1/entryGroups/test")
        monkeypatch.setattr(payloads, "build_defintion_referenced_entry_scopes", lambda x, y: ["projects/test"])
        
        job_id, payload, location = payloads.build_definition_entrylink_payload(file_path, project_id, import_spec_base)
        
        assert payload["type"] == "IMPORT"
        assert "entry_groups" in payload["import_spec"]["scope"]
        assert location == "us-central1"

class TestBuildSynonymRelatedPayload:
    def test_build_synonym_related_payload(self, monkeypatch):
        """Test building synonym/related entrylink payload."""
        file_path = "/path/entrylinks_related_synonyms.json"
        project_id = "test-project"
        import_spec_base = payloads.build_import_spec_base("bucket", "folder")
        
        # Mock required functions
        monkeypatch.setattr(payloads, "build_related_synonym_referenced_entry_scopes", lambda x, y: ["projects/ref1", "projects/ref2"])
        
        job_id, payload, location = payloads.build_synonym_related_entrylink_payload(file_path, project_id, import_spec_base)
        
        assert payload["type"] == "IMPORT"
        assert location == "global"
        assert "entry_link_types" in payload["import_spec"]["scope"]
        # Should have both synonym and related link types
        entry_link_types = payload["import_spec"]["scope"]["entry_link_types"]
        assert any("synonym" in t for t in entry_link_types)
        assert any("related" in t for t in entry_link_types)

class TestGetLinkType:
    def test_get_link_type_definition(self, monkeypatch):
        """Test getting definition link type."""
        monkeypatch.setattr(payloads, "read_first_json_line", lambda x: {
            "entryLink": {"entryLinkType": "definition"}
        })
        
        result = payloads.get_link_type("test.json")
        assert result == "definition"

    def test_get_link_type_synonym(self, monkeypatch):
        """Test getting synonym link type."""
        monkeypatch.setattr(payloads, "read_first_json_line", lambda x: {
            "entryLink": {"entryLinkType": "synonym"}
        })
        
        result = payloads.get_link_type("test.json")
        assert result == "synonym"

    def test_get_link_type_missing(self, monkeypatch):
        """Test when link type is missing."""
        monkeypatch.setattr(payloads, "read_first_json_line", lambda x: {})
        
        result = payloads.get_link_type("test.json")
        # Can return None or empty string based on implementation
        assert result is None or result == ""

    def test_get_link_type_no_data(self, monkeypatch):
        """Test when no JSON data is found."""
        monkeypatch.setattr(payloads, "read_first_json_line", lambda x: None)
        
        result = payloads.get_link_type("test.json")
        assert result is None
