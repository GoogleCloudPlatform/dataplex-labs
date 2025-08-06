import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json
import tempfile
import shutil

from . import export

class ExportScriptTest(unittest.TestCase):

    def setUp(self):
        """Set up a temporary directory for test files and create mock data."""
        self.test_dir = tempfile.mkdtemp()
        self.mock_data = self._get_mock_api_data()
        
        # This context is what the script builds for each run
        self.mock_export_context = {
            "project": "test-project",
            "location": "us-central1",
            "glossary": "test-glossary-normalized",
            "dataplex_entry_group": "projects/test-project/locations/global/entryGroups/@dataplex",
            "project_number": "123456789012",
            "org_ids": ["11111", "22222"]
        }

    def tearDown(self):
        """Remove the temporary directory after tests are complete."""
        shutil.rmtree(self.test_dir)
        # Clear the cache between tests
        export.entrygroup_to_glossaryid_map.clear()

    def _get_mock_api_data(self):
        """Returns a dictionary of mock API responses based on user-provided samples."""
        return {
            "entries_response": {
                "entries": [
                    {
                        "name": "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/test-glossary-id",
                        "displayName": "Test Glossary",
                        "entryType": "glossary",
                        "entryUid": "glossary-uid"
                    },
                    {
                        "name": "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/test-category-id",
                        "displayName": "Test Category",
                        "entryType": "glossary_category",
                        "entryUid": "category-uid",
                        "coreAspects": {"business_context": {"jsonContent": {"description": "Category Description"}}}
                    },
                    {
                        "name": "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/test-term-id",
                        "displayName": "Test Term",
                        "entryType": "glossary_term",
                        "entryUid": "term-uid-1",
                        "coreAspects": {"business_context": {"jsonContent": {"description": "Term Description", "contacts": ["Steward Name <steward@example.com>"]}}}
                    }
                ]
            },
            "relationships_response": {
                "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/test-category-id": [
                    {"relationshipType": "belongs_to", "destinationEntry": {"name": "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/test-glossary-id"}}
                ],
                "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/test-term-id": [
                    {"relationshipType": "belongs_to", "destinationEntry": {"name": "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/test-category-id"}},
                    {
                        "name": "relationships/synonym1",
                        "relationshipType": "is_synonymous_to",
                        "sourceEntry": {"name": "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/test-term-id", "entryType": "glossary_term"},
                        "destinationEntry": {
                            "name": "projects/other-project/locations/us/entryGroups/other-eg/entries/other-term-id",
                            "coreRelationships": [{"destinationEntryName": "projects/other-project/locations/us/entryGroups/other-eg/entries/other-glossary-id"}]
                        }
                    }
                ]
            },
            "search_response": {
                "json": {
                    "results": [{
                        "relativeResourceName": "projects/asset-project/locations/us-central1/entryGroups/@bigquery/entries/some-bigquery-table-id",
                        "linkedResource": "//bigquery.googleapis.com/projects/asset-project/datasets/my_dataset/tables/my_table"
                    }]
                }
            },
            "asset_relationships_response": {
                "json": {
                    "relationships": [{
                        "name": "relationships/definition1",
                        "relationshipType": "is_described_by",
                        "destinationEntryName": "projects/test-project/locations/us-central1/entryGroups/test-eg/entries/term-uid-1" # Matches entryUid
                    }]
                }
            }
        }

    def test_get_entry_id(self):
        """Test the get_entry_id helper function."""
        entry_name = "projects/p/locations/l/entryGroups/eg/entries/my-entry-id"
        self.assertEqual(export.get_entry_id(entry_name), "my-entry-id")
        self.assertEqual(export.get_entry_id("invalid-name"), "")

    def test_process_entry(self):
        """Test the logic for transforming a single V1 entry to V2 format."""
        mock_entry = self.mock_data["entries_response"]["entries"][2] # The test term
        mock_parent_mapping = {"test-term-id": "test-category-id", "test-category-id": "test-glossary-id"}
        mock_id_to_type_map = {"test-term-id": "glossary_term", "test-category-id": "glossary_category"}

        result = export.process_entry(mock_entry, mock_parent_mapping, mock_id_to_type_map, self.mock_export_context)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['entry']['entrySource']['displayName'], 'Test Term')
        self.assertIn("<p>Term Description</p>", result['entry']['aspects']['123456789012.global.overview']['data']['content'])
        self.assertEqual(len(result['entry']['entrySource']['ancestors']), 3)

        self.assertEqual(result['entry']['entrySource']['ancestors'][0]['name'], 'projects/test-project/locations/global/entryGroups/@dataplex/entries/projects/test-project/locations/global/glossaries/test-glossary-normalized')

@patch('migration.export.subprocess.run')
@patch('migration.export.utils.create_glossary')
@patch('migration.export.utils.replace_with_new_glossary_id')
@patch('migration.export.utils.fetch_all_relationships')
@patch('migration.export.utils.fetch_entries')
@patch('migration.export.utils.parse_glossary_url')
@patch('migration.export.api_call_utils.fetch_api_response')
@patch('migration.export.ensure_output_folders_exist')

def test_run_export_success(self, 
                                mock_ensure_folders, 
                                mock_fetch_api, 
                                mock_parse_url, 
                                mock_fetch_entries, 
                                mock_fetch_rels, 
                                mock_replace_id, 
                                mock_create_glossary, 
                                mock_subprocess):
        """Test the entire run_export flow with mocked external dependencies."""
        # --- Setup Mocks ---
        # Configure file paths to use the temporary directory
        glossaries_dir = os.path.join(self.test_dir, "Glossaries")
        entrylinks_dir = os.path.join(self.test_dir, "EntryLinks")
        mock_ensure_folders.return_value = (glossaries_dir, entrylinks_dir)

        mock_parse_url.return_value = {"project": "test-project", "location": "us-central1", "entry_group": "test-eg", "glossary": "test-glossary-raw"}
        
        # We need to mock the util function that is also called inside run_export
        with patch('migration.export.utils.normalize_glossary_id') as mock_normalize:
            mock_normalize.return_value = "test-glossary-normalized"

            mock_fetch_entries.return_value = self.mock_data["entries_response"]["entries"]
            mock_fetch_rels.return_value = self.mock_data["relationships_response"]
            mock_subprocess.return_value = MagicMock(stdout="11111\n22222")

            # Mock the various API calls made inside the link generation functions
            mock_fetch_api.side_effect = [
                # Call from fetch_glossary_id for the synonym link
                {"json": {"name": "projects/other-project/locations/us/entryGroups/other-eg/entries/other-glossary-id"}},
                # Call from process_term_entry_links for catalog:search
                self.mock_data["search_response"],
                # Call from process_term_entry_links for dataplex:lookupEntry
                {"json": {"name": "lookup-success"}},
                 # Call from process_term_entry_links for datacatalog relationships
                self.mock_data["asset_relationships_response"]
            ]

            # --- Run the Function ---
            # Pass a dummy value for user_project as it's required by the function signature
            result = export.run_export(glossary_url="http://.url", user_project="billing-project")

            # --- Assertions ---
            self.assertTrue(result)
            
            # Verify glossary file was created
            glossary_file_path = os.path.join(glossaries_dir, "glossary-test-glossary-normalized.json")
            self.assertTrue(os.path.exists(glossary_file_path))
            with open(glossary_file_path, 'r') as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 2) # Expect one term and one category

            # Verify entry link files were created
            self.assertTrue(any(fname.startswith('entrylinks_related_synonyms_') for fname in os.listdir(entrylinks_dir)))
            self.assertTrue(any(fname.startswith('entrylinks_definition_') for fname in os.listdir(entrylinks_dir)))
            
            # Check that key functions were called
            mock_create_glossary.assert_called_once()
            mock_replace_id.assert_called_once()