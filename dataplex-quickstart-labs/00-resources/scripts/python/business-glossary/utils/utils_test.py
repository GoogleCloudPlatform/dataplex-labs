import unittest
from unittest.mock import patch, MagicMock

from utils import utils

class UtilsTest(unittest.TestCase):

    def test_parse_glossary_url_success(self):
        """Tests that a valid Data Catalog V1 URL is parsed correctly."""
        url = "https://datacatalog.googleapis.com/projects/my-proj-id/locations/us-central1/entryGroups/my-entry-group/entries/my-glossary-id"
        expected = {
            "project": "my-proj-id",
            "location": "us-central1",
            "entry_group": "my-entry-group",
            "glossary": "my-glossary-id"
        }
        self.assertEqual(utils.parse_glossary_url(url), expected)

    def test_parse_glossary_url_with_query_params(self):
        """Tests that a URL with query parameters is parsed correctly."""
        url = "https://console.cloud.google.com/datacatalog/projects/my-proj-id/locations/us/entryGroups/my-eg/entries/my_glossary?e=123&project=my-proj-id"
        result = utils.parse_glossary_url(url)
        self.assertEqual(result['glossary'], 'my_glossary')

    def test_parse_glossary_url_failure(self):
        """Tests that an invalid URL raises a ValueError."""
        url = "http://invalid.url/without/the/correct/pattern"
        with self.assertRaises(ValueError):
            utils.parse_glossary_url(url)

    def test_normalize_glossary_id(self):
        """Tests the glossary ID normalization logic."""
        self.assertEqual(utils.normalize_glossary_id("My_Glossary Name"), "my-glossary-name")
        self.assertEqual(utils.normalize_glossary_id("already-valid"), "already-valid")
        self.assertEqual(utils.normalize_glossary_id("  leading-trailing-spaces--"), "leading-trailing-spaces")
        self.assertEqual(utils.normalize_glossary_id("special!@#chars"), "special-chars")

    @patch('utils.api_call_utils.fetch_api_response')
    def test_get_project_number_success(self, mock_fetch_api):
        """Tests successful conversion of a project ID to a project number."""
        mock_fetch_api.return_value = {
            "json": {
                "name": "projects/123456789012",
                "projectId": "dataplex-types"
            },
            "error_msg": None
        }
        
        project_number = utils.get_project_number("dataplex-types", "billing-project")
        
        # Verify the correct URL was called
        mock_fetch_api.assert_called_with(unittest.mock.ANY, "https://cloudresourcemanager.googleapis.com/v3/projects/dataplex-types", "billing-project")
        
        # Verify the correct number was returned
        self.assertEqual(project_number, "123456789012")

    @patch('utils.utils.logger') 
    @patch('utils.utils.sys.exit')
    @patch('utils.utils.api_call_utils.fetch_api_response')
    def test_get_project_number_failure(self, mock_fetch_api, mock_exit, mock_logger):
        """Tests that the script exits if the project number can't be fetched."""
        mock_fetch_api.return_value = {"json": {}, "error_msg": "API Error"}
        
        utils.get_project_number("invalid-id", "billing-project")
        
        # Verify that the script tried to exit
        mock_exit.assert_called_with(1)

    @patch('utils.utils.logger')
    @patch('utils.utils.sys.exit')
    @patch('utils.utils.api_call_utils.fetch_api_response')
    def test_fetch_entries_failure(self, mock_fetch_api, mock_exit, mock_logger):
        """Tests that the script exits if fetching entries fails."""
        mock_fetch_api.return_value = {"json": {}, "error_msg": "API Error"}
        
        utils.fetch_entries("billing-project", "proj", "loc", "eg")
        
        mock_exit.assert_called_with(1)
    
    @patch('utils.utils.api_call_utils.fetch_api_response')
    def test_get_project_number_failure(self, mock_fetch_api):
        """Tests that the script exits if the project number can't be fetched."""
        mock_fetch_api.return_value = {"json": {}, "error_msg": "API Error"}
        
        # Use assertLogs to capture and silence logger output
        with self.assertLogs(level='ERROR') as cm:
            # Use assertRaises to confirm the function exits as expected
            with self.assertRaises(SystemExit):
                utils.get_project_number("invalid-id", "billing-project")
        
        # Verify the correct error was logged
        self.assertIn("Failed to fetch project number: API Error", cm.output[0])

    @patch('utils.utils.api_call_utils.fetch_api_response')
    def test_fetch_entries_failure(self, mock_fetch_api):
        """Tests that the script exits if fetching entries fails."""
        mock_fetch_api.return_value = {"json": {}, "error_msg": "API Error"}
        
        with self.assertLogs(level='ERROR') as cm:
            with self.assertRaises(SystemExit):
                utils.fetch_entries("billing-project", "proj", "loc", "eg")
        
        self.assertIn("Can't proceed with export. Details: API Error", cm.output[0])

    @patch('utils.utils.api_call_utils.fetch_api_response')
    def test_discover_glossaries_api_error(self, mock_fetch_api):
        """Tests the case where the API call returns an error."""
        mock_fetch_api.return_value = {"json": {}, "error_msg": "Permission Denied"}

        # Use assertLogs to ensure the error is logged
        with self.assertLogs(level='ERROR') as cm:
            result = utils.discover_glossaries("test-project", "billing-project")
            self.assertIn("Failed to search for glossaries: Permission Denied", cm.output[0])
            
        # Verify the function returns an empty list
        self.assertEqual(result, [])


    def test_get_migration_arguments(self):
        """Tests the command-line argument parser."""
        # Test the success case
        args = utils.get_migration_arguments(
            ['--project', 'test-proj', '--buckets', 'b1, b2']
        )
        self.assertEqual(args.project, 'test-proj')
        self.assertEqual(args.buckets, ['b1', 'b2'])

        # Test the failure case (missing required argument)
        with self.assertRaises(SystemExit):
            utils.get_migration_arguments(['--project', 'test-proj'])

if __name__ == '__main__':
    unittest.main()