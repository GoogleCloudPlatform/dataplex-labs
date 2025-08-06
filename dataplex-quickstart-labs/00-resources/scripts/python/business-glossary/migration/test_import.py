import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json

import importlib

import_script = importlib.import_module(".import", package="migration")
class ImportScriptTest(unittest.TestCase):

    @patch('migration.import.storage.Client')
    def test_delete_all_bucket_objects(self, mock_storage_client):
        """Tests that the script attempts to delete all blobs in a bucket."""
        mock_bucket = MagicMock()
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        # Simulate blobs being in the bucket
        mock_bucket.list_blobs.return_value = [MagicMock(), MagicMock()]
        
        import_script.delete_all_bucket_objects("test-bucket")
        
        mock_storage_client.assert_called_once()
        mock_bucket.list_blobs.assert_called_once()
        mock_bucket.delete_blobs.assert_called_once()


    @patch('migration.import.time.sleep')
    @patch('migration.import.get_dataplex_service')
    def test_poll_metadata_job_succeeds(self, mock_get_service, mock_sleep):
        """Tests the polling logic for a successful job and verifies log output."""
        mock_service = MagicMock()
        mock_get_service.return_value = mock_service
        
        mock_service.projects().locations().metadataJobs().get().execute.side_effect = [
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "SUCCEEDED"}},
        ]
        
        with self.assertLogs(level='INFO') as cm:
            result = import_script.poll_metadata_job(mock_service, "proj-1", "loc-1", "job-1")
        
        self.assertTrue(result)
        # Verify the final log message was the success message
        self.assertIn("Job 'job-1' SUCCEEDED", cm.output[-1])


    @patch('migration.import.time.sleep')
    @patch('migration.import.get_dataplex_service')
    def test_poll_metadata_job_fails(self, mock_get_service, mock_sleep):
        """Tests the polling logic for a failed job and verifies log output."""
        mock_service = MagicMock()
        mock_get_service.return_value = mock_service
        
        mock_service.projects().locations().metadataJobs().get().execute.return_value = {
            "status": {
                "state": "FAILED",
                "message": "Something went wrong."
            }
        }
        
        with self.assertLogs(level='ERROR') as cm:
            result = import_script.poll_metadata_job(mock_service, "proj-1", "loc-1", "job-1")
        
        self.assertFalse(result)
        # Verify the error log contains the correct failure message
        self.assertIn("Job 'job-1' FAILED. Reason: Something went wrong.", cm.output[-1])

    def test_get_entry_group_from_file_content(self):
        """Tests that the entry group is correctly extracted from file content."""
        mock_content = '{"entryLink": {"name": "projects/my-proj/locations/us-central1/entryGroups/@bigquery/entryLinks/link-id"}}'
        with patch('builtins.open', mock_open(read_data=mock_content)):
            result = import_script.get_entry_group_from_file_content("fake/path.json")
            self.assertEqual(result, "projects/my-proj/locations/us-central1/entryGroups/@bigquery")

    @patch('migration.import.os.path.exists')
    def test_check_entrylink_dependency(self, mock_path_exists):
        """Tests the dependency check logic for entry links."""
        mock_content = '{"entryLink": {"entryLinkType": ".../related", "entryReferences": [{"name": "projects/p/l/eg/entries/projects/p/l/glossaries/my-glossary-id/terms/t1"}]}}'
        
        # Scenario 1: Glossary file exists, so dependency check should FAIL (return False)
        mock_path_exists.return_value = True
        with patch('builtins.open', mock_open(read_data=mock_content)):
            self.assertFalse(import_script.check_entrylink_dependency("fake/path.json"))
            # Verify it checked for the correct file
            mock_path_exists.assert_called_with(os.path.join(import_script.GLOSSARIES_DIR, "glossary_my-glossary-id.json"))

        # Scenario 2: Glossary file does NOT exist, so dependency check should PASS (return True)
        mock_path_exists.return_value = False
        with patch('builtins.open', mock_open(read_data=mock_content)):
            self.assertTrue(import_script.check_entrylink_dependency("fake/path.json"))

    @patch('migration.import.os.remove')
    @patch('migration.import.create_and_monitor_job')
    @patch('migration.import.upload_to_gcs')
    @patch('migration.import.delete_all_bucket_objects')
    @patch('migration.import.get_link_type_from_file')
    @patch('migration.import.get_referenced_scopes_from_file')
    def test_process_file_for_glossary(self, mock_get_scopes, mock_get_link_type, mock_delete_all, mock_upload, mock_create_job, mock_remove):
        """Tests the process_file function for a glossary file."""
        mock_create_job.return_value = True # Simulate a successful import job
        
        result = import_script.process_file("path/to/glossary_my-glossary.json", "proj-1", "bucket-1")
        
        self.assertTrue(result)
        mock_delete_all.assert_called_with("bucket-1")
        mock_upload.assert_called_with("bucket-1", "path/to/glossary_my-glossary.json", "glossary_my-glossary.json")
        
        # Check that the job prefix was created correctly
        call_args, _ = mock_create_job.call_args
        # The arguments are (service, project_id, location, payload, job_id_prefix).
        # So the job_id_prefix is the 5th argument, at index 4.
        self.assertEqual(call_args[4], "glossary-my-glossary") # Arg 4 is job_id_prefix
        mock_remove.assert_called_once()

if __name__ == '__main__':
    unittest.main()