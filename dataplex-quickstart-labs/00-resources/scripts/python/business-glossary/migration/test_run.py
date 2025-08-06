import unittest
import argparse
from unittest.mock import patch, MagicMock, call
from migration import run

class RunScriptTest(unittest.TestCase):

    @patch('migration.run.importlib.import_module')
    @patch('migration.run.export.run_export')
    @patch('migration.run.utils.discover_glossaries')
    @patch('migration.run.google.auth.default')
    @patch('migration.run.utils.get_migration_arguments')
    def test_main_success_flow(self, mock_get_args, mock_auth, mock_discover, mock_run_export, mock_import_module):
        """Tests the entire successful migration flow with specific assertions."""
        mock_args = argparse.Namespace(
            project='test-project-id', 
            buckets=['bucket-1', 'bucket-2'], 
            orgIds=['1111', '2222'],
            debugging=False
        )
        mock_get_args.return_value = mock_args
        mock_auth.return_value = (None, "billing-project")
        
        mock_urls = ["http://url-1", "http://url-2"]
        mock_discover.return_value = mock_urls
        
        mock_run_export.return_value = True
        mock_importer = MagicMock()
        mock_import_module.return_value = mock_importer

        # Use assertLogs to capture and silence all log output
        with self.assertLogs(level='INFO') as cm:
            run.main(mock_args, "billing-project")
        
        # Verify function calls
        mock_discover.assert_called_once_with("test-project-id", "billing-project")
        self.assertEqual(mock_run_export.call_count, 2)
        mock_importer.main.assert_called_once_with("test-project-id", ["bucket-1", "bucket-2"])

        # Verify key log messages were generated
        log_output = "".join(cm.output)
        self.assertIn("Starting Business Glossary Migration", log_output)
        self.assertIn("Export step finished", log_output)
        self.assertIn("All exports completed successfully", log_output)

    @patch('migration.run.importlib.import_module')
    @patch('migration.run.export.run_export')
    @patch('migration.run.utils.discover_glossaries')
    @patch('migration.run.google.auth.default')
    @patch('migration.run.utils.get_migration_arguments')
    def test_main_export_fails(self, mock_get_args, mock_auth, mock_discover, mock_run_export, mock_import_module):
        """Tests that the import step is skipped and the correct error is logged if any export fails."""
        mock_args = argparse.Namespace(
            project='test-project-id', 
            buckets=['bucket-1'], 
            orgIds=[],
            debugging=False
        )
        mock_get_args.return_value = mock_args
        mock_auth.return_value = (None, "billing-project")
        mock_discover.return_value = ["http://url-1", "http://url-2"]
        mock_run_export.side_effect = [True, False]
        
        # Capture and silence logs, checking for the specifc ERROR message
        with self.assertLogs(level='INFO') as cm:
            run.main(mock_args, "billing-project")
        
        # Verify the specific error message was logged
        self.assertIn("Not all exports were successful. Halting migration", "".join(cm.output))
        
        # Assert that the import module is NEVER called
        mock_import_module.assert_not_called()

    @patch('migration.run.importlib.import_module')
    @patch('migration.run.export.run_export')
    @patch('migration.run.utils.discover_glossaries')
    @patch('migration.run.google.auth.default')
    @patch('migration.run.utils.get_migration_arguments')
    def test_main_no_glossaries_found(self, mock_get_args, mock_auth, mock_discover, mock_run_export, mock_import_module):
        """Tests that the script stops cleanly and logs correctly if no glossaries are found."""
        mock_args = argparse.Namespace(
            project='test-project-id', 
            buckets=['bucket-1'], 
            orgIds=[],
            debugging=False
        )
        mock_get_args.return_value = mock_args
        mock_auth.return_value = (None, "billing-project")
        mock_discover.return_value = []
        
        # Capture and silence logs, checking for the specific INFO message
        with self.assertLogs(level='INFO') as cm:
            run.main(mock_args, "billing-project")
        
        # Verify the specific info message was logged
        self.assertIn("Halting migration as no glossaries were found", "".join(cm.output))
        
        # Assert that the export and import steps were never called
        mock_run_export.assert_not_called()
        mock_import_module.assert_not_called()