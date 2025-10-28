import pytest
from unittest.mock import patch, MagicMock
from business_glossary_export_v2 import _build_export_context, _run_export_workflow, execute_export
from models import Context

def test_build_export_context_success():
    mock_url = "datacatalog://projects/test_project/locations/global/entryGroups/test_group/glossaries/test_glossary"
    mock_org_ids = ["org1", "org2"]
    mock_user_project = "test_user_project"

    with patch("business_glossary_export_v2.parse_glossary_url") as mock_parse_url, \
         patch("business_glossary_export_v2.get_org_ids_from_gcloud") as mock_get_org_ids, \
         patch("business_glossary_export_v2.get_project_number") as mock_get_project_number, \
         patch("sys.exit") as mock_sys_exit:

        mock_parse_url.return_value = {
            "project": "test_project",
            "location_id": "global",
            "entry_group_id": "test_group",
            "glossary_id": "test_glossary"
        }
        mock_get_org_ids.return_value = ["org1", "org2"]
        mock_get_project_number.return_value = "fake_project_number"

        context = _build_export_context(mock_url, mock_user_project, mock_org_ids)

        assert context.project == "fake_project_number"
        assert context.org_ids == ["org1", "org2"]
        assert context.dc_glossary_id == "test_glossary"
        assert context.dp_glossary_id == "test-glossary"


def test_run_export_workflow_success():
    context = MagicMock()

    with patch("business_glossary_export_v2.fetch_dc_glossary_taxonomy_entries", return_value=[{"entry": "test"}]), \
         patch("business_glossary_export_v2.fetch_dc_glossary_taxonomy_relationships", return_value=[{"relationship": "test"}]), \
         patch("business_glossary_export_v2.process_dc_glossary_entries", return_value=([], [], [])), \
         patch("business_glossary_export_v2.write_files") as mock_write_files, \
         patch("business_glossary_export_v2.create_dataplex_glossary") as mock_create_glossary:

        _run_export_workflow(context)

        mock_write_files.assert_called_once()
        mock_create_glossary.assert_called_once()


def test_execute_export_success():
    with patch("business_glossary_export_v2._build_export_context") as mock_build_context, \
         patch("business_glossary_export_v2._run_export_workflow") as mock_workflow:

        mock_build_context.return_value = MagicMock(dp_glossary_id="test_glossary_id")

        result = execute_export("url", "user_project", ["org1", "org2"])

        mock_build_context.assert_called_once()
        mock_workflow.assert_called_once()
        assert result is True


def test_execute_export_failure():
    with patch("business_glossary_export_v2._build_export_context", side_effect=Exception("Test error")), \
         patch("business_glossary_export_v2.logger") as mock_logger:

        result = execute_export("url", "user_project", ["org1", "org2"])

        mock_logger.error.assert_called()
        mock_logger.debug.assert_called()
        assert result is None
