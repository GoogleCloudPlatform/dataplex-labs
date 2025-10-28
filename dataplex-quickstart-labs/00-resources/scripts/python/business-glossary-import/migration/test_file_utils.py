import os
import json
import tempfile
import pytest
import logging
from file_utils import *

# Ensure logging is configured for tests
logging.basicConfig(level=logging.DEBUG)

def write_jsonl_file_with_first_line(data: dict, tmp_path):
    file_path = tmp_path / "test.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(data) + "\n")
    return str(file_path)

def test_get_entrylink_type_and_references_valid(tmp_path):
    entry_link = {
        "entryLink": {
            "entryLinkType": "definition",
            "entryReferences": [{"name": "ref1"}, {"name": "ref2"}]
        }
    }
    file_path = write_jsonl_file_with_first_line(entry_link, tmp_path)
    link_type, entry_references = get_entrylink_type_and_references(file_path)
    assert link_type == "definition"
    assert entry_references == [{"name": "ref1"}, {"name": "ref2"}]

def test_get_entrylink_type_and_references_missing_entrylink(tmp_path):
    data = {}
    file_path = write_jsonl_file_with_first_line(data, tmp_path)
    link_type, entry_references = get_entrylink_type_and_references(file_path)
    assert link_type == ""
    assert entry_references == []

def test_get_entrylink_type_and_references_missing_entrylinktype(tmp_path):
    entry_link = {
        "entryLink": {
            "entryReferences": [{"name": "ref1"}]
        }
    }
    file_path = write_jsonl_file_with_first_line(entry_link, tmp_path)
    link_type, entry_references = get_entrylink_type_and_references(file_path)
    assert link_type == ""
    assert entry_references == [{"name": "ref1"}]

def test_get_entrylink_type_and_references_missing_entryreferences(tmp_path):
    entry_link = {
        "entryLink": {
            "entryLinkType": "related"
        }
    }
    file_path = write_jsonl_file_with_first_line(entry_link, tmp_path)
    link_type, entry_references = get_entrylink_type_and_references(file_path)
    assert link_type == "related"
    assert entry_references == []

def test_get_entrylink_type_and_references_empty_file(tmp_path):
    file_path = tmp_path / "empty.json"
    with open(file_path, "w", encoding="utf-8") as f:
        pass
    link_type, entry_references = get_entrylink_type_and_references(str(file_path))
    assert link_type == ""
    assert entry_references == []

def test_get_entrylink_type_and_references_invalid_json(tmp_path):
    file_path = tmp_path / "invalid.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("{invalid json}\n")
    link_type, entry_references = get_entrylink_type_and_references(str(file_path))
    assert link_type == ""
    assert entry_references == []

def test_extract_dp_glossary_term_name_definition_with_two_refs():
    link_type = "definition"
    entry_references = [{"name": "ref1"}, {"name": "ref2"}]
    result = extract_dp_glossary_term_name(link_type, entry_references)
    assert result == "ref2"

def test_extract_dp_glossary_term_name_definition_with_one_ref():
    link_type = "definition"
    entry_references = [{"name": "ref1"}]
    result = extract_dp_glossary_term_name(link_type, entry_references)
    assert result == ""

def test_extract_dp_glossary_term_name_related_with_refs():
    link_type = "related"
    entry_references = [{"name": "related_ref"}]
    result = extract_dp_glossary_term_name(link_type, entry_references)
    assert result == "related_ref"

def test_extract_dp_glossary_term_name_synonym_with_refs():
    link_type = "synonym"
    entry_references = [{"name": "synonym_ref"}]
    result = extract_dp_glossary_term_name(link_type, entry_references)
    assert result == "synonym_ref"

def test_extract_dp_glossary_term_name_related_with_empty_refs():
    link_type = "related"
    entry_references = []
    result = extract_dp_glossary_term_name(link_type, entry_references)
    assert result == ""

def test_extract_dp_glossary_term_name_other_type():
    link_type = "other"
    entry_references = [{"name": "ref"}]
    result = extract_dp_glossary_term_name(link_type, entry_references)
    assert result == ""

def test_extract_dp_glossary_term_name_synonym_with_no_name_key():
    link_type = "synonym"
    entry_references = [{}]
    result = extract_dp_glossary_term_name(link_type, entry_references)
    assert result == ""

def test_extract_job_location_from_entry_group_with_location():
    entry_group = "projects/proj/locations/us-central1/entryGroups/group1"
    result = extract_job_location_from_entry_group(entry_group)
    assert result == "us-central1"

def test_extract_job_location_from_entry_group_with_different_location():
    entry_group = "projects/proj/locations/europe-west2/entryGroups/group2"
    result = extract_job_location_from_entry_group(entry_group)
    assert result == "europe-west2"

def test_extract_job_location_from_entry_group_without_location():
    entry_group = "projects/proj/entryGroups/group3"
    result = extract_job_location_from_entry_group(entry_group)
    assert result == "global"

def test_extract_job_location_from_entry_group_empty_string():
    entry_group = ""
    result = extract_job_location_from_entry_group(entry_group)
    assert result == "global"

def test_extract_job_location_from_entry_group_none():
    result = extract_job_location_from_entry_group(None)
    assert result == "global"

def test_extract_job_location_from_entry_group_location_at_end():
    entry_group = "locations/asia-east1"
    result = extract_job_location_from_entry_group(entry_group)
    assert result == "asia-east1"

def test_extract_job_location_from_entry_group_multiple_locations():
    entry_group = "locations/us-central1/locations/europe-west2/entryGroups/group4"
    result = extract_job_location_from_entry_group(entry_group)
    assert result == "us-central1"

def test_extract_glossary_id_from_synonym_related_filename_valid():
    filename = "entrylinks_related_synonyms_abc123.json"
    result = extract_glossary_id_from_synonym_related_filename(filename)
    assert result == "abc123"

def test_extract_glossary_id_from_synonym_related_filename_with_dashes():
    filename = "entrylinks_related_synonyms_glossary-456.json"
    result = extract_glossary_id_from_synonym_related_filename(filename)
    assert result == "glossary-456"

def test_extract_glossary_id_from_synonym_related_filename_with_underscores():
    filename = "entrylinks_related_synonyms_glossary_id_789.json"
    result = extract_glossary_id_from_synonym_related_filename(filename)
    assert result == "glossary_id_789"

def test_extract_glossary_id_from_synonym_related_filename_multiple_periods():
    filename = "entrylinks_related_synonyms_id.with.periods.json"
    result = extract_glossary_id_from_synonym_related_filename(filename)
    assert result == "id.with.periods"

def test_extract_glossary_id_from_synonym_related_filename_no_match():
    filename = "entrylinks_otherfile_abc123.json"
    result = extract_glossary_id_from_synonym_related_filename(filename)
    assert result == ""

def test_extract_glossary_id_from_synonym_related_filename_wrong_extension():
    filename = "entrylinks_related_synonyms_abc123.txt"
    result = extract_glossary_id_from_synonym_related_filename(filename)
    assert result == ""

def test_extract_glossary_id_from_synonym_related_filename_empty_string():
    filename = ""
    result = extract_glossary_id_from_synonym_related_filename(filename)
    assert result == ""

def test_extract_glossary_id_from_synonym_related_filename_partial_match():
    filename = "entrylinks_related_synonyms_.json"
    result = extract_glossary_id_from_synonym_related_filename(filename)
    assert result == ""

def test_get_link_type_valid(tmp_path):
    entry_link = {
        "entryLink": {
            "entryLinkType": "definition"
        }
    }
    file_path = tmp_path / "test.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(entry_link) + "\n")
    result = get_link_type(str(file_path))
    assert result == "definition"

def test_get_link_type_missing_entrylink(tmp_path):
    entry_link = {}
    file_path = tmp_path / "test.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(entry_link) + "\n")
    result = get_link_type(str(file_path))
    assert result == None

def test_get_link_type_missing_entrylinktype(tmp_path):
    entry_link = {
        "entryLink": {}
    }
    file_path = tmp_path / "test.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(entry_link) + "\n")
    result = get_link_type(str(file_path))
    assert result == ""

def test_get_link_type_empty_file(tmp_path):
    file_path = tmp_path / "empty.json"
    with open(file_path, "w", encoding="utf-8") as f:
        pass
    result = get_link_type(str(file_path))
    assert result is None

def test_get_link_type_invalid_json(tmp_path):
    file_path = tmp_path / "invalid.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("{invalid json}\n")
    result = get_link_type(str(file_path))
    assert result is None

def test_extract_glossary_id_from_term_name_valid():
    term_name = "projects/proj/locations/us-central1/glossaries/abc123/terms/term1"
    result = extract_glossary_id_from_term_name(term_name)
    assert result == "abc123"

def test_extract_glossary_id_from_term_name_with_dashes():
    term_name = "projects/proj/glossaries/glossary-456/terms/term2"
    result = extract_glossary_id_from_term_name(term_name)
    assert result == "glossary-456"

def test_extract_glossary_id_from_term_name_with_underscores():
    term_name = "glossaries/glossary_id_789"
    result = extract_glossary_id_from_term_name(term_name)
    assert result == "glossary_id_789"

def test_extract_glossary_id_from_term_name_multiple_glossaries():
    term_name = "glossaries/first/glossaries/second"
    result = extract_glossary_id_from_term_name(term_name)
    assert result == "first"

def test_extract_glossary_id_from_term_name_no_glossaries():
    term_name = "projects/proj/locations/us-central1/terms/term1"
    result = extract_glossary_id_from_term_name(term_name)
    assert result is None

def test_extract_glossary_id_from_term_name_empty_string():
    term_name = ""
    result = extract_glossary_id_from_term_name(term_name)
    assert result is None

def test_extract_glossary_id_from_term_name_partial_match():
    term_name = "glossaries/"
    result = extract_glossary_id_from_term_name(term_name)
    assert result is None


def test_ensure_dir_creates_directory(tmp_path, caplog):
    test_dir = tmp_path / "new_dir"
    assert not test_dir.exists()
    with caplog.at_level("DEBUG"):
        ensure_dir(str(test_dir))
    assert test_dir.exists()
    assert any("Created directory" in msg for msg in caplog.text.splitlines())

from unittest.mock import patch
from file_utils import ensure_dir
import shutil
from unittest.mock import patch, MagicMock

def test_ensure_dir_creates_directory(tmp_path, caplog):
    test_dir = tmp_path / "new_dir"
    assert not test_dir.exists()

    with patch("file_utils.logger") as mock_logger:
        with caplog.at_level("DEBUG"):
            ensure_dir(str(test_dir))
        assert test_dir.exists()
        mock_logger.debug.assert_called_with(f"Created directory: {test_dir}")

def test_ensure_dir_nested_directories(tmp_path, caplog):
    nested_dir = tmp_path / "parent" / "child" / "grandchild"
    assert not nested_dir.exists()

    with patch("file_utils.logger") as mock_logger:
        with caplog.at_level("DEBUG"):
            ensure_dir(str(nested_dir))
        assert nested_dir.exists()
        mock_logger.debug.assert_called_with(f"Created directory: {nested_dir}")
        
def test_move_file_to_imported_folder_glossary(tmp_path):
    test_file = tmp_path / "glossary_testid.json"
    test_file.write_text("test")
    imported_dir = os.path.join(os.getcwd(), EXPORTED_FILES_DIRECTORY, IMPORTED_GLOSSARIES_DIRECTORY)
    imported_file = os.path.join(imported_dir, "glossary_testid.json")

    with patch("file_utils.logger") as mock_logger, patch("os.replace") as mock_replace, patch("os.makedirs") as mock_makedirs:
        move_file_to_imported_folder(str(test_file))
        mock_makedirs.assert_called_with(imported_dir, exist_ok=True)
        mock_replace.assert_called_with(str(test_file), imported_file)
        mock_logger.debug.assert_called_with(f"Moved file to: {imported_file}")

def test_move_file_to_imported_folder_entrylinks(tmp_path):
    test_file = tmp_path / "entrylinks_testid.json"
    test_file.write_text("test")
    imported_dir = os.path.join(os.getcwd(), EXPORTED_FILES_DIRECTORY, IMPORTED_ENTRYLINKS_DIRECTORY)
    imported_file = os.path.join(imported_dir, "entrylinks_testid.json")

    with patch("file_utils.logger") as mock_logger, patch("os.replace") as mock_replace, patch("os.makedirs") as mock_makedirs:
        move_file_to_imported_folder(str(test_file))
        mock_makedirs.assert_called_with(imported_dir, exist_ok=True)
        mock_replace.assert_called_with(str(test_file), imported_file)
        mock_logger.debug.assert_called_with(f"Moved file to: {imported_file}")

def test_move_file_to_imported_folder_other_file(tmp_path):
    test_file = tmp_path / "otherfile.json"
    test_file.write_text("test")

    with patch("file_utils.logger") as mock_logger, patch("os.remove") as mock_remove:
        move_file_to_imported_folder(str(test_file))
        mock_remove.assert_called_with(str(test_file))
        mock_logger.debug.assert_called_with(f"Deleted local file: {str(test_file)}")

def test_move_file_to_imported_folder_file_not_found(tmp_path):
    test_file = tmp_path / "notfound.json"
    with patch("file_utils.logger") as mock_logger:
        move_file_to_imported_folder(str(test_file))
        mock_logger.warning.assert_called_with(f"File not found: {str(test_file)}. Skipping move/delete operation.")

def test_move_file_to_imported_folder_move_exception(tmp_path):
    test_file = tmp_path / "glossary_testid.json"
    test_file.write_text("test")
    imported_dir = os.path.join(os.getcwd(), EXPORTED_FILES_DIRECTORY, IMPORTED_GLOSSARIES_DIRECTORY)
    imported_file = os.path.join(imported_dir, "glossary_testid.json")

    with patch("file_utils.logger") as mock_logger, \
            patch("os.replace", side_effect=Exception("move error")), \
            patch("os.makedirs"):
        move_file_to_imported_folder(str(test_file))
        assert mock_logger.error.call_args[0][0].startswith("Failed to move file")

def test_move_file_to_imported_folder_remove_exception(tmp_path):
    test_file = tmp_path / "otherfile.json"
    test_file.write_text("test")
    with patch("file_utils.logger") as mock_logger, \
            patch("os.remove", side_effect=Exception("remove error")):
        move_file_to_imported_folder(str(test_file))
        assert mock_logger.error.call_args[0][0].startswith("Failed to delete local file")

def test_is_file_empty_nonexistent(tmp_path):
    non_existent_file = tmp_path / "does_not_exist.json"
    assert is_file_empty(str(non_existent_file)) is True

def test_is_file_empty_empty_file(tmp_path):
    empty_file = tmp_path / "empty.json"
    empty_file.write_text("")
    assert is_file_empty(str(empty_file)) is True

def test_is_file_empty_non_empty_file(tmp_path):
    non_empty_file = tmp_path / "not_empty.json"
    non_empty_file.write_text("some content")
    assert is_file_empty(str(non_empty_file)) is False

def test_is_file_empty_zero_byte_file(tmp_path):
    zero_byte_file = tmp_path / "zero_byte.json"
    zero_byte_file.write_bytes(b"")
    assert is_file_empty(str(zero_byte_file)) is True

def test_is_file_empty_file_with_whitespace(tmp_path):
    whitespace_file = tmp_path / "whitespace.json"
    whitespace_file.write_text("   \n")
    # File is not zero bytes, so should return False
    assert is_file_empty(str(whitespace_file)) is False
    
def test_get_file_paths_from_directory_nonexistent(tmp_path):
    non_existent_dir = tmp_path / "does_not_exist"
    result = get_file_paths_from_directory(str(non_existent_dir))
    assert result == []

def test_get_file_paths_from_directory_empty(tmp_path):
    empty_dir = tmp_path / "empty_dir"
    empty_dir.mkdir()
    result = get_file_paths_from_directory(str(empty_dir))
    assert result == []

def test_get_file_paths_from_directory_json_files(tmp_path):
    test_dir = tmp_path / "json_dir"
    test_dir.mkdir()
    file1 = test_dir / "a.json"
    file2 = test_dir / "b.json"
    file1.write_text("{}")
    file2.write_text("{}")
    result = get_file_paths_from_directory(str(test_dir))
    expected = sorted([str(file1), str(file2)])
    assert result == expected

def test_get_file_paths_from_directory_mixed_files(tmp_path):
    test_dir = tmp_path / "mixed_dir"
    test_dir.mkdir()
    json_file = test_dir / "file.json"
    txt_file = test_dir / "file.txt"
    json_file.write_text("{}")
    txt_file.write_text("text")
    result = get_file_paths_from_directory(str(test_dir))
    assert result == [str(json_file)]

def test_get_file_paths_from_directory_subdirectories(tmp_path):
    test_dir = tmp_path / "parent_dir"
    test_dir.mkdir()
    sub_dir = test_dir / "sub"
    sub_dir.mkdir()
    json_file = test_dir / "file.json"
    json_file.write_text("{}")
    result = get_file_paths_from_directory(str(test_dir))
    assert result == [str(json_file)]

def test_get_file_paths_from_directory_sorted_order(tmp_path):
    test_dir = tmp_path / "sort_dir"
    test_dir.mkdir()
    file_a = test_dir / "a.json"
    file_b = test_dir / "b.json"
    file_c = test_dir / "c.json"
    file_b.write_text("{}")
    file_c.write_text("{}")
    file_a.write_text("{}")
    result = get_file_paths_from_directory(str(test_dir))
    expected = sorted([str(file_a), str(file_b), str(file_c)])
    assert result == expected


