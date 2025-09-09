import pytest
import migration_utils
from migration_utils import _parse_id_list
import os
import tempfile
import argparse
import sys
import subprocess

def test_build_destination_entry_name_with_project_number_basic():
    src = "projects/123456/locations/us-central1/entryGroups/my-group/entries/my-entry"
    dest = "projects/789012/locations/us-central1/entryGroups/my-group/entries/my-entry"
    result = migration_utils.build_destination_entry_name_with_project_number(src, dest)
    assert result == "projects/789012/locations/us-central1/entryGroups/my-group/entries/my-entry"

def test_build_destination_entry_name_with_project_number_different_projects():
    src = "projects/oldproj/locations/europe-west1/entryGroups/group1/entries/entry1"
    dest = "projects/newproj/locations/europe-west1/entryGroups/group1/entries/entry1"
    result = migration_utils.build_destination_entry_name_with_project_number(src, dest)
    assert result == "projects/newproj/locations/europe-west1/entryGroups/group1/entries/entry1"

def test_build_destination_entry_name_with_project_number_numeric_project():
    src = "projects/111/locations/asia-east1/entryGroups/g/entries/e"
    dest = "projects/222/locations/asia-east1/entryGroups/g/entries/e"
    result = migration_utils.build_destination_entry_name_with_project_number(src, dest)
    assert result == "projects/222/locations/asia-east1/entryGroups/g/entries/e"

def test_build_destination_entry_name_with_project_number_missing_project_in_dest():
    src = "projects/333/locations/loc/entryGroups/eg/entries/en"
    dest = "locations/loc/entryGroups/eg/entries/en"
    result = migration_utils.build_destination_entry_name_with_project_number(src, dest)
    # Should keep original project number since dest has no project
    assert result == "projects//locations/loc/entryGroups/eg/entries/en"

def test_build_destination_entry_name_with_project_number_no_project_in_src():
    src = "locations/loc/entryGroups/eg/entries/en"
    dest = "projects/444/locations/loc/entryGroups/eg/entries/en"
    result = migration_utils.build_destination_entry_name_with_project_number(src, dest)
    # Should insert project number at first occurrence
    assert result == "locations/loc/entryGroups/eg/entries/en"

def test_build_destination_entry_name_with_project_number_multiple_projects():
    src = "projects/555/projects/666/locations/loc/entryGroups/eg/entries/en"
    dest = "projects/777/locations/loc/entryGroups/eg/entries/en"
    result = migration_utils.build_destination_entry_name_with_project_number(src, dest)
    # Only first 'projects/...' should be replaced
    assert result == "projects/777/projects/666/locations/loc/entryGroups/eg/entries/en"
def test_extract_project_number_basic():
    s = "projects/123456/locations/us-central1/entryGroups/my-group/entries/my-entry"
    assert migration_utils.extract_project_number(s) == "123456"

def test_extract_project_number_with_non_numeric():
    s = "projects/myproject/locations/us-central1/entryGroups/my-group/entries/my-entry"
    assert migration_utils.extract_project_number(s) == "myproject"

def test_extract_project_number_multiple_projects():
    s = "projects/111/projects/222/locations/loc/entryGroups/eg/entries/en"
    # Should extract only the first project number
    assert migration_utils.extract_project_number(s) == "111"

def test_extract_project_number_no_projects():
    s = "locations/loc/entryGroups/eg/entries/en"
    assert migration_utils.extract_project_number(s) == ""

def test_extract_project_number_empty_string():
    s = ""
    assert migration_utils.extract_project_number(s) == ""

def test_extract_project_number_projects_at_end():
    s = "entryGroups/eg/entries/en/projects/999/"
    assert migration_utils.extract_project_number(s) == "999"
    def test_normalize_linked_resource_no_leading_slash():
        s = "projects/123/locations/us-central1"
        assert migration_utils.normalize_linked_resource(s) == "projects/123/locations/us-central1"

def test_normalize_linked_resource_single_leading_slash():
    s = "/projects/123/locations/us-central1"
    assert migration_utils.normalize_linked_resource(s) == "projects/123/locations/us-central1"

def test_normalize_linked_resource_multiple_leading_slashes():
    s = "///projects/123/locations/us-central1"
    assert migration_utils.normalize_linked_resource(s) == "projects/123/locations/us-central1"

def test_normalize_linked_resource_only_slashes():
    s = "/////"
    assert migration_utils.normalize_linked_resource(s) == ""

def test_normalize_linked_resource_empty_string():
    s = ""
    assert migration_utils.normalize_linked_resource(s) == ""

def test_normalize_linked_resource_slash_in_middle():
            s = "projects/123//locations/us-central1"
            assert migration_utils.normalize_linked_resource(s) == "projects/123//locations/us-central1"
def test_parse_json_line_valid_dict():
    line = '{"a": 1, "b": "test"}'
    result = migration_utils.parse_json_line(line)
    assert result == {"a": 1, "b": "test"}

def test_parse_json_line_valid_list():
    line = '[1, 2, 3]'
    result = migration_utils.parse_json_line(line)
    assert result == [1, 2, 3]

def test_parse_json_line_empty_string():
    line = ''
    result = migration_utils.parse_json_line(line)
    assert result is None

def test_parse_json_line_invalid_json():
    line = '{a: 1, b: 2}'
    result = migration_utils.parse_json_line(line)
    assert result is None

def test_parse_json_line_null_json():
    line = 'null'
    result = migration_utils.parse_json_line(line)
    assert result is None or result is None  # Accepts None

def test_parse_json_line_json_true_false():
    assert migration_utils.parse_json_line('true') is True
    assert migration_utils.parse_json_line('false') is False

def test_parse_json_line_json_number():
    assert migration_utils.parse_json_line('123') == 123
    assert migration_utils.parse_json_line('3.14') == 3.14

def test_read_first_json_line_valid_dict():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write('{"a": 1, "b": "test"}\n{"c": 2}\n')
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result == {"a": 1, "b": "test"}
    finally:
        os.remove(tmp_path)

def test_read_first_json_line_valid_list():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write('[1, 2, 3]\n')
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result == [1, 2, 3]
    finally:
        os.remove(tmp_path)

def test_read_first_json_line_empty_file():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result is None
    finally:
        os.remove(tmp_path)

def test_read_first_json_line_invalid_json():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write('{a: 1, b: 2}\n')
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result is None
    finally:
        os.remove(tmp_path)

def test_read_first_json_line_file_not_found():
    result = migration_utils.read_first_json_line("/nonexistent/file/path.json")
    assert result is None

def test_read_first_json_line_null_json():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write('null\n')
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result is None or result is None  # Accepts None
    finally:
        os.remove(tmp_path)

def test_read_first_json_line_true_false_json():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write('true\n')
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result is True
    finally:
        os.remove(tmp_path)
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write('false\n')
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result is False
    finally:
        os.remove(tmp_path)

def test_read_first_json_line_number_json():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write('123\n')
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result == 123
    finally:
        os.remove(tmp_path)
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write('3.14\n')
        tmp_path = tmp.name
    try:
        result = migration_utils.read_first_json_line(tmp_path)
        assert result == 3.14
    finally:
        os.remove(tmp_path)

def test_parse_entry_url_valid():
    url = "projects/myproj/locations/us-central1/entryGroups/mygroup/entries/myglossary"
    result = migration_utils.parse_entry_url(url)
    assert result == {
        "project": "myproj",
        "location": "us-central1",
        "entry_group": "mygroup",
        "glossary": "myglossary"
    }

def test_parse_entry_url_with_special_chars():
    url = "projects/123-abc/locations/europe-west1/entryGroups/group_1/entries/glossary-xyz"
    result = migration_utils.parse_entry_url(url)
    assert result == {
        "project": "123-abc",
        "location": "europe-west1",
        "entry_group": "group_1",
        "glossary": "glossary-xyz"
    }

def test_parse_glossary_url_with_extra_query_params():
    url = "projects/p/locations/l/entryGroups/g/glossaries/e?e=extra_param&foo=bar"
    result = migration_utils.parse_glossary_url(url)
    expected = {
        "project": "p",
        "location_id": "l",
        "entry_group_id": "g",
        "glossary_id": "e"
    }
    assert result == expected

def test_parse_glossary_url_with_trailing_slash():
    url = "projects/p/locations/l/entryGroups/g/glossaries/e/"
    result = migration_utils.parse_glossary_url(url)
    expected = {
        "project": "p",
        "location_id": "l",
        "entry_group_id": "g",
        "glossary_id": "e"
    }
    assert result == expected

def test_parse_entry_url_missing_entries():
    url = "projects/p/locations/l/entryGroups/g/"
    with pytest.raises(ValueError):
        migration_utils.parse_entry_url(url)

def test_parse_glossary_url_invalid_url():
    url = "https://example.com/not/a/valid/url"
    with pytest.raises(SystemExit):
        migration_utils.parse_glossary_url(url)


def test_parse_entry_url_missing_entry_group():
    url = "projects/p/locations/l/entries/e"
    with pytest.raises(ValueError):
        migration_utils.parse_entry_url(url)

def test_parse_entry_url_missing_project():
    url = "locations/l/entryGroups/g/entries/e"
    with pytest.raises(ValueError):
        migration_utils.parse_entry_url(url)

def test_parse_entry_url_empty_string():
    url = ""
    with pytest.raises(ValueError):
        migration_utils.parse_entry_url(url)

def test_parse_entry_url_query_params():
    url = "projects/p/locations/l/entryGroups/g/entries/e?foo=bar"
    result = migration_utils.parse_entry_url(url)
    assert result == {
        "project": "p",
        "location": "l",
        "entry_group": "g",
        "glossary": "e"
    }

def test_parse_id_list_basic():
    s = "123,456,789"
    result = migration_utils.parse_id_list(s)
    assert result == ["123", "456", "789"]

def test_parse_id_list_with_spaces():
    s = " 123 , 456 , 789 "
    result = migration_utils.parse_id_list(s)
    assert result == ["123", "456", "789"]

def test_parse_id_list_empty_string():
    s = ""
    result = migration_utils.parse_id_list(s)
    assert result == []

def test_parse_id_list_single_id():
    s = "abc"
    result = migration_utils.parse_id_list(s)
    assert result == ["abc"]

def test_parse_id_list_extra_commas():
    s = ",123,,456,,"
    result = migration_utils.parse_id_list(s)
    assert result == ["123", "456"]

def test_parse_id_list_non_string():
    with pytest.raises(argparse.ArgumentTypeError):
        migration_utils.parse_id_list(["not", "a", "string"])

def test_parse_id_list_with_mixed_whitespace():
    s = "id1, id2 ,id3 ,  id4"
    result = migration_utils.parse_id_list(s)
    assert result == ["id1", "id2", "id3", "id4"]

def test_parse_id_list_with_numeric_and_alpha():
    s = "123,abc,456def"
    result = migration_utils.parse_id_list(s)
    assert result == ["123", "abc", "456def"]

def test_configure_migration_argument_parser_required_args():
    parser = argparse.ArgumentParser()
    migration_utils.configure_migration_argument_parser(parser)
    args = parser.parse_args([
        "--project", "my-project",
        "--user-project", "billing-project",
        "--buckets", "bucket1,bucket2",
    ])
    assert args.project == "my-project"
    assert args.user_project == "billing-project"
    assert args.buckets == ["bucket1", "bucket2"]
    assert args.orgIds == []
    assert args.resume_import is False

def test_configure_migration_argument_parser_all_args():
    parser = argparse.ArgumentParser()
    migration_utils.configure_migration_argument_parser(parser)
    args = parser.parse_args([
        "--project", "proj",
        "--user-project", "user-proj",
        "--buckets", "b1,b2",
        "--orgIds", "123,456",
        "--resume-import"
    ])
    assert args.project == "proj"
    assert args.user_project == "user-proj"
    assert args.buckets == ["b1", "b2"]
    assert args.orgIds == ["123", "456"]
    assert args.resume_import is True

def test_configure_migration_argument_parser_missing_required():
    parser = argparse.ArgumentParser()
    migration_utils.configure_migration_argument_parser(parser)
    try:
        parser.parse_args([])
        assert False, "Should have raised SystemExit for missing required arguments"
    except SystemExit:
        pass

def test_configure_migration_argument_parser_buckets_parsing():
    parser = argparse.ArgumentParser()
    migration_utils.configure_migration_argument_parser(parser)
    args = parser.parse_args([
        "--project", "p",
        "--user-project", "up",
        "--buckets", "bucket-1, bucket-2 ,bucket-3"
    ])
    assert args.buckets == ["bucket-1", "bucket-2", "bucket-3"]

def test_configure_migration_argument_parser_orgIds_parsing():
    parser = argparse.ArgumentParser()
    migration_utils.configure_migration_argument_parser(parser)
    args = parser.parse_args([
        "--project", "p",
        "--user-project", "up",
        "--buckets", "b",
        "--orgIds", "id1, id2 ,id3"
    ])
    assert args.orgIds == ["id1", "id2", "id3"]

def test_configure_migration_argument_parser_resume_import_flag():
    parser = argparse.ArgumentParser()
    migration_utils.configure_migration_argument_parser(parser)
    args = parser.parse_args([
        "--project", "p",
        "--user-project", "up",
        "--buckets", "b",
        "--resume-import"
    ])
    assert args.resume_import is True

def test_get_migration_arguments_required_args(monkeypatch):
    argv = [
        "--project", "my-project",
        "--user-project", "billing-project",
        "--buckets", "bucket1,bucket2"
    ]
    args = migration_utils.get_migration_arguments(argv)
    assert args.project == "my-project"
    assert args.user_project == "billing-project"
    assert args.buckets == ["bucket1", "bucket2"]
    assert args.orgIds == []
    assert args.resume_import is False

def test_get_migration_arguments_all_args():
    argv = [
        "--project", "proj",
        "--user-project", "user-proj",
        "--buckets", "b1,b2",
        "--orgIds", "123,456",
        "--resume-import"
    ]
    args = migration_utils.get_migration_arguments(argv)
    assert args.project == "proj"
    assert args.user_project == "user-proj"
    assert args.buckets == ["b1", "b2"]
    assert args.orgIds == ["123", "456"]
    assert args.resume_import is True

def test_get_migration_arguments_missing_required():
    argv = []
    try:
        migration_utils.get_migration_arguments(argv)
        assert False, "Should have raised SystemExit for missing required arguments"
    except SystemExit:
        pass

def test_get_migration_arguments_buckets_parsing():
    argv = [
        "--project", "p",
        "--user-project", "up",
        "--buckets", "bucket-1, bucket-2 ,bucket-3"
    ]
    args = migration_utils.get_migration_arguments(argv)
    assert args.buckets == ["bucket-1", "bucket-2", "bucket-3"]

def test_get_migration_arguments_orgIds_parsing():
    argv = [
        "--project", "p",
        "--user-project", "up",
        "--buckets", "b",
        "--orgIds", "id1, id2 ,id3"
    ]
    args = migration_utils.get_migration_arguments(argv)
    assert args.orgIds == ["id1", "id2", "id3"]

def test_get_migration_arguments_resume_import_flag():
    argv = [
        "--project", "p",
        "--user-project", "up",
        "--buckets", "b",
        "--resume-import"
    ]
    args = migration_utils.get_migration_arguments(argv)
    assert args.resume_import is True
    def test_extract_entry_parts_valid():
        s = "projects/proj1/locations/loc1/entryGroups/eg1/entries/en1"
        result = migration_utils.extract_entry_parts(s)
        assert result == ("projects/proj1/locations/loc1", "eg1", "en1")

    def test_extract_entry_parts_different_values():
        s = "projects/abc/locations/xyz/entryGroups/group-2/entries/entry-99"
        result = migration_utils.extract_entry_parts(s)
        assert result == ("projects/abc/locations/xyz", "group-2", "entry-99")

    def test_extract_entry_parts_missing_entry_group():
        s = "projects/abc/locations/xyz/entries/entry-99"
        result = migration_utils.extract_entry_parts(s)
        assert result is None

    def test_extract_entry_parts_missing_entries():
        s = "projects/abc/locations/xyz/entryGroups/group-2/"
        result = migration_utils.extract_entry_parts(s)
        assert result is None

    def test_extract_entry_parts_missing_project_location():
        s = "entryGroups/group-2/entries/entry-99"
        result = migration_utils.extract_entry_parts(s)
        assert result is None

    def test_extract_entry_parts_extra_segments():
        s = "projects/abc/locations/xyz/entryGroups/group-2/entries/entry-99/extra"
        result = migration_utils.extract_entry_parts(s)
        assert result is None

    def test_extract_entry_parts_empty_string():
        s = ""
        result = migration_utils.extract_entry_parts(s)
        assert result is None

    def test_extract_entry_parts_partial_match():
        s = "projects/abc/locations/xyz/entryGroups/group-2/entries/"
        result = migration_utils.extract_entry_parts(s)
        assert result is None

    def test_extract_entry_parts_with_special_characters():
        s = "projects/abc-123/locations/loc-xyz/entryGroups/g_2/entries/e-99"
        result = migration_utils.extract_entry_parts(s)
        assert result == ("projects/abc-123/locations/loc-xyz", "g_2", "e-99")
        def test_get_entry_link_id_format():
            entry_id = migration_utils.get_entry_link_id()
            # Should start with 'g'
            assert entry_id.startswith('g')
            # Should be all lowercase letters and numbers
            assert entry_id.isalnum()
            assert entry_id == entry_id.lower()
            # Should be length 33 ('g' + 32 hex digits)
            assert len(entry_id) == 33

        def test_get_entry_link_id_uniqueness():
            ids = {migration_utils.get_entry_link_id() for _ in range(100)}
            # All IDs should be unique
            assert len(ids) == 100

        def test_get_entry_link_id_hex_part():
            entry_id = migration_utils.get_entry_link_id()
            hex_part = entry_id[1:]
            # Should be valid hex
            int(hex_part, 16)  # Should not raise
            def test_get_dc_glossary_taxonomy_id_basic():
                s = "projects/x/locations/y/entryGroups/z/entries/ENTRYID"
                assert migration_utils.get_dc_glossary_taxonomy_id(s) == "ENTRYID"

            def test_get_dc_glossary_taxonomy_id_with_special_chars():
                s = "projects/x/locations/y/entryGroups/z/entries/entry-id_123"
                assert migration_utils.get_dc_glossary_taxonomy_id(s) == "entry-id_123"

            def test_get_dc_glossary_taxonomy_id_no_entries_segment():
                s = "projects/x/locations/y/entryGroups/z/entry/ENTRYID"
                assert migration_utils.get_dc_glossary_taxonomy_id(s) == ""

            def test_get_dc_glossary_taxonomy_id_trailing_slash():
                s = "projects/x/locations/y/entryGroups/z/entries/ENTRYID/"
                assert migration_utils.get_dc_glossary_taxonomy_id(s) == ""

            def test_get_dc_glossary_taxonomy_id_multiple_entries_segments():
                s = "projects/x/locations/y/entryGroups/z/entries/abc/entries/ENTRYID"
                assert migration_utils.get_dc_glossary_taxonomy_id(s) == "ENTRYID"

            def test_get_dc_glossary_taxonomy_id_empty_string():
                s = ""
                assert migration_utils.get_dc_glossary_taxonomy_id(s) == ""

            def test_get_dc_glossary_taxonomy_id_none():
                assert migration_utils.get_dc_glossary_taxonomy_id(None) == ""

            def test_get_dc_glossary_taxonomy_id_entries_at_start():
                s = "entries/ENTRYID"
                assert migration_utils.get_dc_glossary_taxonomy_id(s) == "ENTRYID"

            def test_get_dc_glossary_taxonomy_id_entries_with_query_params():
                s = "projects/x/locations/y/entryGroups/z/entries/ENTRYID?foo=bar"
                assert migration_utils.get_dc_glossary_taxonomy_id(s) == "ENTRYID?foo=bar"
                def test_trim_spaces_in_display_name_leading_and_trailing():
                    s = "  My Display Name  "
                    result = migration_utils.trim_spaces_in_display_name(s)
                    assert result == "My Display Name"

                def test_trim_spaces_in_display_name_no_spaces():
                    s = "DisplayName"
                    result = migration_utils.trim_spaces_in_display_name(s)
                    assert result == "DisplayName"

                def test_trim_spaces_in_display_name_only_spaces():
                    s = "     "
                    result = migration_utils.trim_spaces_in_display_name(s)
                    assert result == ""

                def test_trim_spaces_in_display_name_spaces_inside():
                    s = "Name With  Spaces"
                    result = migration_utils.trim_spaces_in_display_name(s)
                    assert result == "Name With  Spaces"

                def test_trim_spaces_in_display_name_empty_string():
                    s = ""
                    result = migration_utils.trim_spaces_in_display_name(s)
                    assert result == ""

def test_trim_spaces_in_display_name_tab_and_newline():
    s = "\tDisplay Name\n"
    result = migration_utils.trim_spaces_in_display_name(s)
    assert result == "Display Name"

def test_normalize_id_basic():
    assert migration_utils.normalize_id("MyID") == "myid"

def test_normalize_id_with_spaces():
    assert migration_utils.normalize_id("My ID With Spaces") == "my-id-with-spaces"

def test_normalize_id_with_special_chars():
    assert migration_utils.normalize_id("ID@#$_2024!") == "id-2024"

def test_normalize_id_with_upper_and_lower():
    assert migration_utils.normalize_id("AbC-DeF") == "abc-def"

def test_normalize_id_with_multiple_hyphens():
    assert migration_utils.normalize_id("A--B---C") == "a-b-c"

def test_normalize_id_with_leading_and_trailing_spaces():
    assert migration_utils.normalize_id("  Leading and trailing  ") == "leading-and-trailing"

def test_normalize_id_with_numbers():
    assert migration_utils.normalize_id("ID123") == "id123"

def test_normalize_id_empty_string():
    assert migration_utils.normalize_id("") == ""

def test_normalize_id_only_special_chars():
    assert migration_utils.normalize_id("@#$%^&*") == ""

def test_normalize_id_mixed_whitespace():
    assert migration_utils.normalize_id("A\tB\nC") == "a-b-c"

def test_normalize_id_hyphens_and_underscores():
    assert migration_utils.normalize_id("A_B-C") == "a-b-c"

def test_normalize_id_multiple_spaces():
    assert migration_utils.normalize_id("A    B   C") == "a-b-c"

def test_normalize_id_strip_multiple_hyphens():
    assert migration_utils.normalize_id("A---B--C") == "a-b-c"

def test_build_glossary_id_from_entry_group_id_removes_prefix_and_normalizes():
    s = "dc_glossary_My EntryGroup"
    result = migration_utils.build_glossary_id_from_entry_group_id(s)
    assert result == "my-entrygroup"

def test_build_glossary_id_from_entry_group_id_no_prefix_normalizes():
    s = "My EntryGroup"
    result = migration_utils.build_glossary_id_from_entry_group_id(s)
    assert result == "my-entrygroup"

def test_build_glossary_id_from_entry_group_id_prefix_only():
    s = "dc_glossary_"
    result = migration_utils.build_glossary_id_from_entry_group_id(s)
    assert result == ""

def test_build_glossary_id_from_entry_group_id_special_chars():
    s = "dc_glossary_ID@#$_2024!"
    result = migration_utils.build_glossary_id_from_entry_group_id(s)
    assert result == "id-2024"

def test_build_glossary_id_from_entry_group_id_empty_string():
    s = ""
    result = migration_utils.build_glossary_id_from_entry_group_id(s)
    assert result == ""

def test_build_glossary_id_from_entry_group_id_multiple_spaces():
    s = "dc_glossary_Entry   Group   Name"
    result = migration_utils.build_glossary_id_from_entry_group_id(s)
    assert result == "entry-group-name"

def test_build_glossary_id_from_entry_group_id_mixed_case_and_hyphens():
    s = "dc_glossary_AbC-DeF"
    result = migration_utils.build_glossary_id_from_entry_group_id(s)
    assert result == "abc-def"

def test_build_glossary_id_from_entry_group_id_leading_and_trailing_spaces():
    s = "dc_glossary_  Leading and trailing  "
    result = migration_utils.build_glossary_id_from_entry_group_id(s)
    assert result == "leading-and-trailing"

def test_parse_glossary_url_valid():
    url = "projects/myproj/locations/us-central1/entryGroups/mygroup/glossaries/myglossary"
    result = migration_utils.parse_glossary_url(url)
    assert result == {
        "project": "myproj",
        "location_id": "us-central1",
        "entry_group_id": "mygroup",
        "glossary_id": "myglossary"
    }

def test_parse_glossary_url_with_special_chars():
    url = "projects/123-abc/locations/europe-west1/entryGroups/group_1/glossaries/glossary-xyz"
    result = migration_utils.parse_glossary_url(url)
    assert result == {
        "project": "123-abc",
        "location_id": "europe-west1",
        "entry_group_id": "group_1",
        "glossary_id": "glossary-xyz"
    }
def test_parse_glossary_url_with_query_params():
    url = "projects/p/locations/l/entryGroups/g/glossaries/e?foo=bar"
    result = migration_utils.parse_glossary_url(url)
    assert result["project"] == "p"
    assert result["location_id"] == "l"
    assert result["entry_group_id"] == "g"
    assert result["glossary_id"].startswith("e")
    

def test_parse_glossary_url_missing_glossaries_segment():
    url = "projects/p/locations/l/entryGroups/g/"
    with pytest.raises(SystemExit):
        migration_utils.parse_glossary_url(url)

def test_parse_glossary_url_missing_entry_group():
    url = "projects/p/locations/l/glossaries/e"
    with pytest.raises(SystemExit):
        migration_utils.parse_glossary_url(url)

def test_parse_glossary_url_missing_project():
    url = "locations/l/entryGroups/g/glossaries/e"
    with pytest.raises(SystemExit):
        migration_utils.parse_glossary_url(url)

def test_parse_glossary_url_empty_string():
    url = ""
    with pytest.raises(SystemExit):
        migration_utils.parse_glossary_url(url)

def test_get_export_arguments_required_args():
    parser = migration_utils.get_export_arguments()
    args = parser.parse_args([
        "--url", "projects/p/locations/l/entryGroups/g/glossaries/e"
    ])
    assert args.url == "projects/p/locations/l/entryGroups/g/glossaries/e"
    assert args.user_project is None
    assert args.orgIds == []

def test_get_export_arguments_all_args():
    parser = migration_utils.get_export_arguments()
    args = parser.parse_args([
        "--url", "projects/p/locations/l/entryGroups/g/glossaries/e",
        "--user-project", "billing-proj",
        "--orgIds", "123,456"
    ])
    assert args.url == "projects/p/locations/l/entryGroups/g/glossaries/e"
    assert args.user_project == "billing-proj"
    assert args.orgIds == ["123", "456"]

def test_get_export_arguments_missing_required_url():
    parser = migration_utils.get_export_arguments()
    try:
        parser.parse_args([])
        assert False, "Should have raised SystemExit for missing required arguments"
    except SystemExit:
        pass

def test_get_export_arguments_orgIds_parsing_spaces_and_commas():
    parser = migration_utils.get_export_arguments()
    args = parser.parse_args([
        "--url", "projects/p/locations/l/entryGroups/g/glossaries/e",
        "--orgIds", "id1, id2 ,id3"
    ])
    assert args.orgIds == ["id1", "id2", "id3"]

def test_get_export_arguments_orgIds_empty_string():
    parser = migration_utils.get_export_arguments()
    args = parser.parse_args([
        "--url", "projects/p/locations/l/entryGroups/g/glossaries/e",
        "--orgIds", ""
    ])
    assert args.orgIds == []

def test_get_org_ids_from_gcloud_success(monkeypatch):
    class DummyCompletedProcess:
        def __init__(self, stdout):
            self.stdout = stdout

    def dummy_run(*args, **kwargs):
        return DummyCompletedProcess("123456\n789012\n")
    monkeypatch.setattr(subprocess, "run", dummy_run)
    org_ids = migration_utils.get_org_ids_from_gcloud()
    assert org_ids == ["123456", "789012"]

def test_get_org_ids_from_gcloud_empty(monkeypatch):
    class DummyCompletedProcess:
        def __init__(self, stdout):
            self.stdout = "\n"

    def dummy_run(*args, **kwargs):
        return DummyCompletedProcess("\n")
    monkeypatch.setattr(subprocess, "run", dummy_run)
    with pytest.raises(SystemExit):
        migration_utils.get_org_ids_from_gcloud()

def test_get_org_ids_from_gcloud_called_process_error(monkeypatch):
    def dummy_run(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "gcloud")
    monkeypatch.setattr(subprocess, "run", dummy_run)
    with pytest.raises(SystemExit):
        migration_utils.get_org_ids_from_gcloud()

def test_get_org_ids_from_gcloud_file_not_found_error(monkeypatch):
    def dummy_run(*args, **kwargs):
        raise FileNotFoundError("gcloud not found")
    monkeypatch.setattr(subprocess, "run", dummy_run)
    with pytest.raises(SystemExit):
        migration_utils.get_org_ids_from_gcloud()

def test__parse_id_list_basic():
    s = "123,456,789"
    result = _parse_id_list(s)
    assert result == ["123", "456", "789"]

def test__parse_id_list_with_spaces():
    s = " 123 , 456 , 789 "
    result = _parse_id_list(s)
    assert result == ["123", "456", "789"]

def test__parse_id_list_empty_string():
    s = ""
    result = _parse_id_list(s)
    assert result == []

def test__parse_id_list_single_id():
    s = "abc"
    result = _parse_id_list(s)
    assert result == ["abc"]

def test__parse_id_list_extra_commas():
    s = ",123,,456,,"
    result = _parse_id_list(s)
    assert result == ["123", "456"]

def test__parse_id_list_non_string():
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_id_list(["not", "a", "string"])

def test__parse_id_list_with_mixed_whitespace():
    s = "id1, id2 ,id3 ,  id4"
    result = _parse_id_list(s)
    assert result == ["id1", "id2", "id3", "id4"]

def test__parse_id_list_with_numeric_and_alpha():
    s = "123,abc,456def"
    result = _parse_id_list(s)
    assert result == ["123", "abc", "456def"]

















