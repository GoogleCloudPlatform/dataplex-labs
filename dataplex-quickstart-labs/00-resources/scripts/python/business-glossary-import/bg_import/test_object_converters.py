import pytest
from models import CoreAspects
from object_converters import *
from unittest.mock import patch, MagicMock
from models import GlossaryTaxonomyRelationship
from object_converters import convert_to_glossary_taxonomy_relationship
import migration_utils

def get_mock_logger():
        class MockLogger:
            def __init__(self):
                self.last_warning = None
            def warning(self, msg):
                self.last_warning = msg
        return MockLogger()

def test_convert_to_core_aspect_with_full_data():
    core_aspect_dict = {
        "jsonContent": {
            "businessContext": {
                "description": "Test description",
                "contacts": ["contact1", "contact2"]
            }
        }
    }
    result = convert_to_core_aspect(core_aspect_dict)
    assert isinstance(result, CoreAspects)
    assert result.description == "Test description"
    assert result.contacts == ["contact1", "contact2"]

def test_convert_to_core_aspect_with_missing_business_context():
    core_aspect_dict = {
        "jsonContent": {}
    }
    result = convert_to_core_aspect(core_aspect_dict)
    assert result.description == ""
    assert result.contacts == []

def test_convert_to_core_aspect_with_missing_json_content():
    core_aspect_dict = {}
    result = convert_to_core_aspect(core_aspect_dict)
    assert result.description == ""
    assert result.contacts == []

def test_convert_to_core_aspect_with_partial_business_context():
    core_aspect_dict = {
        "jsonContent": {
            "businessContext": {
                "description": "Only description"
            }
        }
    }
    result = convert_to_core_aspect(core_aspect_dict)
    assert result.description == "Only description"
    assert result.contacts == []

def test_convert_to_core_aspect_with_contacts_none():
    core_aspect_dict = {
        "jsonContent": {
            "businessContext": {
                "description": "desc",
                "contacts": None
            }
        }
    }
    result = convert_to_core_aspect(core_aspect_dict)
    assert result.description == "desc"
    assert result.contacts is None or result.contacts == []

def test_convert_glossary_taxonomy_entries_to_objects_with_full_data():
    dc_entries = [
        {
            "name": "entry1",
            "displayName": "Entry One",
            "entryType": "TYPE_A",
            "entryUid": "uid1",
            "coreAspects": {
                "jsonContent": {
                    "businessContext": {
                        "description": "Desc 1",
                        "contacts": ["c1", "c2"]
                    }
                }
            }
        },
        {
            "name": "entry2",
            "displayName": "Entry Two",
            "entryType": "TYPE_B",
            "entryUid": "uid2",
            "coreAspects": {
                "jsonContent": {
                    "businessContext": {
                        "description": "Desc 2",
                        "contacts": []
                    }
                }
            }
        }
    ]
    results = convert_glossary_taxonomy_entries_to_objects(dc_entries)
    assert len(results) == 2
    assert results[0].name == "entry1"
    assert results[0].displayName == "Entry One"
    assert results[0].entryType == "TYPE_A"
    assert results[0].uid == "uid1"
    assert results[0].coreAspects.description == "Desc 1"
    assert results[0].coreAspects.contacts == ["c1", "c2"]
    assert results[1].name == "entry2"
    assert results[1].coreAspects.description == "Desc 2"
    assert results[1].coreAspects.contacts == []

def test_convert_glossary_taxonomy_entries_to_objects_with_missing_fields():
    dc_entries = [
        {
            "name": "entry3",
            # displayName missing
            "entryType": "TYPE_C",
            # entryUid missing
            "coreAspects": {}
        }
    ]
    results = convert_glossary_taxonomy_entries_to_objects(dc_entries)
    assert len(results) == 1
    assert results[0].name == "entry3"
    assert results[0].displayName == ""
    assert results[0].entryType == "TYPE_C"
    assert results[0].uid == ""
    assert results[0].coreAspects.description == ""
    assert results[0].coreAspects.contacts == []

def test_convert_glossary_taxonomy_entries_to_objects_with_empty_list():
    dc_entries = []
    results = convert_glossary_taxonomy_entries_to_objects(dc_entries)
    assert results == []

def test_convert_glossary_taxonomy_entries_to_objects_with_partial_core_aspects():
    dc_entries = [
        {
            "name": "entry4",
            "displayName": "Entry Four",
            "entryType": "TYPE_D",
            "entryUid": "uid4",
            "coreAspects": {
                "jsonContent": {
                    "businessContext": {
                        "description": "Only desc"
                        # contacts missing
                    }
                }
            }
        }
    ]
    results = convert_glossary_taxonomy_entries_to_objects(dc_entries)
    assert len(results) == 1
    assert results[0].coreAspects.description == "Only desc"
    assert results[0].coreAspects.contacts == []

def create_relationship(
    name="rel1",
    source_entry_name="source1",
    relationship_type="RELATED_TO",
    destination_entry=None,
    destination_entry_name_field=None
):
    rel = {
        "name": name,
        "sourceEntry": {"name": source_entry_name},
        "relationshipType": relationship_type,
    }
    if destination_entry is not None:
        rel["destinationEntry"] = destination_entry
    if destination_entry_name_field is not None:
        rel["destinationEntryName"] = destination_entry_name_field
    return rel

def test_convert_glossary_taxonomy_relationships_to_objects_basic(monkeypatch):
    monkeypatch.setattr(
        "object_converters.build_destination_entry_name_with_project_number",
        lambda *args, **kwargs: "built_dest_name"
    )
    dc_relationships = [
        create_relationship(
            name="relA",
            source_entry_name="sourceA",
            destination_entry={"name": "destA"},
            destination_entry_name_field="destA_name",
            relationship_type="TYPE_A"
        ),
        create_relationship(
            name="relB",
            source_entry_name="sourceB",
            destination_entry={"name": "destB"},
            destination_entry_name_field="destB_name",
            relationship_type="TYPE_B"
        ),
    ]
    results = convert_glossary_taxonomy_relationships_to_objects(dc_relationships)
    assert len(results) == 2
    assert all(isinstance(r, GlossaryTaxonomyRelationship) for r in results)
    assert results[0].name == "relA"
    assert results[0].sourceEntryName == "sourceA"
    assert results[0].destinationEntryName == "built_dest_name"
    assert results[0].relationshipType == "TYPE_A"
    assert results[1].name == "relB"
    assert results[1].sourceEntryName == "sourceB"
    assert results[1].destinationEntryName == "built_dest_name"
    assert results[1].relationshipType == "TYPE_B"

def test_convert_glossary_taxonomy_relationships_to_objects_skips_relationship(monkeypatch):
    monkeypatch.setattr(
        "object_converters.build_destination_entry_name_with_project_number",
        lambda *args, **kwargs: "built_dest_name"
    )
    # Should skip because destinationEntry is empty and destinationEntryName is present
    rel_to_skip = create_relationship(
        name="relSkip",
        source_entry_name="sourceSkip",
        destination_entry={},
        destination_entry_name_field="destSkip_name",
        relationship_type="TYPE_SKIP"
    )
    rel_to_include = create_relationship(
        name="relInclude",
        source_entry_name="sourceInc",
        destination_entry={"name": "destInc"},
        destination_entry_name_field="destInc_name",
        relationship_type="TYPE_INC"
    )
    class MockLogging:
        def __init__(self):
            self.warning_calls = []
        def warning(self, msg):
            self.warning_calls.append(msg)
    mock_logging = MockLogging()
    monkeypatch.setattr("object_converters.logging", mock_logging)
    results = convert_glossary_taxonomy_relationships_to_objects([rel_to_skip, rel_to_include])
    assert len(results) == 1
    assert results[0].name == "relInclude"
    assert len(mock_logging.warning_calls) == 1
    assert "Skipping relationship" in mock_logging.warning_calls[0]

def test_convert_glossary_taxonomy_relationships_to_objects_empty_list(monkeypatch):
    monkeypatch.setattr(
        "object_converters.build_destination_entry_name_with_project_number",
        lambda *args, **kwargs: "built_dest_name"
    )
    results = convert_glossary_taxonomy_relationships_to_objects([])
    assert results == []

def test_convert_glossary_taxonomy_relationships_to_objects_missing_fields(monkeypatch):
    monkeypatch.setattr(
        "object_converters.build_destination_entry_name_with_project_number",
        lambda *args, **kwargs: "built_dest_name"
    )
    rel = {
        # name missing
        "sourceEntry": {},
        # destinationEntry missing
        # destinationEntryName missing
        # relationshipType missing
    }
    results = convert_glossary_taxonomy_relationships_to_objects([rel])
    assert len(results) == 1
    assert results[0].name == ""
    assert results[0].sourceEntryName == ""
    assert results[0].destinationEntryName == "built_dest_name"
    assert results[0].relationshipType == ""

def test_skip_relationship_returns_true_when_destination_entry_empty_and_destination_entry_name_present():
    dc_relationship = {
        "destinationEntry": {},
        "destinationEntryName": "some_name"
    }
    assert skip_relationship(dc_relationship) == True

def test_skip_relationship_returns_false_when_destination_entry_present_and_destination_entry_name_present():
    dc_relationship = {
        "destinationEntry": {"name": "dest"},
        "destinationEntryName": "some_name"
    }
    assert skip_relationship(dc_relationship) == False

def test_skip_relationship_returns_false_when_destination_entry_empty_and_destination_entry_name_missing():
    dc_relationship = {
        "destinationEntry": {},
        # destinationEntryName missing
    }
    assert skip_relationship(dc_relationship) == False

def test_skip_relationship_returns_false_when_destination_entry_present_and_destination_entry_name_missing():
    dc_relationship = {
        "destinationEntry": {"name": "dest"},
        # destinationEntryName missing
    }
    assert skip_relationship(dc_relationship) == False

def test_skip_relationship_returns_false_when_both_fields_missing():
    dc_relationship = {}
    assert skip_relationship(dc_relationship) == False

def test_skip_relationship_returns_false_when_destination_entry_is_none_and_destination_entry_name_present():
    dc_relationship = {
        "destinationEntry": None,
        "destinationEntryName": "some_name"
    }
    assert skip_relationship(dc_relationship) == True

def test_skip_relationship_returns_false_when_destination_entry_is_none_and_destination_entry_name_missing():
    dc_relationship = {
        "destinationEntry": None
    }
    assert skip_relationship(dc_relationship) == False

def test_log_skipped_relationship_logs_warning(monkeypatch):
    mock_logger = get_mock_logger()
    monkeypatch.setattr("object_converters.logging", mock_logger)
    dc_relationship = {"name": "relTest"}
    log_skipped_relationship(dc_relationship)
    assert mock_logger.last_warning == (
        "Skipping relationship 'relTest': as the user does not have permission to view the destination entry."
    )

def test_log_skipped_relationship_logs_warning_with_missing_name(monkeypatch):
    mock_logger = get_mock_logger()
    monkeypatch.setattr("object_converters.logging", mock_logger)
    dc_relationship = {}  # name missing
    log_skipped_relationship(dc_relationship)
    assert mock_logger.last_warning == (
        "Skipping relationship '': as the user does not have permission to view the destination entry."
    )
def test_convert_entry_relationships_to_objects_with_full_data():
    dc_relationships = [
        {
            "name": "rel1",
            "sourceColumn": "col1",
            "destinationEntryName": "dest1",
            "relationshipType": "TYPE_A"
        },
        {
            "name": "rel2",
            "sourceColumn": "col2",
            "destinationEntryName": "dest2",
            "relationshipType": "TYPE_B"
        }
    ]
    results = convert_entry_relationships_to_objects(dc_relationships)
    assert len(results) == 2
    assert results[0].name == "rel1"
    assert results[0].sourceColumn == "col1"
    assert results[0].destinationEntryName == "dest1"
    assert results[0].relationshipType == "TYPE_A"
    assert results[1].name == "rel2"
    assert results[1].sourceColumn == "col2"
    assert results[1].destinationEntryName == "dest2"
    assert results[1].relationshipType == "TYPE_B"

def test_convert_entry_relationships_to_objects_with_missing_fields():
    dc_relationships = [
        {
            "name": "rel3"
            # sourceColumn, destinationEntryName, relationshipType missing
        }
    ]
    results = convert_entry_relationships_to_objects(dc_relationships)
    assert len(results) == 1
    assert results[0].name == "rel3"
    assert results[0].sourceColumn is None
    assert results[0].destinationEntryName is None
    assert results[0].relationshipType == ""

def test_convert_entry_relationships_to_objects_with_empty_list():
    dc_relationships = []
    results = convert_entry_relationships_to_objects(dc_relationships)
    assert results == []

def test_convert_entry_relationships_to_objects_with_partial_fields():
    dc_relationships = [
        {
            "sourceColumn": "colX",
            "relationshipType": "TYPE_X"
        }
    ]
    results = convert_entry_relationships_to_objects(dc_relationships)
    assert len(results) == 1
    assert results[0].name == ""
    assert results[0].sourceColumn == "colX"
    assert results[0].destinationEntryName is None
    assert results[0].relationshipType == "TYPE_X"

def test_convert_entry_relationships_to_objects_with_none_values():
    dc_relationships = [
        {
            "name": None,
            "sourceColumn": None,
            "destinationEntryName": None,
            "relationshipType": None
        }
    ]
    results = convert_entry_relationships_to_objects(dc_relationships)
    assert len(results) == 1
    assert results[0].name is None or results[0].name == ""
    assert results[0].sourceColumn is None
    assert results[0].destinationEntryName is None
    assert results[0].relationshipType is None or results[0].relationshipType == ""

def test_convert_entry_search_results_to_objects_with_full_data():
    search_results = [
        {
            "relativeResourceName": "resource1",
            "linkedResource": "linked1"
        },
        {
            "relativeResourceName": "resource2",
            "linkedResource": "linked2"
        }
    ]
    results = convert_entry_search_results_to_objects(search_results)
    assert len(results) == 2
    assert results[0].relativeResourceName == "resource1"
    assert results[0].linkedResource == "linked1"
    assert results[1].relativeResourceName == "resource2"
    assert results[1].linkedResource == "linked2"

def test_convert_entry_search_results_to_objects_with_missing_fields():
    search_results = [
        {
            "relativeResourceName": "resource3"
        },
        {
            "linkedResource": "linked4"
        },
        {
            # both fields missing
        }
    ]
    results = convert_entry_search_results_to_objects(search_results)
    assert len(results) == 3
    assert results[0].relativeResourceName == "resource3"
    assert results[0].linkedResource == ""
    assert results[1].relativeResourceName == ""
    assert results[1].linkedResource == "linked4"
    assert results[2].relativeResourceName == ""
    assert results[2].linkedResource == ""

def test_convert_entry_search_results_to_objects_with_empty_list():
    search_results = []
    results = convert_entry_search_results_to_objects(search_results)
    assert results == []

def test_convert_entry_search_results_to_objects_with_none_values():
    search_results = [
        {
            "relativeResourceName": None,
            "linkedResource": None
        }
    ]
    results = convert_entry_search_results_to_objects(search_results)
    assert len(results) == 1
    assert results[0].relativeResourceName is None or results[0].relativeResourceName == ""
    assert results[0].linkedResource is None or results[0].linkedResource == ""

def test_convert_to_glossary_taxonomy_relationship(monkeypatch):
    dc_relationship = {
        "name": "relationships/rel-123",
        "sourceEntry": {"name": "projects/proj-1/entryGroups/eg1/entries/entry1"},
        "destinationEntry": {"name": "projects/proj-1/entryGroups/eg1/entries/entry2"},
        "destinationEntryName": "projects/proj-2/entryGroups/eg2/entries/entryX",
        "relationshipType": "IS_RELATED_TO"
    }

    def mock_build_destination_entry_name_with_project_number(_, __):
        return "projects/proj-2/entryGroups/eg2/entries/entry-mocked"

    monkeypatch.setattr(
        "object_converters.build_destination_entry_name_with_project_number",
        mock_build_destination_entry_name_with_project_number
    )

    result = convert_to_glossary_taxonomy_relationship(dc_relationship)    
    # Assert the result is as expected
    expected = GlossaryTaxonomyRelationship(
        name="relationships/rel-123",
        sourceEntryName="projects/proj-1/entryGroups/eg1/entries/entry1",
        destinationEntryName="projects/proj-2/entryGroups/eg2/entries/entry-mocked",
        relationshipType="IS_RELATED_TO"
    )

    assert result == expected


