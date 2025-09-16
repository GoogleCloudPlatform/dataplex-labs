import pytest
from data_transformer import *
from collections import defaultdict
from models import *
from unittest.mock import patch, MagicMock
from data_transformer import process_glossary_taxonomy
from models import GlossaryTaxonomyEntry, GlossaryEntry, Context
from unittest.mock import MagicMock

def setup_context():
    return Context(
        user_project="test-project",
        org_ids=[],
        dataplex_entry_group="projects/test-project/locations/global/entryGroups/@dataplex",
        project="test-project",
        location_id="global",
        entry_group_id="@dataplex",
        dc_glossary_id="dc-glossary",
        dp_glossary_id="test-glossary"
    )

def create_glossary_term_entry(name="entry1"):
    return GlossaryTaxonomyEntry(name=name, entryType="TERM", uid="uid1")

def create_search_entry_result(resource="resource1", linked="linked1"):
    return SearchEntryResult(relativeResourceName=resource, linkedResource=linked)

def create_entry_link(name="link1"):
    return EntryLink(name=name, entryLinkType="type1", entryReferences=[])

def create_entry_relationship():
    return DcEntryRelationship(
        name="projects/test/locations/global/entryGroups/@dataplex",
        relationshipType="DESCRIBED_BY",
        destinationEntryName="dest1",
        sourceColumn="col1"
    )

def create_glossary_taxonomy_relationship(source="source_entry", dest="dest_entry", rel_type="is_synonymous_to", name="rel_name"):
        return GlossaryTaxonomyRelationship(
            sourceEntryName=source,
            destinationEntryName=dest,
            relationshipType=rel_type,
            name=name
        )

def test_process_dc_glossary_entries_basic(monkeypatch):
    context = setup_context()
    entries = [
        create_glossary_term_entry(name="entry1"),
        create_glossary_term_entry(name="entry2")
    ]
    relationships_map = {"entry1": [], "entry2": []}

    monkeypatch.setattr("data_transformer.build_maps", lambda e, r: ({}, {}))
    monkeypatch.setattr("data_transformer.process_glossary_entries", lambda c, e, m1, m2: ["glossary_entry1", "glossary_entry2"])
    monkeypatch.setattr("data_transformer.build_term_to_term_links", lambda c, e, r: ["term_link1", "term_link2"])
    monkeypatch.setattr("data_transformer.build_entry_to_term_links", lambda c, e: ["entry_link1", "entry_link2"])
    monkeypatch.setattr("data_transformer.deduplicate_term_to_term_links", lambda links: ["dedup_term_link1"])
    monkeypatch.setattr("data_transformer.group_entry_links_by_project_location_entry_group", lambda links: {"group1": ["entry_link1"], "group2": ["entry_link2"]})

    result = process_dc_glossary_entries(context, entries, relationships_map)
    assert result[0] == ["glossary_entry1", "glossary_entry2"]
    assert result[1] == ["dedup_term_link1"]
    assert result[2] == {"group1": ["entry_link1"], "group2": ["entry_link2"]}

def test_group_entry_links_by_project_location_entry_group_basic():
    entry1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[EntryReference(name="projects/p1/locations/l1/entryGroups/g1/entries/e1")])
    entry2 = EntryLink(name="link2", entryLinkType="type1", entryReferences=[EntryReference(name="projects/p1/locations/l1/entryGroups/g1/entries/e2")])
    entry3 = EntryLink(name="link3", entryLinkType="type1", entryReferences=[EntryReference(name="projects/p2/locations/l2/entryGroups/g2/entries/e3")])
    entry4 = EntryLink(name="link4", entryLinkType="type1", entryReferences=[EntryReference(name="projects/p2/locations/l2/entryGroups/g2/entries/e4")])
    entry5 = EntryLink(name="link5", entryLinkType="type1", entryReferences=[EntryReference(name="invalid/resource/name")])  # Should not be grouped

    links = [entry1, entry2, entry3, entry4, entry5]
    grouped = group_entry_links_by_project_location_entry_group(links)

    assert isinstance(grouped, dict)
    assert extract_project_location_entrygroup("projects/p1/locations/l1/entryGroups/g1/entries/e1") in grouped
    assert extract_project_location_entrygroup("projects/p2/locations/l2/entryGroups/g2/entries/e3") in grouped
    assert entry1 in grouped[extract_project_location_entrygroup(entry1.entryReferences[0].name)]
    assert entry2 in grouped[extract_project_location_entrygroup(entry2.entryReferences[0].name)]
    assert entry3 in grouped[extract_project_location_entrygroup(entry3.entryReferences[0].name)]
    assert entry4 in grouped[extract_project_location_entrygroup(entry4.entryReferences[0].name)]
    # entry5 should not be grouped
    for group in grouped.values():
        assert entry5 not in group

def test_group_entry_links_by_project_location_entry_group_empty():
    grouped = group_entry_links_by_project_location_entry_group([])
    assert grouped == {}

def test_group_entry_links_by_project_location_entry_group_no_entry_references():
    entry = EntryLink(name="link_empty", entryLinkType="type1", entryReferences=[])
    grouped = group_entry_links_by_project_location_entry_group([entry])
    assert grouped == {}

def test_group_entry_links_by_project_location_entry_group_none_entry_reference_name():
    entry = EntryLink(name="link_none", entryLinkType="type1", entryReferences=[EntryReference(name=None)])
    grouped = group_entry_links_by_project_location_entry_group([entry])
    assert grouped == {}

def test_group_entry_links_by_project_location_entry_group_single_group():
    entry1 = EntryLink(
        name="link1",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name="projects/proj1/locations/loc1/entryGroups/group1/entries/entryA")]
    )
    entry2 = EntryLink(
        name="link2",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name="projects/proj1/locations/loc1/entryGroups/group1/entries/entryB")]
    )
    links = [entry1, entry2]
    grouped = group_entry_links_by_project_location_entry_group(links)
    key = extract_project_location_entrygroup(entry1.entryReferences[0].name)
    assert key in grouped
    assert entry1 in grouped[key]
    assert entry2 in grouped[key]
    assert len(grouped[key]) == 2

def test_group_entry_links_by_project_location_entry_group_multiple_groups():
    entry1 = EntryLink(
        name="link1",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name="projects/proj1/locations/loc1/entryGroups/group1/entries/entryA")]
    )
    entry2 = EntryLink(
        name="link2",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name="projects/proj2/locations/loc2/entryGroups/group2/entries/entryB")]
    )
    links = [entry1, entry2]
    grouped = group_entry_links_by_project_location_entry_group(links)
    key1 = extract_project_location_entrygroup(entry1.entryReferences[0].name)
    key2 = extract_project_location_entrygroup(entry2.entryReferences[0].name)
    assert key1 in grouped
    assert key2 in grouped
    assert entry1 in grouped[key1]
    assert entry2 in grouped[key2]
    assert len(grouped[key1]) == 1
    assert len(grouped[key2]) == 1

def test_group_entry_links_by_project_location_entry_group_invalid_reference():
    entry = EntryLink(
        name="link_invalid",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name="invalid/resource/name")]
    )
    grouped = group_entry_links_by_project_location_entry_group([entry])
    assert grouped == {}

def test_group_entry_links_by_project_location_entry_group_empty_list():
    grouped = group_entry_links_by_project_location_entry_group([])
    assert grouped == {}

def test_group_entry_links_by_project_location_entry_group_no_entry_references():
    entry = EntryLink(
        name="link_empty",
        entryLinkType="typeA",
        entryReferences=[]
    )
    grouped = group_entry_links_by_project_location_entry_group([entry])
    assert grouped == {}

def test_group_entry_links_by_project_location_entry_group_none_entry_reference_name():
    entry = EntryLink(
        name="link_none",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name=None)]
    )
    grouped = group_entry_links_by_project_location_entry_group([entry])
    assert grouped == {}

def test_group_entry_links_by_project_location_entry_group_mixed_valid_invalid():
    entry_valid = EntryLink(
        name="link_valid",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name="projects/proj1/locations/loc1/entryGroups/group1/entries/entryA")]
    )
    entry_invalid = EntryLink(
        name="link_invalid",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name="invalid/resource/name")]
    )
    entry_none = EntryLink(
        name="link_none",
        entryLinkType="typeA",
        entryReferences=[EntryReference(name=None)]
    )
    links = [entry_valid, entry_invalid, entry_none]
    grouped = group_entry_links_by_project_location_entry_group(links)
    key = extract_project_location_entrygroup(entry_valid.entryReferences[0].name)
    assert key in grouped
    assert entry_valid in grouped[key]
    assert entry_invalid not in grouped.get(key, [])
    assert entry_none not in grouped.get(key, [])
    assert len(grouped[key]) == 1
def test_deduplicate_term_to_term_links_basic():
    # Two links with same references and type, should deduplicate to one
    ref1 = EntryReference(name="entryA")
    ref2 = EntryReference(name="entryB")
    link1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[ref1, ref2])
    link2 = EntryLink(name="link2", entryLinkType="type1", entryReferences=[ref2, ref1])  # Same set, different order
    links = [link1, link2]
    deduped = deduplicate_term_to_term_links(links)
    assert len(deduped) == 1
    assert deduped[0] in links

def test_deduplicate_term_to_term_links_different_types():
    # Same references, different types, should not deduplicate
    ref1 = EntryReference(name="entryA")
    ref2 = EntryReference(name="entryB")
    link1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[ref1, ref2])
    link2 = EntryLink(name="link2", entryLinkType="type2", entryReferences=[ref2, ref1])
    links = [link1, link2]
    deduped = deduplicate_term_to_term_links(links)
    assert len(deduped) == 2
    assert link1 in deduped
    assert link2 in deduped

def test_deduplicate_term_to_term_links_empty():
    deduped = deduplicate_term_to_term_links([])
    assert deduped == []

def test_deduplicate_term_to_term_links_single():
    ref1 = EntryReference(name="entryA")
    link1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[ref1])
    deduped = deduplicate_term_to_term_links([link1])
    assert deduped == [link1]

def test_deduplicate_term_to_term_links_multiple_unique():
    ref1 = EntryReference(name="entryA")
    ref2 = EntryReference(name="entryB")
    ref3 = EntryReference(name="entryC")
    link1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[ref1, ref2])
    link2 = EntryLink(name="link2", entryLinkType="type1", entryReferences=[ref3])
    link3 = EntryLink(name="link3", entryLinkType="type2", entryReferences=[ref1, ref2])
    links = [link1, link2, link3]
    unique_links = deduplicate_term_to_term_links(links)
    assert len(unique_links) == 3
    for link in links:
        assert any(
            unique.name == link.name and unique.entryLinkType == link.entryLinkType and unique.entryReferences == link.entryReferences
            for unique in unique_links
        )

def test_deduplicate_term_to_term_links_none_reference():
    # Links with None reference names should be handled
    ref1 = EntryReference(name=None)
    link1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[ref1])
    link2 = EntryLink(name="link2", entryLinkType="type1", entryReferences=[ref1])
    deduped = deduplicate_term_to_term_links([link1, link2])
    assert len(deduped) == 1
    assert deduped[0] in [link1, link2]

def test_deduplicate_term_to_term_links_empty_entry_references():
    # Links with empty entryReferences should be deduplicated
    link1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[])
    link2 = EntryLink(name="link2", entryLinkType="type1", entryReferences=[])
    deduped = deduplicate_term_to_term_links([link1, link2])
    assert len(deduped) == 1
    assert deduped[0] in [link1, link2]
    
def test_extract_project_location_entrygroup_valid():
    resource_name = "projects/proj1/locations/loc1/entryGroups/group1/entries/entryA"
    result = extract_project_location_entrygroup(resource_name)
    assert result == "proj1_loc1_group1"

def test_extract_project_location_entrygroup_valid_no_entries():
    resource_name = "projects/proj2/locations/loc2/entryGroups/group2"
    result = extract_project_location_entrygroup(resource_name)
    assert result == "proj2_loc2_group2"

def test_extract_project_location_entrygroup_invalid_missing_parts():
    resource_name = "projects/proj1/locations/loc1/entryGroups"
    result = extract_project_location_entrygroup(resource_name)
    assert result is None

def test_extract_project_location_entrygroup_invalid_format():
    resource_name = "invalid/resource/name"
    result = extract_project_location_entrygroup(resource_name)
    assert result is None

def test_extract_project_location_entrygroup_empty_string():
    resource_name = ""
    result = extract_project_location_entrygroup(resource_name)
    assert result is None

def test_extract_project_location_entrygroup_extra_segments():
    resource_name = "projects/projX/locations/locX/entryGroups/groupX/entries/entryX/extra"
    result = extract_project_location_entrygroup(resource_name)
    assert result == "projX_locX_groupX"
def make_context():
    return Context(
        user_project="test-project",
        org_ids=[],
        dataplex_entry_group="projects/test-project/locations/global/entryGroups/@dataplex",
        project="test-project",
        location_id="global",
        entry_group_id="@dataplex",
        dc_glossary_id="dc-glossary",
        dp_glossary_id="test-glossary"
    )

def make_entry(name="entry1", entryType="TERM"):
    return GlossaryTaxonomyEntry(name=name, entryType=entryType)

def make_entry_link(name="link1"):
    return EntryLink(name=name, entryLinkType="type1", entryReferences=[])

def test_build_entry_to_term_links_basic(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2")]
    def mock_transform_term_entry_links(context, entry):
        if entry.name == "entry1":
            return [make_entry_link("link1"), make_entry_link("link2")]
        elif entry.name == "entry2":
            return [make_entry_link("link3")]
        return []
    monkeypatch.setattr("data_transformer.transform_term_entry_links", mock_transform_term_entry_links)
    result = build_entry_to_term_links(context, entries, max_workers=2)
    assert isinstance(result, list)
    assert len(result) == 3
    assert any(link.name == "link1" for link in result)
    assert any(link.name == "link2" for link in result)
    assert any(link.name == "link3" for link in result)

def test_build_entry_to_term_links_empty_entries(monkeypatch):
    context = make_context()
    entries = []
    monkeypatch.setattr("data_transformer.transform_term_entry_links", lambda context, entry: [])
    result = build_entry_to_term_links(context, entries)
    assert result == []

def test_build_entry_to_term_links_transform_returns_empty(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2")]
    monkeypatch.setattr("data_transformer.transform_term_entry_links", lambda context, entry: [])
    result = build_entry_to_term_links(context, entries)
    assert result == []

def test_build_entry_to_term_links_transform_returns_none(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2")]
    monkeypatch.setattr("data_transformer.transform_term_entry_links", lambda context, entry: None)
    result = build_entry_to_term_links(context, entries)
    assert result == []

def test_build_entry_to_term_links_mixed_empty_and_nonempty(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2"), make_entry("entry3")]
    def mock_transform_term_entry_links(context, entry):
        if entry.name == "entry1":
            return [make_entry_link("link1")]
        elif entry.name == "entry2":
            return []
        elif entry.name == "entry3":
            return [make_entry_link("link2"), make_entry_link("link3")]
        return []
    monkeypatch.setattr("data_transformer.transform_term_entry_links", mock_transform_term_entry_links)
    result = build_entry_to_term_links(context, entries)
    assert len(result) == 3
    assert any(link.name == "link1" for link in result)
    assert any(link.name == "link2" for link in result)
    assert any(link.name == "link3" for link in result)

def test_build_entry_to_term_links_max_workers(monkeypatch):
    context = make_context()
    entries = [make_entry(f"entry{i}") for i in range(5)]
    def mock_transform_term_entry_links(context, entry):
        idx = int(entry.name.replace("entry", ""))
        return [make_entry_link(f"link{idx}")]
    monkeypatch.setattr("data_transformer.transform_term_entry_links", mock_transform_term_entry_links)
    # Patch ThreadPoolExecutor to run sequentially for deterministic test
    class DummyExecutor:
        def __init__(self, max_workers=None):
            self.max_workers = max_workers
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
        def map(self, func, iterable):
            return list(map(func, iterable))
    monkeypatch.setattr("concurrent.futures.ThreadPoolExecutor", DummyExecutor)
    result = build_entry_to_term_links(context, entries, max_workers=3)
    assert len(result) == 5
    for i in range(5):
        assert any(link.name == f"link{i}" for link in result)

def test_build_term_to_term_links_basic(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2")]
    relationships_map = {"entry1": [], "entry2": []}

    def mock_transform_term_term_links(context, entry, rel_map):
        if entry.name == "entry1":
            return [make_entry_link("link1"), make_entry_link("link2")]
        elif entry.name == "entry2":
            return [make_entry_link("link3")]
        return []
    monkeypatch.setattr("data_transformer.transform_term_term_links", mock_transform_term_term_links)
    result = build_term_to_term_links(context, entries, relationships_map)
    assert isinstance(result, list)
    assert len(result) == 3
    assert any(link.name == "link1" for link in result)
    assert any(link.name == "link2" for link in result)
    assert any(link.name == "link3" for link in result)

def test_build_term_to_term_links_empty_entries(monkeypatch):
    context = make_context()
    entries = []
    relationships_map = {}
    monkeypatch.setattr("data_transformer.transform_term_term_links", lambda context, entry, rel_map: [])
    result = build_term_to_term_links(context, entries, relationships_map)
    assert result == []

def test_build_term_to_term_links_transform_returns_empty(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2")]
    relationships_map = {"entry1": [], "entry2": []}
    monkeypatch.setattr("data_transformer.transform_term_term_links", lambda context, entry, rel_map: [])
    result = build_term_to_term_links(context, entries, relationships_map)
    assert result == []

def test_build_term_to_term_links_transform_returns_none(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2")]
    relationships_map = {"entry1": [], "entry2": []}
    monkeypatch.setattr("data_transformer.transform_term_term_links", lambda context, entry, rel_map: None)
    result = build_term_to_term_links(context, entries, relationships_map)
    assert result == []

def test_build_term_to_term_links_mixed_empty_and_nonempty(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2"), make_entry("entry3")]
    relationships_map = {"entry1": [], "entry2": [], "entry3": []}
    def mock_transform_term_term_links(context, entry, rel_map):
        if entry.name == "entry1":
            return [make_entry_link("link1")]
        elif entry.name == "entry2":
            return []
        elif entry.name == "entry3":
            return [make_entry_link("link2"), make_entry_link("link3")]
        return []
    monkeypatch.setattr("data_transformer.transform_term_term_links", mock_transform_term_term_links)
    result = build_term_to_term_links(context, entries, relationships_map)
    assert len(result) == 3
    assert any(link.name == "link1" for link in result)
    assert any(link.name == "link2" for link in result)
    assert any(link.name == "link3" for link in result)

def test_process_glossary_entries_basic(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2")]
    entry_to_parent_map = {"entry1": "parent1", "entry2": "parent2"}
    entry_id_to_type_map = {"entry1": "TERM", "entry2": "CATEGORY"}

    # Patch process_glossary_taxonomy to return a GlossaryEntry for each input
    glossary_entry1 = GlossaryEntry(name="name1", entryType="TERM", parentEntry="parent1", aspects={}, entrySource=None)
    glossary_entry2 = GlossaryEntry(name="name2", entryType="CATEGORY", parentEntry="parent2", aspects={}, entrySource=None)
    def mock_process_glossary_taxonomy(context, entry, parent_map, type_map):
        if entry.name == "entry1":
            return glossary_entry1
        elif entry.name == "entry2":
            return glossary_entry2
        return None
    monkeypatch.setattr("data_transformer.process_glossary_taxonomy", mock_process_glossary_taxonomy)

    result = process_glossary_entries(context, entries, entry_to_parent_map, entry_id_to_type_map)
    assert result == [glossary_entry1, glossary_entry2]

def test_process_glossary_entries_none_returned(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2")]
    entry_to_parent_map = {}
    entry_id_to_type_map = {}

    # Patch process_glossary_taxonomy to always return None
    monkeypatch.setattr("data_transformer.process_glossary_taxonomy", lambda c, e, m1, m2: None)
    result = process_glossary_entries(context, entries, entry_to_parent_map, entry_id_to_type_map)
    assert result == []

def test_process_glossary_entries_mixed_none_and_valid(monkeypatch):
    context = make_context()
    entries = [make_entry("entry1"), make_entry("entry2"), make_entry("entry3")]
    entry_to_parent_map = {}
    entry_id_to_type_map = {}

    glossary_entry1 = GlossaryEntry(name="name1", entryType="TERM", parentEntry="parent1", aspects={}, entrySource=None)
    glossary_entry3 = GlossaryEntry(name="name3", entryType="TERM", parentEntry="parent3", aspects={}, entrySource=None)
    def mock_process_glossary_taxonomy(context, entry, parent_map, type_map):
        if entry.name == "entry1":
            return glossary_entry1
        elif entry.name == "entry2":
            return None
        elif entry.name == "entry3":
            return glossary_entry3
        return None
    monkeypatch.setattr("data_transformer.process_glossary_taxonomy", mock_process_glossary_taxonomy)
    result = process_glossary_entries(context, entries, entry_to_parent_map, entry_id_to_type_map)
    assert result == [glossary_entry1, glossary_entry3]

def test_process_glossary_entries_empty_entries(monkeypatch):
    context = make_context()
    entries = []
    entry_to_parent_map = {}
    entry_id_to_type_map = {}
    # Patch process_glossary_taxonomy just in case
    monkeypatch.setattr("data_transformer.process_glossary_taxonomy", lambda c, e, m1, m2: GlossaryEntry(name="dummy", entryType="TERM", parentEntry="parent", aspects={}, entrySource=None))
    result = process_glossary_entries(context, entries, entry_to_parent_map, entry_id_to_type_map)
    assert result == []

def test_build_type_map_basic(monkeypatch):
    # Patch get_dc_glossary_taxonomy_id to return the name unchanged for simplicity
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    entries = [
        GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term"),
        GlossaryTaxonomyEntry(name="entry2", entryType="glossary-category"),
        GlossaryTaxonomyEntry(name="entry3", entryType="glossary-term"),
    ]
    result = build_type_map(entries)
    assert result == {
        "entry1": "glossary-term",
        "entry2": "glossary-category",
        "entry3": "glossary-term"
    }

def test_build_type_map_empty(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    entries = []
    result = build_type_map(entries)
    assert result == {}

def test_build_type_map_duplicate_names(monkeypatch):
    # If get_dc_glossary_taxonomy_id returns same value for multiple entries, last one wins
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "same_id")
    entries = [
        GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term"),
        GlossaryTaxonomyEntry(name="entry2", entryType="glossary-category"),
    ]
    result = build_type_map(entries)
    # Only last entryType should be present
    assert result == {"same_id": "glossary-category"}

def test_build_type_map_custom_id(monkeypatch):
    # Simulate get_dc_glossary_taxonomy_id returning a custom id
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: f"id_{name}")
    entries = [
        GlossaryTaxonomyEntry(name="foo", entryType="glossary-term"),
        GlossaryTaxonomyEntry(name="bar", entryType="glossary-category"),
    ]
    result = build_type_map(entries)
    assert result == {"id_foo": "glossary-term", "id_bar": "glossary-category"}

def test_build_type_map_none_entry_type(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    entries = [
        GlossaryTaxonomyEntry(name="entry1", entryType=None),
        GlossaryTaxonomyEntry(name="entry2", entryType="glossary-category"),
    ]
    result = build_type_map(entries)
    assert result == {"entry1": None, "entry2": "glossary-category"}

def test_build_parent_map_basic(monkeypatch):
    # Patch get_dc_glossary_taxonomy_id to return the name unchanged
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    # Patch DC_RELATIONSHIP_TYPE_BELONGS_TO
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    entry1 = GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")
    entry2 = GlossaryTaxonomyEntry(name="entry2", entryType="glossary-category")
    rel1 = MagicMock()
    rel1.relationshipType = "BELONGS_TO"
    rel1.destinationEntryName = "parent1"
    rel2 = MagicMock()
    rel2.relationshipType = "BELONGS_TO"
    rel2.destinationEntryName = "parent2"
    relationships_map = {
        "entry1": [rel1],
        "entry2": [rel2]
    }
    result = build_parent_map([entry1, entry2], relationships_map)
    assert result == {"entry1": "parent1", "entry2": "parent2"}

def test_build_parent_map_no_relationships(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    entry1 = GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")
    relationships_map = {"entry1": []}
    result = build_parent_map([entry1], relationships_map)
    assert result == {}

def test_build_parent_map_non_belongs_to_relationship(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    entry1 = GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")
    rel1 = MagicMock()
    rel1.relationshipType = "OTHER_TYPE"
    rel1.destinationEntryName = "parent1"
    relationships_map = {"entry1": [rel1]}
    result = build_parent_map([entry1], relationships_map)
    assert result == {}

def test_build_parent_map_missing_destination_entry_name(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    entry1 = GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")
    rel1 = MagicMock()
    rel1.relationshipType = "BELONGS_TO"
    rel1.destinationEntryName = None
    relationships_map = {"entry1": [rel1]}
    result = build_parent_map([entry1], relationships_map)
    assert result == {}

def test_build_parent_map_missing_entry_id(monkeypatch):
    # Patch get_dc_glossary_taxonomy_id to return None for entry1
    def mock_get_dc_id(name):
        if name == "entry1":
            return None
        return name
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", mock_get_dc_id)
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    entry1 = GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")
    rel1 = MagicMock()
    rel1.relationshipType = "BELONGS_TO"
    rel1.destinationEntryName = "parent1"
    relationships_map = {"entry1": [rel1]}
    result = build_parent_map([entry1], relationships_map)
    assert result == {}

def test_build_parent_map_missing_parent_id(monkeypatch):
    # Patch get_dc_glossary_taxonomy_id to return None for parent1
    def mock_get_dc_id(name):
        if name == "parent1":
            return None
        return name
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", mock_get_dc_id)
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    entry1 = GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")
    rel1 = MagicMock()
    rel1.relationshipType = "BELONGS_TO"
    rel1.destinationEntryName = "parent1"
    relationships_map = {"entry1": [rel1]}
    result = build_parent_map([entry1], relationships_map)
    assert result == {}

def test_build_parent_map_multiple_relationships(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    entry1 = GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")
    rel1 = MagicMock()
    rel1.relationshipType = "BELONGS_TO"
    rel1.destinationEntryName = "parent1"
    rel2 = MagicMock()
    rel2.relationshipType = "BELONGS_TO"
    rel2.destinationEntryName = "parent2"
    relationships_map = {"entry1": [rel1, rel2]}
    result = build_parent_map([entry1], relationships_map)
    # Only the first BELONGS_TO should be used
    assert result == {"entry1": "parent1"}

def test_build_parent_map_empty_entries(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    relationships_map = {}
    result = build_parent_map([], relationships_map)
    assert result == {}

def test_build_parent_map_entry_not_in_relationships_map(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: name)
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_BELONGS_TO", "BELONGS_TO")
    entry1 = GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")
    relationships_map = {}  # entry1 not present
    result = build_parent_map([entry1], relationships_map)
    assert result == {}

def test_build_maps_basic(monkeypatch):
    # Patch build_parent_map and build_type_map to return known values
    monkeypatch.setattr("data_transformer.build_parent_map", lambda entries, rel_map: {"entry1": "parent1"})
    monkeypatch.setattr("data_transformer.build_type_map", lambda entries: {"entry1": "glossary-term"})
    entries = [GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")]
    relationships_map = {"entry1": []}
    entry_to_parent_map, entry_id_to_type_map = build_maps(entries, relationships_map)
    assert entry_to_parent_map == {"entry1": "parent1"}
    assert entry_id_to_type_map == {"entry1": "glossary-term"}

def test_build_maps_empty(monkeypatch):
    monkeypatch.setattr("data_transformer.build_parent_map", lambda entries, rel_map: {})
    monkeypatch.setattr("data_transformer.build_type_map", lambda entries: {})
    entries = []
    relationships_map = {}
    entry_to_parent_map, entry_id_to_type_map = build_maps(entries, relationships_map)
    assert entry_to_parent_map == {}
    assert entry_id_to_type_map == {}

def test_build_maps_multiple_entries(monkeypatch):
    monkeypatch.setattr("data_transformer.build_parent_map", lambda entries, rel_map: {"entry1": "parent1", "entry2": "parent2"})
    monkeypatch.setattr("data_transformer.build_type_map", lambda entries: {"entry1": "glossary-term", "entry2": "glossary-category"})
    entries = [
        GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term"),
        GlossaryTaxonomyEntry(name="entry2", entryType="glossary-category"),
    ]
    relationships_map = {"entry1": [], "entry2": []}
    entry_to_parent_map, entry_id_to_type_map = build_maps(entries, relationships_map)
    assert entry_to_parent_map == {"entry1": "parent1", "entry2": "parent2"}
    assert entry_id_to_type_map == {"entry1": "glossary-term", "entry2": "glossary-category"}

def test_build_maps_parent_map_none(monkeypatch):
    # Simulate build_parent_map returning None (should be handled as empty dict)
    monkeypatch.setattr("data_transformer.build_parent_map", lambda entries, rel_map: None)
    monkeypatch.setattr("data_transformer.build_type_map", lambda entries: {"entry1": "glossary-term"})
    entries = [GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")]
    relationships_map = {"entry1": []}
    entry_to_parent_map, entry_id_to_type_map = build_maps(entries, relationships_map)
    # If build_parent_map returns None, build_maps should still return a tuple
    assert entry_to_parent_map is None
    assert entry_id_to_type_map == {"entry1": "glossary-term"}

def test_build_maps_type_map_none(monkeypatch):
    # Simulate build_type_map returning None (should be handled as empty dict)
    monkeypatch.setattr("data_transformer.build_parent_map", lambda entries, rel_map: {"entry1": "parent1"})
    monkeypatch.setattr("data_transformer.build_type_map", lambda entries: None)
    entries = [GlossaryTaxonomyEntry(name="entry1", entryType="glossary-term")]
    relationships_map = {"entry1": []}
    entry_to_parent_map, entry_id_to_type_map = build_maps(entries, relationships_map)
    assert entry_to_parent_map == {"entry1": "parent1"}
    assert entry_id_to_type_map is None

def test_transform_term_entry_links_non_term_type(monkeypatch):
    # Should return empty list if entryType is not DC_TYPE_GLOSSARY_TERM
    entry = GlossaryTaxonomyEntry(name="entry1", entryType="CATEGORY")
    context = setup_context()
    result = transform_term_entry_links(context, entry)
    assert result == []

def test_transform_term_entry_links_no_search_results(monkeypatch):
    # Should return empty list if search_related_dc_entries returns empty
    entry = GlossaryTaxonomyEntry(name="entry1", entryType=DC_TYPE_GLOSSARY_TERM)
    context = setup_context()
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "term_id")
    monkeypatch.setattr("data_transformer.search_related_dc_entries", lambda ctx, term_id: [])
    result = transform_term_entry_links(context, entry)
    assert result == []

def test_transform_term_entry_links_process_returns_none(monkeypatch):
    # Should skip results where process_entry_to_term_entrylinks returns None
    entry = GlossaryTaxonomyEntry(name="entry1", entryType=DC_TYPE_GLOSSARY_TERM)
    context = setup_context()
    search_result = SearchEntryResult(relativeResourceName="resource1", linkedResource="linked1")
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "term_id")
    monkeypatch.setattr("data_transformer.search_related_dc_entries", lambda ctx, term_id: [search_result])
    monkeypatch.setattr("data_transformer.process_entry_to_term_entrylinks", lambda ctx, entry, result: None)
    result = transform_term_entry_links(context, entry)
    assert result == []

def test_transform_term_entry_links_process_returns_empty(monkeypatch):
    # Should skip results where process_entry_to_term_entrylinks returns empty list
    entry = GlossaryTaxonomyEntry(name="entry1", entryType=DC_TYPE_GLOSSARY_TERM)
    context = setup_context()
    search_result = SearchEntryResult(relativeResourceName="resource1", linkedResource="linked1")
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "term_id")
    monkeypatch.setattr("data_transformer.search_related_dc_entries", lambda ctx, term_id: [search_result])
    monkeypatch.setattr("data_transformer.process_entry_to_term_entrylinks", lambda ctx, entry, result: [])
    result = transform_term_entry_links(context, entry)
    assert result == []

def test_transform_term_entry_links_single_result(monkeypatch):
    # Should return links from process_entry_to_term_entrylinks
    entry = GlossaryTaxonomyEntry(name="entry1", entryType=DC_TYPE_GLOSSARY_TERM)
    context = setup_context()
    search_result = SearchEntryResult(relativeResourceName="resource1", linkedResource="linked1")
    link1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[])
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "term_id")
    monkeypatch.setattr("data_transformer.search_related_dc_entries", lambda ctx, term_id: [search_result])
    monkeypatch.setattr("data_transformer.process_entry_to_term_entrylinks", lambda ctx, entry, result: [link1])
    result = transform_term_entry_links(context, entry)
    assert result == [link1]

def test_transform_term_entry_links_multiple_results(monkeypatch):
    # Should accumulate links from multiple search results
    entry = GlossaryTaxonomyEntry(name="entry1", entryType=DC_TYPE_GLOSSARY_TERM)
    context = setup_context()
    search_result1 = SearchEntryResult(relativeResourceName="resource1", linkedResource="linked1")
    search_result2 = SearchEntryResult(relativeResourceName="resource2", linkedResource="linked2")
    link1 = EntryLink(name="link1", entryLinkType="type1", entryReferences=[])
    link2 = EntryLink(name="link2", entryLinkType="type1", entryReferences=[])
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "term_id")
    monkeypatch.setattr("data_transformer.search_related_dc_entries", lambda ctx, term_id: [search_result1, search_result2])
    def mock_process(ctx, entry, result):
        if result.relativeResourceName == "resource1":
            return [link1]
        elif result.relativeResourceName == "resource2":
            return [link2]
        return []
    monkeypatch.setattr("data_transformer.process_entry_to_term_entrylinks", mock_process)
    result = transform_term_entry_links(context, entry)
    assert len(result) == 2
    assert link1 in result
    assert link2 in result

def test_build_entry_link_for_entry_to_term_basic(monkeypatch):
    # Setup context and inputs
    context = setup_context()
    term_entry = create_glossary_term_entry(name="term1")
    entry_relationship = MagicMock()
    entry_relationship.sourceColumn = "col1"
    entry_link_name = "projects/test-project/locations/global/entryGroups/@dataplex/entryLinks/link1"
    dp_entry_name = "projects/test-project/locations/global/glossaries/test-glossary/terms/asset1"

    # Patch convert_to_dp_entry_id and get_dp_entry_link_type_name
    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "test-dp-entry-id")
    monkeypatch.setattr("data_transformer.get_dp_entry_link_type_name", lambda typ: "projects/123/locations/global/entryLinkTypes/DESCRIBED_BY")

    result = build_entry_link_for_entry_to_term(context, term_entry, entry_relationship, entry_link_name, dp_entry_name)
    assert isinstance(result, EntryLink)
    assert result.name == entry_link_name
    assert result.entryLinkType == "projects/123/locations/global/entryLinkTypes/DESCRIBED_BY"
    assert len(result.entryReferences) == 2
    assert result.entryReferences[0].name == dp_entry_name
    assert result.entryReferences[0].path == "Schema.col1"
    assert result.entryReferences[0].type == "SOURCE"
    assert result.entryReferences[1].type == "TARGET"
    assert result.entryReferences[1].name.startswith(context.dataplex_entry_group + "/entries/")

def test_build_entry_link_for_entry_to_term_no_source_column(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry(name="term2")
    entry_relationship = MagicMock()
    entry_relationship.sourceColumn = None
    entry_link_name = "projects/test-project/locations/global/entryGroups/@dataplex/entryLinks/link2"
    dp_entry_name = "projects/test-project/locations/global/glossaries/test-glossary/terms/asset2"

    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "dp-entry-id-2")
    monkeypatch.setattr("data_transformer.get_dp_entry_link_type_name", lambda typ: "projects/123/locations/global/entryLinkTypes/DESCRIBED_BY")

    result = build_entry_link_for_entry_to_term(context, term_entry, entry_relationship, entry_link_name, dp_entry_name)
    assert result.entryReferences[0].path == ""
    assert result.entryReferences[0].type == "SOURCE"
    assert result.entryReferences[1].type == "TARGET"

def test_build_entry_link_for_entry_to_term_custom_link_type(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry(name="term3")
    entry_relationship = MagicMock()
    entry_relationship.sourceColumn = "colX"
    entry_link_name = "custom/link/name"
    dp_entry_name = "custom/dp/entry/name"

    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "custom-dp-entry-id")
    monkeypatch.setattr("data_transformer.get_dp_entry_link_type_name", lambda typ: "custom-link-type")

    result = build_entry_link_for_entry_to_term(context, term_entry, entry_relationship, entry_link_name, dp_entry_name)
    assert result.name == entry_link_name
    assert result.entryLinkType == "custom-link-type"
    assert result.entryReferences[0].name == dp_entry_name
    assert result.entryReferences[0].path == "Schema.colX"
    assert result.entryReferences[1].type == "TARGET"

def test_build_entry_link_for_entry_to_term_term_entry_name(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry(name="term4")
    entry_relationship = MagicMock()
    entry_relationship.sourceColumn = "colY"
    entry_link_name = "link/name/4"
    dp_entry_name = "dp/entry/name/4"

    # Patch convert_to_dp_entry_id to return a value based on input
    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, type: f"converted-{name}-{type}")
    monkeypatch.setattr("data_transformer.get_dp_entry_link_type_name", lambda type: f"type-{type}")

    result = build_entry_link_for_entry_to_term(context, term_entry, entry_relationship, entry_link_name, dp_entry_name)
    expected_target_name = f"{context.dataplex_entry_group}/entries/converted-term4-glossary_term"
    assert result.entryReferences[1].name == expected_target_name
    assert result.entryReferences[1].type == "TARGET"

def test_build_dp_entry_name_from_params_basic():
    project = "test-project"
    location = "global"
    entry_group = "@dataplex"
    dp_entry_id = "entry123"
    expected = "projects/test-project/locations/global/entryGroups/@dataplex/entries/entry123"
    result = build_dp_entry_name_from_params(project, location, entry_group, dp_entry_id)
    assert result == expected

def test_build_dp_entry_name_from_params_empty_strings():
    result = build_dp_entry_name_from_params("", "", "", "")
    assert result == "projects//locations//entryGroups//entries/"

def test_build_dp_entry_name_from_params_numeric_values():
    result = build_dp_entry_name_from_params("123", "456", "789", "101112")
    assert result == "projects/123/locations/456/entryGroups/789/entries/101112"

def test_build_dp_entry_name_from_params_special_characters():
    result = build_dp_entry_name_from_params("proj$", "loc@", "group#", "id%")
    assert result == "projects/proj$/locations/loc@/entryGroups/group#/entries/id%"

def test_build_dp_entry_name_from_params_long_strings():
    project = "p" * 50
    location = "l" * 50
    entry_group = "g" * 50
    dp_entry_id = "e" * 50
    expected = f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{dp_entry_id}"
    result = build_dp_entry_name_from_params(project, location, entry_group, dp_entry_id)
    assert result == expected

def test_build_dp_entry_name_from_params_none_values():
    # None values will be converted to 'None' string in f-string
    result = build_dp_entry_name_from_params(None, None, None, None)
    assert result == "projects/None/locations/None/entryGroups/None/entries/None"

def test_process_entry_to_term_entrylinks_lookup_false(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry()
    search_result = create_search_entry_result()
    monkeypatch.setattr("data_transformer.lookup_dataplex_entry", lambda ctx, res: False)
    result = process_entry_to_term_entrylinks(context, term_entry, search_result)
    assert result == []

def test_process_entry_to_term_entrylinks_no_relationships(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry()
    search_result = create_search_entry_result()
    monkeypatch.setattr("data_transformer.lookup_dataplex_entry", lambda ctx, res: True)
    monkeypatch.setattr("data_transformer.normalize_linked_resource", lambda lr: lr)
    monkeypatch.setattr("data_transformer.fetch_relationships_dc_glossary_entry", lambda name, proj: [])
    result = process_entry_to_term_entrylinks(context, term_entry, search_result)
    assert result == []

def test_process_entry_to_term_entrylinks_single_relationship(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry()
    search_result = create_search_entry_result()
    rel = MagicMock()
    rel.name = "rel1"
    monkeypatch.setattr("data_transformer.lookup_dataplex_entry", lambda ctx, res: True)
    monkeypatch.setattr("data_transformer.normalize_linked_resource", lambda lr: lr)
    monkeypatch.setattr("data_transformer.fetch_relationships_dc_glossary_entry", lambda name, proj: [rel])
    monkeypatch.setattr("data_transformer.create_entry_to_term_entrylink", lambda ctx, te, r, lr: create_entry_link("link1"))
    result = process_entry_to_term_entrylinks(context, term_entry, search_result)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0].name == "link1"

def test_process_entry_to_term_entrylinks_multiple_relationships(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry()
    search_result = create_search_entry_result()
    rel1 = MagicMock()
    rel1.name = "rel1"
    rel2 = MagicMock()
    rel2.name = "rel2"
    monkeypatch.setattr("data_transformer.lookup_dataplex_entry", lambda ctx, res: True)
    monkeypatch.setattr("data_transformer.normalize_linked_resource", lambda lr: lr)
    monkeypatch.setattr("data_transformer.fetch_relationships_dc_glossary_entry", lambda name, proj: [rel1, rel2])
    def mock_create(ctx, te, r, lr):
        if r.name == "rel1":
            return create_entry_link("link1")
        elif r.name == "rel2":
            return create_entry_link("link2")
        return None
    monkeypatch.setattr("data_transformer.create_entry_to_term_entrylink", mock_create)
    result = process_entry_to_term_entrylinks(context, term_entry, search_result)
    assert len(result) == 2
    assert any(link.name == "link1" for link in result)
    assert any(link.name == "link2" for link in result)

def test_process_entry_to_term_entrylinks_create_returns_none(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry()
    search_result = create_search_entry_result()
    rel = MagicMock()
    rel.name = "rel1"
    monkeypatch.setattr("data_transformer.lookup_dataplex_entry", lambda ctx, res: True)
    monkeypatch.setattr("data_transformer.normalize_linked_resource", lambda lr: lr)
    monkeypatch.setattr("data_transformer.fetch_relationships_dc_glossary_entry", lambda name, proj: [rel])
    monkeypatch.setattr("data_transformer.create_entry_to_term_entrylink", lambda ctx, te, r, lr: None)
    result = process_entry_to_term_entrylinks(context, term_entry, search_result)
    assert result == []

def test_process_entry_to_term_entrylinks_mixed_create(monkeypatch):
    context = setup_context()
    term_entry = create_glossary_term_entry()
    search_result = create_search_entry_result()
    rel1 = MagicMock()
    rel1.name = "rel1"
    rel2 = MagicMock()
    rel2.name = "rel2"
    monkeypatch.setattr("data_transformer.lookup_dataplex_entry", lambda ctx, res: True)
    monkeypatch.setattr("data_transformer.normalize_linked_resource", lambda lr: lr)
    monkeypatch.setattr("data_transformer.fetch_relationships_dc_glossary_entry", lambda name, proj: [rel1, rel2])
    def mock_create(ctx, te, r, lr):
        if r.name == "rel1":
            return create_entry_link("link1")
        return None
    monkeypatch.setattr("data_transformer.create_entry_to_term_entrylink", mock_create)
    result = process_entry_to_term_entrylinks(context, term_entry, search_result)
    assert len(result) == 1
    assert result[0].name == "link1"
def test_build_entry_link_name_from_params_basic(monkeypatch):
    # Patch get_entry_link_id to return a fixed value for deterministic test
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "link123")
    project = "test-project"
    location = "global"
    entry_group = "@dataplex"
    expected = "projects/test-project/locations/global/entryGroups/@dataplex/entryLinks/link123"
    result = build_entry_link_name_from_params(project, location, entry_group)
    assert result == expected

def test_build_entry_link_name_from_params_empty_strings(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "emptyid")
    result = build_entry_link_name_from_params("", "", "")
    assert result == "projects//locations//entryGroups//entryLinks/emptyid"

def test_build_entry_link_name_from_params_numeric(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "numid")
    result = build_entry_link_name_from_params("123", "456", "789")
    assert result == "projects/123/locations/456/entryGroups/789/entryLinks/numid"

def test_build_entry_link_name_from_params_special_characters(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "specialid")
    result = build_entry_link_name_from_params("proj$", "loc@", "group#")
    assert result == "projects/proj$/locations/loc@/entryGroups/group#/entryLinks/specialid"

def test_build_entry_link_name_from_params_long_strings(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "longid")
    project = "p" * 50
    location = "l" * 50
    entry_group = "g" * 50
    expected = f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entryLinks/longid"
    result = build_entry_link_name_from_params(project, location, entry_group)
    assert result == expected

def test_build_entry_link_name_from_params_none_values(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "noneid")
    result = build_entry_link_name_from_params(None, None, None)
    assert result == "projects/None/locations/None/entryGroups/None/entryLinks/noneid"
def test_extract_entry_link_params_valid():
    # Standard valid resource name
    resource_name = "projects/proj1/locations/loc1/entryGroups/group1"
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project == "proj1"
    assert location == "loc1"
    assert entry_group == "group1"

def test_extract_entry_link_params_valid_with_extra_segments():
    # Extra segments after entryGroups should be ignored
    resource_name = "projects/proj2/locations/loc2/entryGroups/group2/entries/entryA"
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project == "proj2"
    assert location == "loc2"
    assert entry_group == "group2"

def test_extract_entry_link_params_missing_entry_group():
    # Missing entry group segment
    resource_name = "projects/proj3/locations/loc3/entryGroups/"
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project is None
    assert location is None
    assert entry_group is None

def test_extract_entry_link_params_missing_location():
    # Missing location segment
    resource_name = "projects/proj4/locations//entryGroups/group4"
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project is None
    assert location is None
    assert entry_group is None

def test_extract_entry_link_params_missing_projects():
    # Missing projects segment
    resource_name = "locations/loc5/entryGroups/group5"
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project is None
    assert location is None
    assert entry_group is None

def test_extract_entry_link_params_invalid_format():
    # Completely invalid format
    resource_name = "invalid/resource/name"
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project is None
    assert location is None
    assert entry_group is None

def test_extract_entry_link_params_empty_string():
    # Empty string input
    resource_name = ""
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project is None
    assert location is None
    assert entry_group is None

def test_extract_entry_link_params_none_input():
    # None input should not match
    project, location, entry_group = extract_entry_link_params(None)
    assert project is None
    assert location is None
    assert entry_group is None

def test_extract_entry_link_params_numeric_values():
    # Numeric values in segments
    resource_name = "projects/123/locations/456/entryGroups/789"
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project == "123"
    assert location == "456"
    assert entry_group == "789"

def test_extract_entry_link_params_special_characters():
    # Special characters in segments
    resource_name = "projects/proj$/locations/loc@/entryGroups/group#"
    project, location, entry_group = extract_entry_link_params(resource_name)
    assert project == "proj$"
    assert location == "loc@"
    assert entry_group == "group#"


def test_create_entry_to_term_entrylink_success(monkeypatch):
    context = setup_context()
    glossary_term_entry = create_glossary_term_entry()
    entry_relationship = create_entry_relationship()
    # Patch dependencies
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_DESCRIBED_BY", "DESCRIBED_BY")
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "uid1")
    monkeypatch.setattr("data_transformer.extract_entry_link_params", lambda name: ("test", "global", "@dataplex"))
    monkeypatch.setattr("data_transformer.build_entry_link_name_from_params", lambda p, l, g: "entry_link_name")
    monkeypatch.setattr("data_transformer.build_dp_entry_name_from_params", lambda p, l, g, i: "dp_entry_name")
    monkeypatch.setattr("data_transformer.build_entry_link_for_entry_to_term", lambda ctx, term, rel, link_name, dp_name: EntryLink(name=link_name, entryLinkType="type", entryReferences=[]))

    result = create_entry_to_term_entrylink(context, glossary_term_entry, entry_relationship, "dpid")
    assert isinstance(result, EntryLink)
    assert result.name == "entry_link_name"
    assert result.entryLinkType == "type"

def test_create_entry_to_term_entrylink_missing_params(monkeypatch):
    context = setup_context()
    glossary_term_entry = create_glossary_term_entry()
    entry_relationship = create_entry_relationship()
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_DESCRIBED_BY", "DESCRIBED_BY")
    # dp_entry_id is None
    result = create_entry_to_term_entrylink(context, glossary_term_entry, entry_relationship, None)
    assert result is None
    # dc_entry_relationship is None
    result = create_entry_to_term_entrylink(context, glossary_term_entry, None, "dpid")
    assert result is None
    # dc_glossary_term_entry is None
    result = create_entry_to_term_entrylink(context, None, entry_relationship, "dpid")
    assert result is None

def test_create_entry_to_term_entrylink_wrong_relationship_type(monkeypatch):
    context = setup_context()
    glossary_term_entry = create_glossary_term_entry()
    entry_relationship = create_entry_relationship()
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_DESCRIBED_BY", "DESCRIBED_BY")
    entry_relationship.relationshipType = "NOT_DESCRIBED_BY"
    result = create_entry_to_term_entrylink(context, glossary_term_entry, entry_relationship, "dpid")
    assert result is None

def test_create_entry_to_term_entrylink_destination_uid_mismatch(monkeypatch):
    context = setup_context()
    glossary_term_entry = create_glossary_term_entry()
    entry_relationship = create_entry_relationship()
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_DESCRIBED_BY", "DESCRIBED_BY")
    # Patch get_dc_glossary_taxonomy_id to return a different UID
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "other_uid")
    result = create_entry_to_term_entrylink(context, glossary_term_entry, entry_relationship, "dpid")
    assert result is None

def test_create_entry_to_term_entrylink_extract_entry_link_params_none(monkeypatch):
    context = setup_context()
    glossary_term_entry = create_glossary_term_entry()
    entry_relationship = create_entry_relationship()
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_DESCRIBED_BY", "DESCRIBED_BY")
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "uid1")
    # Patch extract_entry_link_params to return None values
    monkeypatch.setattr("data_transformer.extract_entry_link_params", lambda name: (None, None, None))
    monkeypatch.setattr("data_transformer.build_entry_link_name_from_params", lambda p, l, g: None)
    monkeypatch.setattr("data_transformer.build_dp_entry_name_from_params", lambda p, l, g, i: None)
    result = create_entry_to_term_entrylink(context, glossary_term_entry, entry_relationship, "dpid")
    assert result is None

def test_create_entry_to_term_entrylink_entry_link_name_or_dp_entry_name_none(monkeypatch):
    context = setup_context()
    glossary_term_entry = create_glossary_term_entry()
    entry_relationship = create_entry_relationship()
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_DESCRIBED_BY", "DESCRIBED_BY")
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "uid1")
    monkeypatch.setattr("data_transformer.extract_entry_link_params", lambda name: ("test", "global", "@dataplex"))
    # entry_link_name is None
    monkeypatch.setattr("data_transformer.build_entry_link_name_from_params", lambda p, l, g: None)
    monkeypatch.setattr("data_transformer.build_dp_entry_name_from_params", lambda p, l, g, i: "dp_entry_name")
    result = create_entry_to_term_entrylink(context, glossary_term_entry, entry_relationship, "dpid")
    assert result is None
    # dp_entry_name is None
    monkeypatch.setattr("data_transformer.build_entry_link_name_from_params", lambda p, l, g: "entry_link_name")
    monkeypatch.setattr("data_transformer.build_dp_entry_name_from_params", lambda p, l, g, i: None)
    result = create_entry_to_term_entrylink(context, glossary_term_entry, entry_relationship, "dpid")
    assert result is None

def test_create_entry_to_term_entrylink_build_entry_link_returns_none(monkeypatch):
    context = setup_context()
    glossary_term_entry = create_glossary_term_entry()
    entry_relationship = create_entry_relationship()
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_DESCRIBED_BY", "DESCRIBED_BY")
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: "uid1")
    monkeypatch.setattr("data_transformer.extract_entry_link_params", lambda name: ("test", "global", "@dataplex"))
    monkeypatch.setattr("data_transformer.build_entry_link_name_from_params", lambda p, l, g: "entry_link_name")
    monkeypatch.setattr("data_transformer.build_dp_entry_name_from_params", lambda p, l, g, i: "dp_entry_name")
    # build_entry_link_for_entry_to_term returns None
    monkeypatch.setattr("data_transformer.build_entry_link_for_entry_to_term", lambda ctx, term, rel, link_name, dp_name: None)
    result = create_entry_to_term_entrylink(context, glossary_term_entry, entry_relationship, "dpid")
    # Should still return None if build_entry_link_for_entry_to_term returns None
    assert result is None
def test_search_related_dc_entries_returns_results(monkeypatch):
    context = setup_context()
    dc_glossary_term_id = "term123"
    expected_results = [
        SearchEntryResult(relativeResourceName="resource1", linkedResource="linked1"),
        SearchEntryResult(relativeResourceName="resource2", linkedResource="linked2"),
    ]
    def mock_search_dc_entries_for_term(ctx, query):
        assert ctx == context
        assert query == "(term:term123)"
        return expected_results
    monkeypatch.setattr("data_transformer.search_dc_entries_for_term", mock_search_dc_entries_for_term)
    results = search_related_dc_entries(context, dc_glossary_term_id)
    assert results == expected_results

def test_search_related_dc_entries_empty_results(monkeypatch):
    context = setup_context()
    dc_glossary_term_id = "term456"
    monkeypatch.setattr("data_transformer.search_dc_entries_for_term", lambda ctx, query: [])
    results = search_related_dc_entries(context, dc_glossary_term_id)
    assert results == []

def test_search_related_dc_entries_none_results(monkeypatch):
    context = setup_context()
    dc_glossary_term_id = "term789"
    monkeypatch.setattr("data_transformer.search_dc_entries_for_term", lambda ctx, query: None)
    results = search_related_dc_entries(context, dc_glossary_term_id)
    assert results is None

def test_search_related_dc_entries_query_format(monkeypatch):
    context = setup_context()
    dc_glossary_term_id = "special_term"
    captured_query = {}
    def mock_search_dc_entries_for_term(ctx, query):
        captured_query['query'] = query
        return []
    monkeypatch.setattr("data_transformer.search_dc_entries_for_term", mock_search_dc_entries_for_term)
    search_related_dc_entries(context, dc_glossary_term_id)
    assert captured_query['query'] == "(term:special_term)"
def test_build_term_to_term_entry_links_empty_relationships(monkeypatch):
    # Should return empty list if dc_relationships is empty
    context = setup_context()
    entry = create_glossary_term_entry()
    relationships = []
    result = build_term_to_term_entry_links(context, entry, relationships)
    assert result == []

def test_build_term_to_term_entry_links_all_none(monkeypatch):
    # convert_term_relationship returns None for all relationships
    context = setup_context()
    entry = create_glossary_term_entry()
    rel1 = MagicMock()
    rel1.name = "rel1"
    rel2 = MagicMock()
    rel2.name = "rel2"
    monkeypatch.setattr("data_transformer.convert_term_relationship", lambda ctx, rel: None)
    result = build_term_to_term_entry_links(context, entry, [rel1, rel2])
    assert result == []

def test_build_term_to_term_entry_links_some_none(monkeypatch):
    # Some relationships return EntryLink, some return None
    context = setup_context()
    entry = create_glossary_term_entry()
    rel1 = MagicMock()
    rel1.name = "rel1"
    rel2 = MagicMock()
    rel2.name = "rel2"
    link1 = create_entry_link("link1")
    monkeypatch.setattr("data_transformer.convert_term_relationship", lambda ctx, rel: link1 if rel.name == "rel1" else None)
    result = build_term_to_term_entry_links(context, entry, [rel1, rel2])
    assert result == [link1]

def test_build_term_to_term_entry_links_all_valid(monkeypatch):
    # All relationships return valid EntryLink
    context = setup_context()
    entry = create_glossary_term_entry()
    rel1 = MagicMock()
    rel1.name = "rel1"
    rel2 = MagicMock()
    rel2.name = "rel2"
    link1 = create_entry_link("link1")
    link2 = create_entry_link("link2")
    def mock_convert(ctx, rel):
        if rel.name == "rel1":
            return link1
        elif rel.name == "rel2":
            return link2
        return None
    monkeypatch.setattr("data_transformer.convert_term_relationship", mock_convert)
    result = build_term_to_term_entry_links(context, entry, [rel1, rel2])
    assert result == [link1, link2]

def test_build_term_to_term_entry_links_order_preserved(monkeypatch):
    context = setup_context()
    entry = create_glossary_term_entry()

    # Create dummy relationships using create_entry_relationship with unique .name attributes
    rels = []
    for i in range(3):
        rel = create_entry_relationship()
        rel.name = f"rel{i}"
        rels.append(rel)
    links = [create_entry_link(f"link{i}") for i in range(3)]

    def mock_convert(ctx, rel):
        idx = int(rel.name.replace("rel", ""))
        return links[idx]

    monkeypatch.setattr("data_transformer.convert_term_relationship", mock_convert)

    result = build_term_to_term_entry_links(context, entry, rels)

    assert result == links


def test_build_term_to_term_entry_links_none_context(monkeypatch):
    # Should still work if context is None (if not used in convert_term_relationship)
    entry = create_glossary_term_entry()
    rel = MagicMock()
    rel.name = "rel1"
    link = create_entry_link("link1")
    monkeypatch.setattr("data_transformer.convert_term_relationship", lambda ctx, rel: link)
    result = build_term_to_term_entry_links(None, entry, [rel])
    assert result == [link]

def test_fetch_term_relationships_with_map():
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    rel1 = MagicMock()
    rel2 = MagicMock()
    relationships_map = {"entry1": [rel1, rel2]}
    result = fetch_term_relationships(context, entry, relationships_map)
    assert result == [rel1, rel2]

def test_fetch_term_relationships_with_map_entry_not_found():
    context = setup_context()
    entry = create_glossary_term_entry(name="entry2")
    relationships_map = {"entry1": [MagicMock()]}
    result = fetch_term_relationships(context, entry, relationships_map)
    assert result == []

def test_fetch_term_relationships_with_empty_map():
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    relationships_map = {}
    result = fetch_term_relationships(context, entry, relationships_map)
    assert result == []

def test_fetch_term_relationships_with_none_map():
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    result = fetch_term_relationships(context, entry, None)
    assert result == []

def test_fetch_term_relationships_with_map_none_value():
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    relationships_map = {"entry1": None}
    result = fetch_term_relationships(context, entry, relationships_map)
    assert result is None or result == []

def test_fetch_term_relationships_with_map_empty_list():
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    relationships_map = {"entry1": []}
    result = fetch_term_relationships(context, entry, relationships_map)
    assert result == []
def test_transform_term_term_links_basic(monkeypatch):
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    relationships_map = {"entry1": [MagicMock(), MagicMock()]}
    # Patch fetch_term_relationships to return a known list
    monkeypatch.setattr("data_transformer.fetch_term_relationships", lambda ctx, e, m: [MagicMock(name="rel1"), MagicMock(name="rel2")])
    # Patch build_term_to_term_entry_links to return EntryLinks
    link1 = create_entry_link("link1")
    link2 = create_entry_link("link2")
    monkeypatch.setattr("data_transformer.build_term_to_term_entry_links", lambda ctx, e, rels: [link1, link2])
    result = transform_term_term_links(context, entry, relationships_map)
    assert result == [link1, link2]

def test_transform_term_term_links_empty_relationships(monkeypatch):
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    relationships_map = {"entry1": []}
    monkeypatch.setattr("data_transformer.fetch_term_relationships", lambda ctx, e, m: [])
    monkeypatch.setattr("data_transformer.build_term_to_term_entry_links", lambda ctx, e, rels: [])
    result = transform_term_term_links(context, entry, relationships_map)
    assert result == []

def test_transform_term_term_links_none_relationships(monkeypatch):
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    relationships_map = {"entry1": None}
    monkeypatch.setattr("data_transformer.fetch_term_relationships", lambda ctx, e, m: None)
    monkeypatch.setattr("data_transformer.build_term_to_term_entry_links", lambda ctx, e, rels: [])
    result = transform_term_term_links(context, entry, relationships_map)
    assert result == []

def test_transform_term_term_links_build_returns_none(monkeypatch):
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    relationships_map = {"entry1": [MagicMock()]}
    monkeypatch.setattr("data_transformer.fetch_term_relationships", lambda ctx, e, m: [MagicMock()])
    monkeypatch.setattr("data_transformer.build_term_to_term_entry_links", lambda ctx, e, rels: None)
    result = transform_term_term_links(context, entry, relationships_map)
    assert result is None

def test_transform_term_term_links_no_relationships_map(monkeypatch):
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    monkeypatch.setattr("data_transformer.fetch_term_relationships", lambda ctx, e, m: [])
    monkeypatch.setattr("data_transformer.build_term_to_term_entry_links", lambda ctx, e, rels: [])
    result = transform_term_term_links(context, entry)
    assert result == []

def test_transform_term_term_links_order_preserved(monkeypatch):
    context = setup_context()
    entry = create_glossary_term_entry(name="entry1")
    rels = [MagicMock(name="relA"), MagicMock(name="relB"), MagicMock(name="relC")]
    monkeypatch.setattr("data_transformer.fetch_term_relationships", lambda ctx, e, m: rels)
    links = [create_entry_link("linkA"), create_entry_link("linkB"), create_entry_link("linkC")]
    monkeypatch.setattr("data_transformer.build_term_to_term_entry_links", lambda ctx, e, rels: links)
    result = transform_term_term_links(context, entry, {"entry1": rels})
    assert result == links
def test_build_entry_link_name_basic(monkeypatch):
    # Patch get_entry_link_id to return a fixed value
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "link123")
    context = setup_context()
    expected = "projects/test-project/locations/global/entryGroups/@dataplex/entryLinks/link123"
    result = build_entry_link_name(context)
    assert result == expected

def test_build_entry_link_name_custom_entry_group(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "customid")
    context = setup_context()
    context.dataplex_entry_group = "projects/custom/locations/global/entryGroups/customgroup"
    context.entry_group_id = "customgroup"
    expected = "projects/custom/locations/global/entryGroups/customgroup/entryLinks/customid"
    result = build_entry_link_name(context)
    assert result == expected

def test_build_entry_link_name_empty_entry_group(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "emptyid")
    context = setup_context()
    context.dataplex_entry_group = ""
    context.entry_group_id = ""
    expected = "/entryLinks/emptyid"
    result = build_entry_link_name(context)
    assert result == expected

def test_build_entry_link_name_none_entry_group(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: "noneid")
    context = setup_context()
    context.dataplex_entry_group = None
    context.entry_group_id = None
    expected = "None/entryLinks/noneid"
    result = build_entry_link_name(context)
    assert result == expected

def test_build_entry_link_name_get_entry_link_id_returns_none(monkeypatch):
    monkeypatch.setattr("data_transformer.get_entry_link_id", lambda: None)
    context = setup_context()
    expected = "projects/test-project/locations/global/entryGroups/@dataplex/entryLinks/None"
    result = build_entry_link_name(context)
    assert result == expected

def test_build_dataplex_entry_name_basic(monkeypatch):
    # Patch dependencies to return known values
    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "dp_entry_id")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.build_entry_group", lambda name: "projects/test/locations/global/entryGroups/@dataplex")
    result = build_dataplex_entry_name("projects/test/locations/global/entryGroups/@dataplex/entries/entry1")
    assert result == "projects/test/locations/global/entryGroups/@dataplex/entries/dp_entry_id"

def test_build_dataplex_entry_name_empty_resource(monkeypatch):
    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.build_entry_group", lambda name: "")
    result = build_dataplex_entry_name("")
    assert result == "/entries/"

def test_build_dataplex_entry_name_none_resource(monkeypatch):
    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: None)
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.build_entry_group", lambda name: None)
    result = build_dataplex_entry_name(None)
    assert result == "None/entries/None"

def test_build_dataplex_entry_name_special_characters(monkeypatch):
    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "id$%")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.build_entry_group", lambda name: "projects/p$/locations/l@/entryGroups/g#")
    result = build_dataplex_entry_name("projects/p$/locations/l@/entryGroups/g#/entries/e%")
    assert result == "projects/p$/locations/l@/entryGroups/g#/entries/id$%"

def test_build_dataplex_entry_name_long_strings(monkeypatch):
    long_entry_group = "projects/" + "p"*50 + "/locations/" + "l"*50 + "/entryGroups/" + "g"*50
    long_entry_id = "id" + "x"*50
    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: long_entry_id)
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.build_entry_group", lambda name: long_entry_group)
    result = build_dataplex_entry_name("dummy_resource_name")
    assert result == f"{long_entry_group}/entries/{long_entry_id}"

def test_build_entry_group_valid(monkeypatch):
    # Patch get_dc_ids_from_entry_name to return expected project_id
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: ("test-project", "glossary", "entry1"))
    resource_name = "projects/test-project/locations/global/entryGroups/@dataplex/entries/entry1"
    result = build_entry_group(resource_name)
    assert result == "projects/test-project/locations/global/entryGroups/@dataplex"

def test_build_entry_group_different_project(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: ("another-project", "glossary", "entry2"))
    resource_name = "projects/another-project/locations/global/entryGroups/@dataplex/entries/entry2"
    result = build_entry_group(resource_name)
    assert result == "projects/another-project/locations/global/entryGroups/@dataplex"

def test_build_entry_group_empty_project(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: ("", "glossary", "entry3"))
    resource_name = "projects//locations/global/entryGroups/@dataplex/entries/entry3"
    result = build_entry_group(resource_name)
    assert result == "projects//locations/global/entryGroups/@dataplex"

def test_build_entry_group_none_project(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: (None, "glossary", "entry4"))
    resource_name = "projects/None/locations/global/entryGroups/@dataplex/entries/entry4"
    result = build_entry_group(resource_name)
    assert result == "projects/None/locations/global/entryGroups/@dataplex"

def test_build_entry_group_invalid_resource(monkeypatch):
    # Patch get_dc_ids_from_entry_name to raise ValueError
    def raise_value_error(name):
        raise ValueError("Invalid entry name format")
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", raise_value_error)
    resource_name = "invalid/resource/name"
    try:
        build_entry_group(resource_name)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "Invalid entry name format" in str(e)
def test_convert_to_dp_entry_id_term(monkeypatch):
    # Patch get_dc_ids_from_entry_name and DC_TYPE_GLOSSARY_TERM, TERMS
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: ("proj1", "gloss1", "entry1"))
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.TERMS", "terms")
    result = convert_to_dp_entry_id("dummy_resource", "TERM")
    assert result == "projects/proj1/locations/global/glossaries/gloss1/terms/entry1"

def test_convert_to_dp_entry_id_category(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: ("proj2", "gloss2", "entry2"))
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.CATEGORIES", "categories")
    result = convert_to_dp_entry_id("dummy_resource", "CATEGORY")
    assert result == "projects/proj2/locations/global/glossaries/gloss2/categories/entry2"

def test_convert_to_dp_entry_id_empty_ids(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: ("", "", ""))
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.TERMS", "terms")
    result = convert_to_dp_entry_id("dummy_resource", "TERM")
    assert result == "projects//locations/global/glossaries//terms/"

def test_convert_to_dp_entry_id_none_ids(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: (None, None, None))
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.TERMS", "terms")
    result = convert_to_dp_entry_id("dummy_resource", "TERM")
    assert result == "projects/None/locations/global/glossaries/None/terms/None"

def test_convert_to_dp_entry_id_special_characters(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: ("proj$", "gloss@", "entry#"))
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.CATEGORIES", "categories")
    result = convert_to_dp_entry_id("dummy_resource", "CATEGORY")
    assert result == "projects/proj$/locations/global/glossaries/gloss@/categories/entry#"

def test_convert_to_dp_entry_id_long_strings(monkeypatch):
    project_id = "p" * 50
    glossary_id = "g" * 50
    entry_id = "e" * 50
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: (project_id, glossary_id, entry_id))
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.TERMS", "terms")
    result = convert_to_dp_entry_id("dummy_resource", "TERM")
    expected = f"projects/{project_id}/locations/global/glossaries/{glossary_id}/terms/{entry_id}"
    assert result == expected

def test_convert_to_dp_entry_id_invalid_entry_type(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", lambda name: ("proj", "gloss", "entry"))
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    # If entry_type is not TERM, should use CATEGORIES
    monkeypatch.setattr("data_transformer.CATEGORIES", "categories")
    result = convert_to_dp_entry_id("dummy_resource", "NOT_A_TERM")
    assert result == "projects/proj/locations/global/glossaries/gloss/categories/entry"

def test_convert_to_dp_entry_id_get_dc_ids_raises(monkeypatch):
    def raise_value_error(name):
        raise ValueError("Invalid entry name format")
    monkeypatch.setattr("data_transformer.get_dc_ids_from_entry_name", raise_value_error)
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.TERMS", "terms")
    with pytest.raises(ValueError):
        convert_to_dp_entry_id("invalid_resource", "TERM")
def test_get_dc_ids_from_entry_name_valid(monkeypatch):
    # Patch build_glossary_id_from_entry_group_id to return entry_group_id unchanged
    monkeypatch.setattr("data_transformer.build_glossary_id_from_entry_group_id", lambda egid: egid)
    entry_name = "projects/proj1/locations/global/entryGroups/group1/entries/entryA"
    project_id, glossary_id, entry_id = get_dc_ids_from_entry_name(entry_name)
    assert project_id == "proj1"
    assert glossary_id == "group1"
    assert entry_id == "entryA"

def test_get_dc_ids_from_entry_name_valid_different_values(monkeypatch):
    monkeypatch.setattr("data_transformer.build_glossary_id_from_entry_group_id", lambda egid: f"glossary-{egid}")
    entry_name = "projects/myproj/locations/us/entryGroups/mygroup/entries/myentry"
    project_id, glossary_id, entry_id = get_dc_ids_from_entry_name(entry_name)
    assert project_id == "myproj"
    assert glossary_id == "glossary-mygroup"
    assert entry_id == "myentry"

def test_get_dc_ids_from_entry_name_invalid_format_raises():
    # Missing 'entries' segment
    entry_name = "projects/proj1/locations/global/entryGroups/group1"
    with pytest.raises(ValueError) as excinfo:
        get_dc_ids_from_entry_name(entry_name)
    assert "Invalid entry name format" in str(excinfo.value)

def test_get_dc_ids_from_entry_name_empty_string_raises():
    entry_name = ""
    with pytest.raises(ValueError) as excinfo:
        get_dc_ids_from_entry_name(entry_name)
    assert "Invalid entry name format" in str(excinfo.value)

def test_get_dc_ids_from_entry_name_none_raises():
    entry_name = None
    with pytest.raises(TypeError):
        get_dc_ids_from_entry_name(entry_name)

def test_get_dc_ids_from_entry_name_special_characters(monkeypatch):
    monkeypatch.setattr("data_transformer.build_glossary_id_from_entry_group_id", lambda egid: egid)
    entry_name = "projects/p$/locations/l@/entryGroups/g#/entries/e%"
    project_id, glossary_id, entry_id = get_dc_ids_from_entry_name(entry_name)
    assert project_id == "p$"
    assert glossary_id == "g#"
    assert entry_id == "e%"

def test_get_dc_ids_from_entry_name_numeric(monkeypatch):
    monkeypatch.setattr("data_transformer.build_glossary_id_from_entry_group_id", lambda egid: egid)
    entry_name = "projects/123/locations/456/entryGroups/789/entries/101112"
    project_id, glossary_id, entry_id = get_dc_ids_from_entry_name(entry_name)
    assert project_id == "123"
    assert glossary_id == "789"
    assert entry_id == "101112"

def test_get_dc_ids_from_entry_name_long_strings(monkeypatch):
    monkeypatch.setattr("data_transformer.build_glossary_id_from_entry_group_id", lambda egid: egid)
    project_id = "p" * 50
    entry_group_id = "g" * 50
    entry_id = "e" * 50
    entry_name = f"projects/{project_id}/locations/global/entryGroups/{entry_group_id}/entries/{entry_id}"
    out_project_id, out_glossary_id, out_entry_id = get_dc_ids_from_entry_name(entry_name)
    assert out_project_id == project_id
    assert out_glossary_id == entry_group_id
    assert out_entry_id == entry_id

def test_get_dc_ids_from_entry_name_build_glossary_id_called(monkeypatch):
    called = {}
    def mock_build_glossary_id(entry_group_id):
        called['id'] = entry_group_id
        return "custom_glossary"
    monkeypatch.setattr("data_transformer.build_glossary_id_from_entry_group_id", mock_build_glossary_id)
    entry_name = "projects/projX/locations/global/entryGroups/groupX/entries/entryX"
    project_id, glossary_id, entry_id = get_dc_ids_from_entry_name(entry_name)
    assert called['id'] == "groupX"
    assert glossary_id == "custom_glossary"
    assert project_id == "projX"
    assert entry_id == "entryX"

def test_is_supported_relationship_synonymous(monkeypatch):
    # Patch constants
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = "source"
    rel.destinationEntryName = "dest"
    rel.relationshipType = "is_synonymous_to"
    assert is_supported_relationship(rel) is True

def test_is_supported_relationship_related(monkeypatch):
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = "source"
    rel.destinationEntryName = "dest"
    rel.relationshipType = "is_related_to"
    assert is_supported_relationship(rel) is True

def test_is_supported_relationship_unsupported_type(monkeypatch):
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = "source"
    rel.destinationEntryName = "dest"
    rel.relationshipType = "OTHER"
    assert is_supported_relationship(rel) is False

def test_is_supported_relationship_missing_source(monkeypatch):
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = None
    rel.destinationEntryName = "dest"
    rel.relationshipType = "is_synonymous_to"
    assert is_supported_relationship(rel) is False

def test_is_supported_relationship_missing_destination(monkeypatch):
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = "source"
    rel.destinationEntryName = None
    rel.relationshipType = "is_related_to"
    assert is_supported_relationship(rel) is False

def test_is_supported_relationship_empty_source(monkeypatch):
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = ""
    rel.destinationEntryName = "dest"
    rel.relationshipType = "is_synonymous_to"
    assert is_supported_relationship(rel) is False

def test_is_supported_relationship_empty_destination(monkeypatch):
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = "source"
    rel.destinationEntryName = ""
    rel.relationshipType = "is_related_to"
    assert is_supported_relationship(rel) is False

def test_is_supported_relationship_both_missing(monkeypatch):
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = None
    rel.destinationEntryName = None
    rel.relationshipType = "is_synonymous_to"
    assert is_supported_relationship(rel) is False

def test_is_supported_relationship_both_empty(monkeypatch):
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_SYNONYMOUS", "is_synonymous_to")
    monkeypatch.setattr("data_transformer.DC_RELATIONSHIP_TYPE_RELATED", "is_related_to")
    rel = type("GlossaryTaxonomyRelationship", (), {})()
    rel.sourceEntryName = ""
    rel.destinationEntryName = ""
    rel.relationshipType = "is_related_to"
    assert is_supported_relationship(rel) is False

def test_convert_term_relationship_supported(monkeypatch):
    context = setup_context()
    rel = create_glossary_taxonomy_relationship()
    monkeypatch.setattr("data_transformer.is_supported_relationship", lambda r: True)
    monkeypatch.setattr("data_transformer.build_dataplex_entry_name", lambda name: f"dp_{name}")
    monkeypatch.setattr("data_transformer.build_entry_link_name", lambda ctx: "entry_link_name")
    monkeypatch.setattr("data_transformer.get_dp_entry_link_type_name", lambda typ: "link_type")
    result = convert_term_relationship(context, rel)
    assert result is not None
    assert result.name == "entry_link_name"
    assert result.entryLinkType == "link_type"
    assert result.entryReferences[0].name == "dp_dest_entry"
    assert result.entryReferences[1].name == "dp_source_entry"

def test_convert_term_relationship_unsupported(monkeypatch):
    context = setup_context()
    rel = create_glossary_taxonomy_relationship()
    monkeypatch.setattr("data_transformer.is_supported_relationship", lambda r: False)
    result = convert_term_relationship(context, rel)
    assert result is None

def test_convert_term_relationship_missing_source(monkeypatch):
    context = setup_context()
    rel = create_glossary_taxonomy_relationship(source=None)
    monkeypatch.setattr("data_transformer.is_supported_relationship", lambda r: True)
    monkeypatch.setattr("data_transformer.build_dataplex_entry_name", lambda name: None if name is None else f"dp_{name}")
    monkeypatch.setattr("data_transformer.build_entry_link_name", lambda ctx: "entry_link_name")
    result = convert_term_relationship(context, rel)
    assert result is None

def test_convert_term_relationship_missing_destination(monkeypatch):
    context = setup_context()
    rel = create_glossary_taxonomy_relationship(dest=None)
    monkeypatch.setattr("data_transformer.is_supported_relationship", lambda r: True)
    monkeypatch.setattr("data_transformer.build_dataplex_entry_name", lambda name: None if name is None else f"dp_{name}")
    monkeypatch.setattr("data_transformer.build_entry_link_name", lambda ctx: "entry_link_name")
    result = convert_term_relationship(context, rel)
    assert result is None

def test_convert_term_relationship_missing_entry_link_name(monkeypatch):
    context = setup_context()
    rel = create_glossary_taxonomy_relationship()
    monkeypatch.setattr("data_transformer.is_supported_relationship", lambda r: True)
    monkeypatch.setattr("data_transformer.build_dataplex_entry_name", lambda name: f"dp_{name}")
    monkeypatch.setattr("data_transformer.build_entry_link_name", lambda ctx: None)
    result = convert_term_relationship(context, rel)
    assert result is None

def test_convert_term_relationship_all_missing(monkeypatch):
    context = setup_context()
    rel = create_glossary_taxonomy_relationship(source=None, dest=None)
    monkeypatch.setattr("data_transformer.is_supported_relationship", lambda r: True)
    monkeypatch.setattr("data_transformer.build_dataplex_entry_name", lambda name: None)
    monkeypatch.setattr("data_transformer.build_entry_link_name", lambda ctx: None)
    result = convert_term_relationship(context, rel)
    assert result is None

def test_convert_term_relationship_custom_types(monkeypatch):
    context = setup_context()
    rel = create_glossary_taxonomy_relationship(rel_type="is_related_to")
    monkeypatch.setattr("data_transformer.is_supported_relationship", lambda r: True)
    monkeypatch.setattr("data_transformer.build_dataplex_entry_name", lambda name: f"custom_{name}")
    monkeypatch.setattr("data_transformer.build_entry_link_name", lambda ctx: "custom_link_name")
    monkeypatch.setattr("data_transformer.get_dp_entry_link_type_name", lambda typ: f"custom_type_{typ}")
    result = convert_term_relationship(context, rel)
    assert result is not None
    assert result.name == "custom_link_name"
    assert result.entryLinkType == "custom_type_is_related_to"
    assert result.entryReferences[0].name == "custom_dest_entry"
    assert result.entryReferences[1].name == "custom_source_entry"

def test_convert_term_relationship_empty_strings(monkeypatch):
    context = setup_context()
    rel = create_glossary_taxonomy_relationship(source="", dest="", rel_type="is_synonymous_to")
    monkeypatch.setattr("data_transformer.is_supported_relationship", lambda r: True)
    monkeypatch.setattr("data_transformer.build_dataplex_entry_name", lambda name: "" if name == "" else f"dp_{name}")
    monkeypatch.setattr("data_transformer.build_entry_link_name", lambda ctx: "entry_link_name")
    result = convert_term_relationship(context, rel)
    assert result is None

def test_extract_description_within_limit(monkeypatch):
    # Patch MAX_DESC_SIZE_BYTES to a known value
    monkeypatch.setattr("data_transformer.MAX_DESC_SIZE_BYTES", 100)
    # Create a GlossaryTaxonomyEntry with a short description
    class CoreAspects:
        description = "short description"
    entry = type("GlossaryTaxonomyEntry", (), {})()
    entry.coreAspects = CoreAspects()
    result = extract_description(entry)
    assert result == "short description"

def test_extract_description_exceeds_limit(monkeypatch):
    monkeypatch.setattr("data_transformer.MAX_DESC_SIZE_BYTES", 10)
    # Create a GlossaryTaxonomyEntry with a long description
    class CoreAspects:
        description = "this description is too long"
    entry = type("GlossaryTaxonomyEntry", (), {})()
    entry.coreAspects = CoreAspects()
    # Patch log_large_description to track calls
    called = {}
    monkeypatch.setattr("data_transformer.log_large_description", lambda e: called.setdefault("logged", True))
    result = extract_description(entry)
    assert result == ""
    assert called.get("logged") is True

def test_extract_description_exact_limit(monkeypatch):
    # Description exactly at the limit
    desc = "a" * 10
    monkeypatch.setattr("data_transformer.MAX_DESC_SIZE_BYTES", len(desc.encode("utf-8")))
    class CoreAspects:
        description = desc
    entry = type("GlossaryTaxonomyEntry", (), {})()
    entry.coreAspects = CoreAspects()
    result = extract_description(entry)
    assert result == desc

def test_extract_description_empty_string(monkeypatch):
    monkeypatch.setattr("data_transformer.MAX_DESC_SIZE_BYTES", 100)
    class CoreAspects:
        description = ""
    entry = type("GlossaryTaxonomyEntry", (), {})()
    entry.coreAspects = CoreAspects()
    result = extract_description(entry)
    assert result == ""

def test_extract_description_non_ascii(monkeypatch):
    # Non-ASCII characters should be counted in bytes
    desc = "ü" * 5  # Each 'ü' is 2 bytes in utf-8
    monkeypatch.setattr("data_transformer.MAX_DESC_SIZE_BYTES", 10)
    class CoreAspects:
        description = desc
    entry = type("GlossaryTaxonomyEntry", (), {})()
    entry.coreAspects = CoreAspects()
    result = extract_description(entry)
    assert result == desc

def test_extract_description_non_ascii_exceeds(monkeypatch):
    desc = "ü" * 6  # 12 bytes
    monkeypatch.setattr("data_transformer.MAX_DESC_SIZE_BYTES", 10)
    class CoreAspects:
        description = desc
    entry = type("GlossaryTaxonomyEntry", (), {})()
    entry.coreAspects = CoreAspects()
    monkeypatch.setattr("data_transformer.log_large_description", lambda e: None)
    result = extract_description(entry)
    assert result == ""

def test_log_large_description_term(monkeypatch):
    # Patch logger.warning to capture calls
    called = {}
    def mock_warning(msg, url):
        called['msg'] = msg
        called['url'] = url
    monkeypatch.setattr("data_transformer.logger.warning", mock_warning)
    # Patch DC_TYPE_GLOSSARY_TERM
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    entry = MagicMock()
    entry.name = "projects/test/locations/global/entryGroups/@dataplex/entries/entry1"
    entry.entryType = "TERM"
    log_large_description(entry)
    assert called['msg'] == "Description for %s exceeds 120KB; omitting."
    assert called['url'].startswith("https://console.cloud.google.com/dataplex/glossaries/projects/test/locations/global/entryGroups/@dataplex/terms/entry1")

def test_log_large_description_category(monkeypatch):
    called = {}
    def mock_warning(msg, url):
        called['msg'] = msg
        called['url'] = url
    monkeypatch.setattr("data_transformer.logger.warning", mock_warning)
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    entry = MagicMock()
    entry.name = "projects/test/locations/global/entryGroups/@dataplex/entries/entry2"
    entry.entryType = "CATEGORY"
    log_large_description(entry)
    assert called['msg'] == "Description for %s exceeds 120KB; omitting."
    assert called['url'].startswith("https://console.cloud.google.com/dataplex/glossaries/projects/test/locations/global/entryGroups/@dataplex/categories/entry2")

def test_log_large_description_no_entry_name(monkeypatch):
    # Should not call logger.warning if entry.name is None or empty
    monkeypatch.setattr("data_transformer.logger.warning", lambda *a, **kw: pytest.fail("Should not log"))
    entry = MagicMock()
    entry.name = None
    entry.entryType = "TERM"
    log_large_description(entry)
    entry.name = ""
    log_large_description(entry)

def test_log_large_description_custom_entry_type(monkeypatch):
    called = {}
    def mock_warning(msg, url):
        called['msg'] = msg
        called['url'] = url
    monkeypatch.setattr("data_transformer.logger.warning", mock_warning)
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    entry = MagicMock()
    entry.name = "projects/test/locations/global/entryGroups/@dataplex/entries/entry3"
    entry.entryType = "CUSTOM_TYPE"
    log_large_description(entry)
    assert called['url'].startswith("https://console.cloud.google.com/dataplex/glossaries/projects/test/locations/global/entryGroups/@dataplex/categories/entry3")

def test_build_contacts_list_basic():
    class CoreAspects:
        contacts = ["Alice <alice@example.com>", "Bob <bob@example.com>"]
    result = build_contacts_list(CoreAspects())
    assert result == [
        {"role": ROLE_STEWARD, "name": "Alice", "id": "alice@example.com"},
        {"role": ROLE_STEWARD, "name": "Bob", "id": "bob@example.com"},
    ]

def test_build_contacts_list_no_angle_brackets():
    class CoreAspects:
        contacts = ["Charlie", "Dana"]
    result = build_contacts_list(CoreAspects())
    assert result == [
        {"role": ROLE_STEWARD, "name": "Charlie", "id": ""},
        {"role": ROLE_STEWARD, "name": "Dana", "id": ""},
    ]

def test_build_contacts_list_mixed_formats():
    class CoreAspects:
        contacts = ["Eve <eve@example.com>", "Frank", "Grace <grace@domain.com>"]
    result = build_contacts_list(CoreAspects())
    assert result == [
        {"role": ROLE_STEWARD, "name": "Eve", "id": "eve@example.com"},
        {"role": ROLE_STEWARD, "name": "Frank", "id": ""},
        {"role": ROLE_STEWARD, "name": "Grace", "id": "grace@domain.com"},
    ]

def test_build_contacts_list_empty_contacts():
    class CoreAspects:
        contacts = []
    result = build_contacts_list(CoreAspects())
    assert result == []

def test_build_contacts_list_empty_strings():
    class CoreAspects:
        contacts = ["", "   "]
    result = build_contacts_list(CoreAspects())
    assert result == [
        {"role": ROLE_STEWARD, "name": "", "id": ""},
        {"role": ROLE_STEWARD, "name": "", "id": ""},
    ]


def test_build_contacts_list_special_characters():
    class CoreAspects:
        contacts = ["J@ne Doe <jane.doe+test@domain.com>"]
    result = build_contacts_list(CoreAspects())
    assert result == [
        {"role": ROLE_STEWARD, "name": "J@ne Doe", "id": "jane.doe+test@domain.com"},
    ]

def test_build_aspects_term_type(monkeypatch):
    # Patch constants
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_TERM", "aspect_term")
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_CATEGORY", "aspect_category")
    monkeypatch.setattr("data_transformer.ASPECT_OVERVIEW", "overview")
    monkeypatch.setattr("data_transformer.ASPECT_CONTACTS", "contacts")
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    # Patch extract_description and build_contacts_list
    monkeypatch.setattr("data_transformer.extract_description", lambda entry: "desc")
    monkeypatch.setattr("data_transformer.build_contacts_list", lambda core: [{"role": "STEWARD", "name": "Alice", "id": "alice@example.com"}])
    entry = MagicMock()
    entry.entryType = "TERM"
    entry.coreAspects = MagicMock()
    result = build_aspects(entry)
    assert f"123456.global.aspect_term" in result
    assert f"123456.global.overview" in result
    assert f"123456.global.contacts" in result
    assert result[f"123456.global.overview"]["data"]["content"] == "<p>desc</p>"
    assert result[f"123456.global.contacts"]["data"]["identities"] == [{"role": "STEWARD", "name": "Alice", "id": "alice@example.com"}]

def test_build_aspects_category_type(monkeypatch):
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_TERM", "aspect_term")
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_CATEGORY", "aspect_category")
    monkeypatch.setattr("data_transformer.ASPECT_OVERVIEW", "overview")
    monkeypatch.setattr("data_transformer.ASPECT_CONTACTS", "contacts")
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.extract_description", lambda entry: "category-desc")
    monkeypatch.setattr("data_transformer.build_contacts_list", lambda core: [])
    entry = MagicMock()
    entry.entryType = "CATEGORY"
    entry.coreAspects = MagicMock()
    result = build_aspects(entry)
    assert f"123456.global.aspect_category" in result
    assert result[f"123456.global.overview"]["data"]["content"] == "<p>category-desc</p>"
    assert result[f"123456.global.contacts"]["data"]["identities"] == []

def test_build_aspects_empty_description_and_contacts(monkeypatch):
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_TERM", "aspect_term")
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_CATEGORY", "aspect_category")
    monkeypatch.setattr("data_transformer.ASPECT_OVERVIEW", "overview")
    monkeypatch.setattr("data_transformer.ASPECT_CONTACTS", "contacts")
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.extract_description", lambda entry: "")
    monkeypatch.setattr("data_transformer.build_contacts_list", lambda core: [])
    entry = MagicMock()
    entry.entryType = "TERM"
    entry.coreAspects = MagicMock()
    result = build_aspects(entry)
    assert result[f"123456.global.overview"]["data"]["content"] == "<p></p>"
    assert result[f"123456.global.contacts"]["data"]["identities"] == []

def test_build_aspects_custom_project_number(monkeypatch):
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_TERM", "aspect_term")
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_CATEGORY", "aspect_category")
    monkeypatch.setattr("data_transformer.ASPECT_OVERVIEW", "overview")
    monkeypatch.setattr("data_transformer.ASPECT_CONTACTS", "contacts")
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "projX")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.extract_description", lambda entry: "descX")
    monkeypatch.setattr("data_transformer.build_contacts_list", lambda core: [{"role": "STEWARD", "name": "Bob", "id": "bob@example.com"}])
    entry = MagicMock()
    entry.entryType = "TERM"
    entry.coreAspects = MagicMock()
    result = build_aspects(entry)
    assert f"projX.global.aspect_term" in result
    assert result[f"projX.global.overview"]["data"]["content"] == "<p>descX</p>"
    assert result[f"projX.global.contacts"]["data"]["identities"] == [{"role": "STEWARD", "name": "Bob", "id": "bob@example.com"}]

def test_build_aspects_none_entry_type(monkeypatch):
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_TERM", "aspect_term")
    monkeypatch.setattr("data_transformer.ASPECT_TYPE_CATEGORY", "aspect_category")
    monkeypatch.setattr("data_transformer.ASPECT_OVERVIEW", "overview")
    monkeypatch.setattr("data_transformer.ASPECT_CONTACTS", "contacts")
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.extract_description", lambda entry: "desc")
    monkeypatch.setattr("data_transformer.build_contacts_list", lambda core: [])
    entry = MagicMock()
    entry.entryType = None
    entry.coreAspects = MagicMock()
    result = build_aspects(entry)
    # Should use ASPECT_TYPE_CATEGORY if entryType is not TERM
    assert f"123456.global.aspect_category" in result

def test_get_dp_entry_type_name_valid(monkeypatch):
    # Patch TYPE_MAP and PROJECT_NUMBER
    monkeypatch.setattr("data_transformer.TYPE_MAP", {"TERM": "glossary_term", "CATEGORY": "glossary_category"})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    result_term = get_dp_entry_type_name("TERM")
    result_category = get_dp_entry_type_name("CATEGORY")
    assert result_term == "projects/123456/locations/global/entryTypes/glossary_term"
    assert result_category == "projects/123456/locations/global/entryTypes/glossary_category"

def test_get_dp_entry_type_name_missing_type(monkeypatch):
    monkeypatch.setattr("data_transformer.TYPE_MAP", {})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    result = get_dp_entry_type_name("UNKNOWN")
    assert result == "projects/123456/locations/global/entryTypes/None"

def test_get_dp_entry_type_name_none_input(monkeypatch):
    monkeypatch.setattr("data_transformer.TYPE_MAP", {"TERM": "glossary_term"})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    result = get_dp_entry_type_name(None)
    assert result == "projects/123456/locations/global/entryTypes/None"

def test_get_dp_entry_type_name_empty_string(monkeypatch):
    monkeypatch.setattr("data_transformer.TYPE_MAP", {"": "empty_type"})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    result = get_dp_entry_type_name("")
    assert result == "projects/123456/locations/global/entryTypes/empty_type"

def test_get_dp_entry_type_name_custom_project_number(monkeypatch):
    monkeypatch.setattr("data_transformer.TYPE_MAP", {"TERM": "glossary_term"})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "projX")
    result = get_dp_entry_type_name("TERM")
    assert result == "projects/projX/locations/global/entryTypes/glossary_term"

def test_get_dp_entry_link_type_name_valid(monkeypatch):
    # Patch LINK_TYPE_MAP and PROJECT_NUMBER
    monkeypatch.setattr("data_transformer.LINK_TYPE_MAP", {"DESCRIBED_BY": "described_by", "RELATED_TO": "related_to"})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    result_described = get_dp_entry_link_type_name("DESCRIBED_BY")
    result_related = get_dp_entry_link_type_name("RELATED_TO")
    assert result_described == "projects/123456/locations/global/entryLinkTypes/described_by"
    assert result_related == "projects/123456/locations/global/entryLinkTypes/related_to"

def test_get_dp_entry_link_type_name_missing_type(monkeypatch):
    monkeypatch.setattr("data_transformer.LINK_TYPE_MAP", {})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    result = get_dp_entry_link_type_name("UNKNOWN")
    assert result == "projects/123456/locations/global/entryLinkTypes/None"

def test_get_dp_entry_link_type_name_none_input(monkeypatch):
    monkeypatch.setattr("data_transformer.LINK_TYPE_MAP", {"DESCRIBED_BY": "described_by"})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    result = get_dp_entry_link_type_name(None)
    assert result == "projects/123456/locations/global/entryLinkTypes/None"

def test_get_dp_entry_link_type_name_empty_string(monkeypatch):
    monkeypatch.setattr("data_transformer.LINK_TYPE_MAP", {"": "empty_link_type"})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "123456")
    result = get_dp_entry_link_type_name("")
    assert result == "projects/123456/locations/global/entryLinkTypes/empty_link_type"

def test_get_dp_entry_link_type_name_custom_project_number(monkeypatch):
    monkeypatch.setattr("data_transformer.LINK_TYPE_MAP", {"DESCRIBED_BY": "described_by"})
    monkeypatch.setattr("data_transformer.PROJECT_NUMBER", "projX")
    result = get_dp_entry_link_type_name("DESCRIBED_BY")
    assert result == "projects/projX/locations/global/entryLinkTypes/described_by"

def test_build_glossary_ancestor_basic(monkeypatch):
    # Patch get_dp_entry_type_name to return a known value
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: "projects/123/locations/global/entryTypes/glossary")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY", "GLOSSARY")
    context = setup_context()
    ancestor = build_glossary_ancestor(context)
    expected_name = "projects/test-project/locations/global/entryGroups/@dataplex/entries/projects/test-project/locations/global/glossaries/test-glossary"
    assert isinstance(ancestor, Ancestor)
    assert ancestor.name == expected_name
    assert ancestor.type == "projects/123/locations/global/entryTypes/glossary"

def test_build_glossary_ancestor_custom_project_and_glossary(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: "custom_type")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY", "GLOSSARY")
    context = setup_context()
    context.project = "custom-proj"
    context.dp_glossary_id = "custom-glossary"
    context.dataplex_entry_group = "projects/custom-proj/locations/global/entryGroups/custom-group"
    ancestor = build_glossary_ancestor(context)
    expected_name = "projects/custom-proj/locations/global/entryGroups/custom-group/entries/projects/custom-proj/locations/global/glossaries/custom-glossary"
    assert ancestor.name == expected_name
    assert ancestor.type == "custom_type"

def test_build_glossary_ancestor_empty_strings(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: "empty_type")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY", "GLOSSARY")
    context = setup_context()
    context.project = ""
    context.dp_glossary_id = ""
    context.dataplex_entry_group = ""
    ancestor = build_glossary_ancestor(context)
    expected_name = "/entries/projects//locations/global/glossaries/"
    assert ancestor.name == expected_name
    assert ancestor.type == "empty_type"

def test_build_glossary_ancestor_none_values(monkeypatch):
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: "none_type")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY", "GLOSSARY")
    context = setup_context()
    context.project = None
    context.dp_glossary_id = None
    context.dataplex_entry_group = None
    ancestor = build_glossary_ancestor(context)
    expected_name = "None/entries/projects/None/locations/global/glossaries/None"
    assert ancestor.name == expected_name
    assert ancestor.type == "none_type"

def test_build_parent_ancestor_success(monkeypatch):
    # Setup context and patch dependencies
    context = setup_context()
    glossary_taxonomy_entry_name = "entry1"
    entry_to_parent_map = {"id_entry1": "parent1"}
    entry_id_to_type_map = {"parent1": "CATEGORY"}

    # Patch get_dc_glossary_taxonomy_id and get_dp_entry_type_name
    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: f"id_{name}")
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: f"type_{typ}")

    ancestor = build_parent_ancestor(context, glossary_taxonomy_entry_name, entry_to_parent_map, entry_id_to_type_map)
    expected_name = "projects/test-project/locations/global/entryGroups/@dataplex/entries/projects/test-project/locations/global/glossaries/test-glossary/categories/parent1"
    assert isinstance(ancestor, Ancestor)
    assert ancestor.name == expected_name
    assert ancestor.type == "type_CATEGORY"

def test_build_parent_ancestor_no_parent(monkeypatch):
    context = setup_context()
    glossary_taxonomy_entry_name = "entry2"
    entry_to_parent_map = {}  # No parent mapping
    entry_id_to_type_map = {}

    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: f"id_{name}")

    ancestor = build_parent_ancestor(context, glossary_taxonomy_entry_name, entry_to_parent_map, entry_id_to_type_map)
    assert ancestor is None

def test_build_parent_ancestor_parent_type_none(monkeypatch):
    context = setup_context()
    context.project = "proj"
    context.dp_glossary_id = "gloss"
    context.dataplex_entry_group = "projects/proj/locations/global/entryGroups/group"
    glossary_taxonomy_entry_name = "entry3"
    entry_to_parent_map = {"id_entry3": "parent3"}
    entry_id_to_type_map = {}  # parent type missing

    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: f"id_{name}")
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: f"type_{typ}")

    ancestor = build_parent_ancestor(context, glossary_taxonomy_entry_name, entry_to_parent_map, entry_id_to_type_map)
    expected_name = "projects/proj/locations/global/entryGroups/group/entries/projects/proj/locations/global/glossaries/gloss/categories/parent3"
    assert isinstance(ancestor, Ancestor)
    assert ancestor.name == expected_name
    assert ancestor.type == "type_None"

def test_build_parent_ancestor_non_empty(monkeypatch):
    context = setup_context()
    context.project = ""
    context.dp_glossary_id = ""
    context.dataplex_entry_group = ""
    glossary_taxonomy_entry_name = ""

    # Ensure parent id is non-empty
    entry_to_parent_map = {"id_": "parent_id"}
    entry_id_to_type_map = {"parent_id": "some_type"}

    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: f"id_{name}")
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda type: f"{type}")

    ancestor = build_parent_ancestor(context, glossary_taxonomy_entry_name, entry_to_parent_map, entry_id_to_type_map)
    expected_name = "/entries/projects//locations/global/glossaries//categories/parent_id"

    assert isinstance(ancestor, Ancestor)
    assert ancestor.name == expected_name
    assert ancestor.type == "some_type"

def test_build_parent_ancestor_none_values(monkeypatch):
    context = setup_context()
    context.project = None
    context.dp_glossary_id = None
    context.dataplex_entry_group = None
    glossary_taxonomy_entry_name = None
    entry_to_parent_map = {"id_None": None}
    entry_id_to_type_map = {None: None}

    monkeypatch.setattr("data_transformer.get_dc_glossary_taxonomy_id", lambda name: f"id_{name}")
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: f"type_{typ}")

    ancestor = build_parent_ancestor(context, glossary_taxonomy_entry_name, entry_to_parent_map, entry_id_to_type_map)
    assert ancestor is None
def test_compute_ancestors_both_glossary_and_parent(monkeypatch):
    # Patch build_glossary_ancestor and build_parent_ancestor to return known Ancestor objects
    glossary_ancestor = Ancestor(name="glossary_ancestor", type="glossary_type")
    parent_ancestor = Ancestor(name="parent_ancestor", type="parent_type")
    monkeypatch.setattr("data_transformer.build_glossary_ancestor", lambda ctx: glossary_ancestor)
    monkeypatch.setattr("data_transformer.build_parent_ancestor", lambda ctx, name, parent_map, type_map: parent_ancestor)
    context = MagicMock()
    result = compute_ancestors(context, "entry_name", {"id": "parent"}, {"parent": "type"})
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == glossary_ancestor
    assert result[1] == parent_ancestor

def test_compute_ancestors_only_glossary(monkeypatch):
    glossary_ancestor = Ancestor(name="glossary_ancestor", type="glossary_type")
    monkeypatch.setattr("data_transformer.build_glossary_ancestor", lambda ctx: glossary_ancestor)
    monkeypatch.setattr("data_transformer.build_parent_ancestor", lambda ctx, name, parent_map, type_map: None)
    context = MagicMock()
    result = compute_ancestors(context, "entry_name", {}, {})
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == glossary_ancestor

def test_compute_ancestors_parent_ancestor_is_none(monkeypatch):
    glossary_ancestor = Ancestor(name="glossary_ancestor", type="glossary_type")
    monkeypatch.setattr("data_transformer.build_glossary_ancestor", lambda ctx: glossary_ancestor)
    # build_parent_ancestor returns None
    monkeypatch.setattr("data_transformer.build_parent_ancestor", lambda ctx, name, parent_map, type_map: None)
    context = MagicMock()
    result = compute_ancestors(context, "entry_name", {"id": None}, {"parent": "type"})
    assert len(result) == 1
    assert result[0] == glossary_ancestor

def test_compute_ancestors_empty_parent_map(monkeypatch):
    glossary_ancestor = Ancestor(name="glossary_ancestor", type="glossary_type")
    monkeypatch.setattr("data_transformer.build_glossary_ancestor", lambda ctx: glossary_ancestor)
    monkeypatch.setattr("data_transformer.build_parent_ancestor", lambda ctx, name, parent_map, type_map: None)
    context = MagicMock()
    result = compute_ancestors(context, "entry_name", {}, {})
    assert len(result) == 1
    assert result[0] == glossary_ancestor

def test_compute_ancestors_none_context(monkeypatch):
    glossary_ancestor = Ancestor(name="glossary_ancestor", type="glossary_type")
    parent_ancestor = Ancestor(name="parent_ancestor", type="parent_type")
    monkeypatch.setattr("data_transformer.build_glossary_ancestor", lambda ctx: glossary_ancestor)
    monkeypatch.setattr("data_transformer.build_parent_ancestor", lambda ctx, name, parent_map, type_map: parent_ancestor)
    result = compute_ancestors(None, "entry_name", {"id": "parent"}, {"parent": "type"})
    assert len(result) == 2
    assert result[0] == glossary_ancestor
    assert result[1] == parent_ancestor

def test_compute_ancestors_none_entry_name(monkeypatch):
    glossary_ancestor = Ancestor(name="glossary_ancestor", type="glossary_type")
    parent_ancestor = Ancestor(name="parent_ancestor", type="parent_type")
    monkeypatch.setattr("data_transformer.build_glossary_ancestor", lambda ctx: glossary_ancestor)
    monkeypatch.setattr("data_transformer.build_parent_ancestor", lambda ctx, name, parent_map, type_map: parent_ancestor)
    context = MagicMock()
    result = compute_ancestors(context, None, {"id": "parent"}, {"parent": "type"})
    assert len(result) == 2
    assert result[0] == glossary_ancestor
    assert result[1] == parent_ancestor

def test_compute_ancestors_parent_ancestor_returns_non_ancestor(monkeypatch):
    glossary_ancestor = Ancestor(name="glossary_ancestor", type="glossary_type")
    # build_parent_ancestor returns a non-Ancestor object (should still append)
    monkeypatch.setattr("data_transformer.build_glossary_ancestor", lambda ctx: glossary_ancestor)
    monkeypatch.setattr("data_transformer.build_parent_ancestor", lambda ctx, name, parent_map, type_map: "not_ancestor")
    context = MagicMock()
    result = compute_ancestors(context, "entry_name", {"id": "parent"}, {"parent": "type"})
    assert len(result) == 2
    assert result[1] == "not_ancestor"

def setup_entry_source_test(entry_name, entry_type, display_name):
    entry = create_glossary_term_entry(name=entry_name)
    entry.entryType = entry_type
    entry.displayName = display_name
    return entry

def test_build_entry_source_basic(monkeypatch):
    context = MagicMock()
    entry = setup_entry_source_test("entry1", "TERM", "Test Entry")
    entry_to_parent_map = {"id_entry1": "parent1"}
    entry_id_to_type_map = {"parent1": "CATEGORY"}

    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "resource_path")
    monkeypatch.setattr("data_transformer.compute_ancestors", lambda ctx, name, parent_map, type_map: ["ancestor1", "ancestor2"])
    monkeypatch.setattr("data_transformer.trim_spaces_in_display_name", lambda dn: dn.strip())

    result = build_entry_source(context, entry, entry_to_parent_map, entry_id_to_type_map)
    assert isinstance(result, EntrySource)
    assert result.resource == "resource_path"
    assert result.displayName == "Test Entry"
    assert result.description == ""
    assert result.ancestors == ["ancestor1", "ancestor2"]

def test_build_entry_source_trim_display_name(monkeypatch):
    context = MagicMock()
    entry = setup_entry_source_test("entry2", "CATEGORY", "  Display Name  ")
    entry_to_parent_map = {}
    entry_id_to_type_map = {}

    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "resource2")
    monkeypatch.setattr("data_transformer.compute_ancestors", lambda ctx, name, parent_map, type_map: [])
    monkeypatch.setattr("data_transformer.trim_spaces_in_display_name", lambda dn: dn.strip())

    result = build_entry_source(context, entry, entry_to_parent_map, entry_id_to_type_map)
    assert result.displayName == "Display Name"

def test_build_entry_source_empty_ancestors(monkeypatch):
    context = MagicMock()
    entry = setup_entry_source_test("entry3", "TERM", "Entry3")
    entry_to_parent_map = {}
    entry_id_to_type_map = {}

    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "resource3")
    monkeypatch.setattr("data_transformer.compute_ancestors", lambda ctx, name, parent_map, type_map: [])
    monkeypatch.setattr("data_transformer.trim_spaces_in_display_name", lambda dn: dn)

    result = build_entry_source(context, entry, entry_to_parent_map, entry_id_to_type_map)
    assert result.ancestors == []

def test_build_entry_source_none_display_name(monkeypatch):
    context = MagicMock()
    entry = setup_entry_source_test("entry4", "TERM", None)
    entry_to_parent_map = {}
    entry_id_to_type_map = {}

    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "resource4")
    monkeypatch.setattr("data_transformer.compute_ancestors", lambda ctx, name, parent_map, type_map: ["ancestor"])
    monkeypatch.setattr("data_transformer.trim_spaces_in_display_name", lambda dn: "" if dn is None else dn.strip())

    result = build_entry_source(context, entry, entry_to_parent_map, entry_id_to_type_map)
    assert result.displayName == ""
    assert result.ancestors == ["ancestor"]

def test_build_entry_source_none_ancestors(monkeypatch):
    context = MagicMock()
    entry = setup_entry_source_test("entry5", "TERM", "Entry5")
    entry_to_parent_map = {}
    entry_id_to_type_map = {}

    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "resource5")
    monkeypatch.setattr("data_transformer.compute_ancestors", lambda ctx, name, parent_map, type_map: None)
    monkeypatch.setattr("data_transformer.trim_spaces_in_display_name", lambda dn: dn)

    result = build_entry_source(context, entry, entry_to_parent_map, entry_id_to_type_map)
    assert result.ancestors is None

def test_build_entry_source_empty_display_name(monkeypatch):
    context = MagicMock()
    entry = setup_entry_source_test("entry6", "TERM", "")
    entry_to_parent_map = {}
    entry_id_to_type_map = {}

    monkeypatch.setattr("data_transformer.convert_to_dp_entry_id", lambda name, typ: "resource6")
    monkeypatch.setattr("data_transformer.compute_ancestors", lambda ctx, name, parent_map, type_map: ["ancestor"])
    monkeypatch.setattr("data_transformer.trim_spaces_in_display_name", lambda dn: dn)

    result = build_entry_source(context, entry, entry_to_parent_map, entry_id_to_type_map)
    assert result.displayName == ""
    assert result.ancestors == ["ancestor"]

def test_process_glossary_taxonomy_term_type(monkeypatch):
    context = setup_context()
    entry = create_glossary_term_entry(name="entry_term")
    entry.displayName = "Test Term"
    # Patch constants
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_CATEGORY", "CATEGORY")
    # Patch dependencies
    entry_source = MagicMock()
    entry_source.resource = "resource1"
    monkeypatch.setattr("data_transformer.build_entry_source", lambda ctx, e, m1, m2: entry_source)
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: f"type_{typ}")
    monkeypatch.setattr("data_transformer.build_aspects", lambda e: {"aspect": "value"})
    result = process_glossary_taxonomy(context, entry, {}, {})
    assert isinstance(result, GlossaryEntry)
    assert result.name == "projects/test-project/locations/global/entryGroups/@dataplex/entries/resource1"
    assert result.entryType == "type_TERM"
    assert result.parentEntry == "projects/test-project/locations/global/entryGroups/@dataplex/entries/projects/test-project/locations/global/glossaries/test-glossary"
    assert result.aspects == {"aspect": "value"}
    assert result.entrySource == entry_source

def test_process_glossary_taxonomy_category_type(monkeypatch):
    context = setup_context()
    entry = GlossaryTaxonomyEntry(name="entry_cat", entryType="CATEGORY", uid="uid2")
    entry.displayName = "Test Category"
    context.dataplex_entry_group = "group"
    context.project = "proj"
    context.dp_glossary_id = "gloss"
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_CATEGORY", "CATEGORY")
    entry_source = MagicMock()
    entry_source.resource = "resource2"
    monkeypatch.setattr("data_transformer.build_entry_source", lambda ctx, e, m1, m2: entry_source)
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: f"type_{typ}")
    monkeypatch.setattr("data_transformer.build_aspects", lambda e: {"aspect": "cat"})
    result = process_glossary_taxonomy(context, entry, {}, {})
    assert isinstance(result, GlossaryEntry)
    assert result.name == "group/entries/resource2"
    assert result.entryType == "type_CATEGORY"
    assert result.parentEntry == "group/entries/projects/proj/locations/global/glossaries/gloss"
    assert result.aspects == {"aspect": "cat"}
    assert result.entrySource == entry_source

def test_process_glossary_taxonomy_invalid_type(monkeypatch):
    context = setup_context()
    entry = GlossaryTaxonomyEntry(name="entry_invalid", entryType="INVALID", uid="uid3")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_CATEGORY", "CATEGORY")
    result = process_glossary_taxonomy(context, entry, {}, {})
    assert result is None

def test_process_glossary_taxonomy_none_entry_type(monkeypatch):
    context = setup_context()
    entry = GlossaryTaxonomyEntry(name="entry_none", entryType=None, uid="uid4")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_CATEGORY", "CATEGORY")
    result = process_glossary_taxonomy(context, entry, {}, {})
    assert result is None

def test_process_glossary_taxonomy_build_entry_source_returns_none(monkeypatch):
    context = setup_context()
    context.dataplex_entry_group = "group"
    context.project = "proj"
    context.dp_glossary_id = "gloss"
    entry = create_glossary_term_entry(name="entry_none_source")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_CATEGORY", "CATEGORY")
    monkeypatch.setattr("data_transformer.build_entry_source", lambda ctx, e, m1, m2: None)
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: f"type_{typ}")
    monkeypatch.setattr("data_transformer.build_aspects", lambda e: {"aspect": "value"})
    # Should raise AttributeError when accessing resource of None
    try:
        process_glossary_taxonomy(context, entry, {}, {})
        assert False, "Expected AttributeError"
    except AttributeError:
        pass

def test_process_glossary_taxonomy_build_aspects_returns_none(monkeypatch):
    context = setup_context()
    context.dataplex_entry_group = "group"
    context.project = "proj"
    context.dp_glossary_id = "gloss"
    entry = create_glossary_term_entry(name="entry_none_aspects")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_TERM", "TERM")
    monkeypatch.setattr("data_transformer.DC_TYPE_GLOSSARY_CATEGORY", "CATEGORY")
    entry_source = MagicMock()
    entry_source.resource = "resource1"
    monkeypatch.setattr("data_transformer.build_entry_source", lambda ctx, e, m1, m2: entry_source)
    monkeypatch.setattr("data_transformer.get_dp_entry_type_name", lambda typ: f"type_{typ}")
    monkeypatch.setattr("data_transformer.build_aspects", lambda e: None)
    result = process_glossary_taxonomy(context, entry, {}, {})
    assert result.aspects is None
