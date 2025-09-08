"""
Module for converting API response dicts into dataclass objects.
"""

from typing import List, Dict, Any, Optional
from models import *
from migration_utils import build_destination_entry_name_with_project_number
def _convert_to_core_aspect(core_aspect: Dict[str, Any]) -> CoreAspects:
    """Converts a dict to a CoreAspect object."""
    business_context = core_aspect.get("jsonContent", {}).get("businessContext", {})
    return CoreAspects(
        description=business_context.get("description", ""),
        contacts=business_context.get("contacts", []),
    )


def convert_glossary_taxonomy_entries_to_objects(dc_entries: List[Dict[str, Any]]) -> List[GlossaryTaxonomyEntry]:
    """
    Converts API response glossary taxonomy entry dicts into GlossaryTaxonomyEntry dataclass objects.
    """
    converted_dc_entries = []
    for dc_entry in dc_entries:
        converted_dc_entries.append(
            GlossaryTaxonomyEntry(
                name=dc_entry.get("name", ""),
                displayName=dc_entry.get("displayName", ""),
                entryType=dc_entry.get("entryType", ""),
                uid=dc_entry.get("entryUid", ""),
                coreAspects=_convert_to_core_aspect(dc_entry.get("coreAspects", {}))
            )
        )
    return converted_dc_entries

def convert_glossary_taxonomy_relationships_to_objects(dc_relationships: List[Dict[str, Any]]) -> List[GlossaryTaxonomyRelationship]:
    """Convert glossary relationship dicts into GlossaryTaxonomyRelationship dataclass objects."""
    return [
        GlossaryTaxonomyRelationship(
            name=dc_relationship.get("name", ""),
            sourceEntryName=dc_relationship.get("sourceEntry", {}).get("name", ""),
            destinationEntryName=build_destination_entry_name_with_project_number(dc_relationship.get("destinationEntry", {}).get("name", ""), dc_relationship.get("destinationEntryName", {})),
            relationshipType=dc_relationship.get("relationshipType", ""),
        )
        for dc_relationship in dc_relationships

        # Because of permission, destinationEntry can be empty but it might have destinationEntryName
        # Log the warning and skip this
    ]

def convert_entry_relationships_to_objects(dc_relationships: List[Dict[str, Any]]) -> List[DcEntryRelationship]:
    """Convert Data Catalog entry relationship dicts into DcEntryRelationship dataclass objects."""
    return [
        DcEntryRelationship(
            name=dc_relationship.get("name", ""),
            sourceColumn=dc_relationship.get("sourceColumn", None),
            destinationEntryName=dc_relationship.get("destinationEntryName", None),
            relationshipType=dc_relationship.get("relationshipType", ""),
        )
        for dc_relationship in dc_relationships
    ]

def convert_entry_search_results_to_objects(search_results: List[Dict[str, Any]]) -> List[SearchEntryResult]:
    """Convert search result dicts into SearchEntryResult dataclass objects."""
    return [
        SearchEntryResult(
            relativeResourceName=result.get("relativeResourceName", ""),
            linkedResource=result.get("linkedResource", ""),
        )
        for result in search_results
    ]
