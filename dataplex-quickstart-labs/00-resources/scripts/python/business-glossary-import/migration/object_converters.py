"""
Module for converting API response dicts into dataclass objects.
"""

from typing import List, Dict, Any, Optional
from models import *
from migration_utils import build_destination_entry_name_with_project_number
import logging_utils

logging = logging_utils.get_logger()

def convert_to_core_aspect(core_aspect: Dict[str, Any]) -> CoreAspects:
    """Converts a dict to a CoreAspect object."""
    business_context = core_aspect.get("business_context", {}).get("jsonContent", {})
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
                coreAspects=convert_to_core_aspect(dc_entry.get("coreAspects", {}))
            )
        )
    return converted_dc_entries

def convert_glossary_taxonomy_relationships_to_objects(dc_relationships: List[Dict[str, Any]]) -> List[GlossaryTaxonomyRelationship]:
    """Convert glossary relationship dicts into GlossaryTaxonomyRelationship dataclass objects."""
    relationships = []
    for dc_relationship in dc_relationships:
        if skip_relationship(dc_relationship):
            log_skipped_relationship(dc_relationship)
            continue
        relationships.append(convert_to_glossary_taxonomy_relationship(dc_relationship))
    return relationships

def skip_relationship(dc_relationship: Dict[str, Any]) -> bool:
    destination_entry = dc_relationship.get("destinationEntry", {})
    destination_entry_name = dc_relationship.get("destinationEntryName", {})
    return not bool(destination_entry) and bool(destination_entry_name)

def log_skipped_relationship(dc_relationship: Dict[str, Any]) -> None:
    logging.warning(
        f"Skipping relationship '{dc_relationship.get('name', '')}': "
        "as the user does not have permission to view the destination entry."
    )

def convert_to_glossary_taxonomy_relationship(dc_relationship: Dict[str, Any]) -> GlossaryTaxonomyRelationship:
    destination_entry = dc_relationship.get("destinationEntry", {})
    destination_entry_name = dc_relationship.get("destinationEntryName", {})
    return GlossaryTaxonomyRelationship(
        name=dc_relationship.get("name", ""),
        sourceEntryName=dc_relationship.get("sourceEntry", {}).get("name", ""),
        destinationEntryName=build_destination_entry_name_with_project_number(
            destination_entry.get("name", ""), destination_entry_name
        ),
        relationshipType=dc_relationship.get("relationshipType", ""),
    )

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
