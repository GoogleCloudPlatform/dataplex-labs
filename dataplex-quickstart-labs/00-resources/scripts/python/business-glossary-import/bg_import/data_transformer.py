"""
Transforms raw Data Catalog data into structured models for export.
"""

import re
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from constants import *
from models import *
from api_layer import *
from migration_utils import *
import logging_utils

logger = logging_utils.get_logger()

def get_dp_entry_type_name(dc_entry_type: str) -> str:
    """Returns the fully qualified entry type name."""
    dp_entry_type = TYPE_MAP.get(dc_entry_type)
    return f"projects/{PROJECT_NUMBER}/locations/global/entryTypes/{dp_entry_type}"

def get_dp_entry_link_type_name(dc_link_type: str) -> str:
    """Returns the fully qualified link type name."""
    dp_entry_link_type = LINK_TYPE_MAP.get(dc_link_type)
    return f"projects/{PROJECT_NUMBER}/locations/global/entryLinkTypes/{dp_entry_link_type}"

def compute_ancestors(context: Context, glossary_taxonomy_entry_name: str, entry_to_parent_map: dict, entry_id_to_type_map: dict) -> List[Ancestor]:
    """Computes ancestors for an entry: only immediate parent."""
    ancestors: List[Ancestor] = []
    # Always include the glossary root
    ancestors.append(build_glossary_ancestor(context))
    # Include immediate parent if exists
    parent_ancestor = build_parent_ancestor(context, glossary_taxonomy_entry_name, entry_to_parent_map, entry_id_to_type_map)
    if parent_ancestor:
        ancestors.append(parent_ancestor)
    return ancestors

def build_glossary_ancestor(context: Context) -> Ancestor:
    """Builds the glossary root ancestor."""
    dp_glossary_id = f"projects/{context.project}/locations/global/glossaries/{context.dp_glossary_id}"
    return Ancestor(
        name=f"{context.dataplex_entry_group}/entries/{dp_glossary_id}",
        type=get_dp_entry_type_name(DC_TYPE_GLOSSARY),
    )

def build_parent_ancestor(context: Context, glossary_taxonomy_entry_name: str, entry_to_parent_map: dict, entry_id_to_type_map: dict) -> Optional[Ancestor]:
    """Builds the immediate parent ancestor if it exists."""
    parent_dc_glossary_taxonomy_name = entry_to_parent_map.get(glossary_taxonomy_entry_name)
    if not parent_dc_glossary_taxonomy_name:
        return None
    parent_dc_glossary_taxonomy_id = get_dc_glossary_taxonomy_id(parent_dc_glossary_taxonomy_name)
    if not parent_dc_glossary_taxonomy_id:
        return None
    parent_type = entry_id_to_type_map.get(parent_dc_glossary_taxonomy_id)
    dp_glossary_taxonomy_id = construct_dp_glossary_category_id(context, parent_dc_glossary_taxonomy_id)
    parent_ancestor = Ancestor(
        name=f"{context.dataplex_entry_group}/entries/{dp_glossary_taxonomy_id}",
        type=get_dp_entry_type_name(parent_type),
    )
    logger.debug(f"Input entry: {glossary_taxonomy_entry_name}, output: parent ancestor: {parent_ancestor}")
    return parent_ancestor

def construct_dp_glossary_category_id(context, parent_dc_glossary_taxonomy_id):
    return f"projects/{context.project}/locations/global/glossaries/{context.dp_glossary_id}/categories/{parent_dc_glossary_taxonomy_id}"

def extract_description(dc_glossary_entry: GlossaryTaxonomyEntry) -> str:
    """Extracts and validates the description from an entry."""
    description_content = dc_glossary_entry.coreAspects.description
    if len(description_content.encode("utf-8")) > MAX_DESC_SIZE_BYTES:
        log_large_description(dc_glossary_entry)
        return ""
    return description_content

def log_large_description(entry: GlossaryTaxonomyEntry) -> None:
    """Logs a warning when entry description exceeds allowed size."""
    entry_name = entry.name
    if not entry_name:
        return
    child_path = "terms" if entry.entryType == DC_TYPE_GLOSSARY_TERM else "categories"
    console_name = entry_name.replace("/entries/", f"/{child_path}/")
    console_url = f"https://console.cloud.google.com/dataplex/glossaries/{console_name}"
    logger.warning("Description for %s exceeds 120KB; omitting.", console_url)

def build_entry_source(context: Context,dc_glossary_entry: GlossaryTaxonomyEntry, entry_to_parent_map: dict, entry_id_to_type_map: dict) -> EntrySource:
    """Constructs the EntrySource object for an entry."""
    logger.debug("checking again: %s", dc_glossary_entry)
    resource_path = convert_to_dp_entry_id(dc_glossary_entry.name, dc_glossary_entry.entryType)
    ancestors = compute_ancestors(context, dc_glossary_entry.name, entry_to_parent_map, entry_id_to_type_map)
    return EntrySource(
        resource=resource_path,
        displayName=trim_spaces_in_display_name(dc_glossary_entry.displayName),
        description="",
        ancestors=ancestors,
    )

def build_contacts_list(core_aspects: CoreAspects) -> List[dict]:
    """
    Build the list of contact identities from the provided CoreAspects.
    """
    contacts = []
    for raw_contact_string in core_aspects.contacts:
        match = re.search(r"<([^>]+)>", raw_contact_string)
        contact_id = match.group(1) if match else ""
        contact_name = re.sub(r"<([^>]+)>", "", raw_contact_string).strip()
        contacts.append({"role": ROLE_STEWARD, "name": contact_name, "id": contact_id})
    return contacts


def build_aspects(dc_glossary_entry: GlossaryTaxonomyEntry) -> dict:
    """
    Build and return the complete aspects dictionary for the entry.
    """
    dc_entry_type = dc_glossary_entry.entryType
    dataplex_aspect_type = ASPECT_TYPE_TERM if dc_entry_type == DC_TYPE_GLOSSARY_TERM else ASPECT_TYPE_CATEGORY
    description = extract_description(dc_glossary_entry)
    contacts_list = build_contacts_list(dc_glossary_entry.coreAspects)

    aspects = {
        f"{PROJECT_NUMBER}.global.{dataplex_aspect_type}": {"data": {}},
        f"{PROJECT_NUMBER}.global.{ASPECT_OVERVIEW}": {"data": {"content": f"<p>{description}</p>"}},
        f"{PROJECT_NUMBER}.global.{ASPECT_CONTACTS}": {"data": {"identities": contacts_list}},

    }
    return aspects


def process_glossary_taxonomy(context: Context, dc_glossary_entry: GlossaryTaxonomyEntry, entry_to_parent_map: dict, entry_id_to_type_map: dict) -> Optional[GlossaryEntry]:
    """
    Transforms a raw entry into a GlossaryEntry model.
    Returns None if entry is not a glossary term or category.
    """
    logger.debug(f"Processing glossary taxonomy entry objeect: {dc_glossary_entry}")
    dc_entry_type = dc_glossary_entry.entryType
    if dc_entry_type not in [DC_TYPE_GLOSSARY_TERM, DC_TYPE_GLOSSARY_CATEGORY]:
        return None

    dataplex_entry_source = build_entry_source(context, dc_glossary_entry, entry_to_parent_map, entry_id_to_type_map)
    dataplex_parent_entry_name = f"{context.dataplex_entry_group}/entries/projects/{context.project}/locations/global/glossaries/{context.dp_glossary_id}"

    return GlossaryEntry(
        name=f"{context.dataplex_entry_group}/entries/{dataplex_entry_source.resource}",
        entryType=get_dp_entry_type_name(dc_entry_type),
        parentEntry=dataplex_parent_entry_name,
        aspects=build_aspects(dc_glossary_entry),
        entrySource=dataplex_entry_source
    )


def convert_term_relationship(context: Context, dc_relationship: GlossaryTaxonomyRelationship) -> Optional[EntryLink]:
    """Processes a single term-term relationship into an EntryLink, if valid."""

    if not is_supported_relationship(dc_relationship):
        return None

    dc_source_entry_name = dc_relationship.sourceEntryName
    dc_destination_entry_name = dc_relationship.destinationEntryName
    logger.debug(f"Converting relationship: {dc_relationship.name} from {dc_source_entry_name} to {dc_destination_entry_name}")
    dataplex_source_entry_name = build_dataplex_entry_name(dc_source_entry_name)
    dataplex_target_entry_name = build_dataplex_entry_name(dc_destination_entry_name)
    dataplex_entry_link_name = build_entry_link_name(context)

    if not dataplex_source_entry_name or not dataplex_target_entry_name or not dataplex_entry_link_name:
        logger.debug(f"Skipping relationship {dc_relationship.name} due to missing source or target entry or entry link name.")
        return None
    
    return EntryLink(
        name=dataplex_entry_link_name,
        entryLinkType=get_dp_entry_link_type_name(dc_relationship.relationshipType),
        entryReferences=[
            EntryReference(name=dataplex_target_entry_name),
            EntryReference(name=dataplex_source_entry_name),
        ],
    )


def is_supported_relationship(glossary_taxonomy_relationship: GlossaryTaxonomyRelationship) -> bool:
    """Check if the relationship type is supported and both entry names are present."""
    if not glossary_taxonomy_relationship.sourceEntryName or not glossary_taxonomy_relationship.destinationEntryName:
        return False
    return glossary_taxonomy_relationship.relationshipType in [DC_RELATIONSHIP_TYPE_SYNONYMOUS, DC_RELATIONSHIP_TYPE_RELATED]

def get_dc_ids_from_entry_name(dc_entry_name: str) -> tuple[str, str, str]:
    """Extracts projectId, glossaryId, entryId from a Data Catalog entry name."""
    pattern = r"projects/([^/]+)/locations/[^/]+/entryGroups/([^/]+)/entries/([^/]+)"
    match = re.match(pattern, dc_entry_name)
    if not match:
        raise ValueError(f"Invalid entry name format: {dc_entry_name}")
    project_id = match.group(1)
    entry_group_id = match.group(2)
    entry_id = match.group(3)
    glossary_id = build_glossary_id_from_entry_group_id(entry_group_id)
    logger.debug(f"Extracting IDs from entry name: {dc_entry_name}, output: {project_id}, {glossary_id}, {entry_id}")

    return project_id, glossary_id, entry_id


def convert_to_dp_entry_id(dc_entry_resource_name: str, entry_type: str) -> str:
    """Extracts source and target entry names for a term relationship."""
    logger.debug(f"Converting DC entry resource name to Dataplex entry ID: {dc_entry_resource_name} of type {entry_type}")
    project_id, glossary_id, dc_entry_id =  get_dc_ids_from_entry_name(dc_entry_resource_name)
    resource_type_segment = TERMS if entry_type == DC_TYPE_GLOSSARY_TERM else CATEGORIES
    return (
        f"projects/{project_id}/locations/global/glossaries/"
        f"{glossary_id}/{resource_type_segment}/{dc_entry_id}"
    )

def build_entry_group(dc_entry_resource_name: str) -> str:
    """Extracts source and target entry names for a term relationship."""
    project_id, _, _ = get_dc_ids_from_entry_name(dc_entry_resource_name)
    return f"projects/{project_id}/locations/global/entryGroups/@dataplex"

def build_dataplex_entry_name(dc_entry_resource_name) -> str:
    """Extracts source and target entry names for a term relationship."""
    dataplex_entry_id = convert_to_dp_entry_id(dc_entry_resource_name, DC_TYPE_GLOSSARY_TERM)
    entry_group = build_entry_group(dc_entry_resource_name)

    logger.debug(f"Input DC entry: {dc_entry_resource_name}, Output: {entry_group} and {dataplex_entry_id}")
    return f"{entry_group}/entries/{dataplex_entry_id}"


def build_entry_link_name(context: Context) -> str:
    """Build the entry link name based on the relationship and context."""
    generated_link_id = get_entry_link_id()
    return f"{context.dataplex_entry_group}/entryLinks/{generated_link_id}"


def fetch_term_relationships(context: Context, dc_glossary_entry: GlossaryTaxonomyEntry, dc_relationships_map: Optional[Dict[str, List[GlossaryTaxonomyRelationship]]] = None) -> List[GlossaryTaxonomyRelationship]:
    """
    Returns relationships for the entry.

    Prefer the provided dc_relationships_map (prefetched by fetch_all_relationships).
    If not provided, fall back to calling the relationships API for that single entry.
    """
    if dc_relationships_map:
        return dc_relationships_map.get(dc_glossary_entry.name, [])
    return []


def build_term_to_term_entry_links(context: Context, dc_glossary_entry: GlossaryTaxonomyEntry, dc_relationships: List[GlossaryTaxonomyRelationship]) -> List[EntryLink]:
    """Transform raw relationships into EntryLink objects for a single entry."""
    links: List[EntryLink] = []
    for dc_relationship in dc_relationships:
        logger.debug(f"Processing term-term dc_relationship: {dc_relationship.name}")
        dataplex_entry_link = convert_term_relationship(context, dc_relationship)
        if dataplex_entry_link:
            logger.debug(f"Adding term-term link: {dataplex_entry_link.name}")
            links.append(dataplex_entry_link)
    return links


def transform_term_term_links(context: Context, dc_glossary_entry: GlossaryTaxonomyEntry, dc_relationships_map: Optional[Dict[str, List[GlossaryTaxonomyRelationship]]] = None) -> List[EntryLink]:
    """
    Fetches relationships for a single data catalog glossary taxonomy entry (from the provided map if any)
    and transforms them into EntryLink objects.
    """
    dc_relationships: List[GlossaryTaxonomyRelationship] = fetch_term_relationships(context, dc_glossary_entry, dc_relationships_map) #build term relationships
    return build_term_to_term_entry_links(context, dc_glossary_entry, dc_relationships)


def search_related_dc_entries(context: Context, dc_glossary_term_id: str) -> List[SearchEntryResult]:
    """Searches for catalog assets linked to a given glossary term."""
    logger.debug(f"Searching for assets linked to term: {dc_glossary_term_id}")
    search_results = search_dc_entries_for_term(context, query=f"(term:{dc_glossary_term_id})")
    logger.debug(f"Found {search_results} potential assets for term {dc_glossary_term_id}")
    return search_results

def extract_entry_link_params(dc_entry_name: str) -> Tuple[str, str, str]:
    """Extract project, location, entry_group from entry name."""
    if not dc_entry_name:
        logger.error("Entry name is None or empty.")
        return None, None, None
    match = re.match(r"projects/([^/]+)/locations/([^/]+)/entryGroups/([^/]+)", dc_entry_name)
    if not match:
        logger.error(f"Invalid entry name format: {dc_entry_name}")
        return None, None, None
    return match.group(1), match.group(2), match.group(3)

def build_entry_link_name_from_params(project: str, location: str, entry_group: str) -> str:
    """Build a unique entry link name for an entry-to-term relationship."""
    entry_link_id = get_entry_link_id()
    return f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entryLinks/{entry_link_id}"

def build_dp_entry_name_from_params(project: str, location: str, entry_group: str, dp_entry_id: str) -> str:
    """Build the Dataplex entry name from params."""
    return f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{dp_entry_id}"

def create_entry_to_term_entrylink(
    context: Context,
    dc_glossary_term_entry: GlossaryTaxonomyEntry,
    dc_entry_relationship: DcEntryRelationship,
    dp_entry_id: str
) -> Optional[EntryLink]:
    """Create an EntryLink for an entry relationship if applicable."""
    if not all([dp_entry_id, dc_entry_relationship, dc_glossary_term_entry]):
        logger.debug("One or more required parameters are None, skipping.")
        return None
    if dc_entry_relationship.relationshipType != DC_RELATIONSHIP_TYPE_DESCRIBED_BY:
        logger.debug("Relationship type is not DESCRIBED_BY, skipping.")
        return None
    if get_dc_glossary_taxonomy_id(dc_entry_relationship.destinationEntryName) != dc_glossary_term_entry.uid:
        logger.debug("Destination entry UID does not match glossary term UID, skipping.")
        return None

    project, location, entry_group = extract_entry_link_params(dc_entry_relationship.name)
    entry_link_name = build_entry_link_name_from_params(project, location, entry_group)
    dp_entry_name = build_dp_entry_name_from_params(project, location, entry_group, dp_entry_id)
    if not entry_link_name or not dp_entry_name:
        logger.debug("Entry link name or Dataplex entry name could not be built, skipping.")
        return None

    entry_link = build_entry_link_for_entry_to_term(context, dc_glossary_term_entry, dc_entry_relationship, entry_link_name, dp_entry_name)
    logger.debug(f"Created EntryLink: {entry_link}")
    return entry_link

def build_entry_link_for_entry_to_term(context, dc_glossary_term_entry, dc_entry_relationship, entry_link_name, dp_entry_name):
    entry_references = [
        EntryReference(
            name=dp_entry_name,
            path=f"Schema.{dc_entry_relationship.sourceColumn}" if dc_entry_relationship.sourceColumn else "",
            type="SOURCE"
        ),
        EntryReference(
            name=f"{context.dataplex_entry_group}/entries/{convert_to_dp_entry_id(dc_glossary_term_entry.name, DC_TYPE_GLOSSARY_TERM)}",
            type="TARGET"
        )
    ]

    entry_link = EntryLink(
        name=entry_link_name,
        entryLinkType=get_dp_entry_link_type_name(DC_RELATIONSHIP_TYPE_DESCRIBED_BY),
        entryReferences=entry_references
    )
    
    return entry_link


def process_entry_to_term_entrylinks(context: Context, dc_glossary_term_entry: GlossaryTaxonomyEntry, search_entry_result: SearchEntryResult) -> List[EntryLink]:
    """Processes asset relationships for a given search result and returns flat list of EntryLink objects."""
    dataplex_entry_links: List[EntryLink] = []
    if not lookup_dataplex_entry(context, search_entry_result):
        return dataplex_entry_links

    dc_asset_entry_name = search_entry_result.relativeResourceName
    dc_linked_resource = normalize_linked_resource(search_entry_result.linkedResource)
    dc_asset_relationships = fetch_relationships_dc_glossary_entry(dc_asset_entry_name, context.user_project)
    logger.debug(f"Found {len(dc_asset_relationships)} relationships for asset {dc_asset_entry_name}")

    for dc_relationship in dc_asset_relationships:
        logger.debug(f"Processing asset dc_relationship: {dc_relationship.name}")
        dataplex_entry_link = create_entry_to_term_entrylink(context, dc_glossary_term_entry, dc_relationship, dc_linked_resource)
        if dataplex_entry_link:
            logger.debug(f"Created term-asset link: {dataplex_entry_link.name} for asset {dc_asset_entry_name}")
            dataplex_entry_links.append(dataplex_entry_link)

    return dataplex_entry_links


def transform_term_entry_links(context: Context, dc_glossary_term_entry: GlossaryTaxonomyEntry) -> List[EntryLink]:
    """Transforms glossary term-to-asset relationships into a flat list of EntryLinks for an entry."""
    dataplex_entry_links: List[EntryLink] = []
    if dc_glossary_term_entry.entryType != DC_TYPE_GLOSSARY_TERM:
        return dataplex_entry_links

    dc_glossary_term_id = get_dc_glossary_taxonomy_id(dc_glossary_term_entry.name)
    for search_entry_result in search_related_dc_entries(context, dc_glossary_term_id):
        dataplex_links_from_result = process_entry_to_term_entrylinks(context, dc_glossary_term_entry, search_entry_result)
        if dataplex_links_from_result:
            dataplex_entry_links.extend(dataplex_links_from_result)
    return dataplex_entry_links


def build_parent_map(dc_glossary_taxonomy_entries: List[GlossaryTaxonomyEntry], dc_relationships_map: Dict[str, List[GlossaryTaxonomyRelationship]]) -> dict:
    """Builds mapping of entry IDs to parent IDs."""
    entry_to_parent_map = {}
    for dc_glossary_taxonomy_entry in dc_glossary_taxonomy_entries:
        dc_glossary_taxonomy_entry_name = dc_glossary_taxonomy_entry.name
        for dc_relationship in dc_relationships_map.get(dc_glossary_taxonomy_entry_name, []):
            if dc_relationship.relationshipType == DC_RELATIONSHIP_TYPE_BELONGS_TO:
                parent_dc_glossary_taxonomy_name = dc_relationship.destinationEntryName
                if parent_dc_glossary_taxonomy_name:
                    dc_glossary_taxonomy_entry_id = get_dc_glossary_taxonomy_id(dc_glossary_taxonomy_entry_name)
                    parent_dc_glossary_taxonomy_id = get_dc_glossary_taxonomy_id(parent_dc_glossary_taxonomy_name)
                    if dc_glossary_taxonomy_entry_id and parent_dc_glossary_taxonomy_id:
                        entry_to_parent_map[dc_glossary_taxonomy_entry_name] = parent_dc_glossary_taxonomy_name
                    break  # Assuming one BELONGS_TO relationship per entry
    return entry_to_parent_map


def build_type_map(dc_glossary_taxonomy_entries: List[GlossaryTaxonomyEntry]) -> dict:
    """Builds mapping of entry IDs to their types."""
    return {get_dc_glossary_taxonomy_id(entry.name): entry.entryType for entry in dc_glossary_taxonomy_entries}

def build_maps(
    dc_glossary_taxonomy_entries: List[GlossaryTaxonomyEntry],
    dc_relationships_map: Dict[str, List[GlossaryTaxonomyRelationship]],
) -> Tuple[dict, dict]:
    """Build parent and type maps used by processing steps."""
    entry_to_parent_map = build_parent_map(dc_glossary_taxonomy_entries, dc_relationships_map)
    entry_id_to_type_map = build_type_map(dc_glossary_taxonomy_entries)
    logger.debug(
        "_build_maps input: dc_glossary_taxonomy_entries=%s, dc_relationships_map=%s\noutput: entry_to_parent_map=%s, entry_id_to_type_map=%s",
        dc_glossary_taxonomy_entries,
        dc_relationships_map,
        entry_to_parent_map,
        entry_id_to_type_map
    )
    return entry_to_parent_map, entry_id_to_type_map

def process_glossary_entries(
    context: Context,
    dc_glossary_taxonomy_entries: List[GlossaryTaxonomyEntry],
    entry_to_parent_map: dict,
    entry_id_to_type_map: dict,
) -> List[GlossaryEntry]:
    """Process glossary taxonomy entries serially into GlossaryEntry objects."""
    dataplex_glossary_entries: List[GlossaryEntry] = []
    for dc_glossary_entry in dc_glossary_taxonomy_entries:
        dataplex_glossary_entry = process_glossary_taxonomy(context, dc_glossary_entry, entry_to_parent_map, entry_id_to_type_map)
        if dataplex_glossary_entry:
            dataplex_glossary_entries.append(dataplex_glossary_entry)
    return dataplex_glossary_entries

def build_term_to_term_links(
    context: Context,
    dc_glossary_taxonomy_entries: List[GlossaryTaxonomyEntry],
    dc_term_relationships_map: Dict[str, List[GlossaryTaxonomyRelationship]],
) -> List[EntryLink]:
    """Build all term-term (term ↔ term) links serially using provided relationships map."""
    dataplex_term_to_term_links: List[EntryLink] = []
    for dc_glossary_entry in dc_glossary_taxonomy_entries:
        dataplex_links = transform_term_term_links(context, dc_glossary_entry, dc_term_relationships_map)
        if dataplex_links:
            dataplex_term_to_term_links.extend(dataplex_links)
    return dataplex_term_to_term_links


def build_entry_to_term_links(
    context: Context,
    dc_glossary_taxonomy_entries: List[GlossaryTaxonomyEntry],
    max_workers: int = 10,
) -> List[EntryLink]:
    """Collect ungrouped list of entry-term links (dc_entry -> dc_glossary_taxonomy) for all entries using threads."""
    dataplex_entry_to_term_links: List[EntryLink] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_entry_to_term_links = {
            executor.submit(transform_term_entry_links, context, dc_glossary_entry): dc_glossary_entry
            for dc_glossary_entry in dc_glossary_taxonomy_entries
        }

        for future in as_completed(future_entry_to_term_links):
            dataplex_links_for_entry = future.result()
            if dataplex_links_for_entry:
                dataplex_entry_to_term_links.extend(dataplex_links_for_entry)
    return dataplex_entry_to_term_links


def extract_project_location_entrygroup(resource_name: str) -> str | None:
    """Extract project, location, entryGroup from entry reference using regex."""
    pattern = r"projects/([^/]+)/locations/([^/]+)/entryGroups/([^/]+)"
    match = re.match(pattern, resource_name)
    if match:
        return f"{match.group(1)}_{match.group(2)}_{match.group(3)}"
    return None

def deduplicate_term_to_term_links(term_to_term_links: List[EntryLink]) -> List[EntryLink]:
    """
    Deduplicate EntryLinks: keep only one entry link for each unique set of entryReferences (ignoring order) but matching entrylinktype.
    """
    seen_entry_link_keys = set()
    unique_entrylinks = []
    for term_to_term_entry_link in term_to_term_links:
        entry_link_references_set = frozenset(ref.name for ref in term_to_term_entry_link.entryReferences)
        link_type = term_to_term_entry_link.entryLinkType
        entry_link_key = (link_type, entry_link_references_set)
        if entry_link_key not in seen_entry_link_keys:
            seen_entry_link_keys.add(entry_link_key)
            unique_entrylinks.append(term_to_term_entry_link)
        link_type = term_to_term_entry_link.entryLinkType
        entry_link_key = (link_type, entry_link_references_set)
        if entry_link_key not in seen_entry_link_keys:
            seen_entry_link_keys.add(entry_link_key)
            unique_entrylinks.append(term_to_term_entry_link)
    return unique_entrylinks

def group_entry_links_by_project_location_entry_group(
    dataplex_entry_links: List[EntryLink],
) -> Dict[str, List[EntryLink]]:
    """Group entry-term links centrally by project_location_entryGroup."""
    grouped_dataplex_entry_links: Dict[str, List[EntryLink]] = defaultdict(list)

    for dataplex_entry_link in dataplex_entry_links:
        source_entry_resource_name = dataplex_entry_link.entryReferences[0].name if dataplex_entry_link.entryReferences else None
        grouping_key = extract_project_location_entrygroup(source_entry_resource_name) if source_entry_resource_name else None
        if grouping_key:
            grouped_dataplex_entry_links[grouping_key].append(dataplex_entry_link)

    return grouped_dataplex_entry_links


def process_dc_glossary_entries(
    context: Context,
    dc_glossary_taxonomy_entries: List[GlossaryTaxonomyEntry],
    dc_relationships_map: Dict[str, List[GlossaryTaxonomyRelationship]],
) -> Tuple[List[GlossaryEntry], List[EntryLink], Dict[str, List[EntryLink]]]:
    """
    Orchestrates processing:
    1) build glossary objects
    2) build term-term links using provided glossary_taxonomy_relationships
    3) build entry-term links (using threads), collect ungrouped entry links list
    4) group entry-term links based on project,location,entrygroup and return grouped structure
    """

    entry_to_parent_map, entry_id_to_type_map = build_maps(dc_glossary_taxonomy_entries, dc_relationships_map)

    dataplex_glossary_entries = process_glossary_entries(context, dc_glossary_taxonomy_entries, entry_to_parent_map, entry_id_to_type_map)
    dataplex_term_to_term_links = build_term_to_term_links(context, dc_glossary_taxonomy_entries, dc_relationships_map)
    dataplex_term_to_entry_links = build_entry_to_term_links(context, dc_glossary_taxonomy_entries)
    deduplicated_term_to_term_links = deduplicate_term_to_term_links(dataplex_term_to_term_links)
    grouped_dataplex_term_to_entry_links = group_entry_links_by_project_location_entry_group(dataplex_term_to_entry_links)

    logger.debug(
    "Processing complete | dataplex_glossary_entries=%s, dataplex_term_to_term_links=%s, grouped_dataplex_term_to_entry_links=%s",
    dataplex_glossary_entries,
    deduplicated_term_to_term_links,
    grouped_dataplex_term_to_entry_links,
)

    return dataplex_glossary_entries, deduplicated_term_to_term_links, grouped_dataplex_term_to_entry_links
