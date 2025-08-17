"""
Transforms raw Data Catalog data into structured models for export.
"""

import re
from typing import List, Dict, Optional, Tuple, Any
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from models import GlossaryEntry, EntryLink, Ancestor, EntrySource, EntryReference
from api_layer import (
    search_catalog,
    lookup_dataplex_entry,
    fetch_glossary_id,
    fetch_relationships_for_entry,
    fetch_all_relationships
)
from migration_utils import get_entry_id, build_entry_key
import logging_utils

logger = logging_utils.get_logger()

MAX_DESC_SIZE_BYTES = 120 * 1024
PROJECT_NUMBER = "655216118709"
MAX_WORKERS = 20


def _get_export_resource(cfg: dict, entry_id: str, entry_type: str) -> str:
    """Constructs the target resource path with normalized IDs."""
    child = "terms" if entry_type == "glossary_term" else "categories"
    return (
        f"projects/{cfg['project']}/locations/global/glossaries/"
        f"{cfg['normalized_glossary']}/{child}/{entry_id}"
    )


def _get_qualified_type_name(entry_type: str) -> str:
    """Returns the fully qualified entry type name."""
    type_map = {"glossary_term": "glossary-term", "glossary_category": "glossary-category"}
    suffix = type_map.get(entry_type, entry_type)
    return f"projects/{PROJECT_NUMBER}/locations/global/entryTypes/{suffix}"


def _get_qualified_link_type_name(link_type: str) -> str:
    """Returns the fully qualified link type name."""
    type_map = {"is_synonymous_to": "synonym", "is_related_to": "related", "is_described_by": "definition"}
    suffix = type_map.get(link_type, link_type)
    return f"projects/{PROJECT_NUMBER}/locations/global/entryLinkTypes/{suffix}"


from typing import List

def _compute_ancestors(cfg: dict, child_id: str, p_map: dict, t_map: dict) -> List[Ancestor]:
    """Computes ancestors for an entry: only root glossary and immediate parent."""
    ancestors: List[Ancestor] = []

    # Always include the glossary root
    glossary_ancestor = Ancestor(
        name=(
            f"{cfg['dataplex_entry_group']}/entries/"
            f"projects/{cfg['project']}/locations/global/glossaries/{cfg['normalized_glossary']}"
        ),
        type=_get_qualified_type_name("glossary"),
    )
    ancestors.append(glossary_ancestor)

    # Add immediate parent if it exists
    parent_id = p_map.get(child_id)
    if parent_id:
        parent_type = t_map.get(parent_id, "glossary_category")
        resource = _get_export_resource(cfg, parent_id, parent_type)
        parent_ancestor = Ancestor(
            name=f"{cfg['dataplex_entry_group']}/entries/{resource}",
            type=_get_qualified_type_name(parent_type),
        )
        ancestors.append(parent_ancestor)

    return ancestors


def _extract_description(entry: dict) -> str:
    """Extracts and validates the description from an entry."""
    desc = (
        entry.get("coreAspects", {})
        .get("business_context", {})
        .get("jsonContent", {})
        .get("description", "")
    )
    if len(desc.encode("utf-8")) > MAX_DESC_SIZE_BYTES:
        _log_large_description(entry)
        return ""
    return desc


def _log_large_description(entry: dict) -> None:
    """Logs a warning when entry description exceeds allowed size."""
    entry_name = entry.get("name")
    if not entry_name:  # if missing or empty, just return
        return
    child_path = "terms" if entry.get("entryType") == "glossary_term" else "categories"
    console_name = entry_name.replace("/entries/", f"/{child_path}/")
    console_url = f"https://console.cloud.google.com/dataplex/glossaries/{console_name}"
    logger.warning("Description for %s exceeds 120KB; omitting.", console_url)


def _build_entry_source(config: dict, entry: dict, entry_id: str, parent_map: dict, type_map: dict) -> EntrySource:
    """Constructs the EntrySource object for an entry."""
    resource_path = _get_export_resource(config, entry_id, entry["entryType"])
    ancestors = _compute_ancestors(config, entry_id, parent_map, type_map)
    return EntrySource(
        resource=resource_path,
        displayName=entry.get("displayName", "").strip(),
        description="",
        ancestors=ancestors,
    )


def _get_aspects(entry_type: str, description: str) -> dict:
    """Builds aspects dictionary for glossary entry."""
    aspect_type = "glossary-term-aspect" if entry_type == "glossary_term" else "glossary-category-aspect"
    return {
        f"{PROJECT_NUMBER}.global.{aspect_type}": {"data": {}},
        f"{PROJECT_NUMBER}.global.overview": {"data": {"content": f"<p>{description}</p>"}},
    }


def process_entry(config: dict, entry: dict, parent_map: dict, type_map: dict) -> Optional[GlossaryEntry]:
    """
    Transforms a raw entry into a GlossaryEntry model.
    Returns None if entry is not a glossary term or category.
    """
    entry_type = entry.get("entryType")
    if entry_type not in ["glossary_term", "glossary_category"]:
        return None

    entry_id = get_entry_id(entry.get("name"))
    description = _extract_description(entry)
    entry_source = _build_entry_source(config, entry, entry_id, parent_map, type_map)

    return GlossaryEntry(
        name=f"{config['dataplex_entry_group']}/entries/{entry_source.resource}",
        entryType=_get_qualified_type_name(entry_type),
        parentEntry=f"{config['dataplex_entry_group']}/entries/projects/{config['project']}/locations/global/glossaries/{config['normalized_glossary']}",
        aspects=_get_aspects(entry_type, description),
        entrySource=entry_source
    )


def _build_target_name(rel: dict, config: dict, dest_entry_id: str, dest_entry_name: str) -> str | None:
    """Builds target name for a relationship link."""
    glossary_id = fetch_glossary_id(dest_entry_name, config["user_project"])
    if not glossary_id:
        logger.warning("Unable to resolve glossary ID for %s. Skipping relationship link.", dest_entry_name)
        return None

    dest_project = re.sub(r"^projects/([^/]+)/.*", r"\1", dest_entry_name)
    return (
        f"projects/{dest_project}/locations/global/entryGroups/@dataplex/entries/projects/"
        f"{dest_project}/locations/global/glossaries/{glossary_id}/terms/{dest_entry_id}"
    )


def _process_term_relationship(config: dict, rel: dict, source_name: str) -> Optional[EntryLink]:
    """Processes a single term-term relationship into an EntryLink, if valid."""

    if not _is_supported_relationship(rel):
        return None
        
    dest_entry_name = _get_destination_entry_name(rel)
    if not dest_entry_name:
        return None

    target_name = _resolve_target_name(config, rel, dest_entry_name)
    if not target_name:
        return None

    entry_link_name = _build_entry_link_name(config, rel)

    return EntryLink(
        name=entry_link_name,
        entryLinkType=_get_qualified_link_type_name(rel["relationshipType"]),
        entryReferences=[
            EntryReference(name=source_name),
            EntryReference(name=target_name),
        ],
    )


def _is_supported_relationship(rel: dict) -> bool:
    """Check if the relationship type is supported."""
    return rel.get("relationshipType") in ["is_synonymous_to", "is_related_to"]


def _get_destination_entry_name(rel: dict) -> Optional[str]:
    """Extract destination entry name, logging if missing."""
    dest_entry_name = rel.get("destinationEntry", {}).get("name")
    if not dest_entry_name:
        logger.debug("Skipping relationship %s due to missing destination.", rel.get("name"))
    return dest_entry_name


def _resolve_target_name(config: dict, rel: dict, dest_entry_name: str) -> Optional[str]:
    """Resolve the target entry name for the relationship."""
    dest_entry_id = get_entry_id(dest_entry_name)
    target_name = _build_target_name(rel, config, dest_entry_id, dest_entry_name)
    if not target_name:
        logger.debug("Skipping relationship %s due to unresolved target name.", rel.get("name"))
    return target_name


def _build_entry_link_name(config: dict, rel: dict) -> str:
    """Build the entry link name based on the relationship and config."""
    link_id = get_entry_id(rel.get("name"))
    return f"{config['dataplex_entry_group']}/entryLinks/{'g' + link_id if link_id else ''}"


def transform_term_term_links(config: dict, entry: dict, rels: List[dict]) -> List[EntryLink]:
    """Transforms term-term relationships into EntryLink objects."""
    links = []
    source_resource = _get_export_resource(config, get_entry_id(entry.get("name")), entry.get("entryType"))
    source_name = f"{config['dataplex_entry_group']}/entries/{source_resource}"
    for rel in rels:
        logger.debug(f"Processing term-term relationship: {rel.get('name', 'Unknown')}")
        link = _process_term_relationship(config, rel, source_name)
        if link:
            logger.debug(f"Created term-term link: {link.name}")
            links.append(link)
    return links


def _search_assets_for_term(config: dict, entry_id: str) -> List[dict]:
    """Searches for catalog assets linked to a given glossary term."""
    logger.debug(f"Searching for assets linked to term: {entry_id}")
    results = search_catalog(config, query=f"(term:{entry_id})")
    logger.debug(f"Found {len(results)} potential assets for term {entry_id}")
    return results


def _create_asset_link_if_applicable(config: dict, entry: dict, entry_id: str, rel: dict, fqn: str) -> Optional[EntryLink]:
    """Creates an EntryLink for an asset relationship if applicable."""
    if rel.get("relationshipType") != "is_described_by":
        return None
    if get_entry_id(rel.get("destinationEntryName")) != entry.get("entryUid"):
        return None
    proj, loc, eg = re.match(r"projects/([^/]+)/locations/([^/]+)/entryGroups/([^/]+)", fqn).groups()
    return EntryLink(
        name=f"projects/{proj}/locations/{loc}/entryGroups/{eg}/entryLinks/{get_entry_id(rel.get('name')) and 'g' + get_entry_id(rel.get('name')) or ''}",
        entryLinkType=_get_qualified_link_type_name("is_described_by"),
        entryReferences=[
            EntryReference(name=fqn, path=f"Schema.{rel.get('sourceColumn', '')}" if rel.get("sourceColumn") else "", type="SOURCE"),
            EntryReference(name=f"{config['dataplex_entry_group']}/entries/{_get_export_resource(config, entry_id, 'glossary_term')}", type="TARGET")
        ]
    )


def _process_asset_relationships(config: dict, entry: dict, entry_id: str, result: dict, grouped_links: Dict[str, List[EntryLink]]) -> Dict[str, List[EntryLink]]:
    """Processes asset relationships and updates grouped_links."""
    if not lookup_dataplex_entry(config, result):
        return grouped_links
    fqn = result.get("relativeResourceName", "")
    relationships = fetch_relationships_for_entry(fqn, config["user_project"])
    logger.debug(f"Found {len(relationships)} relationships for asset {fqn}")
    for rel in relationships:
        logger.debug(f"Processing asset relationship: {rel.get('name')}")
        link = _create_asset_link_if_applicable(config, entry, entry_id, rel, fqn)
        if link:
            proj, loc, eg = re.match(r"projects/([^/]+)/locations/([^/]+)/entryGroups/([^/]+)", fqn).groups()
            ple_key = f"{proj}_{loc}_{eg}"
            logger.debug(f"Created term-asset link: {link.name} for group {ple_key}")
            grouped_links[ple_key].append(link)
    return grouped_links


def transform_term_entry_links(config: dict, entry: dict) -> Dict[str, List[EntryLink]]:
    """Transforms glossary term-to-asset relationships into EntryLinks."""
    if entry.get("entryType") != "glossary_term":
        return {}
    grouped_links = defaultdict(list)
    entry_id = get_entry_id(entry.get("name"))
    for result in _search_assets_for_term(config, entry_id):
        grouped_links = _process_asset_relationships(config, entry, entry_id, result, grouped_links)
    return grouped_links


def _build_parent_map(raw_entries: List[dict], relationships: dict) -> dict:
    """Builds mapping of entry IDs to parent IDs."""
    parent_map = {
        get_entry_id(e["name"]): get_entry_id(r.get("destinationEntry", {}).get("name"))
        for e in raw_entries
        for r in relationships.get(e.get("name"), [])
        if r.get("relationshipType") == "belongs_to"
    }
    logger.debug(
        "_build_parent_map input: raw_entries=%s, relationships=%s\noutput: parent_map=%s",
        raw_entries,
        relationships,
        parent_map
    )
    return parent_map


def _build_type_map(raw_entries: List[dict]) -> dict:
    """Builds mapping of entry IDs to their types."""
    return {get_entry_id(e.get("name")): e.get("entryType") for e in raw_entries}


def _submit_transform_tasks(executor, config, raw_entries, parent_map, type_map, initial_relationships):
    """Submits parallel transformation tasks."""
    return {
        "glossary": {executor.submit(process_entry, config, e, parent_map, type_map): e for e in raw_entries},
        "term_links": {executor.submit(transform_term_term_links, config, e, initial_relationships.get(e.get("name"), [])): e for e in raw_entries},
        "entry_links": {executor.submit(transform_term_entry_links, config, e): e for e in raw_entries},
    }


def _collect_transform_results(futures, glossary_objs, term_term_links, grouped_links):
    """Collects results from futures into aggregated lists and dicts."""
    for future in as_completed(futures["glossary"]):
        result = future.result()
        if result:
            glossary_objs.append(result)
    for future in as_completed(futures["term_links"]):
        term_term_links.extend(future.result())
    for future in as_completed(futures["entry_links"]):
        for key, val in future.result().items():
            grouped_links[key].extend(val)
    return glossary_objs, term_term_links, grouped_links


def process_raw_entries(
    config: Dict[str, Any], raw_entries: List[dict]
) -> Tuple[List[GlossaryEntry], List[EntryLink], Dict[str, List[EntryLink]]]:
    """
    Orchestrates the full processing pipeline: 
    builds relationship maps, executes transformations in parallel, and collects results.
    """
    logger.info("Step 2: Fetching relationships...")
    parent_map, type_map, initial_relationships = _prepare_relationship_maps(
        raw_entries, config["user_project"]
    )

    logger.info("Step 3: Transforming data...")
    return _execute_parallel_transforms(
        config, raw_entries, parent_map, type_map, initial_relationships
    )


def _prepare_relationship_maps(
    raw_entries: List[dict], user_project: str
) -> Tuple[Dict[str, str], Dict[str, str], List[EntryLink]]:
    """
    Prepares the parent map, type map, and initial relationships 
    required before starting parallel transformations.
    """
    initial_relationships = fetch_all_relationships(raw_entries, user_project)
    parent_map = _build_parent_map(raw_entries, initial_relationships)
    type_map = _build_type_map(raw_entries)
    return parent_map, type_map, initial_relationships


def _execute_parallel_transforms(
    config: Dict[str, Any],
    raw_entries: List[dict],
    parent_map: Dict[str, str],
    type_map: Dict[str, str],
    initial_relationships: List[EntryLink],
) -> Tuple[List[GlossaryEntry], List[EntryLink], Dict[str, List[EntryLink]]]:
    """
    Executes transformation tasks in parallel and aggregates results.
    """
    glossary_objs, term_term_links, grouped_term_entry_links = [], [], defaultdict(list)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = _submit_transform_tasks(
            executor, config, raw_entries, parent_map, type_map, initial_relationships
        )
        glossary_objs, term_term_links, grouped_term_entry_links = _collect_transform_results(
            futures, glossary_objs, term_term_links, grouped_term_entry_links
        )
    logger.debug(
        "Parallel transform completed | "
        "raw_entries=%s, config=%s | "
        "glossary_objs=%s\n"
        "term_term_links=%s\n"
        "grouped_term_entry_links_keys=%s",
        raw_entries,
        config,
        glossary_objs,
        term_term_links,
        list(grouped_term_entry_links.keys()),
    )
    return glossary_objs, term_term_links, grouped_term_entry_links
