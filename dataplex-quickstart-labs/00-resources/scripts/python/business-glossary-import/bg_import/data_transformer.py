# data_transformer.py
"""
Transforms raw Data Catalog data into structured models for export.
"""
import re
from typing import List, Dict, Optional
from collections import defaultdict
from models import GlossaryEntry, EntryLink, Ancestor, EntrySource, EntryReference
from api_layer import search_catalog, lookup_dataplex_entry, fetch_glossary_id, fetch_relationships_for_entry
from migration_utils import normalize_id
import logging_utils

logger = logging_utils.get_logger()
MAX_DESC_SIZE_BYTES = 120 * 1024
PROJECT_NUMBER = "655216118709"

def get_entry_id(name: str) -> str:
    match = re.search(r"entries/([^/]+)$", name or "")
    return match.group(1) if match else ""

def get_rel_id(name: str) -> str:
    match = re.search(r"relationships/([^/]+)$", name or "")
    return f"g{match.group(1)}" if match else ""

def _get_export_resource(cfg: dict, entry_id: str, entry_type: str) -> str:
    """Constructs the target resource path with normalized IDs."""
    child = "terms" if entry_type == "glossary_term" else "categories"
    normalized_entry_id = normalize_id(entry_id)
    return f"projects/{cfg['project']}/locations/global/glossaries/{cfg['normalized_glossary']}/{child}/{normalized_entry_id}"

def _get_qualified_type_name(entry_type: str) -> str:
    type_map = {"glossary_term": "glossary-term", "glossary_category": "glossary-category"}
    suffix = type_map.get(entry_type, entry_type)
    return f"projects/{PROJECT_NUMBER}/locations/global/entryTypes/{suffix}"

def _get_qualified_link_type_name(link_type: str) -> str:
    type_map = {"is_synonymous_to": "synonym", "is_related_to": "related", "is_described_by": "definition"}
    suffix = type_map.get(link_type, link_type)
    return f"projects/{PROJECT_NUMBER}/locations/global/entryLinkTypes/{suffix}"

def _compute_ancestors(cfg: dict, child_id: str, p_map: dict, t_map: dict) -> List[Ancestor]:
    ancestors = []
    current_id = p_map.get(child_id)
    while current_id:
        c_type = t_map.get(current_id, "glossary_category")
        resource = _get_export_resource(cfg, current_id, c_type)
        ancestors.append(Ancestor(name=f"{cfg['dataplex_entry_group']}/entries/{resource}", type=_get_qualified_type_name(c_type)))
        current_id = p_map.get(current_id)
    ancestors.append(Ancestor(
        name=f"{cfg['dataplex_entry_group']}/entries/projects/{cfg['project']}/locations/global/glossaries/{cfg['normalized_glossary']}",
        type=_get_qualified_type_name("glossary")
    ))
    return list(reversed(ancestors))

def process_entry(config: dict, entry: dict, parent_map: dict, type_map: dict) -> Optional[GlossaryEntry]:
    entry_type = entry.get("entryType")
    if entry_type not in ["glossary_term", "glossary_category"]: return None
    
    desc = entry.get("coreAspects", {}).get("business_context", {}).get("jsonContent", {}).get("description", "")
    if len(desc.encode('utf-8')) > MAX_DESC_SIZE_BYTES:
        entry_name = entry.get("name", "Unknown")
        # Construct a helpful console URL for the user to find the entry
        child_path_part = "terms" if entry_type == "glossary_term" else "categories"
        console_name = entry_name.replace("/entries/", f"/{child_path_part}/")
        console_url = f"https://console.cloud.google.com/dataplex/glossaries/{console_name}"
        logger.warning(f"Description for {console_url} is > 120KB; omitting.")
        desc = ""
        
    entry_id = get_entry_id(entry.get("name"))
    resource_path = _get_export_resource(config, entry_id, entry_type)
    entry_source = EntrySource(
        resource=resource_path, displayName=entry.get("displayName", "").strip(),
        description="", ancestors=_compute_ancestors(config, entry_id, parent_map, type_map)
    )
    aspect_type = "glossary-term-aspect" if entry_type == "glossary_term" else "glossary-category-aspect"
    return GlossaryEntry(
        name=f"{config['dataplex_entry_group']}/entries/{resource_path}",
        entryType=_get_qualified_type_name(entry_type),
        parentEntry=f"{config['dataplex_entry_group']}/entries/projects/{config['project']}/locations/global/glossaries/{config['normalized_glossary']}",
        aspects={f"{PROJECT_NUMBER}.global.{aspect_type}": {"data": {}}, f"{PROJECT_NUMBER}.global.overview": {"data": {"content": f"<p>{desc}</p>"}}},
        entrySource=entry_source
    )

def transform_term_term_links(config: dict, entry: dict, rels: List[dict]) -> List[EntryLink]:
    links = []
    source_resource = _get_export_resource(config, get_entry_id(entry.get("name")), entry.get("entryType"))
    source_name = f"{config['dataplex_entry_group']}/entries/{source_resource}"
    for rel in rels:
        logger.debug(f"Processing term-term relationship: {rel.get('name', 'Unknown')}")
        link_type = rel.get("relationshipType")
        if link_type not in ["is_synonymous_to", "is_related_to"]:
            continue
        dest_entry_name = rel.get("destinationEntry", {}).get("name")
        if not dest_entry_name:
            logger.debug(f"Skipping relationship {rel.get('name')} due to missing destination.")
            continue
        dest_entry_id = get_entry_id(dest_entry_name)
        glossary_id = fetch_glossary_id(dest_entry_name, config["user_project"])
        dest_project = re.sub(r"^projects/([^/]+)/.*", r"\1", dest_entry_name)
        if not glossary_id:
            logger.warning(f"Could not resolve glossary ID for {dest_entry_name}. Link may be incorrect.")
            target_name = f"projects/{dest_project}/locations/global/entryGroups/@dataplex/entries/UNKNOWN/terms/{normalize_id(dest_entry_id)}"
        else:
            target_name = (f"projects/{dest_project}/locations/global/entryGroups/@dataplex/entries/projects/"
                           f"{dest_project}/locations/global/glossaries/{glossary_id}/terms/{normalize_id(dest_entry_id)}")
        link = EntryLink(
            name=f"{config['dataplex_entry_group']}/entryLinks/{get_rel_id(rel.get('name'))}",
            entryLinkType=_get_qualified_link_type_name(link_type),
            entryReferences=[EntryReference(name=source_name), EntryReference(name=target_name)]
        )
        logger.debug(f"Created term-term link: {link.name}")
        links.append(link)
    return links

def transform_term_entry_links(config: dict, entry: dict) -> Dict[str, List[EntryLink]]:
    if entry.get("entryType") != "glossary_term": return {}
    
    grouped_links = defaultdict(list)
    entry_id = get_entry_id(entry.get("name"))
    logger.debug(f"Searching for assets linked to term: {entry_id}")
    search_results = search_catalog(config, query=f"(term:{entry_id})")
    logger.debug(f"Found {len(search_results)} potential assets for term {entry_id}")

    for result in search_results:
        if not lookup_dataplex_entry(config, result):
            continue
        
        fqn = result.get("relativeResourceName", "")
        asset_relationships = fetch_relationships_for_entry(fqn, config["user_project"])
        logger.debug(f"Found {len(asset_relationships)} relationships for asset {fqn}")

        for rel in asset_relationships:
            logger.debug(f"Processing asset relationship: {rel.get('name')}")
            if rel.get("relationshipType") == "is_described_by" and get_entry_id(rel.get("destinationEntryName")) == entry.get("entryUid"):
                proj, loc, eg = re.match(r"projects/([^/]+)/locations/([^/]+)/entryGroups/([^/]+)", fqn).groups()
                ple_key = f"{proj}_{loc}_{eg}"
                link = EntryLink(
                    name=f"projects/{proj}/locations/{loc}/entryGroups/{eg}/entryLinks/{get_rel_id(rel.get('name'))}",
                    entryLinkType=_get_qualified_link_type_name("is_described_by"),
                    entryReferences=[
                        EntryReference(name=fqn, path=f"Schema.{rel.get('sourceColumn', '')}" if rel.get('sourceColumn') else "", type="SOURCE"),
                        EntryReference(name=f"{config['dataplex_entry_group']}/entries/{_get_export_resource(config, entry_id, 'glossary_term')}", type="TARGET")
                    ]
                )
                logger.debug(f"Created term-asset link: {link.name} for group {ple_key}")
                grouped_links[ple_key].append(link)
    return grouped_links