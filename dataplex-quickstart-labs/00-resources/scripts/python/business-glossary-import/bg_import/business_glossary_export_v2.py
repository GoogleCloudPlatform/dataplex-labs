"""
This script is used to export data from a Data Catalog glossary to JSON files depending on the export mode.
The Glossary JSON file contains the following fields:
- name: The name of the entry.
- entryType: The type of the entry (e.g., glossary_term, glossary_category, glossary).
- aspects: Various aspects of the entry, including glossary-term-aspect, glossary-category-aspect, overview, and contacts.
- parentEntry: The parent entry of the current entry.
- entrySource: The source information of the entry, including resource, displayName, description, and ancestors.
The Entry Links JSON file contains the following fields:
- name: The name of the entry link.
- entryLinkType: The type of the entry link (e.g., synonym, related).
- entryReferences: The references to the source and target entries.
"""

import json
import logging_utils
import utils
import re
from typing import Any, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
logger = logging_utils.get_logger()

# Project number pointing to prod for export 
PROJECT_NUMBER = "655216118709"
MAX_WORKERS = 20

def get_entry_type_name(entry_type: str) -> str:
    """
    Returns the fully qualified entry type name based on the provided entry type.
    Args:
        entry_type (str): The type of the entry. Can be one of "glossary_term", "glossary_category".
    Returns:
        str: The fully qualified entry type name if the entry type is recognized, otherwise an empty string.
    """
    if entry_type == "glossary_term":
        return f"projects/{PROJECT_NUMBER}/locations/global/entryTypes/glossary-term"
    elif entry_type == "glossary_category":
        return f"projects/{PROJECT_NUMBER}/locations/global/entryTypes/glossary-category"
    elif entry_type == "glossary":
        return f"projects/{PROJECT_NUMBER}/locations/global/entryTypes/glossary"
    return ""

def get_entry_link_type_name(entry_link_type: str) -> str:
    """
    Returns the fully qualified entry link type name based on the provided entry link type.
    Args:
        entry_link_type (str): The type of the entry link. Can be one of "synonym", "related".
    Returns:
        str: The fully qualified entry link type name if the entry link type is recognized, otherwise an empty string.
    """
    if entry_link_type == "is_synonymous_to":
        return f"projects/{PROJECT_NUMBER}/locations/global/entryLinkTypes/synonym"
    elif entry_link_type == "is_related_to":
        return f"projects/{PROJECT_NUMBER}/locations/global/entryLinkTypes/related"
    return ""


def get_entry_id(entry_name: str) -> str:
    """
    Extract the entry id from the full entry name.
    
    Args: entry_name (str): The full entry name.
    Returns: str: The entry id.
    """
    match = re.search(r"projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/(.+)$", entry_name)
    if match:
        return match.group(1)
    return ""


def get_export_resource_by_id(entry_id: str, entry_type: str) -> str:
    """
    Construct the export resource name based on the entry id and type.
    
    Args: 
        entry_id (str): The entry id.
        entry_type (str): The entry type.
        project (str): The project id.
        location (str): The location id.
        glossary (str): The glossary id.
    Returns: str: The export resource name.
    """
    glossary_child_resources = ""
    if entry_type == "glossary_term":
        glossary_child_resources = "terms"
    elif entry_type == "glossary_category":
        glossary_child_resources = "categories"
    
    if glossary_child_resources:
        return f"projects/{PROJECT}/locations/{LOCATION}/glossaries/{GLOSSARY}/{glossary_child_resources}/{entry_id}"
    else:
        return f"projects/{PROJECT}/locations/{LOCATION}/glossaries/{GLOSSARY}"


def build_parent_mapping(entries: List[Dict[str, Any]], relationships_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
    """
    Build a mapping: child_entry_id -> parent's entry_id, based on the "belongs_to" relationship.
    
    Args: 
        entries (List[Dict[str, Any]]): The list of entries.
        relationships_data (Dict[str, List[Dict[str, Any]]): The relationships data.
    Returns: Dict[str, str]: The parent mapping.
    """
    parent_mapping = {}
    for entry in entries:
        child_id = get_entry_id(entry["name"])
        rels = relationships_data.get(entry["name"], [])
        for rel in rels:
            if rel["relationshipType"] == "belongs_to":
                dest = rel.get("destinationEntry", {})
                parent_full = dest.get("name", "")
                parent_id = get_entry_id(parent_full)
                if parent_id:
                    parent_mapping[child_id] = parent_id
                    break # There exists only one valid belongs_to, needn't traverse furthur 
    return parent_mapping


def compute_ancestors(child_id: str,
                      parent_mapping: Dict[str, str],
                      map_entry_id_to_entry_type: Dict[str, str]) -> List[Dict[str, str]]:
    """
    Build the ancestors array for an entry (using entry IDs).
    For each ancestor in the chain, construct its export resource using get_export_resource_by_id.
    If no belongs_to exists, assume the glossary is the only ancestor.
    
    Args: 
        child_id (str): The entry id of the child.
        parent_mapping (Dict[str, str]): The parent mapping.
        map_entry_id_to_entry_type (Dict[str, str]): The mapping of entry id to entry type.
    Returns: List[Dict[str, str]]: The ancestors array.
    """
    ancestors = []
    if child_id in parent_mapping:
        current = parent_mapping[child_id]
        while True:
            current_type = map_entry_id_to_entry_type.get(current, "glossary")
            resource = get_export_resource_by_id(current, current_type)
            glossary_child_entry_name = f"{DATAPLEX_ENTRY_GROUP}/entries/{resource}"
            ancestors.append({
                "name": glossary_child_entry_name,
                "type": get_entry_type_name(current_type)
            })
            if current not in parent_mapping:
                break
            current = parent_mapping[current]
    
    glossary_entry_name = f"{DATAPLEX_ENTRY_GROUP}/entries/projects/{PROJECT}/locations/{LOCATION}/glossaries/{GLOSSARY}"
    ancestors.append({
        "name": glossary_entry_name,
        "type": get_entry_type_name("glossary")
    })
    ancestors.reverse()
    return ancestors


def process_entry(entry: Dict[str, Any],
                      parent_mapping: Dict[str, str],
                      map_entry_id_to_entry_type: Dict[str, str],
                      ) -> Dict[str, Any]:
    """
    Process a single entry (only glossary_term or glossary_category) and produce the export JSON.
    - Extract the entry id (the portion after "/entries/") for constructing new resource names.
    - The export "name" is: export_base + "/" + new resource.
    - The parentEntry is always the glossary resource (export_base + "/" + glossary export).
    - The ancestors array is built using the entry ids.
    """
    entry_type = entry.get("entryType", "")
    if entry_type not in ["glossary_term", "glossary_category"]:
        return None

    display_name = entry.get("displayName", "").strip()
    core_aspects = entry.get("coreAspects", {})
    business_context = core_aspects.get("business_context", {}).get("jsonContent", {})
    description = business_context.get("description", "")
    contacts_list = business_context.get("contacts", [])
    child_id = get_entry_id(entry["name"])
    
    glossary_resource = get_export_resource_by_id(child_id, entry_type)
    entry_name = f"{DATAPLEX_ENTRY_GROUP}/entries/{glossary_resource}"
    glossary_entry_id = f"projects/{PROJECT}/locations/{LOCATION}/glossaries/{GLOSSARY}"
    parent_entry_name = f"{DATAPLEX_ENTRY_GROUP}/entries/{glossary_entry_id}"
    parent_entry_name = get_entry_name(glossary_entry_id, "glossary")

    ancestors = compute_ancestors(child_id, parent_mapping, map_entry_id_to_entry_type)
    
    glossary_resource_aspect = "glossary-term-aspect" if entry_type == "glossary_term" else "glossary-category-aspect"
    aspects = {
            f"{PROJECT_NUMBER}.global.{glossary_resource_aspect}": {"data": {}},
            f"{PROJECT_NUMBER}.global.overview": {"data": {"content": f"<p>{description}</p>"}},
            f"{PROJECT_NUMBER}.global.contacts": {"data": {"identities": [{"name": c} for c in contacts_list]}}
        }
    entry_type_name = get_entry_type_name(entry_type)   
    entry_source = {
        "resource": glossary_resource,
        "displayName": display_name,
        "description": "",
        "ancestors": ancestors
    }

    return {
        "entry": {
            "name": entry_name,
            "entryType": entry_type_name,
            "aspects": aspects,
            "parentEntry": parent_entry_name,
            "entrySource": entry_source
        }
    }


def get_entry_link_id(relationship_name: str) -> str:
    """Extracts the id from the full relationship name."""
    # Since relationships are bi-directional, we need to ensure the same entry link ID is used if the relationship involves two glossaries.
    # This ensures that during export and subsequent import, the entry link ID remains consistent for the same relationship.
    match = re.search(r"projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/[^/]+/relationships/(.+)$", relationship_name)
    if match:
        return match.group(1)
    return ""

    

def get_entry_name(glossary_resource_name: str, entry_type: str) -> str:
    """Generates the full entry name."""
    resource_name = get_export_resource_by_id(get_entry_id(glossary_resource_name), entry_type)
    return f"{DATAPLEX_ENTRY_GROUP}/entries/{resource_name}"

def build_entry_link(source_name: str, target_name: str, link_type: str, entry_link_id: str) -> Dict[str, Any]:
    """Constructs an entry link."""
    return {
        "name": f"{DATAPLEX_ENTRY_GROUP}/entryLinks/{entry_link_id}",
        "entryLinkType": get_entry_link_type_name(link_type),
        "entryReferences": [
            {"name": source_name, "type": "SOURCE"},
            {"name": target_name, "type": "TARGET"}
        ]
    }

def build_entry_links(entry: Dict[str, Any], relationships_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Build the entry links JSON data for synonyms and related terms.
    """
    entry_links = []
    entry_name = entry.get("name", "")
    relationships = relationships_data.get(entry_name, [])

    for relationship in relationships:
        entry_link_id = get_entry_link_id(relationship.get("name", ""))

        link_type = relationship.get("relationshipType", "")
        if link_type in ["is_synonymous_to", "is_related_to"]:
            destination_entry = relationship.get("destinationEntry", {}).get("name", "")
            source_entry = relationship.get("sourceEntry", {}).get("name", "")
            source_entry_type = relationship.get("sourceEntry", {}).get("entryType", "")
            
            if destination_entry:
                source_entry_name = get_entry_name(source_entry, source_entry_type)
                destination_entry_name = get_entry_name(destination_entry, source_entry_type)
                
                entry_links.append(build_entry_link(source_entry_name, destination_entry_name, link_type, entry_link_id))
    
    return entry_links


def export_glossary_entries_json(entries: List[Dict[str, Any]],
                                 output_json: str,
                                 parent_mapping: Dict[str, str],
                                 map_entry_id_to_entry_type: Dict[str, str],
                                 max_workers: int = MAX_WORKERS):
    """
    Process each entry and write the export JSON as one object per line.
    """
    with open(output_json, mode="w", encoding="utf-8") as outputfile:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for e in entries:
                futures.append(
                    executor.submit(
                        process_entry,
                        e,
                        parent_mapping,
                        map_entry_id_to_entry_type,
                    )
                )
            for future in as_completed(futures):
                result = future.result()
                if result:
                    outputfile.write(json.dumps(result, indent=4) + "\n")


def export_entry_links_json(entries: List[Dict[str, Any]], relationships_data: Dict[str, List[Dict[str, Any]]], output_json: str):
    """
    Export the entry links JSON data for synonyms and related terms.
    """
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(build_entry_links, entry, relationships_data): entry for entry in entries}
        
        unique_entry_links = {}
        filtered_links = []

        # For entry links within glossary, same entry link will be present in relationships data of two entries
        # so we have to remove entrylinks with same name
        for future in as_completed(futures):
            result = future.result()
            if result:
                for link in result:
                    link_name = link["name"]
                    if link_name not in unique_entry_links:
                        unique_entry_links[link_name] = link
                        filtered_links.append(link)  # Store the unique link in a list

    with open(output_json, mode="w", encoding="utf-8") as outputfile:
        json.dump(filtered_links, outputfile, indent=4)



def main():
    args = utils.get_export_v2_arguments()
    utils.validate_export_v2_args(args)

    global DATAPLEX_ENTRY_GROUP, PROJECT, LOCATION, GLOSSARY
    PROJECT = args.project
    LOCATION = args.location
    GLOSSARY = args.glossary
    DATAPLEX_ENTRY_GROUP = f"projects/{args.project}/locations/{args.location}/entryGroups/@dataplex"
    
    logger.info("Fetching entries in the Glossary...")
    entries = utils.fetch_entries(args.project, args.location, args.group)
    logger.info("Fetching entry links in the Glossary...")
    relationships_data = utils.fetch_all_relationships(entries, args.project)
    map_entry_id_to_entry_type = {get_entry_id(e["name"]): e.get("entryType", "") for e in entries}
    parent_mapping = build_parent_mapping(entries, relationships_data)

    if args.export_mode in ["glossary_only", "all"]:
        export_glossary_entries_json(
            entries,
            args.glossary_json,
            parent_mapping,
            map_entry_id_to_entry_type,
        )
        logger.info(f"Glossary exported to {args.glossary_json}")

    if args.export_mode in ["entry_links_only", "all"]:
        export_entry_links_json(entries, relationships_data, args.entrylinks_json)
        logger.info(f"Entry links exported to {args.entrylinks_json}")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(int(elapsed_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        logger.info(f"Export completed in {hours} hr {minutes} min {seconds} sec")
    elif minutes > 0:
        logger.info(f"Export completed in {minutes} min {seconds} sec")
    else:
        logger.info(f"Export completed in {seconds} sec")
