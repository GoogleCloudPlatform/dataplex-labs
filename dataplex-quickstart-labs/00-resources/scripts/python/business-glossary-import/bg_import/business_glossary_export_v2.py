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
import api_call_utils
import requests
import subprocess
import os
import sys
from collections import defaultdict

logger = logging_utils.get_logger()

MAX_WORKERS = 20
GLOSSARY_EXPORT_LOCATION = "global"


def get_project_number(project_id: str) -> str:
    """Call Cloud Resource Manager to look up the numeric project number."""
    url = f"https://cloudresourcemanager.googleapis.com/v3/projects/{project_id}"
    resp = api_call_utils.fetch_api_response(requests.get, url, "")
    if resp["error_msg"]:
        logger.error(f"Could not fetch project number for {project_id}: {resp['error_msg']}")
        return project_id
    name = resp["json"].get("name", "")
    parts = name.split("/")
    return parts[1] if len(parts) == 2 else project_id

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
        entry_link_type (str): The type of the entry link. Can be one of "synonym", "related", "definition".
    Returns:
        str: The fully qualified entry link type name if the entry link type is recognized, otherwise an empty string.
    """
    if entry_link_type == "is_synonymous_to":
        return f"projects/{PROJECT_NUMBER}/locations/global/entryLinkTypes/synonym"
    elif entry_link_type == "is_related_to":
        return f"projects/{PROJECT_NUMBER}/locations/global/entryLinkTypes/related"
    elif entry_link_type == "is_described_by":
        return f"projects/{PROJECT_NUMBER}/locations/global/entryLinkTypes/definition"
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
    Returns:
        str: The export resource name.
    """
    glossary_child_resources = ""
    if entry_type == "glossary_term":
        glossary_child_resources = "terms"
    elif entry_type == "glossary_category":
        glossary_child_resources = "categories"

    if glossary_child_resources:
        return f"projects/{PROJECT}/locations/{GLOSSARY_EXPORT_LOCATION}/glossaries/{GLOSSARY}/{glossary_child_resources}/{entry_id}"
    else:
        return f"projects/{PROJECT}/locations/{GLOSSARY_EXPORT_LOCATION}/glossaries/{GLOSSARY}"


def build_parent_mapping(
    entries: List[Dict[str, Any]], relationships_data: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, str]:
    """
    Build a mapping: child_entry_id -> parent's entry_id, based on the "belongs_to" relationship.

    Args:
        entries (List[Dict[str, Any]]): The list of entries.
        relationships_data (Dict[str, List[Dict[str, Any]]]): The relationships data.
    Returns:
        Dict[str, str]: The parent mapping.
    """
    parent_mapping: Dict[str, str] = {}
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
                    break  # There is exactly one valid belongs_to
    return parent_mapping


def compute_ancestors(
    child_id: str,
    parent_mapping: Dict[str, str],
    map_entry_id_to_entry_type: Dict[str, str],
) -> List[Dict[str, str]]:
    """
    Build the ancestors array for an entry (using entry IDs).
    For each ancestor in the chain, construct its export resource using get_export_resource_by_id.
    If no belongs_to exists, assume the glossary is the only ancestor.

    Args:
        child_id (str): The entry id of the child.
        parent_mapping (Dict[str, str]): The parent mapping.
        map_entry_id_to_entry_type (Dict[str, str]): The mapping of entry id to entry type.
    Returns:
        List[Dict[str, str]]: The ancestors array.
    """
    ancestors: List[Dict[str, str]] = []
    if child_id in parent_mapping:
        current = parent_mapping[child_id]
        while True:
            current_type = map_entry_id_to_entry_type.get(current, "glossary")
            resource = get_export_resource_by_id(current, current_type)
            glossary_child_entry_name = f"{DATAPLEX_ENTRY_GROUP}/entries/{resource}"
            ancestors.append({"name": glossary_child_entry_name, "type": get_entry_type_name(current_type)})
            if current not in parent_mapping:
                break
            current = parent_mapping[current]

    glossary_entry_name = (
        f"{DATAPLEX_ENTRY_GROUP}/entries/projects/{PROJECT}/locations/{GLOSSARY_EXPORT_LOCATION}/glossaries/{GLOSSARY}"
    )
    ancestors.append({"name": glossary_entry_name, "type": get_entry_type_name("glossary")})
    ancestors.reverse()
    return ancestors


def process_entry(
    entry: Dict[str, Any],
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
    contacts_list = [
        {
            "role": "steward",
            "name": re.sub(r"<([^>]+)>", "", contact).strip(),
            "id": re.search(r"<([^>]+)>", contact).group(1) if re.search(r"<([^>]+)>", contact) else "",
        }
        for contact in business_context.get("contacts", [])
    ]
    child_id = get_entry_id(entry["name"])

    glossary_resource = get_export_resource_by_id(child_id, entry_type)
    entry_name = f"{DATAPLEX_ENTRY_GROUP}/entries/{glossary_resource}"
    glossary_entry_id = f"projects/{PROJECT}/locations/{GLOSSARY_EXPORT_LOCATION}/glossaries/{GLOSSARY}"
    parent_entry_name = get_entry_name(glossary_entry_id, "glossary")

    ancestors = compute_ancestors(child_id, parent_mapping, map_entry_id_to_entry_type)

    glossary_resource_aspect = "glossary-term-aspect" if entry_type == "glossary_term" else "glossary-category-aspect"
    aspects = {
        f"{PROJECT_NUMBER}.global.{glossary_resource_aspect}": {"data": {}},
        f"{PROJECT_NUMBER}.global.overview": {"data": {"content": f"<p>{description}</p>"}},
        f"{PROJECT_NUMBER}.global.contacts": {"data": {"identities": contacts_list}},
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
            "entrySource": entry_source,
        }
    }


def get_entry_link_id(relationship_name: str) -> str:
    """Extracts the id from the full relationship name."""
    match = re.search(
        r"projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/[^/]+/relationships/(.+)$",
        relationship_name,
    )
    if match:
        return "g"+ match.group(1) # adding prefix 'g' to ensure entrylink_id always starts with a letter
    return ""


def get_entry_name(glossary_resource_name: str, entry_type: str) -> str:
    """Generates the full entry name."""
    resource_name = get_export_resource_by_id(get_entry_id(glossary_resource_name), entry_type)
    return f"{DATAPLEX_ENTRY_GROUP}/entries/{resource_name}"


def build_entry_link(source_name: str, target_name: str, link_type: str, entry_link_id: str) -> Dict[str, Any]:
    """Constructs an entry link."""
    return {
        "entryLink": {
            "name": f"{DATAPLEX_ENTRY_GROUP}/entryLinks/{entry_link_id}",
            "entryLinkType": get_entry_link_type_name(link_type),
            "entryReferences": [
                {"name": source_name},
                {"name": target_name}
            ]
        }
    }


def build_entry_links(entry: Dict[str, Any], relationships_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Build the entry links JSON data for synonyms and related terms.
    """
    entry_links: List[Dict[str, Any]] = []
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


def export_glossary_entries_json(
    entries: List[Dict[str, Any]],
    output_json: str,
    parent_mapping: Dict[str, str],
    map_entry_id_to_entry_type: Dict[str, str],
    max_workers: int = MAX_WORKERS,
):
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
                    outputfile.write(json.dumps(result) + "\n")


def ensure_directory_exists(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def write_links_to_file(links, filepath, mode="w"):
    with open(filepath, mode=mode, encoding="utf-8") as outputfile:
        for link in links:
            outputfile.write(json.dumps(link) + "\n")
    utils.replace_with_new_glossary_id(filepath, GLOSSARY)

def parse_entrylinktype_arg(raw_entrylinktype: str) -> set:
    """
    Parse the raw `--entrylinktype` string (e.g. "{related,synonym}") into a set
    of full relationship types: {"is_related_to", "is_synonymous_to", "is_described_by"}.
    """
    cleaned = (raw_entrylinktype or "").strip("{} ")
    tokens = [t.strip() for t in cleaned.split(",") if t.strip()]
    mapping = {"synonym": "is_synonymous_to", "related": "is_related_to", "definition": "is_described_by"}
    if not tokens:
        return set(mapping.values())
    entrylinktype_set = set()
    for t in tokens:
        t_lower = t.lower()
        if t_lower in mapping:
            entrylinktype_set.add(mapping[t_lower])
        else:
            logger.error(f"Invalid entrylinktype token: '{t}'. Must be one of {list(mapping.keys())}")
            sys.exit(1)
    return entrylinktype_set


def export_combined_entry_links_json(
    entries: List[Dict[str, Any]],
    relationships_data: Dict[str, List[Dict[str, Any]]],
    project_id: str,
    entrylinktype_set: set,
):
    """
    Export term-term and term-entry entry links with enhanced filtering and dynamic file handling.

    entrylinktype_set is a set of full relationshipType strings, e.g.:
       {"is_synonymous_to", "is_related_to", "is_described_by"} 
    """
    all_links: List[Dict[str, Any]] = []
    seen_link_names = set()
    # group definition links by PROJECT_LOCATION_ENTRYGROUP
    definition_links_by_ple = defaultdict(list)
    term_links: List[Dict[str, Any]] = []

    def process_term_links(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        entry_links: List[Dict[str, Any]] = []
        entry_name = entry.get("name", "")
        relationships = relationships_data.get(entry_name, [])

        for relationship in relationships:
            entry_link_id = get_entry_link_id(relationship.get("name", ""))
            link_type = relationship.get("relationshipType", "")
            if link_type not in entrylinktype_set:
                continue
            if link_type in ["is_synonymous_to", "is_related_to"]:
                destination_entry = relationship.get("destinationEntry", {}).get("name", "")
                source_entry = relationship.get("sourceEntry", {}).get("name", "")
                source_entry_type = relationship.get("sourceEntry", {}).get("entryType", "")

                if destination_entry:
                    source_entry_name = get_entry_name(source_entry, source_entry_type)
                    destination_entry_name = get_entry_name(destination_entry, source_entry_type)
                    link = build_entry_link(source_entry_name, destination_entry_name, link_type, entry_link_id)
                    if link["entryLink"]["name"] not in seen_link_names:
                        seen_link_names.add(link["entryLink"]["name"])
                        entry_links.append(link)
                        term_links.append(link)
        return entry_links

    def process_term_entry_links(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        entry_links: List[Dict[str, Any]] = []
        if entry.get("entryType") != "glossary_term":
            return entry_links

        entry_id = get_entry_id(entry.get("name", ""))
        entry_uid = entry.get("entryUid", "")
        search_url = "https://datacatalog.googleapis.com/v1/catalog:search"
        request_body = {
            "orderBy": "relevance",
            "pageSize": 1000,
            "query": f"(term:{entry_id})",
            "scope": {
                "includeGcpPublicDatasets": False,
                "includeOrgIds": ORG_IDS,
            }
        }

        search_response = api_call_utils.fetch_api_response(requests.post, search_url, USER_PROJECT, request_body)
        results = search_response.get("json", {}).get("results", [])

        for result in results:
            linked_resource = result.get("linkedResource", "").lstrip("/")
            relative_resource_name = result.get("relativeResourceName", "")

            if not linked_resource or not relative_resource_name:
                continue

            new_entry_id = re.sub(r"^/+", "", linked_resource)
            relative_resource_name_v2 = re.sub(r"entries/[^/]+$", f"entries/{new_entry_id}", relative_resource_name)

            requested_project_name = ""
            match = re.match(r"projects/([^/]+)/locations/([^/]+)/", relative_resource_name_v2)
            if match:
                project_id_from_relative_resource_name_v2 = match.group(1)
                location_from_relative_resource_name_v2 = match.group(2)
                requested_project_name = f"projects/{project_id_from_relative_resource_name_v2}/locations/{location_from_relative_resource_name_v2}"

            entry_get_url = f"https://dataplex.googleapis.com/v1/{requested_project_name}:lookupEntry?entry={relative_resource_name_v2}"
            entry_check = api_call_utils.fetch_api_response(requests.get, entry_get_url, USER_PROJECT)
            if not entry_check.get("json") or entry_check.get("error_msg"):
                logger.warning(f"Dataplex entry not found for linked resource: {linked_resource}")
                continue

            rel_url = f"https://datacatalog.googleapis.com/v2/{relative_resource_name}/relationships"
            response = api_call_utils.fetch_api_response(requests.get, rel_url, USER_PROJECT)
            relationships = response.get("json", {}).get("relationships", [])

            for rel in relationships:
                dest_entry = rel.get("destinationEntryName", "")
                source_column = rel.get("sourceColumn", "")
                if get_entry_id(dest_entry) == entry_uid:
                    rel_id = get_entry_link_id(rel.get("name", ""))
                    # extract project & location from the relative resource name
                    m = re.match(r"projects/([^/]+)/locations/([^/]+)/", relative_resource_name_v2)
                    proj = m.group(1) if m else PROJECT
                    loc  = m.group(2) if m else LOCATION
                    entrygroup_match = re.search(r"entryGroups/([^/]+)/", relative_resource_name)
                    eg = entrygroup_match.group(1) if entrygroup_match else "@dataplex"
                    # Sanitize project, location, and entry group for filename safety
                    def sanitize(s):
                        return re.sub(r"[^a-zA-Z0-9_\-]", "_", s or "")
                    ple = f"{sanitize(proj)}_{sanitize(loc)}_{sanitize(eg)}"
                    entry_link_name = f"projects/{PROJECT}/locations/global/entryGroups/{eg}/entryLinks/{rel_id}"
                    entry_reference_source = {
                        "name": relative_resource_name_v2,
                        "path": f"Schema.{source_column}" if source_column else "",
                        "type": "SOURCE",
                    }
                    entry_reference_target = {
                        "name": f"projects/{PROJECT}/locations/global/entryGroups/@dataplex/entries/{get_export_resource_by_id(entry_id, entry.get('entryType', 'glossary_term'))}",
                        "path": "",
                        "type": "TARGET",
                    }

                    link = {
                        "entryLink": {
                            "name": entry_link_name,
                            "entryLinkType": get_entry_link_type_name("is_described_by"),
                            "entryReferences": [entry_reference_source, entry_reference_target],
                        }
                    }
                    if link["entryLink"]["name"] not in seen_link_names:
                        seen_link_names.add(link["entryLink"]["name"])
                        definition_links_by_ple[ple].append(link)
                        entry_links.append(link)
        return entry_links

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        if {"is_synonymous_to", "is_related_to"} & entrylinktype_set:
            futures += [executor.submit(process_term_links, e) for e in entries]
        if "is_described_by" in entrylinktype_set:
            futures += [executor.submit(process_term_entry_links, e) for e in entries]

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_links.extend(result)

    # Always write into "Exported_Files/" folder
    default_dir = os.path.join(os.getcwd(), "Exported_Files")

    # "Definition" only filter: one file per entrygroup
    if entrylinktype_set == {"is_described_by"}:
        ensure_directory_exists(default_dir)
        for ple, links in definition_links_by_ple.items():
            filename = f"entrylinks_definition_{GLOSSARY}_{ple}.json"
            output_path = os.path.join(default_dir, filename)
            write_links_to_file(links, output_path)
            logger.info(f"Exported definition links for {ple} → {output_path}")

    # "Related" and/or "Synonym" only (no definitions)
    elif entrylinktype_set <= {"is_related_to", "is_synonymous_to"} and "is_described_by" not in entrylinktype_set:
        ensure_directory_exists(default_dir)
        tags = []
        if "is_related_to" in entrylinktype_set:
            tags.append("related")
        if "is_synonymous_to" in entrylinktype_set:
            tags.append("synonym")
        tag_part = "_".join(tags)
        filename = f"entrylinks_{tag_part}_{GLOSSARY}.json"
        output_path = os.path.join(default_dir, filename)
        write_links_to_file(all_links, output_path)
        logger.info(f"Exported {tag_part} links to {output_path}")

    # Mixed case: "related" or "synonym" plus "definition"
    else:
        ensure_directory_exists(default_dir)
        # First: write term-term (related+synonym) into one file
        relsyn_filename = f"entrylinks_related_synonyms_{GLOSSARY}.json"
        relsyn_path = os.path.join(default_dir, relsyn_filename)
        write_links_to_file(term_links, relsyn_path, mode="w")
        logger.info(f"Exported related/synonym links to {relsyn_path}")
        # Then: write each definition group separately
        for ple, links in definition_links_by_ple.items():
            filename = f"entrylinks_definition_{GLOSSARY}_{ple}.json"
            output_path = os.path.join(default_dir, filename)
            write_links_to_file(links, output_path)
            logger.info(f"Exported definition links to {output_path}")


def create_export_folder() -> str:
    """
    Create (if necessary) and return the path to the "Exported_Files" folder under CWD.
    """
    export_folder = os.path.join(os.getcwd(), "Exported_Files")
    if not os.path.exists(export_folder):
        os.makedirs(export_folder)
    return export_folder


def compute_glossary_path(export_folder: str, glossary_id: str) -> str:
    """
    Return the full path for the glossary JSON file under Exported_Files.
    """
    return os.path.join(export_folder, f"glossary_{glossary_id}.json")


def main():
    args = utils.get_export_v2_arguments()
    utils.validate_export_v2_args(args)
    utils.maybe_override_args_from_url(args)

    global DATAPLEX_ENTRY_GROUP, USER_PROJECT, PROJECT, LOCATION, GLOSSARY, PROJECT_NUMBER, DATACATALOG_BASE_URL, ORG_IDS
    USER_PROJECT = args.user_project if args.user_project else args.project
    PROJECT = get_project_number(args.project)
    LOCATION = args.location
    GLOSSARY = args.glossary

    DATAPLEX_ENTRY_GROUP = f"projects/{PROJECT}/locations/{GLOSSARY_EXPORT_LOCATION}/entryGroups/@dataplex"
    if args.testing:
        PROJECT_NUMBER = "418487367933"  # Staging project number
    else:
        PROJECT_NUMBER = "655216118709"  # Prod project number

    # Create "Exported_Files" and compute glossary path
    export_folder = create_export_folder()
    glossary_output_path = compute_glossary_path(export_folder, GLOSSARY)

    # Run gcloud to fetch organization IDs
    result = subprocess.run(
        ["gcloud", "organizations", "list", "--format=value(ID)"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.stderr:
        logger.error("Error:", result.stderr)
    org_ids = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
    ORG_IDS = org_ids


    logger.info("Fetching entries in the Glossary...")
    entries = utils.fetch_entries(USER_PROJECT,PROJECT, LOCATION, args.group)

    # Parse entrylinktype into a set of full relationshipType strings
    entrylinktype_set = parse_entrylinktype_arg(args.entrylinktype)

    # Fetch "term-term" relationships only if needed
    need_relationships = (
        args.export_mode != "glossary_only"
        and bool({"is_synonymous_to", "is_related_to"} & entrylinktype_set)
    )
    if need_relationships:
        logger.info("Fetching entry relationships...")
        relationships_data = utils.fetch_all_relationships(entries, USER_PROJECT, PROJECT)
    else:
        relationships_data = {}

    # Build parent_mapping if exporting glossary entries
    if args.export_mode != "entry_links_only":
        map_entry_id_to_entry_type = {get_entry_id(e["name"]): e.get("entryType", "") for e in entries}
        parent_mapping = build_parent_mapping(entries, relationships_data)
    else:
        map_entry_id_to_entry_type = {}
        parent_mapping = {}

    # Export glossary entries if requested
    if args.export_mode in ["glossary_only", "all"]:
        export_glossary_entries_json(
            entries,
            glossary_output_path,
            parent_mapping,
            map_entry_id_to_entry_type,
        )
        logger.info(f"Glossary exported to {glossary_output_path}")
        utils.replace_with_new_glossary_id(glossary_output_path, GLOSSARY)

    # Export entry links if requested
    if args.export_mode in ["entry_links_only", "all"]:
        export_combined_entry_links_json(
            entries,
            relationships_data,
            PROJECT,
            entrylinktype_set,
        )
        logger.info(f"Entry links exported under {export_folder}/")

     # Create Glossary in Dataplex if it does not exist
    utils.create_glossary(USER_PROJECT, PROJECT, args.location, args.group, GLOSSARY)


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
