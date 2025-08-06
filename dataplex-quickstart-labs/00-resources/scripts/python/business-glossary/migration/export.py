"""
This script exports all entries and entry links from a v1 Data Catalog glossary.
"""
import json
import re
import os
import subprocess
import requests
from utils import logging_utils, api_call_utils, utils
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Dict, Tuple

logger = logging_utils.get_logger()
MAX_WORKERS = 20
GLOSSARY_EXPORT_LOCATION = "global"

entrygroup_to_glossaryid_map = {}


def ensure_output_folders_exist() -> (str, str):
    base_dir = os.path.join(os.getcwd(), "migration/Exported_Files")
    glossaries_dir = os.path.join(base_dir, "Glossaries")
    entrylinks_dir = os.path.join(base_dir, "EntryLinks")
    logger.debug(f"Ensuring output folders exist at: {glossaries_dir}, {entrylinks_dir}")
    os.makedirs(glossaries_dir, exist_ok=True)
    os.makedirs(entrylinks_dir, exist_ok=True)
    return glossaries_dir, entrylinks_dir


def get_entry_type_name(entry_type: str, export_context: Dict) -> str:
    """Returns the fully qualified entry type name."""
    project_number = export_context["project_number"]
    logger.debug(f"Getting entry type name for type: {entry_type}, project_number: {project_number}")
    if entry_type == "glossary_term":
        return f"projects/{project_number}/locations/global/entryTypes/glossary-term"
    elif entry_type == "glossary_category":
        return f"projects/{project_number}/locations/global/entryTypes/glossary-category"
    elif entry_type == "glossary":
        return f"projects/{project_number}/locations/global/entryTypes/glossary"
    return ""


def get_entry_link_type_name(entry_link_type: str, export_context: Dict) -> str:
    """Returns the fully qualified entry link type name."""
    project_number = export_context["project_number"]
    logger.debug(f"Getting entry link type name for type: {entry_link_type}, project_number: {project_number}")
    if entry_link_type == "is_synonymous_to":
        return f"projects/{project_number}/locations/global/entryLinkTypes/synonym"
    elif entry_link_type == "is_related_to":
        return f"projects/{project_number}/locations/global/entryLinkTypes/related"
    elif entry_link_type == "is_described_by":
        return f"projects/{project_number}/locations/global/entryLinkTypes/definition"
    return ""


def get_entry_id(entry_name: str) -> str:
    """Extracts the entry id from the full entry name."""
    match = re.search(r"projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/(.+)$", entry_name)
    entry_id = match.group(1) if match else ""
    logger.debug(f"Extracted entry id: {entry_id}")
    return entry_id


def get_export_resource_by_id(entry_id: str, entry_type: str, export_context: Dict) -> str:
    """Constructs the export resource name."""
    glossary_child_resources = ""
    logger.debug(f"Constructing export resource for entry_id: {entry_id}, entry_type: {entry_type}")
    if entry_type == "glossary_term":
        glossary_child_resources = "terms"
    elif entry_type == "glossary_category":
        glossary_child_resources = "categories"
    
    project = export_context["project"]
    glossary = export_context["glossary"]

    if glossary_child_resources:
        resource = f"projects/{project}/locations/{GLOSSARY_EXPORT_LOCATION}/glossaries/{glossary}/{glossary_child_resources}/{entry_id}"
    else:
        resource = f"projects/{project}/locations/{GLOSSARY_EXPORT_LOCATION}/glossaries/{glossary}"
    logger.debug(f"Export resource: {resource}")
    return resource


def fetch_glossary_id(entry_full_name: str, user_project: str) -> str:
    """ Given an entry name, fetch the glossary ID (entry ID) if not already cached. """
    logger.debug(f"Fetching glossary id for entry_full_name: {entry_full_name}, user_project: {user_project}")
    match = re.search(r"(projects/[^/]+/locations/[^/]+)/entryGroups/([^/]+)/entries/([^/]+)", entry_full_name)
    if not match:
        logger.debug("No match found for entry_full_name regex.")
        return None

    project_loc, entry_group_id, entry_id = match.groups()
    key = f"{project_loc}/entryGroups/{entry_group_id}"
    if key in entrygroup_to_glossaryid_map:
        logger.debug(f"Glossary id found in cache for key: {key}")
        return entrygroup_to_glossaryid_map[key]

    url = f"https://datacatalog.googleapis.com/v2/{key}/entries/{entry_id}"
    logger.debug(f"API request to fetch glossary id: {url}")
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    logger.debug(f"API response for glossary id: {response}")
    glossary_name = response.get("json", {}).get("name", "")

    glossary_id = get_entry_id(glossary_name)
    if glossary_id:
        entrygroup_to_glossaryid_map[key] = glossary_id
    normalized_id = utils.normalize_glossary_id(glossary_id)
    logger.debug(f"Normalized glossary id: {normalized_id}")
    return normalized_id

def build_parent_mapping(
    entries: List[Dict[str, Any]], relationships_data: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, str]:
    """Builds a mapping of child entries to their parent entry."""
    logger.debug("Building parent mapping for entries.")
    parent_mapping: Dict[str, str] = {}
    for entry in entries:
        child_id = get_entry_id(entry["name"])
        rels = relationships_data.get(entry["name"], [])
        for rel in rels:
            if rel["relationshipType"] == "belongs_to":
                parent_full = rel.get("destinationEntry", {}).get("name", "")
                parent_id = get_entry_id(parent_full)
                if parent_id:
                    parent_mapping[child_id] = parent_id
                    logger.debug(f"Parent mapping: {child_id} -> {parent_id}")
                    break
    logger.debug(f"Final parent mapping: {parent_mapping}")
    return parent_mapping


def compute_ancestors(
    child_id: str, parent_mapping: Dict[str, str], map_entry_id_to_entry_type: Dict[str, str], export_context: Dict
) -> List[Dict[str, str]]:
    """Builds the ancestors array for an entry."""
    logger.debug(f"Computing ancestors for child_id: {child_id}")
    ancestors: List[Dict[str, str]] = []
    current = parent_mapping.get(child_id)
    dataplex_entry_group = export_context["dataplex_entry_group"]

    while current:
        current_type = map_entry_id_to_entry_type.get(current, "glossary")
        resource = get_export_resource_by_id(current, current_type, export_context)
        ancestor_name = f"{dataplex_entry_group}/entries/{resource}"
        ancestors.append({"name": ancestor_name, "type": get_entry_type_name(current_type, export_context)})
        logger.debug(f"Ancestor added: {ancestor_name}")
        current = parent_mapping.get(current)

    glossary_entry_name = f"{dataplex_entry_group}/entries/projects/{export_context['project']}/locations/{GLOSSARY_EXPORT_LOCATION}/glossaries/{export_context['glossary']}"
    ancestors.append({"name": glossary_entry_name, "type": get_entry_type_name("glossary", export_context)})
    ancestors.reverse()
    logger.debug(f"Final ancestors list: {ancestors}")
    return ancestors


def process_entry(
    entry: Dict[str, Any], parent_mapping: Dict[str, str], map_entry_id_to_entry_type: Dict[str, str], export_context: Dict
) -> Dict[str, Any]:
    """Processes a single entry to produce the export JSON."""
    logger.debug(f"Processing entry: {entry.get('name', '')}")
    entry_type = entry.get("entryType", "")
    if entry_type not in ["glossary_term", "glossary_category"]:
        logger.debug(f"Skipping entry with type: {entry_type}")
        return None

    dataplex_entry_group = export_context["dataplex_entry_group"]
    project_number = export_context["project_number"]
    project = export_context["project"]
    glossary = export_context["glossary"]
    
    display_name = entry.get("displayName", "").strip()
    business_context = entry.get("coreAspects", {}).get("business_context", {}).get("jsonContent", {})
    description = business_context.get("description", "")
    contacts = business_context.get("contacts", [])
    contacts_list = [
        {
            "role": "steward",
            "name": re.sub(r"<[^>]+>", "", contact).strip(),
            "id": (re.search(r"<([^>]+)>", contact).group(1) if re.search(r"<([^>]+)>", contact) else ""),
        } for contact in contacts
    ]
    child_id = get_entry_id(entry["name"])
    glossary_resource = get_export_resource_by_id(child_id, entry_type, export_context)
    entry_name = f"{dataplex_entry_group}/entries/{glossary_resource}"
    parent_entry_name = f"{dataplex_entry_group}/entries/projects/{project}/locations/{GLOSSARY_EXPORT_LOCATION}/glossaries/{glossary}"

    aspect_key = "glossary-term-aspect" if entry_type == "glossary_term" else "glossary-category-aspect"
    result = {
        "entry": {
            "name": entry_name,
            "entryType": get_entry_type_name(entry_type, export_context),
            "aspects": {
                f"{project_number}.global.{aspect_key}": {"data": {}},
                f"{project_number}.global.overview": {"data": {"content": f"<p>{description}</p>"}},
                f"{project_number}.global.contacts": {"data": {"identities": contacts_list}},
            },
            "parentEntry": parent_entry_name,
            "entrySource": {
                "resource": glossary_resource,
                "displayName": display_name,
                "description": "",
                "ancestors": compute_ancestors(child_id, parent_mapping, map_entry_id_to_entry_type, export_context),
            },
        }
    }
    logger.debug(f"Processed entry result: {result}")
    return result


def Normalize_name(name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_\-@]", "_", name or "")
    logger.debug(f"Normalized name: {name} -> {normalized}")
    return normalized


def get_entry_name(glossary_resource_name: str, entry_type: str, export_context: Dict) -> str:
    """Generates the full entry name."""
    entry_id = get_entry_id(glossary_resource_name)
    resource_name = get_export_resource_by_id(entry_id, entry_type, export_context)
    full_name = f"{export_context['dataplex_entry_group']}/entries/{resource_name}"
    logger.debug(f"Generated entry name: {full_name}")
    return full_name


def export_glossary_entries_json(
    entries: List[Dict[str, Any]], output_json: str, parent_mapping: Dict[str, str], map_entry_id_to_entry_type: Dict[str, str], export_context: Dict
):
    """Processes and writes all glossary entries to a JSONL file."""
    logger.debug(f"Exporting glossary entries to JSONL file: {output_json}")
    with open(output_json, "w", encoding="utf-8") as outputfile:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_entry, e, parent_mapping, map_entry_id_to_entry_type, export_context) for e in entries]
            for future in as_completed(futures):
                if result := future.result():
                    outputfile.write(json.dumps(result) + "\n")
                    logger.debug(f"Wrote entry to file: {result}")


def write_links_to_file(links, filepath, glossary_id, mode="w"):
    logger.debug(f"Writing {len(links)} links to file: {filepath} with glossary_id: {glossary_id}")
    with open(filepath, mode, encoding="utf-8") as outputfile:
        for link in links:
            outputfile.write(json.dumps(link) + "\n")
            logger.debug(f"Wrote link: {link}")
    utils.replace_with_new_glossary_id(filepath, glossary_id)
    logger.debug(f"Replaced glossary id in file: {filepath}")


def export_combined_entry_links_json(
    entries: List[Dict[str, Any]],
    relationships_data: Dict[str, List[Dict[str, Any]]],
    user_project: str,
    output_dir: str,
    export_context: Dict,
):
    """Exports all term-term and term-entry entry links."""
    logger.debug("Exporting combined entry links JSON.")
    all_links: List[Dict[str, Any]] = []
    seen_link_names = set()
    definition_links_by_ple = defaultdict(list)
    term_links: List[Dict[str, Any]] = []
    
    def get_entry_link_id(relationship_name: str) -> str:
        logger.debug(f"Getting entry link id from relationship name: {relationship_name}")
        match = re.search(r"relationships/([^/]+)$", relationship_name)
        entry_link_id = "g" + match.group(1) if match else ""
        logger.debug(f"Extracted entry link id: {entry_link_id}")
        return entry_link_id

    def build_entry_link(source_name: str, target_name: str, link_type: str, entry_link_id: str) -> Dict[str, Any]:
        logger.debug(f"Building entry link: source={source_name}, target={target_name}, type={link_type}, id={entry_link_id}")
        return {
            "entryLink": {
                "name": f"{export_context['dataplex_entry_group']}/entryLinks/{entry_link_id}",
                "entryLinkType": get_entry_link_type_name(link_type, export_context),
                "entryReferences": [{"name": source_name}, {"name": target_name}],
            }
        }

    def process_term_links(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        logger.debug(f"Processing term links for entry: {entry.get('name', '')}")
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
                glossary_entry = relationship.get("destinationEntry", {}).get("coreRelationships", {})[0].get("destinationEntryName", {})
                if destination_entry:
                    source_entry_name = get_entry_name(source_entry, source_entry_type, export_context)
                    destination_entry_id = get_entry_id(destination_entry)
                    destination_project = re.sub(r"^projects/([^/]+)/.*", r"\1", glossary_entry)
                    glossary_id = fetch_glossary_id(glossary_entry, user_project)
                    destination_entry_name = (
                                f"projects/{destination_project}/locations/global/entryGroups/@dataplex/entries/projects/"
                                f"{destination_project}/locations/global/glossaries/{glossary_id}/terms/{destination_entry_id}"
                            )
                    link = build_entry_link(source_entry_name, destination_entry_name, link_type, entry_link_id)
                    if link["entryLink"]["name"] not in seen_link_names:
                        seen_link_names.add(link["entryLink"]["name"])
                        entry_links.append(link)
                        logger.debug(f"Added term link: {link}")

        return entry_links
    
    def process_term_entry_links(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        logger.debug(f"Processing term-entry links for entry: {entry.get('name', '')}")
        entry_links: List[Dict[str, Any]] = []
        if entry.get("entryType") != "glossary_term":
            logger.debug("Entry is not a glossary_term, skipping.")
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
                "includeOrgIds": export_context["org_ids"],
            }
        }

        logger.debug(f"API request for catalog search: {search_url}, body: {request_body}")
        search_response = api_call_utils.fetch_api_response(requests.post, search_url, user_project, request_body)
        logger.debug(f"API response for catalog search: {search_response}")
        results = search_response.get("json", {}).get("results", [])

        for result in results:
            linked_resource = result.get("linkedResource", "").lstrip("/")
            relative_resource_name = result.get("relativeResourceName", "")

            if not linked_resource or not relative_resource_name:
                logger.debug("Missing linkedResource or relativeResourceName, skipping result.")
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
            logger.debug(f"API request for entry lookup: {entry_get_url}")
            entry_check = api_call_utils.fetch_api_response(requests.get, entry_get_url, user_project)
            logger.debug(f"API response for entry lookup: {entry_check}")
            if not entry_check.get("json") or entry_check.get("error_msg"):
                logger.warning(f"Dataplex entry not found for linked resource: {linked_resource}")
                continue

            rel_url = f"https://datacatalog.googleapis.com/v2/{relative_resource_name}/relationships"
            logger.debug(f"API request for relationships: {rel_url}")
            response = api_call_utils.fetch_api_response(requests.get, rel_url, user_project)
            logger.debug(f"API response for relationships: {response}")
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
                    # Normalize project, location, and entry group for filename safety
                    ple = f"{Normalize_name(proj)}_{Normalize_name(loc)}_{Normalize_name(eg)}"
                    entry_link_name = f"projects/{proj}/locations/{loc}/entryGroups/{eg}/entryLinks/{rel_id}"
                    entry_reference_source = {
                        "name": relative_resource_name_v2,
                        "path": f"Schema.{source_column}" if source_column else "",
                        "type": "SOURCE",
                    }
                    entry_reference_target = {
                        "name": f"projects/{export_context['project']}/locations/global/entryGroups/@dataplex/entries/{get_export_resource_by_id(entry_id, 'glossary_term', export_context)}",
                        "path": "",
                        "type": "TARGET",
                    }

                    link = {
                        "entryLink": {
                            "name": entry_link_name,
                            "entryLinkType": get_entry_link_type_name("is_described_by", export_context),
                            "entryReferences": [entry_reference_source, entry_reference_target],
                        }
                    }
                    if link["entryLink"]["name"] not in seen_link_names:
                        seen_link_names.add(link["entryLink"]["name"])
                        definition_links_by_ple[ple].append(link)
                        entry_links.append(link)
                        logger.debug(f"Added definition link: {link}")
        return entry_links


    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        term_link_futures = [executor.submit(process_term_links, e) for e in entries]
        definition_futures = [executor.submit(process_term_entry_links, e) for e in entries]

        for future in as_completed(term_link_futures):
            result = future.result()
            if result:
                term_links.extend(result)
                logger.debug(f"Term links extended with: {result}")

        for future in as_completed(definition_futures):
            future.result()

    glossary_id = export_context["glossary"]

    if term_links:
        relsyn_filename = f"entrylinks_related_synonyms_{glossary_id}.json"
        relsyn_path = os.path.join(output_dir, relsyn_filename)
        logger.debug(f"Writing term-term links to file: {relsyn_path}")
        write_links_to_file(term_links, relsyn_path, glossary_id, mode="w")

    if definition_links_by_ple:
        for ple, links in definition_links_by_ple.items():
            filename = f"entrylinks_definition_{glossary_id}_{ple}.json"
            output_path = os.path.join(output_dir, filename)
            logger.debug(f"Writing definition links to file: {output_path}")
            write_links_to_file(links, output_path, glossary_id)


def run_export(glossary_url: str, user_project: str, org_ids: list[str]) -> bool:
    """
    Executes the full export for a single glossary URL. This is the main entry point.
    """ 
    logger.info(f"Starting run_export for glossary_url: {glossary_url}, user_project: {user_project}, org_ids: {org_ids}")
    extracted = utils.parse_glossary_url(glossary_url)
    logger.debug(f"Parsed glossary_url: {extracted}")
    if not extracted:
        logger.error(f"Could not parse required IDs from URL: {glossary_url}")
        return False

    project = extracted["project"]
    location = extracted["location"]
    glossary = extracted["glossary"]
    entry_group = extracted["entry_group"]

    glossary_normalized = utils.normalize_glossary_id(glossary)
    logger.info(f"Starting export for glossary: {glossary_normalized}")

    if org_ids:
        org_ids = org_ids
        logger.debug(f"Using provided org_ids: {org_ids}")
    else:
        logger.debug("Fetching organization IDs using gcloud.")
        result = subprocess.run(
            ["gcloud", "organizations", "list", "--format=value(ID)"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        logger.debug(f"gcloud organizations list result: stdout={result.stdout}, stderr={result.stderr}")
        if result.stderr:
            logger.error("Error fetching organization IDs: %s", result.stderr)
        org_ids_list = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
        if not org_ids_list:
            logger.error(
                "No organization IDs found. Please ensure you have permission to list organizations "
                "or pass the organization ids in the org_ids parameter. For example org_ids=['123','456']"
            )
            return False
        org_ids = org_ids_list
        logger.debug(f"Fetched org_ids: {org_ids}")

    export_context = {
        "project": project,
        "location": location,
        "glossary": glossary,
        "dataplex_entry_group": f"projects/{project}/locations/{GLOSSARY_EXPORT_LOCATION}/entryGroups/@dataplex",
        "project_number": "655216118709", 
        "org_ids": org_ids
    }
    logger.debug(f"Export context: {export_context}")

    glossaries_folder, entrylinks_folder = ensure_output_folders_exist()
    glossary_output_path = os.path.join(glossaries_folder, f"glossary_{glossary}.json")
    glossary_output_path = os.path.join(glossaries_folder, f"glossary_{glossary_normalized}.json")
    logger.debug(f"Glossary output path: {glossary_output_path}")

    logger.debug("Fetching all entries for glossary.")
    entries = utils.fetch_entries(user_project, project, location, entry_group)
    logger.debug(f"Fetched entries: {entries}")
    relationships_data = utils.fetch_all_relationships(entries, user_project, project)
    logger.debug(f"Fetched relationships data: {relationships_data}")
    
    map_entry_id_to_entry_type = {get_entry_id(e["name"]): e.get("entryType", "") for e in entries}
    logger.debug(f"Entry ID to type mapping: {map_entry_id_to_entry_type}")
    parent_mapping = build_parent_mapping(entries, relationships_data)
    logger.debug(f"Parent mapping: {parent_mapping}")

    export_glossary_entries_json(entries, glossary_output_path, parent_mapping, map_entry_id_to_entry_type, export_context)
    logger.debug(f"Glossary entries exported to: {glossary_output_path}")
    utils.replace_with_new_glossary_id(glossary_output_path, glossary)
    logger.debug(f"Glossary id replaced in file: {glossary_output_path}")

    export_combined_entry_links_json(entries, relationships_data, user_project, entrylinks_folder, export_context)
    logger.debug(f"Entry links exported to folder: {entrylinks_folder}")

    utils.create_glossary(user_project, project, location, entry_group, glossary)
    logger.debug(f"Ensured glossary exists in Dataplex: {glossary}")

    logger.info(f"Successfully finished export for glossary: '{glossary}'")
    return True
