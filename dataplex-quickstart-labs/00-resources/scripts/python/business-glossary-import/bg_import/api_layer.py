# api_layer.py
"""
Handles all direct API interactions with Google Cloud services (DAO).
"""
import requests
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

import api_call_utils
import logging_utils
from migration_utils import normalize_id

logger = logging_utils.get_logger()
DATACATALOG_BASE_URL = "https://datacatalog.googleapis.com/v2"
DATAPLEX_BASE_URL = "https://dataplex.googleapis.com/v1"
PAGE_SIZE = 1000
MAX_WORKERS = 20
entrygroup_to_glossaryid_map = {}

def fetch_glossary_id(entry_full_name: str, user_project: str) -> str:
    from data_transformer import get_entry_id
    match = re.search(r"(projects/[^/]+/locations/[^/]+)/entryGroups/([^/]+)/entries/([^/]+)", entry_full_name)
    if not match: return None

    project_loc, entry_group_id, entry_id = match.groups()
    key = f"{project_loc}/entryGroups/{entry_group_id}"
    if key in entrygroup_to_glossaryid_map:
        return entrygroup_to_glossaryid_map[key]

    url = f"https://datacatalog.googleapis.com/v2/{key}/entries/{entry_id}"
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    
    if response.get("error_msg"):
        logger.warning(f"Could not fetch glossary details for entry {entry_full_name}: {response['error_msg']}")
        return None
        
    glossary_name = response.get("json", {}).get("name", "")
    glossary_id = get_entry_id(glossary_name)
    if glossary_id:
        normalized_id = normalize_id(glossary_id)
        entrygroup_to_glossaryid_map[key] = normalized_id
        return normalized_id
    return None

def fetch_entries(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries, page_token = [], None
    base_url = (f"{DATACATALOG_BASE_URL}/projects/{config['project']}/locations/"
                f"{config['location']}/entryGroups/{config['group']}/entries"
                f"?view=FULL&pageSize={PAGE_SIZE}")
    while True:
        url = f"{base_url}&pageToken={page_token}" if page_token else base_url
        response = api_call_utils.fetch_api_response(requests.get, url, config['user_project'])
        if response["error_msg"]:
            logger.error(f"Cannot fetch entries, which is a fatal error: {response['error_msg']}")
            sys.exit(1)
        entries.extend(response.get("json", {}).get("entries", []))
        page_token = response.get("json", {}).get("nextPageToken")
        if not page_token: break
    return entries

def fetch_relationships_for_entry(entry_name: str, user_project: str) -> List[Dict[str, Any]]:
    """Fetches all relationships for a single entry, handling pagination."""
    relationships, page_token = [], None
    base_url = f"{DATACATALOG_BASE_URL}/{entry_name}/relationships?view=FULL&pageSize={PAGE_SIZE}"
    while True:
        url = f"{base_url}&pageToken={page_token}" if page_token else base_url
        response = api_call_utils.fetch_api_response(requests.get, url, user_project)
        
        if response["error_msg"]:
            logger.warning(f"Could not fetch relationships page for {entry_name}: {response['error_msg']}")
            break
            
        relationships.extend(response.get("json", {}).get("relationships", []))
        page_token = response.get("json", {}).get("nextPageToken")
        if not page_token: break
    return relationships

def fetch_all_relationships(entries: List[Dict], user_project: str) -> Dict[str, List[Dict]]:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(fetch_relationships_for_entry, e["name"], user_project): e["name"] for e in entries}
        return {future_map[f]: f.result() for f in as_completed(future_map)}

def search_catalog(config: dict, query: str) -> list:
    results, page_token = [], None
    url = "https://datacatalog.googleapis.com/v1/catalog:search"
    body = {"orderBy": "relevance", "pageSize": 1000, "query": query, "scope": {"includeOrgIds": config.get("org_ids", [])}}
    
    while True:
        if page_token:
            body["pageToken"] = page_token
            
        response = api_call_utils.fetch_api_response(requests.post, url, config["user_project"], body)
        
        if response["error_msg"]:
            logger.warning(f"Catalog search page for query '{query}' failed: {response['error_msg']}")
            break
            
        page_results = response.get("json", {}).get("results", [])
        if not page_results and page_token:
            logger.debug("Search API returned a page token but no results. Ending pagination.")
            break
            
        results.extend(page_results)
        page_token = response.get("json", {}).get("nextPageToken")
        if not page_token: break
        
    return results

def lookup_dataplex_entry(config: dict, search_result: Dict[str, Any]) -> bool:
    """Replicates original logic for looking up a Dataplex entry from a search result."""
    linked_resource = search_result.get("linkedResource", "").lstrip("/")
    relative_resource_name = search_result.get("relativeResourceName", "")

    if not linked_resource or not relative_resource_name:
        return False

    new_entry_id = re.sub(r"^/+", "", linked_resource)
    relative_resource_name_v2 = re.sub(r"entries/[^/]+$", f"entries/{new_entry_id}", relative_resource_name)

    match = re.match(r"projects/([^/]+)/locations/([^/]+)/", relative_resource_name_v2)
    if not match:
        logger.warning(f"Could not parse project/location from FQN for Dataplex lookup: {relative_resource_name_v2}")
        return False

    project_location = f"projects/{match.group(1)}/locations/{match.group(2)}"
    url = f"https://dataplex.googleapis.com/v1/{project_location}:lookupEntry?entry={relative_resource_name_v2}"
    
    logger.debug(f"Fetching entry for linked resource: {linked_resource} from URL: {url}")
    response = api_call_utils.fetch_api_response(requests.get, url, config["user_project"])
    logger.debug(f"Entry check response: {response}")

    if not response.get("json") or response.get("error_msg"):
        logger.warning(f"Dataplex entry not found for linked resource: {linked_resource}")
        return False
        
    return True

def create_glossary(config: Dict[str, Any]) -> None:
    user_project, project, location, group = (
        config["user_project"], config["project"], config["location"], config["group"]
    )
    glossary_id = config["normalized_glossary"]
    original_glossary_name = config["glossary"]
    
    catalog_url = f"{DATACATALOG_BASE_URL}/projects/{project}/locations/{location}/entryGroups/{group}/entries/{original_glossary_name}"
    dc_response = api_call_utils.fetch_api_response(requests.get, catalog_url, user_project)
    if dc_response["error_msg"]:
        logger.error(f"Failed to get original glossary details: {dc_response['error_msg']}")
        sys.exit(1)

    display_name = dc_response.get("json", {}).get("displayName", glossary_id)
    
    post_url = f"{DATAPLEX_BASE_URL}/projects/{project}/locations/global/glossaries?glossary_id={glossary_id}"
    body = {"displayName": display_name}
    dp_response = api_call_utils.fetch_api_response(requests.post, post_url, user_project, body)
    
    if dp_response.get("json", {}).get("error", {}).get("status") == "ALREADY_EXISTS":
        logger.info(f"Dataplex glossary '{glossary_id}' already exists. Skipping creation.")
    elif dp_response["error_msg"]:
        logger.error(f"Error creating Dataplex glossary: {dp_response['error_msg']}")
    else:
        logger.info(f"Successfully initiated creation of Dataplex glossary '{glossary_id}'.")