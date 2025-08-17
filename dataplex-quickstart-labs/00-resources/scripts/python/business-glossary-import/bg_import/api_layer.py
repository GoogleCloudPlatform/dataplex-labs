"""
Handles all direct API interactions with Google Cloud services (DAO).
This module focuses on building URLs and calling api_call_utils.fetch_api_response.
Parsing/ID helpers are delegated to migration_utils.
"""

import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

import requests
import time
import api_call_utils
import logging_utils
from migration_utils import (
    build_entry_key,
    clean_linked_resource,
    extract_entry_parts,
    extract_project_location_from_relative,
    get_entry_id,
    update_relative_resource_name,
    normalize_id
)

logger = logging_utils.get_logger()

DATACATALOG_BASE_URL = "https://datacatalog.googleapis.com/v2"
DATAPLEX_BASE_URL = "https://dataplex.googleapis.com/v1"
PAGE_SIZE = 1000
MAX_WORKERS = 20
_entrygroup_to_glossaryid_map: Dict[str, str] = {}


def _fetch_entry_from_catalog(key: str, entry_id: str, user_project: str) -> dict:
    """Fetches entry details from Data Catalog for a single entry ID."""
    url = f"{DATACATALOG_BASE_URL}/{key}/entries/{entry_id}"
    return api_call_utils.fetch_api_response(requests.get, url, user_project)


def fetch_glossary_id(entry_full_name: str, user_project: str) -> Optional[str]:
    """
    Returns the normalized glossary ID for a given entry, fetching from Data Catalog if needed.
    """
    parts = extract_entry_parts(entry_full_name)
    if not parts:
        return None

    project_loc, entry_group_id, entry_id = parts
    key = build_entry_key(project_loc, entry_group_id)

    cached_id = get_cached_glossary_id(key)
    if cached_id:
        return cached_id

    response = _fetch_entry_from_catalog(key, entry_id, user_project)
    if _has_fetch_error(response, entry_full_name):
        return None

    glossary_id = extract_glossary_id_from_response(response)
    return normalize_and_cache_glossary_id(key, glossary_id) if glossary_id else None


def get_cached_glossary_id(key: str) -> Optional[str]:
    """Return glossary ID from cache if available."""
    return _entrygroup_to_glossaryid_map.get(key)


def _has_fetch_error(response: dict, entry_full_name: str) -> bool:
    """Check if response contains error and log if needed."""
    if response.get("error_msg"):
        logger.warning(
            f"Could not fetch glossary details for entry {entry_full_name}: {response['error_msg']}"
        )
        return True
    return False


def extract_glossary_id_from_response(response: dict) -> Optional[str]:
    """Extract glossary ID from Data Catalog response JSON."""
    glossary_name = response.get("json", {}).get("name", "")
    return get_entry_id(glossary_name) if glossary_name else None


def normalize_and_cache_glossary_id(key: str, glossary_name: str) -> str:
    """Normalize glossary id and cache it for reuse."""
    normalized = normalize_id(glossary_name)
    _entrygroup_to_glossaryid_map[key] = normalized
    return normalized


def _build_entries_url(config: Dict[str, Any]) -> str:
    """Builds the base URL for fetching entries."""
    return (
        f"{DATACATALOG_BASE_URL}/projects/{config['project']}/locations/"
        f"{config['location']}/entryGroups/{config['group']}/entries"
        f"?view=FULL&pageSize={PAGE_SIZE}"
    )


def _fetch_entries_page(url: str, user_project: str) -> dict:
    """Fetches a single page of entries from Data Catalog."""
    return api_call_utils.fetch_api_response(requests.get, url, user_project)


def fetch_entries(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Fetches all entries for a given entry group, handling pagination."""
    entries, page_token = [], None
    base_url = _build_entries_url(config)

    while True:
        url = f"{base_url}&pageToken={page_token}" if page_token else base_url
        response = _fetch_entries_page(url, config["user_project"])
        if response["error_msg"]:
            logger.error(f"Cannot fetch entries, which is a fatal error: {response['error_msg']}")
            sys.exit(1)
        entries.extend(response.get("json", {}).get("entries", []))
        page_token = response.get("json", {}).get("nextPageToken")
        if not page_token:
            break
    logger.debug(f"fetch_entries input: config={config} | output: {entries}")
    return entries


def _build_relationships_url(entry_name: str) -> str:
    """Constructs URL for fetching entry relationships."""
    return f"{DATACATALOG_BASE_URL}/{entry_name}/relationships?view=FULL&pageSize={PAGE_SIZE}"


def fetch_relationships_for_entry(entry_name: str, user_project: str) -> List[Dict[str, Any]]:
    """Fetches all relationships for a single entry, handling pagination."""
    relationships, page_token = [], None
    base_url = _build_relationships_url(entry_name)
    while True:
        url = f"{base_url}&pageToken={page_token}" if page_token else base_url
        response = api_call_utils.fetch_api_response(requests.get, url, user_project)
        if response["error_msg"]:
            logger.warning(f"Could not fetch relationships page for {entry_name}: {response['error_msg']}")
            break
        relationships.extend(response.get("json", {}).get("relationships", []))
        page_token = response.get("json", {}).get("nextPageToken")
        if not page_token:
            break
    logger.debug(f"fetch_relationships_for_entry input: entry_name={entry_name}, user_project={user_project} | output: {relationships}")
    return relationships


def fetch_all_relationships(entries: List[Dict], user_project: str) -> Dict[str, List[Dict]]:
    """Fetches relationships for all entries concurrently."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(fetch_relationships_for_entry, e["name"], user_project): e["name"]
            for e in entries
        }
        return {future_map[f]: f.result() for f in as_completed(future_map)}


def _build_search_body(config: dict, query: str, page_token: Optional[str] = None) -> dict:
    """Builds request body for Data Catalog search."""
    body = {
        "orderBy": "relevance",
        "pageSize": PAGE_SIZE,
        "query": query,
        "scope": {"includeOrgIds": config.get("org_ids", [])},
    }
    if page_token:
        body["pageToken"] = page_token
    return body


def search_catalog(config: dict, query: str) -> list:
    """Searches Data Catalog for entries matching query, handling pagination."""
    results, page_token = [], None
    url = "https://datacatalog.googleapis.com/v1/catalog:search"
    while True:
        body = _build_search_body(config, query, page_token)
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
        if not page_token:
            break
    logger.debug(f"search_catalog input: config={config}, query={query} | output: {results}")
    return results


def _normalize_linked_resource(search_result: dict) -> Optional[tuple]:
    """Normalizes linkedResource and relativeResourceName from a search result."""
    linked_resource = clean_linked_resource(search_result.get("linkedResource", ""))
    relative_resource_name = search_result.get("relativeResourceName", "")
    if not linked_resource or not relative_resource_name:
        return None
    new_entry_id = re.sub(r"^/+", "", linked_resource)
    relative_resource_name_v2 = update_relative_resource_name(relative_resource_name, new_entry_id)
    return linked_resource, relative_resource_name_v2


def lookup_dataplex_entry(config: dict, search_result: Dict[str, Any]) -> bool:
    """Checks if a Dataplex entry exists for a search result."""
    normalized = _normalize_linked_resource(search_result)
    if not normalized:
        return False
    linked_resource, relative_resource_name_v2 = normalized
    proj_loc = extract_project_location_from_relative(relative_resource_name_v2)
    if not proj_loc:
        logger.warning(f"Could not parse project/location from FQN for Dataplex lookup: {relative_resource_name_v2}")
        return False
    project, location = proj_loc
    project_location = f"projects/{project}/locations/{location}"
    url = f"{DATAPLEX_BASE_URL}/{project_location}:lookupEntry?entry={relative_resource_name_v2}"
    logger.debug(f"Fetching entry for linked resource: {linked_resource} from URL: {url}")
    response = api_call_utils.fetch_api_response(requests.get, url, config["user_project"])
    logger.debug(f"Entry check response: {response}")
    if not response.get("json") or response.get("error_msg"):
        logger.warning(f"Dataplex entry not found for linked resource: {linked_resource}")
        return False
    return True


def _fetch_glossary_display_name(config: Dict[str, Any]) -> str:
    """Fetches glossary display name from Data Catalog."""
    catalog_url = (
        f"{DATACATALOG_BASE_URL}/projects/{config['project']}/locations/"
        f"{config['location']}/entryGroups/{config['group']}/entries/{config['glossary']}"
    )
    dc_response = api_call_utils.fetch_api_response(requests.get, catalog_url, config["user_project"])
    if dc_response["error_msg"]:
        logger.error(f"Failed to get original glossary details: {dc_response['error_msg']}")
        sys.exit(1)
    return dc_response.get("json", {}).get("displayName", config["normalized_glossary"])


def _post_glossary(config: Dict[str, Any], display_name: str) -> dict:
    """Sends request to create glossary in Dataplex."""
    post_url = f"{DATAPLEX_BASE_URL}/projects/{config['project']}/locations/global/glossaries?glossary_id={config['normalized_glossary']}"
    body = {"displayName": display_name}
    return api_call_utils.fetch_api_response(requests.post, post_url, config["user_project"], body)


def create_glossary(config: Dict[str, Any]) -> None:
    """Ensures glossary exists in Dataplex, creating it if necessary."""
    logger.info("Step 5: Ensuring destination glossary exists in Dataplex...")
    display_name = _fetch_glossary_display_name(config)
    dp_response = _post_glossary(config, display_name)
    time.sleep(60)
    if dp_response.get("json", {}).get("error", {}).get("status") == "ALREADY_EXISTS":
        logger.info(f"Dataplex glossary '{config['normalized_glossary']}' already exists.")
    elif dp_response["error_msg"]:
        logger.error(f"Error creating Dataplex glossary: {dp_response['error_msg']}")
    else:
        logger.info(f"Successfully initiated creation of glossary '{config['normalized_glossary']}'.")
