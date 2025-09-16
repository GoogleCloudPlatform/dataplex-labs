"""
Handles all direct API interactions with Google Cloud services (DAO).
This module focuses on building URLs and calling api_call_utils.fetch_api_response.
Parsing/ID helpers are delegated to migration_utils.
"""

import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
import requests
import time
import api_call_utils
import logging_utils
from models import *
from migration_utils import *
from object_converters import *
from constants import (DATACATALOG_BASE_URL, DATAPLEX_BASE_URL, SEARCH_BASE_URL, PAGE_SIZE, MAX_WORKERS)

logger = logging_utils.get_logger()

def _build_dc_entry_url(context: Context) -> str:
    """Builds the base URL for fetching entries."""
    return (
        f"{DATACATALOG_BASE_URL}/projects/{context.project}/locations/"
        f"{context.location_id}/entryGroups/{context.entry_group_id}/entries"
        f"?view=FULL&pageSize={PAGE_SIZE}"
    )


def _build_dc_relationship_url(dc_entry_name: str, view: str) -> str:
    """Constructs URL for fetching entry relationships."""
    return f"{DATACATALOG_BASE_URL}/{dc_entry_name}/relationships?view={view}&pageSize={PAGE_SIZE}"


def _fetch_glossary_taxonomy_entries_page(url: str, user_project: str) -> dict:
    """Fetches a single page of entries from Data Catalog."""
    return api_call_utils.fetch_api_response(requests.get, url, user_project)

def _build_search_body(context: Context, query: str, page_token: Optional[str] = None) -> dict:
    """Builds request body for Data Catalog search."""
    body = {
        "orderBy": "relevance",
        "pageSize": PAGE_SIZE,
        "query": query,
        "scope": {"includeOrgIds": context.org_ids},
    }
    if page_token:
        body["pageToken"] = page_token
    return body


def _build_glossary_search_request(project_id: str) -> dict:
    """Builds the request body for searching glossaries via Catalog Search API."""
    return {
        "query": "type=glossary",
        "scope": {"includeProjectIds": [project_id]},
        "pageSize": 1000,
    }


def _extract_glossary_urls(results: list[dict]) -> list[str]:
    """Extracts the full glossary URLs from search results."""
    return [
        f"https:{res['linkedResource']}"
        for res in results
        if res.get("searchResultSubtype") == "entry.glossary"
    ]



def _build_dataplex_lookup_entry_url(search_entry_result: SearchEntryResult) -> str:
  """Constructs the Dataplex lookupEntry API URL by replacing the entryId from the relativeResourceName with id from linkedResource."""
  linked_resource = search_entry_result.linkedResource
  relative_resource_name = search_entry_result.relativeResourceName
  new_entry_id = re.sub(r"^/+", "", linked_resource)
  relative_resource_name_v2 = re.sub(r"entries/[^/]+$", f"entries/{new_entry_id}", relative_resource_name)

  requested_project_name = ""
  match = re.match(r"projects/([^/]+)/locations/([^/]+)/", relative_resource_name_v2)
  if match:
    project_id_from_relative_resource_name_v2 = match.group(1)
    location_from_relative_resource_name_v2 = match.group(2)
    requested_project_name = f"projects/{project_id_from_relative_resource_name_v2}/locations/{location_from_relative_resource_name_v2}"

  return f"https://dataplex.googleapis.com/v1/{requested_project_name}:lookupEntry?entry={relative_resource_name_v2}"


def _fetch_glossary_display_name(context: Context) -> str:
    """Fetches glossary display name from Data Catalog."""
    catalog_url = (
        f"{DATACATALOG_BASE_URL}/projects/{context.project}/locations/"
        f"{context.location_id}/entryGroups/{context.entry_group_id}/entries/{context.dc_glossary_id}"
    )
    api_response = api_call_utils.fetch_api_response(requests.get, catalog_url, context.user_project)
    if api_response["error_msg"]:
        logger.error(f"Failed to get original glossary details: {api_response['error_msg']}")
        sys.exit(1)
    return api_response.get("json", {}).get("displayName", context.dp_glossary_id)

def _get_dataplex_glossary(context):
    get_url = f"{DATAPLEX_BASE_URL}/projects/{context.project}/locations/global/glossaries/{context.dp_glossary_id}"
    api_response = api_call_utils.fetch_api_response(requests.get, get_url, context.user_project)
    return api_response

def _post_dataplex_glossary(context: Context, display_name: str) -> dict:
    """Sends request to create glossary in Dataplex."""
    dataplex_post_url = f"{DATAPLEX_BASE_URL}/projects/{context.project}/locations/global/glossaries?glossary_id={context.dp_glossary_id}"
    new_display_name = trim_spaces_in_display_name(display_name)
    request_body = {"displayName": new_display_name}
    return api_call_utils.fetch_api_response(requests.post, dataplex_post_url, context.user_project, request_body)

def _build_project_url(project_id: str) -> str:
    """Builds the Cloud Resource Manager project URL."""
    return f"https://cloudresourcemanager.googleapis.com/v3/projects/{project_id}"


def _fetch_project_info(project_id: str, user_project: str) -> dict:
    """Calls the Cloud Resource Manager API and returns the project JSON payload."""
    url = _build_project_url(project_id)
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    if response["error_msg"]:
        logger.error(f"Failed to fetch project info for '{project_id}': {response['error_msg']}")
        sys.exit(1)
    return response.get("json", {})


def _extract_project_number_from_info(project_info: dict) -> str:
    """Extracts the numeric project number from the project info 'name' field."""
    name = project_info.get("name", "")
    match = re.search(r"projects/(\d+)", name)
    if match:
        return match.group(1)
    logger.error("Project number not found in project info.")
    sys.exit(1)


def get_project_number(project_id: str, user_project: str) -> str:
    """Fetches the project number from the project ID (composed from smaller helpers)."""
    project_info = _fetch_project_info(project_id, user_project)
    return _extract_project_number_from_info(project_info)


def fetch_dc_glossary_taxonomy_entries(context: Context) -> List[GlossaryTaxonomyEntry]:
    """Fetches all entries for a given entry group, handling pagination."""
    dc_entries, page_token = [], None
    base_url = _build_dc_entry_url(context)

    while True:
        url = f"{base_url}&pageToken={page_token}" if page_token else base_url
        api_response = _fetch_glossary_taxonomy_entries_page(url, context.user_project)
        if api_response["error_msg"]:
            logger.error(f"Cannot fetch entries, which is a fatal error: {api_response['error_msg']}")
            sys.exit(1)
        dc_entries.extend(api_response.get("json", {}).get("entries", []))
        page_token = api_response.get("json", {}).get("nextPageToken")
        if not page_token:
            break
    logger.debug(f"fetch_entries input: context={context} | output: {dc_entries}")
    return convert_glossary_taxonomy_entries_to_objects(dc_entries)


def fetch_relationships_dc_glossary_term(dc_glossary_taxonomy_name: str, user_project: str) -> List[GlossaryTaxonomyRelationship]:
    """Fetches all relationships for a single entry, handling pagination."""
    dc_relationships, page_token = [], None
    base_url = _build_dc_relationship_url(dc_glossary_taxonomy_name, view="FULL")
    while True:
        url = f"{base_url}&pageToken={page_token}" if page_token else base_url
        api_response = api_call_utils.fetch_api_response(requests.get, url, user_project)
        if api_response["error_msg"]:
            logger.warning(f"Could not fetch relationships page for {dc_glossary_taxonomy_name}: {api_response['error_msg']}")
            break
        dc_relationships.extend(api_response.get("json", {}).get("relationships", []))
        page_token = api_response.get("json", {}).get("nextPageToken")
        if not page_token:
            break
    logger.debug(f"fetch_relationships_for_dc_glossary_term input: dc_glossary_taxonomy_name={dc_glossary_taxonomy_name}, user_project={user_project} | output: {dc_relationships}")
    return convert_glossary_taxonomy_relationships_to_objects(dc_relationships)

def fetch_relationships_dc_glossary_entry(dc_entry_name: str, user_project: str) -> List[DcEntryRelationship]:
    dc_relationships, page_token = [], None
    base_url = _build_dc_relationship_url(dc_entry_name, view="BASIC")
    while True:
        url = f"{base_url}&pageToken={page_token}" if page_token else base_url
        api_response = api_call_utils.fetch_api_response(requests.get, url, user_project)
        if api_response["error_msg"]:
            logger.warning(f"Could not fetch relationships page for {dc_entry_name}: {api_response['error_msg']}")
            break
        dc_relationships.extend(api_response.get("json", {}).get("relationships", []))
        page_token = api_response.get("json", {}).get("nextPageToken")
        if not page_token:
            break
    logger.debug(f"fetch_relationships_for_dc_glossary_entry input: dc_entry_name={dc_entry_name}, user_project={user_project} | output: {dc_relationships}")
    return convert_entry_relationships_to_objects(dc_relationships)

def fetch_dc_glossary_taxonomy_relationships(context: Context, dc_entries: List[GlossaryTaxonomyEntry]) -> Dict[str, List[GlossaryTaxonomyRelationship]]:
    """Fetches relationships for all entries concurrently."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        dc_relationships_future_map = {
            executor.submit(fetch_relationships_dc_glossary_term, dc_entry.name, context.user_project): dc_entry.name
            for dc_entry in dc_entries
        }
        return {dc_relationships_future_map[f]: f.result() for f in as_completed(dc_relationships_future_map)}


def search_dc_entries_for_term(context: Context, query: str) -> List[SearchEntryResult]:
    """Searches Data Catalog for entries matching query, handling pagination."""
    search_results, page_token = [], None
    while True:
        request_body = _build_search_body(context, query, page_token)
        api_response = api_call_utils.fetch_api_response(requests.post, SEARCH_BASE_URL, context.user_project, request_body)
        if api_response["error_msg"]:
            logger.warning(f"Catalog search page for query '{query}' failed: {api_response['error_msg']}")
            break
        current_page_results = api_response.get("json", {}).get("results", [])
        search_results.extend(current_page_results)
        page_token = api_response.get("json", {}).get("nextPageToken")
        if not page_token:
            break
    logger.debug(f"search_dc_entries_for_term input: context={context}, query={query} | output: {search_results}")
    return convert_entry_search_results_to_objects(search_results)


def lookup_dataplex_entry(context: Context, search_entry_result: SearchEntryResult) -> bool:
    """Checks if a Dataplex entry exists for a search result."""
    dataplex_lookup_entry_url = _build_dataplex_lookup_entry_url(search_entry_result)
    api_response = api_call_utils.fetch_api_response(requests.get, dataplex_lookup_entry_url, context.user_project)
    if not api_response.get("json") or api_response.get("error_msg"):
        logger.warning(f"Dataplex entry not found for data catalog entry: {search_entry_result.linkedResource}")
        return False
    return True



def create_dataplex_glossary(context: Context) -> None:
    """Create glossary in Dataplex."""
    display_name = _fetch_glossary_display_name(context)
    dataplex_api_response = _post_dataplex_glossary(context, display_name)

    if _is_glossary_already_exists(dataplex_api_response):
        logger.info(f"Glossary '{context.dp_glossary_id}' already exists in Dataplex.")
        return

    if _is_glossary_creation_successful(dataplex_api_response):
        _wait_for_glossary_creation()
    else:
        _handle_unexpected_dataplex_response(dataplex_api_response)
        return

    api_response = _get_dataplex_glossary(context)
    _handle_dataplex_glossary_response(api_response, context)


def _is_glossary_already_exists(api_response: dict) -> bool:
    error = api_response.get("json", {}).get("error")
    return bool(error and error.get("code") == 409 and error.get("status") == "ALREADY_EXISTS")

def _is_glossary_creation_successful(api_response: dict) -> bool:
    return api_response.get("error_msg") is None


def _wait_for_glossary_creation() -> None:
    logger.info("Glossary creation initiated. Waiting for operation to complete...")
    time.sleep(60)

def _handle_unexpected_dataplex_response(api_response: dict) -> None:
    logger.error(f"Unexpected response from Dataplex API: {api_response}")

def _handle_dataplex_glossary_response(api_response, context):
    """Handles the response from fetching a Dataplex glossary."""
    if api_response.get("error_msg"):
        logger.error(f"Failed to fetch Dataplex glossary: {api_response['error_msg']}")
        return

    if api_response.get("error_msg") is None and api_response.get("json"):
        logger.info(f"Dataplex glossary '{context.dp_glossary_id}' created successfully.")
        return
    else:
        logger.error(f"Unexpected response when fetching Dataplex glossary: {api_response}")
        return

#TODO: Add pagination handling
def discover_glossaries(project_id: str, user_project: str) -> list[str]:
    """Uses the Catalog Search API to find all v1 glossaries in a project."""
    request_body = _build_glossary_search_request(project_id)

    response = api_call_utils.fetch_api_response(
        requests.post, SEARCH_BASE_URL, user_project, request_body
    )

    if response.get("error_msg"):
        logger.error(f"Failed to search for glossaries: {response['error_msg']}")
        return []

    results = response.get("json", {}).get("results", [])
    if not results:
        logger.warning(f"No datacatalog glossaries found in project '{project_id}'.")
        return []

    glossary_urls = _extract_glossary_urls(results)
    logger.info(f"Found {len(glossary_urls)} glossaries to migrate.")
    return glossary_urls
