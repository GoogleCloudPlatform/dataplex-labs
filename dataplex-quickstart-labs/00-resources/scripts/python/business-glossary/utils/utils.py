"""Utility functions for the Business Glossary import tool.
"""

import argparse
import os
import sys

from . import logging_utils
from typing import Any, List, Dict
from . import api_call_utils
import requests

import re
import csv
import os
import requests
import sys
from typing import Any, List, Dict

import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from datetime import datetime


logger = logging_utils.get_logger()
DATACATALOG_BASE_URL = "https://datacatalog.googleapis.com/v2"
DATAPLEX_BASE_URL = "https://dataplex.googleapis.com/v1"
PAGE_SIZE = 1000
MAX_WORKERS = 20

def access_token_exists() -> bool:
  return bool(os.environ.get("GCLOUD_ACCESS_TOKEN"))

def get_arguments() -> argparse.Namespace:
  """Gets arguments for the program.

  Returns:
    Namespace object containing the program arguments.
  """
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawTextHelpFormatter
  )
  configure_argument_parser(parser)
  return parser.parse_args()

def get_migration_arguments(argv=None) -> argparse.Namespace:
    """Gets arguments for the migration program."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    configure_migration_argument_parser(parser)
    return parser.parse_args(argv)

def configure_migration_argument_parser(parser: argparse.ArgumentParser) -> None:
    """Defines flags and parses arguments related to migration."""
    parser.add_argument(
        "--project",
        help="Google Cloud Project ID that has the V1 glossaries to be migrated.",
        metavar="[Project ID]",
        type=str,
        required=True
    )
    parser.add_argument(
        "--buckets",
        help="Comma-separated list of GCS bucket names for staging the import.",
        type=lambda s: [item.strip() for item in s.split(",") if item.strip()],
        required=True,
        metavar="[bucket-1,bucket-2,...]"
    )
    parser.add_argument(
        "--orgIds",
        type=parse_id_list,  
        default=[],
        help="A list of org IDs enclosed in brackets. Delimiters can be spaces or commas. Example: --org-ids=\"[id1,id2 id3]\""
    )
    log_filename = f"logs_{datetime.now().strftime('%Y-%b-%d_%I-%M%p')}.txt"
    parser.add_argument(
        "--debugging",
        action="store_true",
        help=f"If set, enables detailed logging to {log_filename} in the current directory."
    )

def parse_id_list(value):
        if not isinstance(value, str):
            raise argparse.ArgumentTypeError(f"Invalid list format: '{value}'. --org-ids=\"123,789\".")
        items = [item.strip() for item in value.split(',') if item.strip()]
        return items

def parse_glossary_url(url: str) -> dict:
    pattern = (
        r"projects/(?P<project>[^/]+)/locations/(?P<location>[^/]+)/"
        r"entryGroups/(?P<entry_group>[^/]+)/entries/(?P<glossary>[^/?#]+)"
    )
    match = re.search(pattern, url)
    if not match:
        raise ValueError("Invalid glossary URL provided. It must contain the pattern: "
                         "projects/.../locations/.../entryGroups/.../entries/...")
    return match.groupdict()

def fetch_relationships(entry_name: str, user_project: str) -> List[Dict[str, Any]]:
    """Fetches relationships for a specific entry from the Data Catalog.

    Args:
        entry_name: The full resource name of the entry.
        project: The Google Cloud Project ID.

    Returns:
        A list of dictionaries containing the relationships.
    """
    fetch_relationships_url = (
        DATACATALOG_BASE_URL + f"/{entry_name}/relationships?view=FULL"
    )

    response = api_call_utils.fetch_api_response(
        requests.get, fetch_relationships_url, user_project
    )
    if response["error_msg"]:
        logger.error(f"Error fetching relationships: {response['error_msg']}")
        sys.exit(1)
    return response["json"].get("relationships", [])

def fetch_all_relationships(
    entries: List[Dict[str, Any]], user_project: str,project: str, max_workers: int = MAX_WORKERS
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetches relationships for all entries concurrently, processing in batches."""
    relationships_data = {}
    chunk_size = max_workers
    num_batches = math.ceil(len(entries) / chunk_size)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for batch in range(num_batches):
            start = batch * chunk_size
            end = start + chunk_size
            entries_batch = entries[start:end]
            future_to_entry = {
                executor.submit(fetch_relationships, entry["name"], user_project): entry[
                    "name"
                ]
                for entry in entries_batch
            }

            for future in as_completed(future_to_entry):
                entry_name = future_to_entry[future]
                try:
                    relationships_data[entry_name] = future.result()
                except Exception as exc:
                    logger.error(
                        f"Error fetching relationships for {entry_name}: {exc}"
                    )

    return relationships_data


def fetch_entries(
    user_project: str, project: str, location: str, entry_group: str
) -> List[Dict[str, Any]]:
    """Fetches all entries in the glossary.

    Args:
        project: The Google Cloud Project ID.
        location: The location of the glossary.
        entry_group: The entry group of the glossary.

    Returns:
        A list of dictionaries containing the entries.
    """
    entries = []
    get_full_entry_url = (
        DATACATALOG_BASE_URL
        + f"/projects/{project}/locations/{location}/entryGroups/{entry_group}/entries?view=FULL&pageSize={PAGE_SIZE}"
    )
    response = api_call_utils.fetch_api_response(
        requests.get, get_full_entry_url, user_project
    )

    if response["error_msg"]:
        logger.error(
        f"Can't proceed with export. Details: {response['error_msg']}"
        )
        logger.error(get_full_entry_url)
        sys.exit(1)

    with ThreadPoolExecutor() as executor:
        futures = []
        page_token = response["json"].get("nextPageToken", None)
        
        if "entries" in response["json"]:
            entries.extend(response["json"]["entries"])

        while page_token:
            endpoint_url = f"{get_full_entry_url}&pageToken={page_token}"
            
            future = executor.submit(
                api_call_utils.fetch_api_response, requests.get, endpoint_url, user_project
            )
            futures.append(future)
            response_page = future.result()
            
            if response_page["error_msg"]:
                logger.error(f"Error fetching paginated entries: {response_page['error_msg']}")
                sys.exit(1) 
            
            if "entries" in response_page["json"]:
                entries.extend(response_page["json"]["entries"])
            page_token = response_page["json"].get("nextPageToken", None)

        return entries

def normalize_glossary_id(glossary: str) -> str:
    """Converts a string to a valid Dataplex glossary_id (lowercase, numbers, hyphens only)."""
    glossary_id = glossary.lower()
    glossary_id = re.sub(r"[ _]", "-", glossary_id)
    glossary_id = re.sub(r"[^a-z0-9\-]", "-", glossary_id)
    glossary_id = re.sub(r"-+", "-", glossary_id)
    glossary_id = glossary_id.strip("-")
    return glossary_id

def replace_with_new_glossary_id(file_path, glossary_id: str) -> None:
    new_glossary_id = normalize_glossary_id(glossary_id)
    pattern = re.compile(rf"glossaries/{re.escape(glossary_id)}")
    with open(file_path, "r") as file:
        content = file.read()
    new_content = pattern.sub(f"glossaries/{new_glossary_id}", content)
    with open(file_path, "w") as file:
        file.write(new_content)

def create_glossary(
    user_project: str,
    project: str,
    location: str,
    entry_group: str,
    glossary: str
) -> None:
    """Creates a new Dataplex glossary."""
    catalog_url = (
        f"{DATACATALOG_BASE_URL}/projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{glossary}"
    )
    glossary_id = normalize_glossary_id(glossary)
    dataplex_post_url = f"{DATAPLEX_BASE_URL}/projects/{project}/locations/global/glossaries?glossary_id={glossary_id}"
    dataplex_get_url = f"{DATAPLEX_BASE_URL}/projects/{project}/locations/global/glossaries/{glossary_id}"

    datacatalog_response = api_call_utils.fetch_api_response(
        requests.get, catalog_url, user_project
    )
    if datacatalog_response["error_msg"]:
        logger.warning(f"Failed to fetch Data Catalog entry:\n  {datacatalog_response['error_msg']}")
        sys.exit(1)

    display_name = datacatalog_response["json"].get("displayName", "")

    request_body = {"displayName": display_name}
    dp_resp = api_call_utils.fetch_api_response(
        requests.post, dataplex_post_url, user_project, request_body
    )
    if dp_resp["error_msg"]:
        logger.warning(f"{dp_resp['error_msg']}")
        return

    time.sleep(30)
    glossary_creation_response = api_call_utils.fetch_api_response(
        requests.get, dataplex_get_url, user_project
    )
    if glossary_creation_response["error_msg"]:
        logger.warning(
            f"Error occurred while creating the glossary {glossary_creation_response['error_msg']}. Please try again manually."
        )
        sys.exit(1)

    logger.info(f"Dataplex glossary created successfully: {glossary_creation_response['json'].get('name', '')}")

def get_project_number(project_id: str, user_project) -> str:
    """Fetches the project number from the project ID."""
    url = f"https://cloudresourcemanager.googleapis.com/v3/projects/{project_id}"
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    if response["error_msg"]:
        logger.error(f"Failed to fetch project number: {response['error_msg']}")
        sys.exit(1)
    # Extract project number from the 'name' field: "projects/862195633006"
    name = response["json"].get("name", "")
    match = re.search(r"projects/(\d+)", name)
    if match:
        return match.group(1)
    else:
        logger.error("Project number not found in response.")
        sys.exit(1)

def get_project_id(project:str, user_project:str) -> str:
    """Fetches the project ID from the project number."""
    url = f"https://cloudresourcemanager.googleapis.com/v3/projects/{project}"
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    if response["error_msg"]:
        logger.error(f"Failed to fetch project ID: {response['error_msg']}")
        sys.exit(1)
    return response["json"].get("projectId", "")


def discover_glossaries(project_id: str, user_project: str) -> list[str]:
    """Uses the Catalog Search API to find all v1 glossaries in a project."""
    search_url = "https://datacatalog.googleapis.com/v1/catalog:search"
    query = "type=glossary"
    request_body = {
        "query": query,
        "scope": {
            "includeProjectIds": [project_id]
        },
        "pageSize": 1000,
    }

    response = api_call_utils.fetch_api_response(
        requests.post, search_url, user_project, request_body
    )

    if response.get("error_msg"):
        logger.error(f"Failed to search for glossaries: {response['error_msg']}")
        return []

    results = response.get("json", {}).get("results", [])
    if not results:
        logger.warning(f"No datacatalog glossaries found in project '{project_id}'.")
        return []

    # Construct the full glossary URL from the 'linkedResource' field
    glossary_urls = [
        f"https:{res['linkedResource']}"
        for res in results
        if res.get("searchResultSubtype") == "entry.glossary"
    ]

    logger.info(f"Found {len(glossary_urls)} glossaries to migrate.")
    return glossary_urls