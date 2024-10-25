"""This script is used to export the data from a Data Catalog glossary to CSV files - one for categories and one for terms.

Categories CSV file contains the following columns:
  - category_display_name: The display name of the category.
  - description:  Plain text or rich text encoded as plain text description for
  the category
  - steward:  List of data stewards for the current category, with each steward
  separated by a comma
  - belongs_to_category: Display name of a category to which the category
  belongs


Terms CSV file contains the following columns:
  - term_display_name: Unique name for the entry term
  - description: Plain text or rich text encoded as plain text description for
  the term.
  - steward: List of data stewards for the current term, with each steward
  separated by a comma
  - tagged_assets: List of assets tagged with the term, with each asset
  separated by a comma (not implemented yet)
  - synonyms: List of terms that have a synonym relation with the current term,
  with each term separated by a comma
  - related_terms: List of terms that have a related-to relation with the
  current term, with each term separated by a comma
  - belongs_to_category: Display name of a category to which the term belong
"""


import csv
import os
import requests
import sys
from typing import Any, List, Dict
import glossary as dc_glossary
import glossary_identification
import api_call_utils
import logging_utils
import utils
import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

logger = logging_utils.get_logger()
DATACATALOG_BASE_URL = "https://datacatalog.googleapis.com/v2"
PAGE_SIZE = 1000
MAX_WORKERS = 20
csv.field_size_limit(sys.maxsize)


def fetch_entries(
    project: str, location: str, entry_group: str
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
        requests.get, get_full_entry_url, project
    )

    if response["error_msg"]:
        logger.error(
            "Can't proceed with export. Please select a valid glossary.",
            response["error_msg"],
        )
        sys.exit(1)

    with ThreadPoolExecutor() as executor:
        futures = []
        page_token = None

        while True:
            if page_token:
                endpoint_url = f"{get_full_entry_url}&pageToken={page_token}"
            else:
                endpoint_url = get_full_entry_url

            future = executor.submit(
                api_call_utils.fetch_api_response, requests.get, endpoint_url, project
            )
            futures.append(future)

            # Wait for the current future to complete and process its results
            for future in as_completed(futures):
                response = future.result()
                if response["error_msg"]:
                    raise ValueError(response["error_msg"])
                if "entries" in response["json"]:
                    entries.extend(response["json"]["entries"])
                page_token = response["json"].get("nextPageToken", None)
                if not page_token:
                    return entries
            # clear the futures list to avoid any memory build-up
            futures = []


def fetch_relationships(entry_name: str, project: str) -> List[Dict[str, Any]]:
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
        requests.get, fetch_relationships_url, project
    )
    if response["error_msg"]:
        raise ValueError(response["error_msg"])
    return response["json"].get("relationships", [])


def fetch_all_relationships(
    entries: List[Dict[str, Any]], project: str, max_workers: int = MAX_WORKERS
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
                executor.submit(fetch_relationships, entry["name"], project): entry[
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


def is_same_glossary(relationship_resource_name: str, term_name: str):
    num_components = 6
    components1 = relationship_resource_name.split("/")
    components2 = term_name.split("/")

    # Extract the entry group components from both strings
    entry_group_1 = components1[:num_components]
    entry_group_2 = components2[:num_components]

    return entry_group_1 == entry_group_2


import json


def process_entry(
    entry: Dict[str, Any],
    relationships_data: Dict[str, List[Dict[str, Any]]],
    project: str,
) -> Dict[str, Any]:
    """
    Processes a single glossary entry and returns data for either terms or categories.
    """
    entry_type = entry["entryType"]
    display_name = entry["displayName"]
    core_aspects = entry.get("coreAspects", {})

    business_context = core_aspects.get("business_context", {}).get("jsonContent", {})
    description = business_context.get("description", "")
    stewards = ", ".join(business_context.get("contacts", []))

    relationships = relationships_data.get(entry["name"], [])
    belongs_to_category = ""
    synonyms = ""
    related_terms = ""

    core_relationships = entry.get("coreRelationships", {})
    glossary_entry_name = ""
    if len(core_relationships):
        glossary_entry_name = core_relationships[0].get("destinationEntryName", {})

    for rel in relationships:
        if is_same_glossary(rel["destinationEntryName"], glossary_entry_name) == False:
            continue
        json.dumps(rel, indent=4)
        displayName = rel["destinationEntry"]["displayName"]
        if rel["relationshipType"] == "belongs_to":
            belongs_to_category = f'"{displayName}",'
        elif rel["relationshipType"] == "is_synonymous_to":
            synonyms += f'"{displayName}",'
        elif rel["relationshipType"] == "is_related_to":
            related_terms += f'"{displayName}",'

    synonyms = synonyms.rstrip(", ")
    related_terms = related_terms.rstrip(", ")

    if entry_type == "glossary_term":
        return {
            "type": "term",
            "data": {
                "term_display_name": display_name,
                "description": description,
                "steward": stewards,
                "tagged_assets": "",
                "synonyms": synonyms,
                "related_terms": related_terms,
                "belongs_to_category": belongs_to_category,
            },
        }
    elif entry_type == "glossary_category":
        return {
            "type": "category",
            "data": {
                "category_display_name": display_name,
                "description": description,
                "steward": stewards,
                "belongs_to_category": belongs_to_category,
            },
        }
    return None


def export_glossary_entries(
    entries: List[Dict[str, Any]],
    categories_csv: str,
    terms_csv: str,
    project: str,
    max_workers: int = MAX_WORKERS,
):
    """Exports the glossary entries to a CSV file.

    Args:
        entries: The list of entries to export.
        categories_csv: The path to the CSV file to export the categories data.
        terms_csv: The path to the CSV file to export the terms data.
        project: The Google Cloud Project ID.
    """
    categories_fields = [
        "category_display_name",
        "description",
        "steward",
        "belongs_to_category",
    ]
    terms_fields = [
        "term_display_name",
        "description",
        "steward",
        "tagged_assets",
        "synonyms",
        "related_terms",
        "belongs_to_category",
    ]

    with open(categories_csv, mode="w", newline="") as categories_file, open(
        terms_csv, mode="w", newline=""
    ) as terms_file:
        categories_writer = csv.DictWriter(
            categories_file, fieldnames=categories_fields, quoting=csv.QUOTE_ALL
        )
        terms_writer = csv.DictWriter(
            terms_file, fieldnames=terms_fields, quoting=csv.QUOTE_ALL
        )

        relationships_data = fetch_all_relationships(entries, project, max_workers)
        chunk_size = max_workers
        num_batches = math.ceil(len(entries) / chunk_size)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch in range(num_batches):
                start = batch * chunk_size
                end = start + chunk_size
                entries_batch = entries[start:end]

                futures = [
                    executor.submit(process_entry, entry, relationships_data, project)
                    for entry in entries_batch
                ]

                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        if result["type"] == "term":
                            terms_writer.writerow(result["data"])
                        elif result["type"] == "category":
                            categories_writer.writerow(result["data"])


def main():
    args = utils.get_export_arguments()
    utils.validate_export_args(args)
    entries = fetch_entries(args.project, args.location, args.group)
    export_glossary_entries(entries, args.categories_csv, args.terms_csv, args.project)


if __name__ == "__main__":
    main()