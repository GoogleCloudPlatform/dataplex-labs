import sys
import os
import requests
import argparse
import time
import importlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import logging_utils, api_call_utils, utils
from . import export

logger = logging_utils.get_logger()

MAX_EXPORT_WORKERS = 20
MAX_BUCKETS = 20

def set_gcloud_access_token():
    """Fetches gcloud access token and sets it as an environment variable."""
    logger.info("Attempting to fetch Google Cloud access token...")
    try:
        # Use Application Default Credentials to get the credentials object
        credentials, project = google.auth.default()
        auth_req = google.auth.transport.requests.Request()  
        credentials.refresh(auth_req)
        os.environ['GCLOUD_ACCESS_TOKEN'] = credentials.token

    except Exception as e:
        logger.error("FATAL: Could not get Google Cloud credentials.")
        logger.error("Please ensure you have authenticated by running 'gcloud auth application-default login'.")
        logger.error(f"Details: {e}")
        sys.exit(1)


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

def run_export_worker(glossary_url: str, user_project: str, project_id: str) -> bool:
    """A worker function that a thread can execute to export one glossary."""
    try:
        return export.run_export(glossary_url, user_project)
    except Exception as e:
        logger.error(f"Export failed for {glossary_url} with error: {e}", exc_info=True)
        return False

def main():
    """Main orchestrator for the entire migration process."""
    set_gcloud_access_token()
    project = input("Enter the GCP project Id that has glossaries to be migrated: ").strip()
    user_project = input("Enter the GCP project Id to use for billing and API calls: ").strip()
    project_id = utils.get_project_id(project, user_project)
    buckets_str = input(f"Enter a comma-separated list of GCS bucket names (max {MAX_BUCKETS}): ").strip()
    buckets = [b.strip() for b in buckets_str.split(',') if b.strip()][:MAX_BUCKETS]

    if not buckets:
        logger.error("At least one GCS bucket is required.")
        sys.exit(1)

    logger.info("="*50)
    logger.info("Starting Business Glossary Migration: V1 to V2")
    logger.info("="*50)

    start_time = time.time()

    glossary_urls = discover_glossaries(project_id, user_project)
    if not glossary_urls:
        logger.info("Halting migration as no glossaries were found.")
        sys.exit(0)

    successful_exports = 0
    with ThreadPoolExecutor(max_workers=MAX_EXPORT_WORKERS) as executor:
        future_to_url = {
            executor.submit(run_export_worker, url, user_project, project_id): url
            for url in glossary_urls
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            if future.result():
                successful_exports += 1

    end_time = time.time()
    logger.info(f"Export step finished in {end_time - start_time:.2f} seconds.")
    logger.info(f"Completed {successful_exports}/{len(glossary_urls)} export tasks.")

    if successful_exports < len(glossary_urls):
        logger.error("Not all exports were successful. Halting migration before import step.")
        sys.exit(1)
    
    logger.info("All exports completed successfully.")

    try:
        # Using importlib to dynamically import the 'import' module as 'import' is a reserved keyyword in python
        importer = importlib.import_module("import")
        importer.main(project_id, buckets)
        logger.info("Import process complete. Check logs for status of individual jobs.")
        logger.info("Migration finished :)")
    except Exception as e:
        logger.error(f"An error occurred during the import step: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()