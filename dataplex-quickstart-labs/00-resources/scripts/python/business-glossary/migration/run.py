# In migration/run.py

import sys
import argparse
import time
import importlib
import google.auth
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import logging_utils, utils
from . import export

logger = logging_utils.get_logger()
MAX_EXPORT_WORKERS = 20

def run_export_worker(glossary_url: str, user_project: str) -> bool:
    """A worker function that a thread can execute to export one glossary."""
    try:
        return export.run_export(glossary_url, user_project)
    except Exception as e:
        logger.error(f"Export failed for {glossary_url} with error: {e}", exc_info=True)
        return False

# --- CHANGE: main() now accepts the parsed arguments as a parameter ---
def main(args, user_project: str):
    """Main orchestrator for the entire migration process."""
    project_id = args.project
    buckets = args.buckets

    logger.info("="*50)
    logger.info("Starting Business Glossary Migration: V1 to V2")
    logger.info(f"Source Project: {project_id}")
    logger.info(f"Using {len(buckets)} GCS bucket(s) for import.")
    logger.info("="*50)

    start_time = time.time()

    glossary_urls = utils.discover_glossaries(project_id, user_project)
    if not glossary_urls:
        logger.info("Halting migration as no glossaries were found.")
        return # Use return instead of sys.exit to be test-friendly
    
    successful_exports = 0
    with ThreadPoolExecutor(max_workers=MAX_EXPORT_WORKERS) as executor:
        future_to_url = {
            executor.submit(run_export_worker, url, user_project): url
            for url in glossary_urls
        }
        for future in as_completed(future_to_url):
            if future.result():
                successful_exports += 1

    end_time = time.time()
    logger.info(f"Export step finished in {end_time - start_time:.2f} seconds.")
    logger.info(f"Completed {successful_exports}/{len(glossary_urls)} export tasks.")

    if successful_exports < len(glossary_urls):
        logger.error("Not all exports were successful. Halting migration before import step.")
        return
    
    logger.info("All exports completed successfully.")

    try:
        importer = importlib.import_module(".import", package="migration") 
        importer.main(project_id, buckets)
    except Exception as e:
        logger.error(f"An error occurred during the import step: {e}", exc_info=True)

if __name__ == "__main__":
    try:
        _, user_project = google.auth.default()
    except google.auth.exceptions.DefaultCredentialsError:
        logger.error("Authentication failed. Please run 'gcloud auth application-default login'.")
        sys.exit(1)

    parsed_args = utils.get_migration_arguments()
    main(parsed_args, user_project)