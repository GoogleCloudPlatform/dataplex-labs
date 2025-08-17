"""
Main script to export a Data Catalog glossary, including all its terms,
categories, and relationships, to a set of JSON files.
"""
import time
import sys
from typing import Dict, Any, List, Tuple
from typing import Dict, Any, List, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging_utils
import migration_utils
from api_layer import fetch_entries, create_glossary, fetch_all_relationships
from data_transformer import (
    process_entry,
    transform_term_term_links,
    transform_term_entry_links,
    get_entry_id,
    process_raw_entries
)
from file_writer import write_jsonl_file, write_grouped_jsonl_files, write_files
from models import GlossaryEntry, EntryLink

RawEntry = Dict[str, Any]
Relationships = Dict[str, List[RawEntry]]
logger = logging_utils.get_logger()
MAX_WORKERS = 20


def _build_export_config(glossary_url: str, user_project: str, org_ids: List[str]) -> Dict[str, Any]:
    """Parses inputs, validates org IDs, and builds the main config dictionary."""
    url_parts = migration_utils.parse_glossary_url(glossary_url)

    final_org_ids = org_ids
    if not final_org_ids:
        logger.info(f"No --orgIds provided, fetching from gcloud...")
        final_org_ids = migration_utils.get_org_ids_from_gcloud()
    if not final_org_ids:
        logger.error(
            "No organization IDs found. Provide --orgIds or configure gcloud correctly. "
            "You need 'resourcemanager.organizations.get' permission on all Organizations with linked Entries."
        )
        sys.exit(1)

    config = {
        "user_project": user_project or url_parts["project"],
        "org_ids": final_org_ids,
        "dataplex_entry_group": f"projects/{url_parts['project']}/locations/global/entryGroups/@dataplex",
        **url_parts
    }
    config["normalized_glossary"] = migration_utils.normalize_id(config["glossary"])
    logger.debug(f"_build_export_config input: glossary_url={glossary_url}, user_project={user_project}, org_ids={org_ids} | output: {config}")
    return config


def _run_export_workflow(config: Dict[str, Any]):
    """Executes the core data processing steps: fetch, transform, write, and create."""
    raw_entries = fetch_entries(config)
    if not raw_entries:
        logger.info(f"No entries found in glossary '{config['glossary']}'. Nothing to export.")
        return

    processed_data = process_raw_entries(config, raw_entries)
    write_files(config, *processed_data)
    create_glossary(config)


def execute_export(glossary_url: str, user_project: str, org_ids: List[str], debugging: bool = False):
    """
    Orchestrates the business glossary export by setting up config and running the workflow.
    """
    start_time = time.time()
    if debugging:
        logging_utils.setup_file_logging()

    try:
        config = _build_export_config(glossary_url, user_project, org_ids)
        logger.info(f"Starting export for glossary: {config['glossary']}")

        _run_export_workflow(config)

        logger.info(f"Export complete for {config['glossary']}. Total time: {time.time() - start_time:.2f} seconds.")
    except SystemExit: # Allow sys.exit to propagate from helpers
        raise
    except Exception as e:
        logger.error(f"Export failed for {glossary_url} with an unexpected error: {e}", exc_info=debugging)
        # Re-raise the exception so the calling script (like run.py) knows it failed
        raise

def main():
    """Parses command-line arguments and runs the export."""
    parser = migration_utils.get_export_arguments()
    args = parser.parse_args()
    
    try:
        execute_export(
            glossary_url=args.url,
            user_project=args.user_project,
            org_ids=args.orgIds,
            debugging=args.debugging
        )
    except Exception as e:
        logger.error(f"A fatal error occurred during export: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()