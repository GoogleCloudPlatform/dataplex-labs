# business_glossary_export_v2.py
"""
Main script to export a Data Catalog glossary, including all its terms,
categories, and relationships, to a set of JSON files.
Can be run as a standalone script or imported and called programmatically.
"""
import time
import sys
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
)
from file_writer import write_jsonl_file, write_grouped_jsonl_files
from models import GlossaryEntry, EntryLink

RawEntry = Dict[str, Any]
Relationships = Dict[str, List[RawEntry]]
logger = logging_utils.get_logger()
MAX_WORKERS = 20

def _transform_data_parallel(
    config: Dict[str, Any], raw_entries: List[RawEntry]
) -> Tuple[List[GlossaryEntry], List[EntryLink], Dict[str, List[EntryLink]]]:
    """Transforms raw API data into structured objects in parallel."""
    logger.info("Step 2 & 3: Fetching relationships and transforming data...")
    
    initial_relationships = fetch_all_relationships(raw_entries, config["user_project"])
    
    parent_map = {
        get_entry_id(e["name"]): get_entry_id(r.get("destinationEntry", {}).get("name"))
        for e in raw_entries for r in initial_relationships.get(e.get("name"), [])
        if r.get("relationshipType") == "belongs_to"
    }
    type_map = {get_entry_id(e.get("name")): e.get("entryType") for e in raw_entries}

    glossary_objs, term_term_links = [], []
    grouped_term_entry_links = defaultdict(list)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_glossary = {executor.submit(process_entry, config, e, parent_map, type_map): e for e in raw_entries}
        future_term_links = {executor.submit(transform_term_term_links, config, e, initial_relationships.get(e.get("name"), [])): e for e in raw_entries}
        future_entry_links = {executor.submit(transform_term_entry_links, config, e): e for e in raw_entries}
        
        for future in as_completed(future_glossary):
            result = future.result()
            if result: glossary_objs.append(result)

        for future in as_completed(future_term_links):
            term_term_links.extend(future.result())

        for future in as_completed(future_entry_links):
            for key, val in future.result().items():
                grouped_term_entry_links[key].extend(val)
            
    return glossary_objs, term_term_links, grouped_term_entry_links

def _write_files(
    config: Dict[str, Any],
    glossary_data: List[GlossaryEntry],
    term_term_data: List[EntryLink],
    term_entry_data: Dict[str, List[EntryLink]]
):
    """Writes the transformed data objects to their respective files."""
    logger.info("Step 4: Writing transformed data to files...")
    write_jsonl_file(glossary_data, f"glossary_{config['normalized_glossary']}.jsonl")
    write_jsonl_file(term_term_data, f"term_term_links_{config['normalized_glossary']}.jsonl")
    write_grouped_jsonl_files(config, term_entry_data)

def execute_export(glossary_url: str, user_project: str, org_ids: List[str], debugging: bool = False):
    """
    Orchestrates the business glossary export process. Can be called programmatically.
    """
    if debugging:
        logging_utils.setup_file_logging()

    url_parts = migration_utils.parse_glossary_url(glossary_url)
    
    final_org_ids = org_ids
    if not final_org_ids:
        logger.info(f"No --orgIds provided for {glossary_url}, fetching from gcloud...")
        final_org_ids = migration_utils.get_org_ids_from_gcloud()
    
    config = {
        "user_project": user_project or url_parts["project"],
        "org_ids": final_org_ids,
        "dataplex_entry_group": f"projects/{url_parts['project']}/locations/global/entryGroups/@dataplex",
        **url_parts
    }
    config["normalized_glossary"] = migration_utils.normalize_id(config["glossary"])
    
    start_time = time.time()
    logger.info(f"Starting export for glossary: {config['glossary']}")
    
    logger.info("Step 1: Fetching all entries from the glossary...")
    raw_entries = fetch_entries(config)
    if not raw_entries:
        logger.info("No entries found. Exiting.")
        return
    
    processed_data = _transform_data_parallel(config, raw_entries)
    
    _write_files(config, *processed_data)
    
    logger.info("Step 5: Ensuring destination glossary exists in Dataplex...")
    create_glossary(config)
    
    logger.info(f"Export complete for {config['glossary']}. Total time: {time.time() - start_time:.2f} seconds.")

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