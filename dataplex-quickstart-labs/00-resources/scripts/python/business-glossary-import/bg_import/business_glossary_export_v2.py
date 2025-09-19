"""
Main script to export a Data Catalog glossary, including all its terms,
categories, and relationships, to a set of JSON files.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import time
from typing import Dict, Any, List, Tuple

from api_layer import fetch_dc_glossary_taxonomy_entries, create_dataplex_glossary, fetch_dc_glossary_taxonomy_relationships, get_project_number
from data_transformer import process_dc_glossary_entries
from file_utils import write_files
import logging_utils
from migration_utils import *
from models import GlossaryEntry, EntryLink, Context


logger = logging_utils.get_logger()
MAX_WORKERS = 20


def _build_export_context(glossary_url: str, user_project: str, org_ids: List[str]) -> Context:
    """Parses inputs, validates org IDs, and builds the main export context."""
    url_parts = parse_glossary_url(glossary_url)

    final_org_ids = org_ids or get_org_ids_from_gcloud()
    if not final_org_ids:
        logger.error(
            "No organization IDs found. Provide --orgIds or configure gcloud correctly. "
            "You need 'resourcemanager.organizations.get' permission on all Organizations with linked Entries."
        )
        sys.exit(1)

    dp_glossary_id = build_glossary_id_from_entry_group_id(url_parts["entry_group_id"])
    user_project = user_project or url_parts["project"]
    project_number = get_project_number(url_parts["project"], user_project=user_project)
    context = Context(
        user_project=user_project,
        org_ids=final_org_ids,
        dataplex_entry_group=f"projects/{project_number}/locations/global/entryGroups/@dataplex",
        project=project_number,
        location_id=url_parts["location_id"],
        entry_group_id=url_parts["entry_group_id"],
        dc_glossary_id=url_parts["glossary_id"],
        dp_glossary_id=dp_glossary_id,
    )

    logger.debug(
        f"_build_export_context input: glossary_url={glossary_url}, user_project={user_project}, org_ids={org_ids} | output: {context}"
    )
    return context


def _run_export_workflow(context: Context):
    """Executes the core data processing steps: fetch, transform, write, and create."""
    glossary_taxonomy_entries = fetch_dc_glossary_taxonomy_entries(context)
    if not glossary_taxonomy_entries:
        logger.info(f"No entries found in glossary '{context.dp_glossary_id}'. Nothing to export.")
        return
    glossary_taxonomy_relationships = fetch_dc_glossary_taxonomy_relationships(context, glossary_taxonomy_entries)
    dp_glossary_data = process_dc_glossary_entries(context, glossary_taxonomy_entries, glossary_taxonomy_relationships)
    write_files(context, *dp_glossary_data)
    create_dataplex_glossary(context)


def execute_export(glossary_url: str, user_project: str, org_ids: List[str]):
    """
    Orchestrates the business glossary export by setting up context and running the workflow.
    """
    start_time = time.time()
    try:
        context = _build_export_context(glossary_url, user_project, org_ids)
        logger.info(f"Starting export for glossary: {context.dp_glossary_id}")
        _run_export_workflow(context)
        logger.info(f"Export complete for {context.dp_glossary_id}. Total time: {time.time() - start_time:.2f} seconds.")
        return True
    except Exception as e:
        logger.error(f"Export failed for {glossary_url}")
        logger.debug(f"Export failed for {glossary_url} with an unexpected error: {e}", exc_info=True)


def main():
    """Parses command-line arguments and runs the export."""
    parser = get_export_arguments()
    args = parser.parse_args() 
    logging_utils.setup_file_logging()
    execute_export(
            glossary_url=args.url,
            user_project=args.user_project,
            org_ids=args.orgIds,
        )


if __name__ == "__main__":
    main()