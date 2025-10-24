import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import logging_utils
import api_layer
import migration_utils
import business_glossary_import_v2
from file_utils import group_files_by_entry_group_name, export_summary
from business_glossary_export_v2 import execute_export
from gcs_dao import check_all_buckets_permissions

logger = logging_utils.get_logger()
MAX_EXPORT_WORKERS = 10


def run_export_worker(glossary_url: str, user_project: str, org_ids: list[str]) -> bool:
    """Worker function to export a single glossary via business_glossary_export_v2."""
    try:
        return execute_export(glossary_url, user_project, org_ids)
    except Exception as e:
        logger.error(f"Export failed for {glossary_url} with error: {e}")
        logger.debug(f"Export failed for {glossary_url} with error: {e}", exc_info=True)
        return False


def find_glossaries_in_project(project_id: str, user_project: str) -> list[str]:
    """Finds and prepares glossary URLs for export."""
    raw_urls = api_layer.discover_glossaries(project_id, user_project)
    if not raw_urls:
        logger.info("No glossaries discovered in the source project.")
        return []

    # Convert `/entries/` URLs to `/glossaries/` for export compatibility
    glossary_urls = [url.replace("/entries/", "/glossaries/") for url in raw_urls]
    return glossary_urls

def scope_glossaries_to_project(glossary_urls: list[str], project_id: str, project_number: str) -> list[str]:
    """Filters glossary URLs to only include those within the specified project."""
    scoped_urls = []
    for url in glossary_urls:
        url_parts = migration_utils.parse_glossary_url(url)
        if url_parts and (url_parts.get("project") == project_id or url_parts.get("project") == project_number):
            scoped_urls.append(url)
        else:
            logger.warning(f"Glossary URL '{url}' does not belong to project '{project_id}' and will be skipped.")
    if not scoped_urls:
        logger.warning(f"Glossaries in the scope aren't part of the project '{project_id}'.")
    return scoped_urls
 
def perform_exports(glossary_urls: list[str], user_project: str, org_ids: list[str]) -> int:
    """Exports all glossaries in parallel and returns the count of successful exports."""
    successful_exports = 0
    with ThreadPoolExecutor(max_workers=MAX_EXPORT_WORKERS) as executor:
        future_to_url = {
            executor.submit(run_export_worker, url, user_project, org_ids): url
            for url in glossary_urls
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            if future.result():
                logger.info(f"Successfully exported glossary: {url}")
                successful_exports += 1
    return successful_exports


def perform_imports(project_id: str, buckets: list[str]):
    """Calls the import step after successful exports."""
    try:
        return business_glossary_import_v2.main(project_id, buckets)   
    except Exception as e:
        logger.error(f"An error occurred during the import step: {e}")
        logger.debug(f"Import step failed for project {project_id} with an unexpected error: {e}", exc_info=True)
        return False


def log_migration_start(project_id):
    logger.info("=" * 50)
    logger.info("Starting Business Glossary Migration: V1 to V2 for project %s", project_id)
    logger.info("=" * 50)

def log_export_summary(successful_exports, total_exports):
    logger.info(f"Successfully exported {successful_exports}/{total_exports} glossaries.")

def all_exports_successful(successful_exports, total_exports):
    return successful_exports == total_exports

def log_export_start(total_exports):
    logger.info(f"Initiating export for {total_exports} glossar{'y' if total_exports == 1 else 'ies'}.")

def export_glossaries(user_project: str, org_ids: list[str], glossary_urls: list[str]) -> bool:
    """Finds and exports business glossaries."""
    if not glossary_urls:
        logger.warning("Halting Export as no glossaries were found.")
        return True
    
    log_export_start(len(glossary_urls))
    successful_exports = perform_exports(glossary_urls, user_project, org_ids)
    num_glossaries = len(glossary_urls)
    log_export_summary(successful_exports, num_glossaries)
    if not all_exports_successful(successful_exports, num_glossaries):
        logger.error("Not all exports were successful.")
        return False
    return True


def main(args: argparse.Namespace) -> None:
    """Main orchestrator for the migration process."""
    project_id = args.project
    buckets = args.buckets
    org_ids = args.orgIds
    user_project = args.user_project
    glossary_urls = args.glossaries

    logging_utils.setup_file_logging()
    log_migration_start(project_id)

    project_number = api_layer.get_project_number(project_id, user_project=user_project)  
    # Check GCS permissions before starting export/import
    if not check_all_buckets_permissions(buckets, project_number):
        sys.exit(1)

    export_status = True
    
    if not args.resume_import:
        if not glossary_urls:
            glossary_urls = find_glossaries_in_project(project_id, user_project)
        else:
            glossary_urls = scope_glossaries_to_project(glossary_urls, project_id, project_number)
        export_status = export_glossaries(user_project, org_ids, glossary_urls)

    if not export_status and not args.resume_import:
        logger.warning("Migration halted due to export failures.")
        sys.exit(1)

    if not args.resume_import:
        group_files_by_entry_group_name()
        export_summary(project_id)
        
    import_status = perform_imports(project_id, buckets)
    if import_status and export_status:
        logger.info("Business Glossary Migration completed successfully.")

if __name__ == "__main__":
    parsed_args = migration_utils.get_migration_arguments()
    main(parsed_args)
