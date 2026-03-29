#!/usr/bin/env python3
"""EntryLink Export Utility - Exports EntryLinks from Dataplex glossary terms to Google Sheets."""

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import api_layer, argument_parser, business_glossary_utils, logging_utils, sheet_utils, error
from utils.constants import MAX_WORKERS, SYMMETRIC_LINK_TYPES

logger = logging_utils.get_logger()

SHEET_HEADERS = ["entry_link_type", "source_entry", "target_entry", "source_path"]


def _build_deduplication_key(entry_link_row: list) -> tuple:
    """Build a unique key for detecting duplicate entry links."""
    link_type = entry_link_row[0]
    source_entry = entry_link_row[1]
    target_entry = entry_link_row[2]
    source_path = entry_link_row[3] if len(entry_link_row) > 3 else ''
    
    if link_type in SYMMETRIC_LINK_TYPES:
        return (link_type, tuple(sorted([source_entry, target_entry])))
    return (link_type, source_entry, target_entry, source_path)


def deduplicate_entry_links(entry_links: list) -> list:
    """Remove duplicate entry links from the list."""
    processed_link_keys = set()
    unique_entry_links = []
    
    for entry_link_row in entry_links:
        dedup_key = _build_deduplication_key(entry_link_row)
        if dedup_key not in processed_link_keys:
            processed_link_keys.add(dedup_key)
            unique_entry_links.append(entry_link_row)
    
    return unique_entry_links


def fetch_entry_links_for_region(entry_name: str, region: str, billing_project: str) -> list:
    """Fetch entry links from a single region."""
    try:
        region_links = api_layer.lookup_entry_links_for_term(entry_name, billing_project, location=region)
        return region_links or []
    except Exception as region_error:
        logger.warning(f"Region {region} failed: {region_error}")
        return []


def _resolve_regions_for_term(term_name: str, billing_project: str) -> list:
    """Resolve which regions to query for a term's entry links."""
    term_location = business_glossary_utils.extract_location_from_name(term_name)
    try:
        return api_layer.resolve_regions_to_query(term_location, billing_project)
    except Exception as resolve_error:
        logger.error(f"Failed to resolve regions for '{term_location}': {resolve_error}")
        return []


def _fetch_links_from_regions_parallel(entry_name: str, regions: list, billing_project: str) -> list:
    """Fetch entry links from multiple regions in parallel."""
    collected_links = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        region_futures = {
            executor.submit(fetch_entry_links_for_region, entry_name, region, billing_project): region 
            for region in regions
        }
        for completed_future in as_completed(region_futures):
            try:
                collected_links.extend(completed_future.result())
            except Exception as fetch_error:
                logger.warning(f"Region {region_futures[completed_future]} exception: {fetch_error}")
    return collected_links


def fetch_entry_links_for_term(glossary_term: dict, billing_project: str) -> list:
    """Fetch all entry links for a term across relevant regions."""
    term_name = glossary_term["name"]
    entry_name = business_glossary_utils.generate_entry_name_from_term_name(term_name)
    regions_to_query = _resolve_regions_for_term(term_name, billing_project)
    
    if not regions_to_query:
        return []
    
    collected_links = _fetch_links_from_regions_parallel(entry_name, regions_to_query, billing_project)
    return sheet_utils.entry_links_to_rows(collected_links) if collected_links else []


def fetch_all_entry_links(glossary_terms: list, billing_project: str) -> list:
    """Fetch entry links for all terms in parallel."""
    all_entry_links = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        term_futures = {
            executor.submit(fetch_entry_links_for_term, term, billing_project): term 
            for term in glossary_terms
        }
        for completed_future in as_completed(term_futures):
            try:
                all_entry_links.extend(completed_future.result())
            except Exception as term_error:
                failed_term_name = term_futures[completed_future].get('name')
                logger.error(f"Failed for term {failed_term_name}: {term_error}")
    return all_entry_links


def _write_entry_links_to_sheet(entry_links: list, spreadsheet_url: str, sheets_service) -> str:
    """Write entry links to spreadsheet and return sheet name."""
    spreadsheet_id = sheet_utils.get_spreadsheet_id(spreadsheet_url)
    export_data = [SHEET_HEADERS] + entry_links
    return sheet_utils.write_to_sheet(sheets_service, spreadsheet_id, export_data)


def export_entry_links(glossary_resource: str, spreadsheet_url: str, billing_project: str) -> bool:
    """Export all EntryLinks from a glossary to Google Sheets."""
    dataplex_service = api_layer.authenticate_dataplex()
    sheets_service = sheet_utils.authenticate_sheets()
    api_layer.initialize_locations_cache(billing_project)

    glossary_terms = api_layer.list_glossary_terms(dataplex_service, glossary_resource)
    if not glossary_terms:
        logger.warning("No terms found in the glossary")
        return False

    all_entry_links = fetch_all_entry_links(glossary_terms, billing_project)
    if not all_entry_links:
        logger.info("No entry links found")
        return False

    unique_entry_links = deduplicate_entry_links(all_entry_links)
    logger.debug(f"Deduplicated {len(all_entry_links)} -> {len(unique_entry_links)} links")

    sheet_name = _write_entry_links_to_sheet(unique_entry_links, spreadsheet_url, sheets_service)
    logger.info(f"Data exported to sheet: '{sheet_name}' ({len(unique_entry_links)} entry links)")
    return True


def _is_network_error(exception: Exception) -> bool:
    """Check if exception is a network-related error."""
    network_error_indicators = ["network", "connection", "unreachable", "timed out", "name resolution"]
    error_message = str(exception).lower()
    return any(indicator in error_message for indicator in network_error_indicators)


def _handle_export_exception(exception: Exception) -> int:
    """Handle exceptions during export and return exit code."""
    if isinstance(exception, KeyboardInterrupt):
        logger.info("Export cancelled by user")
        return 1
    if isinstance(exception, (error.DataplexAPIError, error.SheetsAPIError)):
        logger.error(f"Export failed: {exception}")
        return 1
    if isinstance(exception, (error.InvalidSpreadsheetURLError, error.InvalidGlossaryNameError)):
        logger.error(f"Invalid input: {exception}")
        return 1
    if _is_network_error(exception):
        logger.error("Network error. Check your connection and try again.")
        return 1
    
    logger.error(f"Export failed: {type(exception).__name__}: {exception}")
    logger.debug("Full exception:", exc_info=True)
    return 1


def _run_export() -> int:
    """Execute the export workflow and return exit code."""
    logging_utils.setup_file_logging()
    parsed_args = argument_parser.get_export_entrylinks_arguments()
    glossary_resource = business_glossary_utils.extract_glossary_name(parsed_args.glossary_url)
    
    billing_project = api_layer.get_default_project()
    if not billing_project:
        logger.error("No project configured. Set a default project via 'gcloud config set project PROJECT_ID'")
        return 1
    
    logger.debug(f"Using project from ADC: {billing_project}")
    logger.info(f"Starting EntryLink Export for: {glossary_resource}")
    
    export_successful = export_entry_links(glossary_resource, parsed_args.spreadsheet_url, billing_project)
    logger.info("Export completed successfully" if export_successful else "No EntryLinks found to export")
    return 0


def main() -> int:
    """Main entry point."""
    try:
        return _run_export()
    except Exception as export_exception:
        return _handle_export_exception(export_exception)


if __name__ == "__main__":
    sys.exit(main())