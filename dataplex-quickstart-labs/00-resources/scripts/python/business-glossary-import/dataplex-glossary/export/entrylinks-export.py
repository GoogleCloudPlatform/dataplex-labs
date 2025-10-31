#!/usr/bin/env python3
"""
EntryLink Export Utility

This utility exports EntryLinks from Google Cloud Dataplex glossary terms to Google Sheets.
Given a glossary URL and spreadsheet URL, it fetches all terms, performs lookupEntryLink API calls,
and saves the results to a Google Sheet.

Usage:
    python entrylinks-export.py --glossary-url <url> --spreadsheet-url <url> --user-project <project-id>
"""

# Standard library imports
import os
import sys

# Add parent directories to Python path to find modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # For api_layer
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # For utils

# Local application imports
from utils import api_layer, argument_parser, business_glossary_utils, logging_utils, sheet_utils

logger = logging_utils.get_logger()

# Constants
SHEET_HEADERS = ["entry_link_type", "source_entry", "target_entry", "source_path"]


def export_entry_links(
    glossary_resource: str, spreadsheet_url: str, user_project: str 
) -> bool:
    """
    Export EntryLinks for all terms in a glossary to a Google Sheet.
    
    Args:
        glossary_resource: The full resource name of the glossary
        spreadsheet_url: The URL of the Google Sheet to write to
        user_project: The project to bill for the export
    
    Returns:
        True if links were exported successfully, False if no links found
    """
    dataplex_service = api_layer.authenticate_dataplex()
    sheets_service = api_layer.authenticate_sheets()
    terms = api_layer.list_glossary_terms(dataplex_service, glossary_resource)

    if not terms:
        logger.warning("No terms found in the glossary")
        return False

    # Fetch entry links for each term
    all_links = []
    for term in terms:
        entry_name = api_layer.generate_entry_name_from_term_name(term["name"])
        term_links = api_layer.lookup_entry_links_for_term(entry_name, user_project)
        if term_links:
            all_links.extend(sheet_utils.entry_links_to_rows(term_links))

    if not all_links:
        logger.info("No entry links found for any terms")
        return False

    # Get spreadsheet ID
    spreadsheet_id = api_layer.get_spreadsheet_id(spreadsheet_url)

    # Prepare data for sheet (headers + rows)
    sheet_data = [SHEET_HEADERS]
    sheet_data.extend(all_links)

    # Write to Google Sheet
    api_layer.write_to_sheet(sheets_service, spreadsheet_id, sheet_data)
    logger.info(f"Successfully wrote {len(all_links)} entry links to spreadsheet")
    return True


def main() -> int:
    """Main entry point."""
    try:
        logging_utils.setup_file_logging()
        args = argument_parser.get_export_entrylinks_arguments()
        glossary_resource = business_glossary_utils.extract_glossary_name(args.glossary_url)

        logger.info("Starting EntryLink Export from Glossary: %s", glossary_resource)
        logger.info("Exporting to Google Sheet: %s", args.spreadsheet_url)
        
        success = export_entry_links(glossary_resource, args.spreadsheet_url, args.user_project)
        if success:
            logger.info("EntryLinks are successfully exported to Google Sheet")
        else:
            logger.info("No EntryLinks found to export")
        return 0
    except Exception as exc:
        logger.exception("Failed to export entry links: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())