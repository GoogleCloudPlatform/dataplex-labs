"""
EntryLink Import Utility

This utility imports EntryLinks into Google Cloud Dataplex from Google Sheets.
It validates the input data, converts entries to the correct format,
and creates metadata import jobs to add the EntryLinks.

Usage:
    python entrylinks-import.py --spreadsheet-url <url> --project-id <project> --gcs-bucket <bucket>
"""

# Standard library imports
import os
import sys
from typing import Dict, List

# Third-party imports
from googleapiclient.errors import HttpError

# Configure Python path for module imports
curr_dir = os.path.dirname(os.path.abspath(__file__))
import_dir = os.path.dirname(curr_dir)  # import directory
glossary_dir = os.path.dirname(import_dir)  # dataplex-glossary

# Add all necessary paths
sys.path.append(import_dir)
sys.path.append(glossary_dir)

# Local imports
from utils import api_layer, argument_parser, business_glossary_utils, constants, import_utils, logging_utils, sheet_utils
from utils.constants import (
    BIGQUERY_SYSTEM_ENTRY_GROUP,
    DATAPLEX_ENTRY_PATTERN,
    DP_LINK_TYPE_DEFINITION,
    DP_LINK_TYPE_RELATED,
    DP_LINK_TYPE_SYNONYM,
    ENTRY_REFERENCE_TYPE_SOURCE,
    ENTRY_REFERENCE_TYPE_TARGET,
    SOURCE_ENTRY_PATTERN,
)
from utils.models import EntryLink, EntryReference, SpreadsheetRow

logger = logging_utils.get_logger()


def prompt_user_on_missing_entries(not_found_entries):
    if len(not_found_entries) > 0:
        logger.warning(f"Found {len(not_found_entries)} entries are not found in Dataplex")
        user_input = input("Continue with import? [y/N]: ")
        if not user_input.lower().startswith('y'):
            sys.exit(1)


def extract_entry_references_from_spreadsheet(spreadsheet_url: str):
    """Extract entry references from Google Sheet."""
    sheets_service = api_layer.authenticate_sheets()
    spreadsheet_id = api_layer.get_spreadsheet_id(spreadsheet_url)
    
    # Read data from sheet
    data = api_layer.read_from_sheet(sheets_service, spreadsheet_id)
    
    if not data or len(data) < 2:  # Need at least headers and one row
        logger.warning("Spreadsheet is empty or has no data rows")
        return set()
    
    # Parse headers to find column indices
    headers = [header.lower().strip() for header in data[0]]
    try:
        source_idx = headers.index('source_entry')
        target_idx = headers.index('target_entry')
    except ValueError as e:
        logger.error(f"Required column not found in spreadsheet headers: {e}")
        raise ValueError(f"Spreadsheet must have 'source_entry' and 'target_entry' columns")
    
    entry_references = set()
    for row in data[1:]:  # Skip header row
        if len(row) > source_idx and row[source_idx].strip():
            entry_references.add(row[source_idx].strip())
        if len(row) > target_idx and row[target_idx].strip():
            entry_references.add(row[target_idx].strip())
    return entry_references

def check_entry_existence(entrylinks: List[EntryLink], dataplex_service) -> List[str]:
    """Check if entries exist by extracting entry references from entrylinks."""
    missing_entries = []
    checked_entries = set()  # Avoid duplicate checks
    
    for entrylink in entrylinks:
        try:
            # Extract entry references from the EntryLinkData model
            entry_refs = entrylink.entryReferences
            lookup_entries(dataplex_service, missing_entries, checked_entries, entry_refs)          
        except HttpError as e:
            logger.error(f"Error while checking entry existence: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while checking entry: {e}")    
    return missing_entries


def lookup_entries(dataplex_service, missing_entries, checked_entries, entry_references: List[EntryReference]):
    for entry_reference in entry_references:
        entry_name = entry_reference.name
        if not entry_name or entry_name in checked_entries:
            continue
                
        checked_entries.add(entry_name)    
        match = DATAPLEX_ENTRY_PATTERN.match(entry_name)
        if not match:
            logger.warning(f"Invalid entry name format: {entry_name}")
            continue
                
        project_id = match.group('project_id')
        location = match.group('location_id')
        project_location_name = f"projects/{project_id}/locations/{location}"
                    
        if not api_layer.lookup_entry(dataplex_service, entry_name, project_location_name):
            missing_entries.append(entry_name)
            logger.warning(f"Entry not found in Dataplex: {entry_name}")


def convert_spreadsheet_to_entrylinks(spreadsheet_url: str) -> List[EntryLink]:
    """Convert spreadsheet rows to EntryLink entries for import."""
    sheets_service = api_layer.authenticate_sheets()
    spreadsheet_id = api_layer.get_spreadsheet_id(spreadsheet_url)
    
    # Read data from sheet
    data = api_layer.read_from_sheet(sheets_service, spreadsheet_id)
    
    if not data or len(data) < 2:
        logger.warning("Spreadsheet is empty or has no data rows")
        return []
    
    # Parse headers
    type_idx, source_idx, target_idx, path_idx = sheet_utils.extract_column_indices(data)
    
    # Convert rows to entry link dictionaries
    row_dicts = sheet_utils.rows_to_entry_link_dicts(data, type_idx, source_idx, target_idx, path_idx)
    
    # Build EntryLink models from the dictionaries
    entries = []
    for row_dict in row_dicts:
        row = SpreadsheetRow.from_dict(row_dict)
        entry = build_entry_link(row)
        entries.append(entry)
            
    return entries

def build_entry_link(row: SpreadsheetRow) -> EntryLink:
    """Build EntryLink model from row data."""
    match = SOURCE_ENTRY_PATTERN.match(row.source_entry)
    if not match:
        logger.error(f"Invalid source entry format: {row.source_entry}")
        raise ValueError(f"Invalid source entry format: {row.source_entry}")
    
    project_id = match.group('project_id')
    location = match.group('location_id')
    entry_group = match.group('entry_group')

    link_type = row.entry_link_type.lower()
    entry_references = build_entry_references(row, entry_group, link_type)

    # EntryLink name uses the source entry's project/location/entryGroups path
    entrylink_base = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}"
    
    entrylink = EntryLink(
        name=f"{entrylink_base}/entryLinks/{business_glossary_utils.get_entry_link_id()}",
        entryLinkType=constants.LINK_TYPES[link_type],
        entryReferences=entry_references
    )
    
    logger.debug(f"input row: {row}, output entrylink: {entrylink}")
    return entrylink

def build_entry_references(row: SpreadsheetRow, entry_group: str, link_type: str) -> List[EntryReference]:
    """Build list of EntryReference models from row data."""
    entry_references = []
    if link_type == DP_LINK_TYPE_DEFINITION:
        source_path = row.source_path.strip()
        
        # Handle path formatting for BigQuery
        if entry_group == BIGQUERY_SYSTEM_ENTRY_GROUP and source_path and not source_path.startswith('Schema.'):
            source_path = f"Schema.{source_path}"
            
        entry_references.append(EntryReference(
            name=row.source_entry,
            path=source_path,
            type=ENTRY_REFERENCE_TYPE_SOURCE
        ))
        entry_references.append(EntryReference(
            name=row.target_entry,
            path='',
            type=ENTRY_REFERENCE_TYPE_TARGET
        ))
    else:
        entry_references.append(EntryReference(name=row.source_entry))
        entry_references.append(EntryReference(name=row.target_entry))
    return entry_references


def extract_entrylink_components(entrylink_name: str) -> tuple[str, str, str]:
    """Extract project_id, location, and entry_group from an entrylink name."""
    match = constants.ENTRYLINK_NAME_PATTERN.match(entrylink_name)
    if not match:
        logger.error(f"Invalid entryLink name format: {entrylink_name}")
        raise ValueError(f"Invalid entryLink name format: {entrylink_name}")
    
    project_id = match.group('project_id')
    location = match.group('location_id')
    entry_group = match.group('entry_group')
    
    return project_id, location, entry_group

def group_entrylinks_by_entry_type_and_entry_group(entrylinks: List[EntryLink]) -> Dict[str, Dict[str, List[Dict]]]:
    """Group entrylinks by entry link type and project/location from the entrylink name."""
    grouped_entrylinks = {}
    for entrylink in entrylinks:
        # Get the link type using regex pattern
        entry_link_type_name = entrylink.entryLinkType
        type_match = constants.ENTRYLINK_TYPE_PATTERN.match(entry_link_type_name)
        if not type_match:
            logger.warning(f"Invalid entryLinkType format: {entry_link_type_name}")
            continue
        
        entry_link_type = type_match.group('link_type')
        # Normalize related/synonym to the same category
        if entry_link_type in [DP_LINK_TYPE_RELATED, DP_LINK_TYPE_SYNONYM]:
            entry_link_type = 'related-synonym'  # Group both types together
            
        # Extract project/location/entryGroup from the entryLink name
        entrylink_name = entrylink.name
        project_id, location, entry_group = extract_entrylink_components(entrylink_name)
        
        # Create group key using project_id, location, and entryGroup
        # Convert model back to dict for compatibility with downstream code
        group_entrylinks(grouped_entrylinks, entrylink.to_dict(), entry_link_type, project_id, location, entry_group)

    logger.debug(f"input: {entrylinks}, output: {grouped_entrylinks}")
    return grouped_entrylinks

def group_entrylinks(grouped_entrylinks, entrylink, entry_link_type, project_id, location, entry_group):
    group_key = f"{project_id}_{location}_{entry_group}"

    if entry_link_type not in grouped_entrylinks:
        grouped_entrylinks[entry_link_type] = {}
    if group_key not in grouped_entrylinks[entry_link_type]:
        grouped_entrylinks[entry_link_type][group_key] = []
    grouped_entrylinks[entry_link_type][group_key].append(entrylink)


def main():
    """Main entry point for EntryLink import."""
    logging_utils.setup_file_logging()
    args = argument_parser.get_import_entrylinks_arguments()
    logger.info("Starting EntryLink Import from Google Sheet")
    logger.info("Spreadsheet URL: %s", args.spreadsheet_url)
    dataplex_service = api_layer.authenticate_dataplex()

    try:
        # Convert spreadsheet rows to entries first
        entrylinks = convert_spreadsheet_to_entrylinks(args.spreadsheet_url)
        if not entrylinks:
            logger.warning("Spreadsheet is empty or has no valid entries")
            return 1

        # Step 1: Validate entry existence
        not_found_entries = check_entry_existence(entrylinks, dataplex_service)
        prompt_user_on_missing_entries(not_found_entries)

        # Step 2: Group entries by type and entry group
        grouped_entrylinks = group_entrylinks_by_entry_type_and_entry_group(entrylinks)
        
        # Step 3: Create JSON files in archive folder
        archive_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "archive")
        import_files = import_utils.create_import_json_files(grouped_entrylinks, archive_dir)
        
        if not import_files:
            logger.warning("No files to process")
            return 1
        
        # Step 4: Process each group with bucket cycling
        buckets = args.buckets
        all_success = import_utils.run_import_files(import_files, buckets)

        if all_success:
            logger.info("EntryLink Import Completed Successfully!")
            return 0
        else:
            logger.error("Some import jobs failed. Check logs for details.")
            return 1

    except Exception as e:
        logger.error(f"Unexpected error during import: {e}")
        logger.debug(f"Error details:", exc_info=True)
        return 1


if __name__ == "__main__":
    main()