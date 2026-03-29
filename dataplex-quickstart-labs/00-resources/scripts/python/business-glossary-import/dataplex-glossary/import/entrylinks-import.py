"""EntryLink Import Utility - Imports EntryLinks into Dataplex from Google Sheets."""

import os
import select
import sys
from typing import Dict, List

from googleapiclient.errors import HttpError

curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(curr_dir))
sys.path.append(os.path.dirname(os.path.dirname(curr_dir)))

from utils import api_layer, argument_parser, business_glossary_utils, constants, import_utils, logging_utils, sheet_utils
from utils.constants import (
    BIGQUERY_SYSTEM_ENTRY_GROUP,
    CATALOG_ENTRY_PATTERN,
    DP_LINK_TYPE_DEFINITION,
    DP_LINK_TYPE_RELATED,
    DP_LINK_TYPE_SYNONYM,
    ENTRY_REFERENCE_TYPE_SOURCE,
    ENTRY_REFERENCE_TYPE_TARGET,
    SOURCE_ENTRY_PATTERN,
)
from utils.models import EntryLink, EntryReference, SpreadsheetRow

logger = logging_utils.get_logger()

USER_INPUT_TIMEOUT = 60  # seconds


def _read_user_input_with_select(timeout: int) -> str:
    """Read user input using select for timeout support."""
    ready_streams, _, _ = select.select([sys.stdin], [], [], timeout)
    if ready_streams:
        return sys.stdin.readline().strip()
    print()  # newline after timeout
    return ''


def get_user_input_with_timeout(prompt: str, timeout: int = USER_INPUT_TIMEOUT) -> str:
    """Get user input with a timeout. Returns empty string on timeout."""
    print(prompt, end='', flush=True)
    return _read_user_input_with_select(timeout)


def prompt_user_on_missing_entries(missing_entry_names: List[str]):
    """Prompt user to continue if entries are missing."""
    if not missing_entry_names:
        return
    
    missing_count = len(missing_entry_names)
    logger.warning(f"Found {missing_count} entries not found in Dataplex. "
                  f"EntryLinks associated with these entries will be skipped.")
    user_response = get_user_input_with_timeout("Continue with import? [y/N]: ")
    if not user_response.lower().startswith('y'):
        logger.info("Import aborted.")
        sys.exit(1)


def _get_existing_archive_files(archive_dir: str) -> List[str]:
    """Get list of existing JSON files in archive directory."""
    if not os.path.exists(archive_dir):
        return []
    return [filename for filename in os.listdir(archive_dir) if filename.endswith('.json')]


def _remove_archive_files(archive_dir: str, filenames: List[str]) -> None:
    """Remove specified files from archive directory."""
    for filename in filenames:
        file_path = os.path.join(archive_dir, filename)
        try:
            os.remove(file_path)
            logger.debug(f"Removed: {filename}")
        except Exception as remove_error:
            logger.error(f"Failed to remove {filename}: {remove_error}")


def check_and_clean_archive_folder(archive_dir: str) -> bool:
    """Check if archive folder has existing files and prompt user to clean."""
    existing_json_files = _get_existing_archive_files(archive_dir)
    if not existing_json_files:
        return True
    
    file_count = len(existing_json_files)
    logger.warning(f"Found {file_count} existing file(s) in archive folder from a previous incomplete import")
    
    clear_response = get_user_input_with_timeout("Do you want to clear the archive folder and start a fresh import? [y/N]: ")
    if clear_response.lower().startswith('y'):
        _remove_archive_files(archive_dir, existing_json_files)
        logger.info("Archive folder cleared. Proceeding with fresh import.")
        return True
    
    continue_response = get_user_input_with_timeout("Continue using existing files? [y/N]: ")
    if continue_response.lower().startswith('y'):
        logger.info("Continuing with existing files in archive folder.")
        return True
    
    logger.info("Import aborted.")
    return False


def _find_column_indices_for_entries(header_row: List[str]) -> tuple:
    """Find column indices for source and target entry columns."""
    normalized_headers = [header.lower().strip() for header in header_row]
    try:
        source_column_idx = normalized_headers.index('source_entry')
        target_column_idx = normalized_headers.index('target_entry')
        return source_column_idx, target_column_idx
    except ValueError:
        raise ValueError("Spreadsheet must have 'source_entry' and 'target_entry' columns")


def _extract_entry_from_row(data_row: List[str], column_idx: int) -> str:
    """Extract entry name from a row at the specified column index."""
    if len(data_row) > column_idx and data_row[column_idx].strip():
        return data_row[column_idx].strip()
    return ''


def extract_entry_references_from_spreadsheet(spreadsheet_url: str) -> set:
    """Extract all unique entry references from spreadsheet."""
    spreadsheet_data = sheet_utils.read_from_spreadsheet_url(spreadsheet_url)
    
    if not spreadsheet_data or len(spreadsheet_data) < 2:
        logger.warning("Spreadsheet is empty or has no data rows")
        return set()
    
    source_idx, target_idx = _find_column_indices_for_entries(spreadsheet_data[0])
    
    unique_entry_names = set()
    for data_row in spreadsheet_data[1:]:
        source_entry = _extract_entry_from_row(data_row, source_idx)
        target_entry = _extract_entry_from_row(data_row, target_idx)
        if source_entry:
            unique_entry_names.add(source_entry)
        if target_entry:
            unique_entry_names.add(target_entry)
    
    return unique_entry_names


def _collect_unique_entry_references(entrylinks: List[EntryLink]) -> List:
    """Collect all unique entry references from entrylinks."""
    unique_references = []
    processed_entry_names = set()
    
    for entrylink in entrylinks:
        for entry_ref in entrylink.entryReferences:
            if entry_ref.name and entry_ref.name not in processed_entry_names:
                processed_entry_names.add(entry_ref.name)
                unique_references.append(entry_ref)
    
    return unique_references


def _lookup_and_check_entry(entry_ref, dataplex_service, missing_entries: List, lock) -> None:
    """Lookup a single entry and track if missing."""
    entry_name = entry_ref.name
    entry_name_match = CATALOG_ENTRY_PATTERN.match(entry_name)
    
    if not entry_name_match:
        logger.warning(f"Invalid entry name format: {entry_name}")
        return
    
    project_id = entry_name_match.group('project_id')
    location = entry_name_match.group('location_id')
    project_location = f"projects/{project_id}/locations/{location}"
    
    if not api_layer.lookup_entry(dataplex_service, entry_name, project_location):
        with lock:
            missing_entries.append(entry_name)
        logger.debug(f"Entry not found: {entry_name}")


def check_entry_existence(entrylinks: List[EntryLink], dataplex_service) -> List[str]:
    """Check if entries exist using parallel lookups."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    
    unique_entry_refs = _collect_unique_entry_references(entrylinks)
    logger.info(f"Validating {len(unique_entry_refs)} unique entries...")
    
    missing_entry_names = []
    missing_lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        lookup_futures = [
            executor.submit(_lookup_and_check_entry, ref, dataplex_service, missing_entry_names, missing_lock)
            for ref in unique_entry_refs
        ]
        for completed_future in as_completed(lookup_futures):
            try:
                completed_future.result()
            except Exception as lookup_error:
                logger.error(f"Error checking entry existence: {lookup_error}")
    
    return missing_entry_names


def convert_spreadsheet_to_entrylinks(spreadsheet_url: str) -> List[EntryLink]:
    """Convert spreadsheet rows to EntryLink entries."""
    spreadsheet_data = sheet_utils.read_from_spreadsheet_url(spreadsheet_url)
    
    if not spreadsheet_data or len(spreadsheet_data) < 2:
        return []
    
    type_idx, source_idx, target_idx, path_idx = sheet_utils.extract_column_indices(spreadsheet_data)
    row_dicts = sheet_utils.rows_to_entry_link_dicts(spreadsheet_data, type_idx, source_idx, target_idx, path_idx)
    
    return [build_entry_link(SpreadsheetRow.from_dict(row_dict)) for row_dict in row_dicts]


def _parse_source_entry_components(source_entry: str) -> tuple:
    """Parse source entry to extract project, location, and entry group."""
    source_entry_match = SOURCE_ENTRY_PATTERN.match(source_entry)
    if not source_entry_match:
        logger.error(f"Invalid source entry format: {source_entry}")
        raise ValueError(f"Invalid source entry format: {source_entry}")
    
    return (
        source_entry_match.group('project_id'),
        source_entry_match.group('location_id'),
        source_entry_match.group('entry_group')
    )


def _generate_entrylink_name(project_id: str, location: str, entry_group: str) -> str:
    """Generate a unique entrylink name."""
    entrylink_base = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}"
    entrylink_id = business_glossary_utils.get_entry_link_id()
    return f"{entrylink_base}/entryLinks/{entrylink_id}"


def build_entry_link(spreadsheet_row: SpreadsheetRow) -> EntryLink:
    """Build EntryLink model from spreadsheet row data."""
    project_id, location, entry_group = _parse_source_entry_components(spreadsheet_row.source_entry)
    link_type = spreadsheet_row.entry_link_type.lower()
    
    entry_refs = build_entry_references(spreadsheet_row, entry_group, link_type)
    entrylink_name = _generate_entrylink_name(project_id, location, entry_group)
    
    entrylink = EntryLink(
        name=entrylink_name,
        entryLinkType=constants.LINK_TYPES[link_type],
        entryReferences=entry_refs
    )
    
    logger.debug(f"input row: {spreadsheet_row}, output entrylink: {entrylink}")
    return entrylink

def _format_source_path_for_bigquery(source_path: str, entry_group: str) -> str:
    """Format source path for BigQuery entries."""
    if entry_group == BIGQUERY_SYSTEM_ENTRY_GROUP and source_path and not source_path.startswith('Schema.'):
        return f"Schema.{source_path}"
    return source_path


def _build_definition_references(spreadsheet_row: SpreadsheetRow, entry_group: str) -> List[EntryReference]:
    """Build entry references for definition link type."""
    source_path = spreadsheet_row.source_path.strip()
    formatted_path = _format_source_path_for_bigquery(source_path, entry_group)
    
    return [
        EntryReference(
            name=spreadsheet_row.source_entry,
            path=formatted_path,
            type=ENTRY_REFERENCE_TYPE_SOURCE
        ),
        EntryReference(
            name=spreadsheet_row.target_entry,
            path='',
            type=ENTRY_REFERENCE_TYPE_TARGET
        )
    ]


def build_entry_references(spreadsheet_row: SpreadsheetRow, entry_group: str, link_type: str) -> List[EntryReference]:
    """Build list of EntryReference models from row data."""
    if link_type == DP_LINK_TYPE_DEFINITION:
        return _build_definition_references(spreadsheet_row, entry_group)
    
    return [
        EntryReference(name=spreadsheet_row.source_entry),
        EntryReference(name=spreadsheet_row.target_entry)
    ]


def extract_entrylink_components(entrylink_name: str) -> tuple[str, str, str]:
    """Extract project_id, location, and entry_group from an entrylink name."""
    entrylink_name_match = constants.ENTRYLINK_NAME_PATTERN.match(entrylink_name)
    if not entrylink_name_match:
        logger.error(f"Invalid entryLink name format: {entrylink_name}")
        raise ValueError(f"Invalid entryLink name format: {entrylink_name}")
    
    return (
        entrylink_name_match.group('project_id'),
        entrylink_name_match.group('location_id'),
        entrylink_name_match.group('entry_group')
    )

def _extract_normalized_link_type(entrylink_type_name: str) -> str:
    """Extract and normalize link type from full type name."""
    link_type_match = constants.ENTRYLINK_TYPE_PATTERN.match(entrylink_type_name)
    if not link_type_match:
        return None
    
    link_type = link_type_match.group('link_type')
    if link_type in [DP_LINK_TYPE_RELATED, DP_LINK_TYPE_SYNONYM]:
        return 'related-synonym'  # Group both types together
    return link_type


def _add_entrylink_to_group(
    grouped_entrylinks: Dict, 
    entrylink_dict: Dict, 
    link_type: str, 
    project_id: str, 
    location: str, 
    entry_group: str
) -> None:
    """Add an entrylink to the appropriate group."""
    group_key = f"{project_id}_{location}_{entry_group}"
    
    if link_type not in grouped_entrylinks:
        grouped_entrylinks[link_type] = {}
    if group_key not in grouped_entrylinks[link_type]:
        grouped_entrylinks[link_type][group_key] = []
    
    grouped_entrylinks[link_type][group_key].append(entrylink_dict)


def group_entrylinks_by_type_and_entry_group(entrylinks: List[EntryLink]) -> Dict[str, Dict[str, List[Dict]]]:
    """Group entrylinks by link type and project/location."""
    grouped_entrylinks = {}
    
    for entrylink in entrylinks:
        normalized_link_type = _extract_normalized_link_type(entrylink.entryLinkType)
        if not normalized_link_type:
            logger.warning(f"Invalid entryLinkType format: {entrylink.entryLinkType}")
            continue
        
        project_id, location, entry_group = extract_entrylink_components(entrylink.name)
        _add_entrylink_to_group(
            grouped_entrylinks, 
            entrylink.to_dict(), 
            normalized_link_type, 
            project_id, 
            location, 
            entry_group
        )

    logger.debug(f"input: {entrylinks}, output: {grouped_entrylinks}")
    return grouped_entrylinks


def _get_archive_directory() -> str:
    """Get the path to the archive directory."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "archive")


def _is_network_error(exception: Exception) -> bool:
    """Check if exception is network-related."""
    network_indicators = ["network", "connection", "unreachable", "timed out", "name resolution", "no route"]
    error_message = str(exception).lower()
    return any(indicator in error_message for indicator in network_indicators)


def _handle_import_exception(exception: Exception) -> int:
    """Handle exceptions during import and return exit code."""
    if isinstance(exception, KeyboardInterrupt):
        logger.info("Import cancelled by user")
        return 130
    
    if _is_network_error(exception):
        logger.error("Network connectivity issue. Please check your internet connection and try again.")
    else:
        logger.error(f"Unexpected error during import: {exception}")
    
    logger.debug("Error details:", exc_info=True)
    return 1


def _validate_and_get_project() -> str:
    """Validate and return the default project from ADC."""
    billing_project = api_layer.get_default_project()
    if not billing_project:
        logger.error("No project configured. Set a default project via 'gcloud config set project PROJECT_ID'")
        return None
    logger.debug(f"Using project from ADC: {billing_project}")
    return billing_project


def _run_import_workflow(parsed_args) -> int:
    """Execute the main import workflow."""
    sheet_name = sheet_utils.get_sheet_name_for_url(parsed_args.spreadsheet_url)
    logger.info(f"Starting EntryLink import from sheet: '{sheet_name}'")
    
    dataplex_service = api_layer.authenticate_dataplex()
    
    billing_project = _validate_and_get_project()
    if not billing_project:
        return 1
    
    archive_dir = _get_archive_directory()
    if not check_and_clean_archive_folder(archive_dir):
        return 1
    
    entrylinks = convert_spreadsheet_to_entrylinks(parsed_args.spreadsheet_url)
    if not entrylinks:
        logger.warning("Spreadsheet is empty or has no valid entries")
        return 1
    
    missing_entries = check_entry_existence(entrylinks, dataplex_service)
    prompt_user_on_missing_entries(missing_entries)
    
    grouped_entrylinks = group_entrylinks_by_type_and_entry_group(entrylinks)
    entry_group_count = len(grouped_entrylinks)
    logger.info(f"Found {entry_group_count} entry group(s). This will result in {entry_group_count} separate import job(s).")
    
    import_files = import_utils.create_import_json_files(grouped_entrylinks, archive_dir)
    if not import_files:
        logger.warning("No files to process")
        return 1
    
    import_success = import_utils.run_import_files(import_files, parsed_args.buckets)
    
    if import_success:
        logger.info("EntryLink Import Completed Successfully!")
        return 0
    
    logger.error("Some import jobs failed. Check logs for details.")
    return 1


def main():
    """Main entry point for EntryLink import."""
    try:
        logging_utils.setup_file_logging()
        parsed_args = argument_parser.get_import_entrylinks_arguments()
        return _run_import_workflow(parsed_args)
    except Exception as import_exception:
        return _handle_import_exception(import_exception)


if __name__ == "__main__":
    main()