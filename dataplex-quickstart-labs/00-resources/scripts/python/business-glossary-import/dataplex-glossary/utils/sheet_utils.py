"""
Sheet Utility Functions

Common utility functions for converting data between Google Sheets and Dataplex EntryLinks.
Handles parsing spreadsheet data and transforming it to/from EntryLink format.
"""

# Standard library imports
from typing import Any, Dict, List, Tuple

# Local imports
from utils import logging_utils
from utils.constants import (
    ENTRYLINK_TYPE_PATTERN,
    ENTRY_REFERENCE_TYPE_SOURCE,
    ENTRY_REFERENCE_TYPE_TARGET,
)

logger = logging_utils.get_logger()


def entry_links_to_rows(entry_links: List[Dict[str, Any]]) -> List[List[str]]:
    """
    Convert EntryLinks to CSV rows format for export to spreadsheet.
    
    Args:
        entry_links: List of EntryLink dictionaries from Dataplex API
        
    Returns:
        List of rows, where each row is [link_type, source_entry, target_entry, source_path]
        
    Example:
        >>> links = [{
        ...     'entryLinkType': 'projects/.../entryLinkTypes/definition',
        ...     'entryReferences': [
        ...         {'name': 'entry1', 'type': 'SOURCE', 'path': '/path'},
        ...         {'name': 'entry2', 'type': 'TARGET'}
        ...     ]
        ... }]
        >>> rows = entry_links_to_rows(links)
        >>> rows[0][0]  # link_type
        'definition'
    """
    rows = []
    for link in entry_links:
        # Extract link type using regex (e.g., 'definition', 'synonym', 'related')
        link_type_full = link.get('entryLinkType', '')
        match = ENTRYLINK_TYPE_PATTERN.match(link_type_full)
        if not match:
            logger.warning(f"Invalid entryLinkType format: {link_type_full}")
            continue
        link_type = match.group('link_type')
        
        # Get entry references
        first_ref = None
        second_ref = None
        entry_refs = link.get('entryReferences', [])
        if not entry_refs:
            continue
            
        # Try to find explicit source/target references first
        source_ref = next((ref for ref in entry_refs if ref.get('type') == ENTRY_REFERENCE_TYPE_SOURCE), None)
        target_ref = next((ref for ref in entry_refs if ref.get('type') == ENTRY_REFERENCE_TYPE_TARGET), None)
        
        if source_ref and target_ref:
            # For directional links with explicit source/target
            first_ref = source_ref
            second_ref = target_ref
        else:
            # For non-directional links or when source/target not specified
            # Just use the references in order they appear
            first_ref = entry_refs[0]
            second_ref = entry_refs[1] if len(entry_refs) > 1 else None
            
        if first_ref and second_ref:
            _add_entry_link_to_rows(rows, link_type, first_ref, second_ref)
            
    return rows


def _add_entry_link_to_rows(rows: List[List[str]], link_type: str, 
                             first_ref: Dict[str, Any], second_ref: Dict[str, Any]) -> None:
    """
    Helper function to add a single entry link as a row.
    
    Args:
        rows: The list of rows to append to
        link_type: The type of link (e.g., 'definition', 'synonym')
        first_ref: First entry reference dictionary
        second_ref: Second entry reference dictionary
    """
    row = [
        link_type,
        first_ref.get('name', ''),
        second_ref.get('name', ''),
        first_ref.get('path', '')  # Empty string if no path
    ]
    rows.append(row)


def extract_column_indices(data: List[List[str]]) -> Tuple[int, int, int, int]:
    """
    Extract column indices from spreadsheet headers.
    
    Args:
        data: Spreadsheet data with headers in first row
        
    Returns:
        Tuple of (type_idx, source_idx, target_idx, path_idx)
        
    Raises:
        ValueError: If required columns are not found
    """
    headers = [h.lower().strip() for h in data[0]]
    try:
        type_idx = headers.index('entry_link_type')
        source_idx = headers.index('source_entry')
        target_idx = headers.index('target_entry')
        path_idx = headers.index('source_path') if 'source_path' in headers else -1
    except ValueError as e:
        logger.error(f"Required column not found in spreadsheet: {e}")
        raise ValueError(f"Spreadsheet must have required columns: {e}")
    return type_idx, source_idx, target_idx, path_idx


def rows_to_entry_link_dicts(data: List[List[str]], type_idx: int, source_idx: int, 
                              target_idx: int, path_idx: int) -> List[Dict[str, str]]:
    """
    Convert spreadsheet rows to entry link dictionaries.
    
    Args:
        data: Spreadsheet data (first row should be headers, already processed)
        type_idx: Column index for entry_link_type
        source_idx: Column index for source_entry
        target_idx: Column index for target_entry  
        path_idx: Column index for source_path (-1 if not present)
        
    Returns:
        List of dictionaries with entry link data
        
    Example:
        >>> data = [['type', 'source', 'target'], ['definition', 'entry1', 'entry2']]
        >>> dicts = rows_to_entry_link_dicts(data, 0, 1, 2, -1)
        >>> dicts[0]['entry_link_type']
        'definition'
    """
    entries = []
    for row_num, row in enumerate(data[1:], start=2):
        if len(row) <= max(type_idx, source_idx, target_idx):
            logger.warning(f"Row {row_num} has insufficient columns, skipping")
            continue
            
        # Create row dict
        row_dict = {
            'entry_link_type': row[type_idx].strip(),
            'source_entry': row[source_idx].strip(),
            'target_entry': row[target_idx].strip(),
            'source_path': row[path_idx].strip() if path_idx >= 0 and len(row) > path_idx else ''
        }
        
        # Validate required fields
        if not row_dict['source_entry'] or not row_dict['target_entry']:
            logger.warning(f"Row {row_num} missing source or target entry, skipping")
            continue
            
        entries.append(row_dict)
        
    return entries
