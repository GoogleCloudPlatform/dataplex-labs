"""
Helper functions for writing data to files and reading metadata from files.
"""

# Standard library imports
import json
import os

# Local imports
from utils import logging_utils

logger = logging_utils.get_logger()

def ensure_dir(path: str):
    """Create directory if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)
        logger.debug(f"Created directory: {path}")


def parse_json_line(line: str) -> dict | None:
    """Parse a JSON line, return None if invalid."""
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def read_first_json_line(file_path: str) -> dict | None:
    """Returns the JSON object from the first line of a file, or None if unreadable."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if not first_line:
                return None
            return json.loads(first_line)
    except (IOError, json.JSONDecodeError):
        return None


def is_file_empty(file_path: str) -> bool:
    """Check if a file is empty or doesn't exist."""
    if not os.path.exists(file_path):
        return True
    return os.path.getsize(file_path) == 0


def move_file_to_imported_folder(file_path: str):
    """Delete the processed file."""
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}. Skipping move/delete operation.")
        return
    try:
        os.remove(file_path)
        logger.debug(f"Deleted local file: {file_path}")
    except Exception as e:
            logger.error(f"Failed to delete local file {file_path}: {e}")
            logger.debug(f"Failed to delete local file {file_path}: {e}", exc_info=True)


def get_link_type(file_path: str) -> str | None:
    """Get the entry link type from a JSON file."""
    data = read_first_json_line(file_path)
    if data:
        return data.get("entryLink", {}).get("entryLinkType", "")
    return None


def get_entry_group(file_path: str) -> str | None:
    """Extract entry group from an entrylink file."""
    data = read_first_json_line(file_path)
    if data:
        name = data.get("entryLink", {}).get("name", "")
        return name.split('/entryLinks/')[0] if name else None
    return None


def write_entrylinks_to_file(entrylinks: list[dict], output_dir: str, filename: str) -> str:
    """Write entrylinks to a JSON file (one entry per line)."""
    ensure_dir(output_dir)
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        for entrylink in entrylinks:
            f.write(f"{json.dumps(entrylink)}\n")
    
    logger.debug(f"Wrote {len(entrylinks)} entrylinks to {filepath}")
    return filepath