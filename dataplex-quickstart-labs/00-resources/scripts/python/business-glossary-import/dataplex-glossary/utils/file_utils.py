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


def delete_file(file_path: str) -> bool:
    """Delete a file. Returns True on success, False on failure."""
    if not os.path.exists(file_path):
        return True
    try:
        os.remove(file_path)
        logger.debug(f"Deleted file: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete file {file_path}: {e}")
        return False


def move_file(source_path: str, dest_dir: str) -> bool:
    """Move a file to destination directory. Returns True on success."""
    if not os.path.exists(source_path):
        logger.warning(f"Source file does not exist: {source_path}")
        return False
    ensure_dir(dest_dir)
    filename = os.path.basename(source_path)
    dest_path = os.path.join(dest_dir, filename)
    try:
        os.rename(source_path, dest_path)
        logger.debug(f"Moved file: {source_path} -> {dest_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to move file {source_path} to {dest_dir}: {e}")
        return False


def move_file_to_imported_folder(file_path: str):
    """Delete the processed file. (Legacy name, calls delete_file)"""
    delete_file(file_path)


def get_link_type(file_path: str) -> str | None:
    """Get the entry link type from a JSON file."""
    data = read_first_json_line(file_path)
    if data:
        return data.get("entryLink", {}).get("entryLinkType", "")
    logger.debug(f"Failed to read link type from file: {file_path}")
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