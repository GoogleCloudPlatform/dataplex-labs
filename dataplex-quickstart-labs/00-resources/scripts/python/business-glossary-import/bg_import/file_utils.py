"""
Helper functions for writing data to files and reading metadata from files.
"""
import json
import os
from typing import List, Dict, Any, Union
import logging_utils
from models import GlossaryEntry, EntryLink
from constants import *
import re
logger = logging_utils.get_logger()

BASE_DIRECTORY = os.path.join(os.getcwd(), EXPORTED_FILES_DIRECTORY)
GLOSSARIES_DIRECTORY_PATH = os.path.join(BASE_DIRECTORY, GLOSSARIES_DIRECTORY)
ENTRYLINKS_DIRECTORY_PATH = os.path.join(BASE_DIRECTORY, ENTRYLINKS_DIRECTORY)


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)
        logger.debug(f"Created directory: {path}")


def parse_json_line(line: str) -> dict | None:
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
    if not os.path.exists(file_path):
        return True
    return os.path.getsize(file_path) == 0


def get_file_paths_from_directory(directory: str) -> list[str]:
    if not os.path.isdir(directory):
        return []
    return sorted([os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.json')])


def write_jsonl_file(export_list: List[Union[GlossaryEntry, EntryLink]], filepath: str):
    """Writes a list of dataclass objects to a JSON file."""
    valid_items = [item for item in export_list if item]
    
    with open(filepath, "w", encoding="utf-8") as f:
        for item in valid_items:
            f.write(json.dumps(item.to_dict()) + "\n")
    
    logger.info(f"Successfully exported {len(valid_items)} items to {filepath}")


def write_glossary_file(glossary_data: List[GlossaryEntry], filename: str):
    """Writes glossary entries to the Glossaries folder."""
    ensure_dir(GLOSSARIES_DIRECTORY_PATH)
    filepath = os.path.join(GLOSSARIES_DIRECTORY_PATH, filename)
    write_jsonl_file(glossary_data, filepath)


def write_term_term_links_file(term_term_data: List[EntryLink], filename: str):
    """Writes term-term links to the EntryLinks folder."""
    ensure_dir(ENTRYLINKS_DIRECTORY_PATH)
    filepath = os.path.join(ENTRYLINKS_DIRECTORY_PATH, filename)
    write_jsonl_file(term_term_data, filepath)


def write_grouped_entry_links_files(context: dict, grouped_links: Dict[str, List[EntryLink]]):
    """Writes term-entry links grouped by Project,Location,EntryGroup key into the EntryLinks folder."""
    ensure_dir(ENTRYLINKS_DIRECTORY_PATH)
    for ple_key, links in grouped_links.items():
        filename = f"entrylinks_definiton_{context.dp_glossary_id}_{ple_key}.json"
        filepath = os.path.join(ENTRYLINKS_DIRECTORY_PATH, filename)
        write_jsonl_file(links, filepath)


def write_files(
    context: Dict[str, Any],
    glossary_data: List[GlossaryEntry],
    term_term_data: List[EntryLink],
    term_entry_data: Dict[str, List[EntryLink]]
):
    """Writes all transformed data objects to their respective files."""

    write_glossary_file(glossary_data, f"glossary_{context.dp_glossary_id}.json")
    write_term_term_links_file(term_term_data, f"entrylinks_related_synonyms_{context.dp_glossary_id}.json")
    write_grouped_entry_links_files(context, term_entry_data)


def move_file_to_imported_folder(file_path: str):
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}. Skipping move/delete operation.")
        return

    filename = os.path.basename(file_path)
    if filename.startswith("glossary_"):
        imported_entry_links_directory_path = os.path.join(BASE_DIRECTORY, IMPORTED_GLOSSARIES_DIRECTORY)
    elif filename.startswith("entrylinks_"):
        imported_entry_links_directory_path = os.path.join(BASE_DIRECTORY, IMPORTED_ENTRYLINKS_DIRECTORY)
    else:
        imported_entry_links_directory_path = None

    if imported_entry_links_directory_path:
        os.makedirs(imported_entry_links_directory_path, exist_ok=True)
        imported_entry_link_file_path = os.path.join(imported_entry_links_directory_path, filename)
        try:
            os.replace(file_path, imported_entry_link_file_path)
            logger.debug(f"Moved file to: {imported_entry_link_file_path}")
        except Exception as e:
            logger.error(f"Failed to move file {file_path} to {imported_entry_link_file_path}: {e}")
            logger.debug(f"Failed to move file {file_path} to {imported_entry_link_file_path}: {e}", exc_info=True)
    else:
        try:
            os.remove(file_path)
            logger.debug(f"Deleted local file: {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete local file {file_path}: {e}")
            logger.debug(f"Failed to delete local file {file_path}: {e}", exc_info=True)


# EntryLink / glossary relationship helpers

def get_entrylink_type_and_references(file_path: str) -> tuple[str, list[dict]]:
    """Extracts entryLinkType and entryReferences from the first JSON line of the file."""
    data = read_first_json_line(file_path)
    if not data:
        return "", []
    entry_link = data.get("entryLink", {})
    link_type = entry_link.get("entryLinkType", "")
    entry_references = entry_link.get("entryReferences", [])
    return link_type, entry_references


def extract_dp_glossary_term_name(link_type: str, entry_references: list[dict]) -> str:
    """Returns the Dataplex glossary term name from entryReferences based on link_type."""
    if "definition" in link_type and len(entry_references) > 1:
        return entry_references[1].get("name", "")
    elif ("related" in link_type or "synonym" in link_type) and entry_references:
        return entry_references[0].get("name", "")
    return ""


def extract_glossary_id_from_term_name(dp_glossary_term_name: str) -> str | None:
    """Extracts the glossary ID from a Dataplex glossary term name."""
    match = re.search(r'glossaries/([^/]+)', dp_glossary_term_name)
    if match:
        return match.group(1)
    return None


def build_parent_glossary_file_path(glossary_id: str) -> str:
    """Builds the expected parent glossary file path from a glossary ID."""
    return os.path.join(GLOSSARIES_DIRECTORY_PATH, f"glossary_{glossary_id}.json")


def get_parent_glossary_file(file_path: str) -> str | None:
    """Returns the expected parent glossary file path for an entrylink file, or None if not found."""
    link_type, entry_references = get_entrylink_type_and_references(file_path)
    dp_glossary_term_name = extract_dp_glossary_term_name(link_type, entry_references)
    if not dp_glossary_term_name:
        return None
    glossary_id = extract_glossary_id_from_term_name(dp_glossary_term_name)
    if not glossary_id:
        return None
    return build_parent_glossary_file_path(glossary_id)


def check_entrylink_dependency(file_path: str) -> bool:
    """Checks if the parent glossary has been imported i.e file should be deleted."""
    glossary_file = get_parent_glossary_file(file_path)
    if not glossary_file:
        logger.debug(f"No parent glossary file found for {file_path}; treating as ready for import.")
        return True
    return not os.path.exists(glossary_file)


def get_entry_group(file_path: str) -> str | None:
    data = read_first_json_line(file_path)
    if data:
        name = data.get("entryLink", {}).get("name", "")
        return name.split('/entryLinks/')[0] if name else None
    return None


def extract_job_location_from_entry_group(entry_group: str) -> str:
    if not entry_group:
        return "global"
    match = re.search(r'locations/([^/]+)', entry_group)
    return match.group(1) if match else "global"


def get_link_type(file_path: str) -> str | None:
    data = read_first_json_line(file_path)
    if data:
        return data.get("entryLink", {}).get("entryLinkType", "")
    return None

def extract_glossary_id_from_synonym_related_filename(filename: str) -> str:
    match = re.search(r'entrylinks_related_synonyms_(.*?)\.json', filename)
    return match.group(1) if match else "unknown"
