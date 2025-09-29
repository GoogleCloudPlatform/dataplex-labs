"""
Helper functions for writing data to files and reading metadata from files.
"""
import json
import os
from typing import List, Dict, Any, Union
import logging_utils
from models import GlossaryEntry, EntryLink
from googleapiclient.errors import HttpError
from constants import *
import re
from collections import defaultdict
import threading
logger = logging_utils.get_logger()

BASE_DIRECTORY = os.path.join(os.getcwd(), EXPORTED_FILES_DIRECTORY)
GLOSSARIES_DIRECTORY_PATH = os.path.join(BASE_DIRECTORY, GLOSSARIES_DIRECTORY)
ENTRYLINKS_DIRECTORY_PATH = os.path.join(BASE_DIRECTORY, ENTRYLINKS_DIRECTORY)
UNGROUPED_ENTRYLINKS_DIRECTORY_PATH = os.path.join(BASE_DIRECTORY, UNGROUPED_ENTRYLINKS_DIRECTORY)
SUMMARY_DIRECTORY_PATH = os.path.join(os.getcwd(), SUMMARY_DIRECTORY)

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
    if len(valid_items) == 0:
        return
    with open(filepath, "w", encoding="utf-8") as f:
        for item in valid_items:
            f.write(json.dumps(item.to_dict()) + "\n")

    logger.info(f"Successfully exported {len(valid_items)} items to {filepath}")


def write_glossary_file(glossary_data: List[GlossaryEntry], filename: str):
    """Writes glossary entries to the Glossaries folder."""
    ensure_dir(GLOSSARIES_DIRECTORY_PATH)
    filepath = os.path.join(GLOSSARIES_DIRECTORY_PATH, filename)
    write_jsonl_file(glossary_data, filepath)


def write_term_term_links_file(context: dict, term_term_data: List[EntryLink]):
    """Writes term-term links to the EntryLinks folder."""
    ensure_dir(UNGROUPED_ENTRYLINKS_DIRECTORY_PATH)
    folder = context.dp_glossary_id
    # Create a subfolder for each glossary inside the EntryLinks directory
    glossary_folder_path = os.path.join(UNGROUPED_ENTRYLINKS_DIRECTORY_PATH, folder)
    ensure_dir(glossary_folder_path)
    filepath = os.path.join(glossary_folder_path, f"entrylinks_related_synonyms.json")
    write_jsonl_file(term_term_data, filepath)


def write_grouped_entry_links_files(context: dict, grouped_links: Dict[str, List[EntryLink]]):
    """Writes term-entry links grouped by Project,Location,EntryGroup key into the EntryLinks folder."""
    for ple_key, links in grouped_links.items():
        folder = context.dp_glossary_id
        filename = f"entrylinks_definition_{ple_key}.json"
        # Create a subfolder for each glossary inside the EntryLinks directory
        glossary_folder_path = os.path.join(UNGROUPED_ENTRYLINKS_DIRECTORY_PATH, folder)
        ensure_dir(glossary_folder_path)
        filepath = os.path.join(glossary_folder_path, filename)
        write_jsonl_file(links, filepath)


def write_files(
    context: Dict[str, Any],
    glossary_data: List[GlossaryEntry],
    term_term_data: List[EntryLink],
    term_entry_data: Dict[str, List[EntryLink]]
):
    """Writes all transformed data objects to their respective files."""

    write_glossary_file(glossary_data, f"glossary_{context.dp_glossary_id}.json")
    write_term_term_links_file(context, term_term_data)
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
    return match.group(1) if match else ""

def group_files_by_entry_group_name():
    """Groups entrylink files by their file name (regardless of subfolder), and writes merged content to a single file per name."""
    # Collect all .json files recursively under UNGROUPED_ENTRYLINKS_DIRECTORY_PATH
    entrylink_files = collect_json_entrylinks()

    # Group file paths by file name
    files_by_name = group_files_by_name(entrylink_files)

    # For each group, merge all lines and write to a single file in the top-level EntryLinks directory
    merge_grouped_files(files_by_name)

def merge_grouped_files(files_by_name):
    for filename, paths in files_by_name.items():
        merged_lines = []
        for path in paths:
            with open(path, 'r', encoding='utf-8') as f:
                merged_lines.extend(f.readlines())
        # Write merged content to the top-level EntryLinks directory
        ensure_dir(ENTRYLINKS_DIRECTORY_PATH)
        output_path = os.path.join(ENTRYLINKS_DIRECTORY_PATH, filename)
        with open(output_path, 'w', encoding='utf-8') as out_f:
            out_f.writelines(merged_lines)
        logger.info(f"Merged {len(paths)} files into {output_path}")

def group_files_by_name(entrylink_files):
    files_by_name = defaultdict(list)
    for path in entrylink_files:
        filename = os.path.basename(path)
        files_by_name[filename].append(path)
    return files_by_name

def collect_json_entrylinks():
    entrylink_files = []
    for root, _, files in os.walk(UNGROUPED_ENTRYLINKS_DIRECTORY_PATH):
        for f in files:
            if f.endswith('.json'):
                entrylink_files.append(os.path.join(root, f))
    return entrylink_files

def export_summary(project_id: str):
    glossary_files = get_file_paths_from_directory(GLOSSARIES_DIRECTORY_PATH)
    entrylink_files = get_file_paths_from_directory(ENTRYLINKS_DIRECTORY_PATH)

    glossaries_count = len(glossary_files)
    glossary_entries_count = 0
    definition_entrylinks_count = 0
    related_synonym_entrylinks_count = 0

    # Count lines in all glossary files
    glossary_entries_count = calculate_glossary_entry_count(glossary_files)

    # Count lines in entrylink files by type
    definition_entrylinks_count, related_synonym_entrylinks_count = calculate_entrylinks_count(entrylink_files)

    stats_path = write_export_stats(project_id, glossaries_count, glossary_entries_count, definition_entrylinks_count, related_synonym_entrylinks_count)

    logger.info(f"Export Summary written to {stats_path}")

def write_export_stats(project_id: str, glossaries_count: int, glossary_entries_count: int, definition_entrylinks_count: int, related_synonym_entrylinks_count: int):
    stats_path = os.path.join(SUMMARY_DIRECTORY_PATH, f"export-stats-{project_id}.txt")
    # Ensure the summary directory exists
    ensure_dir(SUMMARY_DIRECTORY_PATH)
    with open(stats_path, "w", encoding="utf-8") as stats_file:
        stats_file.write(f"Project ID: {project_id}\n")
        stats_file.write(f"Number of glossaries exported: {glossaries_count}\n")
        stats_file.write(f"Number of glossary entries (including terms, categories): {glossary_entries_count}\n")
        stats_file.write(f"Number of definition entrylinks exported: {definition_entrylinks_count}\n")
        stats_file.write(f"Number of related and synonym entrylinks exported: {related_synonym_entrylinks_count}\n")
    return stats_path

def calculate_glossary_entry_count(glossary_files):
    glossary_entries_count = 0
    for glossary_file in glossary_files:
        with open(glossary_file, 'r', encoding='utf-8') as f:
            glossary_entries_count += sum(1 for _ in f)
    return glossary_entries_count

def calculate_entrylinks_count(entrylink_files):
    definition_entrylinks_count = 0
    related_synonym_entrylinks_count = 0 
    for ef in entrylink_files:
        filename = os.path.basename(ef)
        with open(ef, 'r', encoding='utf-8') as f:
            line_count = sum(1 for _ in f)
            if filename.startswith("entrylinks_definition_"):
                definition_entrylinks_count += line_count
            elif filename.startswith("entrylinks_related_synonyms"):
                related_synonym_entrylinks_count += line_count
    return definition_entrylinks_count, related_synonym_entrylinks_count


def write_import_stats(project_id: str, job: dict):
    logger.debug(f"Writing import stats for job: {job}")
    stats_path = os.path.join(SUMMARY_DIRECTORY_PATH, f"import-stats-{project_id}.txt")
    # Use a global lock for this module to synchronize threads
    if not hasattr(write_import_stats, "_lock"):
        write_import_stats._lock = threading.Lock()
    lock = write_import_stats._lock

    import_type = extract_import_type(job)
    import_result, fields = import_job_result(job)

    # Only include fields that are present and not zero
    stats_lines = build_import_stats_lines(import_result, fields)

    # Add job name, import type, and status
    status = job.get("status", {})
    state = status.get("state", "")
    job_name = job.get("name", "Unknown Job")
    stats_lines.insert(0, f"Job Name: {job_name}")
    stats_lines.insert(1, f"Import Type: {import_type}")
    stats_lines.insert(2, f"State: {state}")

    if not stats_lines:
        return  # Nothing to write

    # Lock file for thread safety
    with lock:
        with open(stats_path, "a", encoding="utf-8") as f:
            f.write("\n".join(stats_lines) + "\n\n")

    logger.debug(f"input: project_id={project_id}, job={job} | output: stats_path={stats_path}")

def build_import_stats_lines(import_result, fields):
    stats_lines = []
    for key, label in fields:
        value = import_result.get(key)
        if value is not None and str(value) != "0":
            stats_lines.append(f"{label}: {value}")
    return stats_lines

def extract_import_type(job):
    import_type = "Unknown"
    scope = job.get("importSpec", {}).get("scope", {})
    if scope.get("glossaries"):
        import_type = "Glossary Import"
    elif scope.get("entry_link_types"):
        import_type = "EntryLink Import"
    return import_type

def import_job_result(job):
    import_result = job.get("importResult", {})
    fields = import_job_result_fields()
    
    return import_result,fields

def import_job_result_fields():
    fields = [
        ("deletedEntries", "Entries Deleted"),
        ("updatedEntries", "Entries Updated"),
        ("createdEntries", "Entries Created"),
        ("unchangedEntries", "Entries Unchanged"),
        ("recreatedEntries", "Entries Recreated"),
        ("deletedEntryLinks", "EntryLinks Deleted"),
        ("createdEntryLinks", "EntryLinks Created"),
        ("unchangedEntryLinks", "EntryLinks Unchanged"),
    ] 
    return fields
