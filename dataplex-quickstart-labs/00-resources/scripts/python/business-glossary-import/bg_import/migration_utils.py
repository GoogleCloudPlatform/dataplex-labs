"""
Utility functions specifically for the Business Glossary V2 export script.

This file contains string, ID, and parsing helpers that are used across the
codebase (api_layer, data_transformer, and the main export script). 
"""

import argparse
import re
import subprocess
import sys
import uuid
import json
from typing import Dict, List, Optional, Tuple

import logging_utils
logger = logging_utils.get_logger()


def parse_org_ids_list(value: str) -> List[str]:
    """Helper to parse comma-separated string lists from argparse."""
    if not isinstance(value, str):
        raise argparse.ArgumentTypeError(f"Invalid list format: '{value}'.")
    return [item.strip() for item in value.split(",") if item.strip()]


def get_org_ids_from_gcloud() -> List[str]:
    """Uses gcloud to fetch all organization IDs the user can see."""
    try:
        result = subprocess.run(
            ["gcloud", "organizations", "list", "--format=value(ID)"],
            capture_output=True,
            text=True,
            check=True,
        )
        org_ids = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
        if not org_ids:
            logger.error("gcloud found no organization IDs.")
            sys.exit(1)
        return org_ids
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error(
            f"Error fetching organization IDs via gcloud: {e}. "
            "Please pass IDs via --orgIds or ensure gcloud is authenticated."
        )
        sys.exit(1)


def get_export_arguments() -> argparse.ArgumentParser:
    """Creates the argument parser for the export script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="Export a Data Catalog Business Glossary.",
    )
    parser.add_argument("--url", help="Full Data Catalog Glossary URL.", required=True)
    parser.add_argument("--user-project", help="Google Cloud Project ID for billing/quota.", type=str)
    parser.add_argument("--orgIds", type=parse_org_ids_list, default=[], help='Comma-separated org IDs, e.g., "123,456"')
    return parser


def parse_glossary_url(url: str) -> Dict[str, str]:
    """Extracts components from a Data Catalog glossary URL."""
    pattern = (
        r"projects/(?P<project>[^/]+)/locations/(?P<location_id>[^/]+)/"
        r"entryGroups/(?P<entry_group_id>[^/]+)/glossaries/(?P<glossary_id>[^/?#]+)"
    )
    match = re.search(pattern, url)
    if not match:
        logger.error(
            "Invalid --url provided. It must contain the pattern: "
            "projects/.../locations/.../entryGroups/.../glossaries/..."
        )
        sys.exit(1)
    return match.groupdict()

def build_glossary_id_from_entry_group_id(entry_group_id: str) -> str:
    """Constructs a glossary ID from the entry group ID."""
    prefix = "dc_glossary_"
    if entry_group_id.startswith(prefix):
        entry_group_id = entry_group_id[len(prefix):]
    normalized_id = normalize_id(entry_group_id)
    return normalized_id
    

def normalize_id(name: str) -> str:
    """Converts a string to a valid Dataplex ID (lowercase, numbers, hyphens)."""
    if not name:
        return ""
    normalized_name = name.lower().replace(" ", "-")
    normalized_name = re.sub(r"[^a-z0-9\-]", "-", normalized_name)
    normalized_name = re.sub(r"-+", "-", normalized_name)
    return normalized_name.strip("-")
  
def trim_spaces_in_display_name(display_name: str) -> str:
    return display_name.strip()


def get_dc_glossary_taxonomy_id(glossary_taxonomy_name: str) -> str:
    """
    Extracts the final dc_entry id from a Data Catalog resource name.

    e.g. "projects/x/locations/y/entryGroups/z/entries/ENTRYID" -> "ENTRYID"
    """
    if not glossary_taxonomy_name:
        return ""
    match = re.search(r"entries/([^/]+)$", glossary_taxonomy_name)
    # nornalize as well IMPORATANT
    return match.group(1) if match else ""


def get_entry_link_id() -> str:
    """Generate a unique dc_entry link ID: starts with a lowercase letter, contains only lowercase letters and numbers."""
    entrylink_id = 'g' + uuid.uuid4().hex
    return entrylink_id


def extract_entry_parts(entry_full_name: str) -> Optional[Tuple[str, str, str]]:
    """
    Extracts (project_location, entry_group_id, entry_id) from an dc_entry resource.

    Returns None if the pattern doesn't match.
    """
    pattern = r"(projects/[^/]+/locations/[^/]+)/entryGroups/([^/]+)/entries/([^/]+)"
    match = re.search(pattern, entry_full_name)
    return match.groups() if match else None

def get_migration_arguments(argv=None) -> argparse.Namespace:
    """Gets arguments for the migration program."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    configure_migration_argument_parser(parser)
    return parser.parse_args(argv)

def configure_migration_argument_parser(parser: argparse.ArgumentParser) -> None:
    """Defines flags and parses arguments related to migration."""
    parser.add_argument(
        "--project",
        help="Google Cloud Project ID that has the V1 glossaries to be migrated.",
        metavar="[Project ID]",
        type=str,
        required=True
    )
    parser.add_argument(
        "--user-project",
        type=str,
        required=True,
        help="Google Cloud Project used for billing/quota."
    )
    parser.add_argument(
        "--buckets",
        help="Comma-separated list of GCS bucket ids. Example: --buckets=\"bucket-1,bucket-2\"",
        type=lambda s: [item.strip() for item in s.split(",") if item.strip()],
        required=True,
        metavar="[bucket-1,bucket-2,...]"
    )
    parser.add_argument(
        "--orgIds",
        type=parse_org_ids_list,  
        default=[],
        help="A list of org IDs where glossaries and entries are present. Delimiters can be spaces or commas. Example: --org-ids=\"[id1,id2 id3]\""
    )
    parser.add_argument(
        "--glossaries",
        help='(Optional) Double-quote enclosed, comma-separated list of specific glossary resource names to migrate. If not provided, all glossaries in the project will be migrated. Example: --glossaries="projects/my-project/locations/us/entryGroups/dc_glossary_1/glossaries/glossary_1,projects/my-project/locations/us/entryGroups/dc_glossary_2/glossaries/glossary_2"',
        metavar='"[glossary_1,glossary_2,...]"',
        type=parse_glossary_ids_list,
    )
    parser.add_argument(
        "--resume-import",
        action="store_true",
        help="Skip the export step and resume directly with the import step."
    )


def parse_glossary_ids_list(value: str):
    if not isinstance(value, str):
        raise argparse.ArgumentTypeError(f"Invalid list format: '{value}'")

    items = []
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            continue

        # Keep only the part before "?" to drop query params
        base = raw.split("?", 1)[0]

        # Only keep URLs that look like glossary links
        if "/glossaries/" in base:
            items.append(base)
        else:
            continue
    return items

def parse_entry_url(url: str) -> dict:
    pattern = (
        r"projects/(?P<project>[^/]+)/locations/(?P<location>[^/]+)/"
        r"entryGroups/(?P<entry_group>[^/]+)/entries/(?P<glossary>[^/?#]+)"
    )
    match = re.search(pattern, url)
    if not match:
        raise ValueError("Invalid glossary URL provided. It must contain the pattern: "
                         "projects/.../locations/.../entryGroups/.../entries/...")
    return match.groupdict()

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

def parse_json_line(line: str) -> dict | None:
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None
    
def normalize_linked_resource(linked_resource: str) -> str:
    """Remove the leading forwarded slahes from linkedResource field of the search result."""
    return re.sub(r"^/+", "", linked_resource)

def extract_project_number(glossary_resource_name: str) -> str:
    """Extracts the project number from a Data Catalog resource name."""
    project_match = re.search(r"projects/([^/]+)/", glossary_resource_name)
    extracted_project_number = project_match.group(1) if project_match else ""
    return extracted_project_number

def build_destination_entry_name_with_project_number(glossary_resource_name: str, glossary_resource_name_with_project_number: str) -> str:
    """Constructs the destination entry name with project number."""
    destination_project_number = extract_project_number(glossary_resource_name_with_project_number)
    project_pattern = r"projects/[^/]+/"
    project_with_number = f"projects/{destination_project_number}/"
    updated_entry_name = re.sub(project_pattern, project_with_number, glossary_resource_name, count=1)
    return updated_entry_name
