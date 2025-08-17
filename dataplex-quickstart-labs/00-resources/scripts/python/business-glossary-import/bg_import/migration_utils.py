"""
Utility functions specifically for the Business Glossary V2 export script.

This file contains string, ID, and parsing helpers that are used across the
codebase (api_layer, data_transformer, and the main export script). 
"""

import argparse
import re
import subprocess
import sys
from typing import Dict, List, Optional, Tuple

import logging_utils

logger = logging_utils.get_logger()


def _parse_id_list(value: str) -> List[str]:
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
    parser.add_argument("--orgIds", type=_parse_id_list, default=[], help='Comma-separated org IDs, e.g., "123,456"')
    parser.add_argument("--debugging", action="store_true", help="Enable detailed file logging.")
    return parser


def parse_glossary_url(url: str) -> Dict[str, str]:
    """Extracts components from a Data Catalog glossary URL."""
    pattern = (
        r"projects/(?P<project>[^/]+)/locations/(?P<location>[^/]+)/"
        r"entryGroups/(?P<group>[^/]+)/glossaries/(?P<glossary>[^/?#]+)"
    )
    match = re.search(pattern, url)
    if not match:
        logger.error(
            "Invalid --url provided. It must contain the pattern: "
            "projects/.../locations/.../entryGroups/.../glossaries/..."
        )
        sys.exit(1)
    return match.groupdict()


def normalize_id(name: str) -> str:
    """Converts a string to a valid Dataplex ID (lowercase, numbers, hyphens)."""
    if not name:
        return ""
    normalized_name = name.lower().replace(" ", "-")
    normalized_name = re.sub(r"[^a-z0-9\-]", "-", normalized_name)
    normalized_name = re.sub(r"-+", "-", normalized_name)
    return normalized_name.strip("-")
    

def get_entry_id(resource_name: str) -> str:
    """
    Extracts the final entry id from a Data Catalog resource name.

    e.g. "projects/x/locations/y/entryGroups/z/entries/ENTRYID" -> "ENTRYID"
    """
    if not resource_name:
        return ""
    match = re.search(r"entries/([^/]+)$", resource_name)
    return match.group(1) if match else resource_name.split("/")[-1]


def extract_entry_parts(entry_full_name: str) -> Optional[Tuple[str, str, str]]:
    """
    Extracts (project_location, entry_group_id, entry_id) from an entry resource.

    Returns None if the pattern doesn't match.
    """
    pattern = r"(projects/[^/]+/locations/[^/]+)/entryGroups/([^/]+)/entries/([^/]+)"
    match = re.search(pattern, entry_full_name)
    return match.groups() if match else None


def build_entry_key(project_loc: str, entry_group_id: str) -> str:
    """Builds cache key used by API layer for mapping entryGroups to glossary IDs."""
    return f"{project_loc}/entryGroups/{entry_group_id}"


def clean_linked_resource(linked_resource: str) -> str:
    """Removes leading slashes from a linked resource."""
    return linked_resource.lstrip("/") if linked_resource else ""


def update_relative_resource_name(relative_name: str, new_entry_id: str) -> str:
    """Replaces the last 'entries/...' component in a relative resource name."""
    return re.sub(r"entries/[^/]+$", f"entries/{new_entry_id}", relative_name)


def extract_project_location_from_relative(relative_resource_name: str) -> Optional[Tuple[str, str]]:
    """Extracts project and location from a relative resource name, returns (project, location)."""
    match = re.match(r"projects/([^/]+)/locations/([^/]+)/", relative_resource_name)
    if not match:
        return None
    return match.group(1), match.group(2)
