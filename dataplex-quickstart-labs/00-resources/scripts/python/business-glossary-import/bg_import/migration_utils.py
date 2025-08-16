"""
Utility functions specifically for the Business Glossary V2 export script.
"""
import argparse
import re
import sys
import subprocess
import logging_utils
from typing import Dict, List

logger = logging_utils.get_logger()

def _parse_id_list(value: str) -> list[str]:
    """Helper to parse comma-separated string lists from argparse."""
    if not isinstance(value, str):
        raise argparse.ArgumentTypeError(f"Invalid list format: '{value}'.")
    return [item.strip() for item in value.split(',') if item.strip()]

def get_org_ids_from_gcloud() -> List[str]:
    """Uses gcloud to fetch all organization IDs the user can see."""
    try:
        result = subprocess.run(
            ["gcloud", "organizations", "list", "--format=value(ID)"],
            capture_output=True, text=True, check=True
        )
        org_ids = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
        if not org_ids:
            logger.error("gcloud found no organization IDs.")
            sys.exit(1)
        return org_ids
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error(f"Error fetching organization IDs via gcloud: {e}. "
                     "Please pass IDs via --orgIds or ensure gcloud is authenticated.")
        sys.exit(1)

def get_export_arguments() -> argparse.ArgumentParser:
    """Creates the argument parser for the export script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="Export a Data Catalog Business Glossary."
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
        logger.error("Invalid --url provided. It must contain the pattern: "
                     "projects/.../locations/.../entryGroups/.../glossaries/...")
        sys.exit(1)
    return match.groupdict()

def normalize_id(name: str) -> str:
    """Converts a string to a valid Dataplex ID (lowercase, numbers, hyphens)."""
    if not name: return ""
    normalized_name = name.lower().replace(" ", "-")
    normalized_name = re.sub(r"[^a-z0-9\-]", "-", normalized_name)
    normalized_name = re.sub(r"-+", "-", normalized_name)
    return normalized_name.strip("-")