"""
Utility functions for handling command-line arguments in the EntryLink export script.
"""

import argparse
import re
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

def extract_glossary_info_from_url(glossary_url: str) -> Tuple[str, str, str]:
    """Extract project ID, location, and glossary ID from Dataplex glossary resource name."""
    logger.debug(f"Extracting glossary info from resource name: {glossary_url}")
    
    # Pattern to match the glossary resource name
    pattern = r'projects/([^/]+)/locations/([^/]+)/glossaries/([^/?&#]+)'
    
    match = re.search(pattern, glossary_url)
    if not match:
        raise ValueError(
            f"Invalid glossary resource name format. Expected pattern: "
            f"'projects/PROJECT_ID/locations/LOCATION/glossaries/GLOSSARY_ID'. Got: {glossary_url}"
        )
    
    project_id = match.group(1)
    location = match.group(2)
    glossary_id = match.group(3)
    
    logger.debug(f"Extracted: project_id={project_id}, location={location}, glossary_id={glossary_id}")
    return project_id, location, glossary_id

def configure_export_entrylinks_argument_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Create and return the command line argument parser."""
    parser.add_argument(
        "--glossary-url",
        required=True,
        help="Dataplex Glossary URL to export EntryLinks"
    )
    parser.add_argument(
        "--spreadsheet-url",
        required=True,
        help="Google Sheets URL to export EntryLinks data"
    )
    parser.add_argument(
        "--user-project",
        required=True,
        help="Project ID to use for billing and API quota (e.g., 'my-project-id')"
    )
    return parser

def get_export_entrylinks_arguments(argv=None) -> argparse.Namespace:
    """Gets arguments for the entry links export program."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    configure_export_entrylinks_argument_parser(parser)
    return parser.parse_args(argv)


def get_import_entrylinks_arguments(argv=None) -> argparse.Namespace:
    """Gets arguments for the entry links import program."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    configure_import_entrylinks_argument_parser(parser)
    return parser.parse_args(argv)


def configure_import_entrylinks_argument_parser(parser):
    parser.add_argument(
        "--spreadsheet-url",
        required=True,
        help="Google Sheets URL containing EntryLinks to import"
    )
    parser.add_argument(
        "--buckets",
        help="Comma-separated list of GCS bucket ids. Example: --buckets=\"bucket-1,bucket-2\"",
        type=lambda s: [item.strip() for item in s.split(",") if item.strip()],
        required=True,
        metavar="[bucket-1,bucket-2,...]"
    )
    parser.add_argument(
        "--user-project",
        required=True,
        help="Project ID to use for billing and API quota (e.g., 'my-project-id')"
    )