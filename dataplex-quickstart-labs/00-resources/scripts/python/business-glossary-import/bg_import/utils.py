"""Utility functions for the Business Glossary import tool.
"""

import argparse
import os
import sys
import error
import import_mode as import_mode_lib
import logging_utils
from typing import Any, List, Dict
import api_call_utils
import requests
import re
import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed


logger = logging_utils.get_logger()
DATACATALOG_BASE_URL = "https://datacatalog.googleapis.com/v2"
DATAPLEX_BASE_URL = "https://dataplex.googleapis.com/v1"
PAGE_SIZE = 1000
MAX_WORKERS = 20

def access_token_exists() -> bool:
  return bool(os.environ.get("GCLOUD_ACCESS_TOKEN"))


def csv_file_exists(path: str) -> bool:
  """Verifies if the provided file path exists.

  Args:
    path: Path of the CSV file provided by the user.

  Returns:
    Boolean value indicating whether the file exists in the filesystem.
  """
  return os.path.isfile(path)


def get_arguments() -> argparse.Namespace:
  """Gets arguments for the program.

  Returns:
    Namespace object containing the program arguments.
  """
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawTextHelpFormatter
  )
  configure_argument_parser(parser)
  return parser.parse_args()


def get_import_mode(args: argparse.Namespace) -> import_mode_lib.ImportMode:
  modes = [mode.value for mode in import_mode_lib.ImportMode]
  import_mode = vars(args).get("import_mode")

  if import_mode and import_mode.lower() in modes:
    return import_mode_lib.ImportMode(import_mode.lower())

  return import_mode_lib.ImportMode.STRICT


def end_program_execution() -> None:
  logger.warning("Program execution finished ahead of time due to errors.")
  sys.exit(1)

def glossary_argument_parser(parser: argparse.Namespace) -> None:
    """
    Parses the arguments related to the glossary.
    
    Args: 
    parser: argparse.ArgumentParser().
    """
    parser.add_argument(
      "--project",
      help="ID of Google Cloud Project containing the destination glossary.",
      metavar="<project_id>",
      type=str,
    )
    parser.add_argument(
        "--group",
        help=(
            "Identifier of an existing Entry Group where the target glossary is"
            " located."
        ),
        metavar="<entry_group_id>",
        type=str,
    )
    parser.add_argument(
        "--glossary",
        help=(
            "Identifier of the destination glossary to which data will be"
            " imported."
        ),
        metavar="<glossary_id>",
        type=str,
    )
    parser.add_argument(
        "--location",
        help="Location code where the glossary resource exists.",
        metavar="<location_code>",
        type=str,
    )

def configure_argument_parser(parser: argparse.ArgumentParser) -> None:
  """Defines flags and parses arguments related to preprocess_csv().

  Args:
    parser: argparse.ArgumentParser().
  """
  
  glossary_argument_parser(parser)
  parser.add_argument(
      "terms_csv_legacy",
      help="Path to the CSV file containing the terms data to import.",
      metavar="[Terms CSV file (legacy)]",
      nargs="?",
      type=str,
  )
  parser.add_argument(
      "--categories-csv",
      help="Path to the CSV file containing the categories data to import.",
      metavar="[Categories CSV file]",
      type=str,
  )
  parser.add_argument(
      "--terms-csv",
      help="Path to the CSV file containing the terms data to import.",
      metavar="[Terms CSV file]",
      type=str,
  )
  parser.add_argument(
      "--import-mode",
      choices=["strict", "clear"],
      default="strict",
      type=str,
      help=(
          "Sets level of permissiviness with which the data is imported into"
          ' Data Catalog. The default value is "strict".:\n'
          "strict\tCheck if the target glossary does not contain any entries,"
          " and if it does, stops executing the program.\n"
          "clear\tRemove all the pre-existing entries in the target glossary"
          " before proceeding with validation and import.\n"
      )
  )


def display_parsing_errors(errors: list[error.ParseError]) -> None:
  for err in errors:
    logger.error(err.to_string())


def validate_args(args: argparse.Namespace) -> None:
  """Validates script run arguments.

  Args:
    args: script run arguments
  """

  # Verify access token is available
  if not access_token_exists():
    logger.error("Environment variable GCLOUD_ACCESS_TOKEN doesn't exist.")
    sys.exit(1)

  # Verify that at least one csv parameter is provided
  if (
      not args.terms_csv_legacy
      and not args.categories_csv
      and not args.terms_csv
  ):
    logger.error("At least one csv filepath parameter must be provided.")
    sys.exit(1)

  # Verify only one terms csv is provided:
  if args.terms_csv and args.terms_csv_legacy:
    logger.error(
        "Only one of the following can be provided: --terms-csv or"
        " terms_csv-legacy."
    )
    exit(1)

  # Warn users when legacy terms csv argument is used.
  if args.terms_csv_legacy:
    logger.warning(
        "Terms CSV file was passed in a legacy way. Terms CSV file should be"
        " passed in --terms-csv argument."
    )

  _verify_csv_file_existence(args, "terms_csv_legacy")
  _verify_csv_file_existence(args, "terms_csv", prefix="--")
  _verify_csv_file_existence(args, "categories_csv", prefix="--")


def _verify_csv_file_existence(
    args: argparse.Namespace, arg_name: str, prefix: str = ""
):
  """Logs an error if the provided CSV file path doesn't exist.

  Args:
    args: script run arguments
    arg_name: CSV file path argument
    prefix: argument prefix e.g. for --terms_csv prefix="--"
  """
  file_path = vars(args).get(arg_name)
  if file_path and not csv_file_exists(file_path):
    logger.error(
        f"The CSV file path provided for {prefix}{arg_name} doesn't exist."
    )
    sys.exit(1)

def get_export_arguments() -> argparse.Namespace:
    """Gets arguments for the export program.

    Returns:
        Namespace object containing the export program arguments.
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    glossary_argument_parser(parser)
    configure_export_argument_parser(parser)
    return parser.parse_args()

def configure_export_argument_parser(parser: argparse.ArgumentParser) -> None:
    """Defines flags and parses arguments related to export.

    Args:
        parser: argparse.ArgumentParser().
    """
    parser.add_argument(
        "--categories-csv",
        help="Path to the CSV file to export the categories data.",
        metavar="[Categories CSV file for export]",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--terms-csv",
        help="Path to the CSV file to export the terms data.",
        metavar="[Terms CSV file for export]",
        type=str,
        required=True,
    )

def parse_glossary_url(url: str) -> dict:
    pattern = (
        r"projects/(?P<project>[^/]+)/locations/(?P<location>[^/]+)/"
        r"entryGroups/(?P<entry_group>[^/]+)/glossaries/(?P<glossary>[^/?#]+)"
    )
    match = re.search(pattern, url)
    if not match:
        raise ValueError("Invalid glossary URL provided. It must contain the pattern: "
                         "projects/.../locations/.../entryGroups/.../glossaries/...")
    return match.groupdict()

def maybe_override_args_from_url(args):
    if hasattr(args, "url") and args.url:
        try:
            extracted = parse_glossary_url(args.url)
            if args.project or args.location or args.group or args.glossary:
                logger.warning(
                    "Glossary parameters were provided via both URL and individual flags. "
                    "Using values extracted from --url and overriding --project, --location, --group, and --glossary."
                )
            args.project = extracted["project"]
            args.location = extracted["location"]
            args.group = extracted["entry_group"]
            args.glossary = extracted["glossary"]
        except ValueError as ve:
            logger.error(str(ve))
            sys.exit(1)


def parse_id_list(value):
        if not isinstance(value, str):
            raise argparse.ArgumentTypeError(f"Invalid list format: '{value}'. --org-ids=\"123,789\".")
        items = [item.strip() for item in value.split(',') if item.strip()]
        return items

def validate_export_args(args: argparse.Namespace) -> None:
    """Validates script run arguments for exporting.

    Args:
        args: script run arguments
    """
    if not args.categories_csv or not args.terms_csv:
        logger.error("Both --categories-csv and --terms-csv arguments must be provided for export.")
        sys.exit(1)

    if not os.path.isdir(os.path.dirname(args.categories_csv)):
        logger.error(f"Directory for categories CSV export path does not exist: {args.categories_csv}")
        sys.exit(1)

    if not os.path.isdir(os.path.dirname(args.terms_csv)):
        logger.error(f"Directory for terms CSV export path does not exist: {args.terms_csv}")
        sys.exit(1)


def get_export_v2_arguments() -> argparse.Namespace:
    """
    Gets arguments for the export v2 program.
    Returns:
        Namespace object containing the export v2  program arguments.
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    configure_export_v2_arg_parser(parser)
    return parser.parse_args()

def configure_export_v2_arg_parser(parser: argparse.ArgumentParser) -> None:
    """
    Defines flags and parses arguments related to the export v2.
    For the JSON export, we require a project, group, glossary,
    location, and output JSON file path.
    """
    glossary_argument_parser(parser)

    parser.add_argument(
        "--user-project",
        help=(
            "Google Cloud Project ID to use for billing purposes when exporting data."
        ),
        metavar="[User Project ID]",
        type=str,
    )
    parser.add_argument(
        "--export-mode",
        choices=["glossary_only", "entry_links_only", "all"],
        default="all",
        type=str,
        help=(
            "Sets the export mode for the data:\n"
            "glossary\tExport only the glossary entries to the specified JSON file.\n"
            "entry_links\tExport only the entry links to the specified JSON file.\n"
            "all\tExport both the glossary entries and entry links to the specified JSON files.\n"
        )
    )

    parser.add_argument(
        "--entrylinktype",
        help=(
            "Filter entry links by type. Options (comma-separated, braces optional):\n"
            "  synonym    → synonym links only\n"
            "  related    → related links only\n"
            "  definition → definition (term-entry) links only\n"
            "If omitted, exports all link types."
        ),
        default=None,
        type=str,
    )

    parser.add_argument(
    "--url",
    help=(
        "Full Glossary URL.\n"
        "Supports both internal and external formats like:\n"
        "https://console.cloud.google.com/dataplex/glossaries/projects/PROJECT_ID/locations/LOC/entryGroups/ENTRY_GROUP/glossaries/GLOSSARY"
    ),
    metavar="[Glossary URL]",
    type=str
    )

    parser.add_argument(
        "--orgIds",
        type=parse_id_list,  
        default=[],
        help="A list of org IDs enclosed in brackets. Delimiters can be spaces or commas. Example: --org-ids=\"[id1,id2 id3]\""
    )

    parser.add_argument(
        "--testing",
        metavar="<true>",
        type=lambda x: x.lower() == "true",
        default=False,
        help="If true, use staging environment instead of prod"
    )

    parser.add_argument(
        "--debugging",
        action="store_true",
        help="If set, enables detailed logging to logs.txt in the current directory."
    )


def validate_export_v2_args(args: argparse.Namespace) -> None:
    """
    Validates script run arguments for the export v2.
    Args:
        args: Parsed script run arguments.
    """
    # Check mutual requirement: either --url or all of project, location, group, glossary
    if not args.url:
        missing = [flag for flag in ["project", "location", "group", "glossary"] if not getattr(args, flag)]
        if missing:
            logger.error(
                f"You must either provide --url OR all of the following flags: --project, --location, --group, --glossary. Missing: {', '.join(missing)}"
            )
            sys.exit(1)


def fetch_relationships(entry_name: str, user_project: str) -> List[Dict[str, Any]]:
    """Fetches relationships for a specific entry from the Data Catalog.

    Args:
        entry_name: The full resource name of the entry.
        project: The Google Cloud Project ID.

    Returns:
        A list of dictionaries containing the relationships.
    """
    fetch_relationships_url = (
        DATACATALOG_BASE_URL + f"/{entry_name}/relationships?view=FULL"
    )

    response = api_call_utils.fetch_api_response(
        requests.get, fetch_relationships_url, user_project
    )
    if response["error_msg"]:
        logger.error(f"Error fetching relationships: {response['error_msg']}")
        sys.exit(1)
    return response["json"].get("relationships", [])

def fetch_all_relationships(
    entries: List[Dict[str, Any]], user_project: str,project: str, max_workers: int = MAX_WORKERS
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetches relationships for all entries concurrently, processing in batches."""
    relationships_data = {}
    chunk_size = max_workers
    num_batches = math.ceil(len(entries) / chunk_size)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for batch in range(num_batches):
            start = batch * chunk_size
            end = start + chunk_size
            entries_batch = entries[start:end]
            future_to_entry = {
                executor.submit(fetch_relationships, entry["name"], user_project): entry[
                    "name"
                ]
                for entry in entries_batch
            }

            for future in as_completed(future_to_entry):
                entry_name = future_to_entry[future]
                try:
                    relationships_data[entry_name] = future.result()
                except Exception as exc:
                    logger.error(
                        f"Error fetching relationships for {entry_name}: {exc}"
                    )

    return relationships_data


def fetch_entries(
    user_project: str, project: str, location: str, entry_group: str
) -> List[Dict[str, Any]]:
    """Fetches all entries in the glossary.

    Args:
        project: The Google Cloud Project ID.
        location: The location of the glossary.
        entry_group: The entry group of the glossary.

    Returns:
        A list of dictionaries containing the entries.
    """
    entries = []
    get_full_entry_url = (
        DATACATALOG_BASE_URL
        + f"/projects/{project}/locations/{location}/entryGroups/{entry_group}/entries?view=FULL&pageSize={PAGE_SIZE}"
    )
    response = api_call_utils.fetch_api_response(
        requests.get, get_full_entry_url, user_project
    )

    if response["error_msg"]:
        logger.error(
        f"Can't proceed with export. Details: {response['error_msg']}"
        )
        sys.exit(1)

    with ThreadPoolExecutor() as executor:
        futures = []
        page_token = None

        while True:
            if page_token:
                endpoint_url = f"{get_full_entry_url}&pageToken={page_token}"
            else:
                endpoint_url = get_full_entry_url

            future = executor.submit(
                api_call_utils.fetch_api_response, requests.get, endpoint_url, user_project
            )
            futures.append(future)

            # Wait for the current future to complete and process its results
            for future in as_completed(futures):
                response = future.result()
                if response["error_msg"]:
                    raise ValueError(response["error_msg"])
                if "entries" in response["json"]:
                    entries.extend(response["json"]["entries"])
                page_token = response["json"].get("nextPageToken", None)
                if not page_token:
                    return entries
            # clear the futures list to avoid any memory build-up
            futures = []

def normalize_glossary_id(glossary: str) -> str:
    """Converts a string to a valid Dataplex glossary_id (lowercase, numbers, hyphens only)."""
    glossary_id = glossary.lower()
    glossary_id = re.sub(r"[ _]", "-", glossary_id)
    glossary_id = re.sub(r"[^a-z0-9\-]", "-", glossary_id)
    glossary_id = re.sub(r"-+", "-", glossary_id)
    glossary_id = glossary_id.strip("-")
    return glossary_id

def replace_with_new_glossary_id(file_path, glossary_id: str) -> None:
    new_glossary_id = normalize_glossary_id(glossary_id)
    pattern = re.compile(rf"glossaries/{re.escape(glossary_id)}")
    with open(file_path, "r") as file:
        content = file.read()
    new_content = pattern.sub(f"glossaries/{new_glossary_id}", content)
    with open(file_path, "w") as file:
        file.write(new_content)

def create_glossary(
    user_project: str,
    project: str,
    location: str,
    entry_group: str,
    glossary: str
) -> None:
    """Creates a new Dataplex glossary."""
    catalog_url = (
        f"{DATACATALOG_BASE_URL}/projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{glossary}"
    )
    glossary_id = normalize_glossary_id(glossary)
    dataplex_post_url = f"{DATAPLEX_BASE_URL}/projects/{project}/locations/global/glossaries?glossary_id={glossary_id}"
    dataplex_get_url = f"{DATAPLEX_BASE_URL}/projects/{project}/locations/global/glossaries/{glossary_id}"

    datacatalog_response = api_call_utils.fetch_api_response(
        requests.get, catalog_url, user_project
    )
    if datacatalog_response["error_msg"]:
        logger.warning(f"Failed to fetch Data Catalog entry:\n  {datacatalog_response['error_msg']}")
        sys.exit(1)

    display_name = datacatalog_response["json"].get("displayName", "")

    logger.info("Creating dataplex business glossary...")
    request_body = {"displayName": display_name}
    dp_resp = api_call_utils.fetch_api_response(
        requests.post, dataplex_post_url, user_project, request_body
    )
    if dp_resp["error_msg"]:
        logger.warning(f"Error creating Dataplex glossary:\n  {dp_resp['error_msg']}")
        sys.exit(1)

    time.sleep(30)
    glossary_creation_response = api_call_utils.fetch_api_response(
        requests.get, dataplex_get_url, user_project
    )
    if glossary_creation_response["error_msg"]:
        logger.warning(
            f"Error occurred while creating the glossary {glossary_creation_response['error_msg']}. Please try again manually."
        )
        sys.exit(1)

    logger.info(f"Dataplex glossary created successfully: {glossary_creation_response['json'].get('name', '')}")

def get_project_id(project:str, user_project:str) -> str:
    """Fetches the project ID from the project number."""
    url = f"https://cloudresourcemanager.googleapis.com/v3/projects/{project}"
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    if response["error_msg"]:
        logger.error(f"Failed to fetch project ID: {response['error_msg']}")
        sys.exit(1)
    return response["json"].get("projectId", "")