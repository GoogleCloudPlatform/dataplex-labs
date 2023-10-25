"""Utility functions for the Business Glossary import tool.
"""

import argparse
import os
import sys

import error
import import_mode as import_mode_lib
import logging_utils


logger = logging_utils.get_logger()


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


def configure_argument_parser(parser: argparse.ArgumentParser) -> None:
  """Defines flags and parses arguments related to preprocess_csv().

  Args:
    parser: argparse.ArgumentParser().
  """
  parser.add_argument(
      "terms_csv_legacy",
      help="Path to the CSV file containing the terms data to import.",
      metavar="[Terms CSV file (legacy)]",
      nargs="?",
      type=str,
  )
  parser.add_argument(
      "--project",
      help="ID of Google Cloud Project containing the destination glossary.",
      metavar="<project_id>",
      type=str,
      required=True,
  )
  parser.add_argument(
      "--group",
      help=(
          "Identifier of an existing Entry Group where the target glossary is"
          " located."
      ),
      metavar="<entry_group_id>",
      type=str,
      required=True,
  )
  parser.add_argument(
      "--glossary",
      help=(
          "Identifier of the destination glossary to which data will be"
          " imported."
      ),
      metavar="<glossary_id>",
      type=str,
      required=True,
  )
  parser.add_argument(
      "--location",
      help="Location code where the glossary resource exists.",
      metavar="<location_code>",
      type=str,
      required=True,
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
