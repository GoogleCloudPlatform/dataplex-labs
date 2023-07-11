"""Utility functions for the Business Glossary import tool.
"""

import argparse
import os
import sys

import error
import logging_utils


logger = logging_utils.get_logger()


def access_token_exists() -> bool:
  return bool(os.environ.get("GCLOUD_ACCESS_TOKEN"))


def csv_file_exists(path: str) -> bool:
  """Verifies if the provided file path exists.

  Args:
    path: Path of the CSV provided by the user.

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


def end_program_execution() -> None:
  logger.warning("Program execution finished ahead of time due to errors.")
  sys.exit(1)


def configure_argument_parser(parser: argparse.ArgumentParser) -> None:
  """Defines flags and parses arguments related to preprocess_csv().

  Args:
    parser: argparse.ArgumentParser().
  """
  parser.add_argument(
      "csv",
      help="Path to the CSV file containing the data to import.",
      metavar="[CSV file]",
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
      "--import-mode",
      choices=["strict", "clear"],
      default="strict",
      type=str,
      help=(
          "Sets level of permissiviness with which the data is imported into"
          " Data Catalog. The default value is \"strict\".:\n"
          "strict\tCheck if the target glossary does not contain any entries,"
          " and if it does, stops executing the program.\n"
          "clear\tRemove all the pre-existing entries in the target glossary"
          " before proceeding with validation and import.\n"
      )
  )
  parser.add_argument(
      "--strict-parsing",
      help=(
          "If set, the program will finish its execution if there are any"
          " parsing errors without importing any terms."
      ),
      action="store_true"
  )


def display_parsing_errors(errors: list[error.ParseError]) -> None:
  for err in errors:
    logger.error(err.to_string())
