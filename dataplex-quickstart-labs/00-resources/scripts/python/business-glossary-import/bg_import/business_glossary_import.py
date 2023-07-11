import sys

import csv_parser
import glossary as dc_glossary
import glossary_identification
import logging_utils
import user_report
import utils


logger = logging_utils.get_logger()


def main() -> None:
  args = utils.get_arguments()

  # Verify access token is available
  if not utils.access_token_exists():
    logger.error("Environment variable GCLOUD_ACCESS_TOKEN doesn't exist.")
    sys.exit(1)

  # Verify the provided csv file exists
  if not utils.csv_file_exists(args.csv):
    logger.error("The provided CSV file path doesn't exist.")
    sys.exit(1)

  # Create glossary using provided information
  try:
    glossary = dc_glossary.Glossary(
        glossary_identification.GlossaryId(
            project_id=args.project,
            location=args.location,
            entry_group=args.group,
            glossary_id=args.glossary,
        )
    )
  except ValueError:
    logger.error("Can't proceed with import. Please select a valid glossary.")
    utils.end_program_execution()

  logger.info("Parsing input CSV...")
  parser_output, parse_errors, lines_read = csv_parser.parse_glossary_csv(
      args.csv
  )

  if parse_errors:
    utils.display_parsing_errors(parse_errors)
    if args.strict_parsing:
      utils.end_program_execution()

  if not glossary.is_glossary_empty():
    if args.import_mode == "strict":
      logger.error(
          "Can't proceed with import in strict mode. Please select a more "
          "permissive import mode or provide an empty glossary."
      )
      utils.end_program_execution()

    elif args.import_mode == "clear":
      while True:
        confirm = input(
            "Are you sure you want to clear the target glossary? (y)es/(n)o:"
        )
        if confirm.lower() == "yes" or confirm.lower() == "y":
          break
        elif confirm.lower() == "no" or confirm.lower() == "n":
          utils.end_program_execution()

      if not glossary.clear_glossary():
        logger.error("Could not clear the target glossary.")
        utils.end_program_execution()

  logger.info("Importing CSV terms into Business Glossary...")
  imported_terms, imported_relations, import_errors = glossary.import_glossary(
      parser_output
  )

  user_report.print_report(
      lines_read,
      imported_terms,
      imported_relations,
      import_errors
  )
  if import_errors:
    logger.warning("Import script execution finalized with some errors.")
    sys.exit(1)


if __name__ == "__main__":
  main()
