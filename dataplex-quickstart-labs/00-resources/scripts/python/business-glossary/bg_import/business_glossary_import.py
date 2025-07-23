import argparse
import sys



import categories_csv_parser
import entry_type as entry_type_lib
import glossary as dc_glossary
import glossary_identification
import import_mode as import_mode_lib
import import_types
import logging_utils
import parser_types
import terms_csv_parser
import user_report
import utils


logger = logging_utils.get_logger()


def main() -> None:
  args = utils.get_arguments()
  utils.validate_args(args)
  import_mode = utils.get_import_mode(args)

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

  parsers_results = _parse_all_csv_files(args)
  _print_parsing_errors(parsers_results)
  if not glossary.is_glossary_empty():
    _handle_non_empty_glossary(import_mode, glossary)

  imported_entries, imported_relations, import_errors = (
      _import_glossary_entries(glossary, parsers_results)
  )
  lines_read = _lines_read(parsers_results)

  user_report.print_report(
      lines_read,
      imported_entries,
      imported_relations,
      import_errors,
  )
  if import_errors:
    logger.warning("Import script execution finalized with some errors.")
    sys.exit(1)


def _lines_read(
    parsers_results: dict[entry_type_lib.EntryType, parser_types._ParserReturnType]
) -> dict[entry_type_lib.EntryType, int]:
  """Returns lines read for each EntryType."""
  lines_read = {}

  for entry_type in [
      entry_type_lib.EntryType.TERM,
      entry_type_lib.EntryType.CATEGORY,
  ]:
    if entry_type in parsers_results:
      _, _, entry_lines_read = parsers_results[entry_type]
      lines_read[entry_type] = entry_lines_read

  return lines_read


def _import_glossary_entries(
    glossary: dc_glossary.Glossary,
    parsers_results: dict[entry_type_lib.EntryType, parser_types._ParserReturnType],
) -> import_types._ImportResult:
  """Unpacks parsed terms and categories and imports them into business glossary.

  Args:
    glossary: glossary object used to import terms and categories
    parsers_results: dictionary indicating parser result for entry type parser
      result consists of parsed_entries list, parse_errors list and line_parsed
      integer.

  Returns:
    A tuple consisting of:
      * dictionary mapping EntryType to list of imported Entries (Terms or
        Categories)
      * dictionary mapping EntryType to list of imported relations
      * lit of import errors
  """

  parsed_terms = None
  parsed_categories = None
  if entry_type_lib.EntryType.TERM in parsers_results:
    parsed_terms, _, _ = parsers_results[entry_type_lib.EntryType.TERM]
  if entry_type_lib.EntryType.CATEGORY in parsers_results:
    parsed_categories, _, _ = parsers_results[entry_type_lib.EntryType.CATEGORY]

  entries_to_import = ""
  if parsed_terms:
    entries_to_import += "terms and "
  if parsed_categories:
    entries_to_import += "categories and "
  entries_to_import = entries_to_import.removesuffix(" and ")

  logger.info(
      "Importing CSV file %s into Business Glossary...", entries_to_import
  )
  return glossary.import_glossary(
      terms=parsed_terms, categories=parsed_categories
  )


def _handle_non_empty_glossary(
    import_mode: import_mode_lib.ImportMode, glossary: dc_glossary.Glossary
) -> None:
  """Handles non-empty glossary depending on the import mode.

  Args:
    import_mode: strict,clear
    glossary: glossary object
  """
  # If the operation mode is Strict, we check that the glossary exists and is
  # empty, otherwise we log an error and finish
  if import_mode == import_mode_lib.ImportMode.STRICT:
    logger.error(
        "Can't proceed with import in strict mode. Please select a more "
        "permissive import mode or provide an empty glossary."
    )
    utils.end_program_execution()

  elif import_mode == import_mode_lib.ImportMode.CLEAR:
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


def _print_parsing_errors(
    parsers_results: dict[entry_type_lib.EntryType, parser_types._ParserReturnType],
) -> None:
  if any_errors(parsers_results):
    for _, parse_errors, _ in parsers_results.values():
      utils.display_parsing_errors(parse_errors)
    
    utils.end_program_execution()


def _parse_all_csv_files(
    args: argparse.Namespace,
) -> dict[entry_type_lib.EntryType, parser_types._ParserReturnType]:
  """Parse all CSV files.

  Args:
    args: script run arguments

  Returns:
    dictionary mapping EntryType to _ParserReturnType (a tuple of list of
    successfully parsed terms, a list of errors and the number of lines we read
    in the CSV file).
  """
  parsers_results = {}
  terms_csv = (
      args.terms_csv if args.terms_csv is not None else args.terms_csv_legacy
  )
  if terms_csv:
    logger.info("Parsing terms input CSV file...")
    parsers_results[entry_type_lib.EntryType.TERM] = (
        terms_csv_parser.parse_glossary_csv(terms_csv)
    )

  if args.categories_csv:
    logger.info("Parsing categories input CSV file...")
    parsers_results[entry_type_lib.EntryType.CATEGORY] = (
        categories_csv_parser.parse_glossary_csv(args.categories_csv)
    )

  if not parsers_results:
    logger.error("Could not parse any records.")
    utils.end_program_execution()

  return parsers_results


def any_errors(
    parsers_results: dict[entry_type_lib.EntryType, parser_types._ParserReturnType]
) -> bool:
  return any([err for _, err, _ in parsers_results.values()])


if __name__ == "__main__":
  main()
