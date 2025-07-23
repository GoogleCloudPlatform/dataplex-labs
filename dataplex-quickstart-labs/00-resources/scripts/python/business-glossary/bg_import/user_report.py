"""User report generation."""
import category as bg_category
import entry_type as entry_type_lib
import error
import logging_utils
import relation_type
import term as bg_term

logger = logging_utils.get_logger()
entries_name_type_pairs = [
    ("terms", entry_type_lib.EntryType.TERM),
    ("categories", entry_type_lib.EntryType.CATEGORY),
]


def print_report_for_erroneous_categories_import(
    imported_categories: list[bg_category.Category],
    categories_import_errors: list[error.EntryImportError],
) -> None:

  logger.info("Categories import errors:")
  for err in categories_import_errors:
    logger.error(err.to_string())
  _print_imported_categories(imported_categories)


def print_report(
    lines_read: dict[entry_type_lib.EntryType, int],
    imported_entries: dict[
      entry_type_lib.EntryType, list[bg_term.Term | bg_category.Category]
    ],
    imported_relations: dict[
      entry_type_lib.EntryType,
      list[tuple[str, str, relation_type.RelationshipType]],
    ],
    import_errors: list[error.EntryImportError],
) -> None:
  """Implements the User Report generated after Term and Categories import.

  The user report contains information on errors that happened during import as
  well as aggregated data.
  Error messages contain:
    - The line and column in the CSV input file that generates the issue.
    - The display name of the entry(ies) and/or data assets involved in the error.
    - The operation that generated the issue.
    - A useful error message explaining the issue in detail.

  Errors per category should be also recorded and summarized to
  the user:
  - Successful term creations in the format
    (successful_term_imports / total_term_imports)
  - Successful categories creations in the format
    (successful_term_imports / total_term_imports)

  Args:
    lines_read: dict[entry_type_lib.EntryType, int] A dictionary mapping entry
      type (Term, Category) to number of entries in the original CSV file.
    imported_entries: dict[entry_type_lib.EntryType, list[bg_term.Term |
      bg_category.Category]] A dictionary mapping entry type to list of all
      entries (Terms or Categories) that were successfully created.
    imported_relations: A dictionary mapping entry type to list[tuple[str, str,
      RelationshipType]] - list of all relations successfully created
      represented as the source and destination assets and the relationship
      type.
    import_errors: list[TermImportError] List of all error instances that
      happened during term import.
  """
  _print_import_errors(import_errors)
  _print_imported_entries(imported_entries)
  _print_imported_relations(imported_relations)
  _print_statistics(lines_read, imported_entries)


def _print_import_errors(import_errors: list[error.EntryImportError]) -> None:
  if import_errors:
    logger.info("Import errors report:")
    for err in import_errors:
      logger.error(err.to_string())


def _print_imported_entries(
    imported_entries: dict[
      entry_type_lib.EntryType, list[bg_term.Term | bg_category.Category]
    ]
) -> None:
  """Prints the imported entries (terms and categories).

  Args:
    imported_entries: dict[entry_type_lib.EntryType, list[bg_term.Term |
      bg_category.Category]] A dictionary mapping entry type to list of all
      entries (Terms or Categories) that were successfully created.
  """
  for entries_name, entry_type in entries_name_type_pairs:
    if entry_type in imported_entries:
      logger.info(f"{entries_name.capitalize()} successfully imported:")
      for entry in sorted(
          imported_entries[entry_type], key=lambda t: t.display_name
      ):
        logger.info(f'\t- "{entry.display_name}"')


def _print_imported_categories(
    imported_categories: list[bg_category.Category],
) -> None:
  logger.info("Categories successfully imported:")
  for entry in sorted(imported_categories, key=lambda t: t.display_name):
    logger.info(f'\t- "{entry.display_name}"')


def _print_imported_relations(
    imported_relations: dict[
      entry_type_lib.EntryType,
      list[tuple[str, str, relation_type.RelationshipType]],
    ]
) -> None:
  """Prints the imported relationships.

  Args:
    imported_relations: A dictionary mapping entry type to list[tuple[str, str,
      RelationshipType]] - list of all relations successfully created
      represented as the source and destination assets and the relationship
      type.
  """
  for entries_name, entry_type in entries_name_type_pairs:
    if entry_type in imported_relations and imported_relations[entry_type]:
      logger.info(
          f"Relationships successfully imported from {entries_name} csv:"
      )
      for src, dst, relationship_type in imported_relations[entry_type]:
        logger.info(f'\t- "{src}" {relationship_type.value} "{dst}"')


def _print_statistics(
    lines_read: dict[entry_type_lib.EntryType, int],
    imported_entries: dict[
      entry_type_lib.EntryType, list[bg_term.Term | bg_category.Category]
    ],
) -> None:
  """Prints statistics about imported entries.

  Args:
    lines_read: dict[entry_type_lib.EntryType, int] A dictionary mapping entry
      type (Term, Category) to number of entries in the original CSV file.
    imported_entries: dict[entry_type_lib.EntryType, list[bg_term.Term |
      bg_category.Category]] A dictionary mapping entry type to list of all
      entries (Terms or Categories) that were successfully created.
  """
  for entries_name, entry_type in entries_name_type_pairs:
    if entry_type in lines_read and entry_type in imported_entries:
      imported_entries_count = len(imported_entries[entry_type])
      parsed_entries_count = lines_read[entry_type]
      logger.info(f"Statistics of imported {entries_name}:")
      logger.info(
          f"{entries_name.capitalize()} successfully created:"
          f" {imported_entries_count}/{parsed_entries_count}."
      )
