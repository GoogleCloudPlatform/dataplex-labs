"""User report generation."""
import error
import glossary
import logging_utils
import term as bg_term


def print_report(csv_terms: int,
                 terms_created: list[bg_term.Term],
                 relations_created: list[
                     tuple[str, str, glossary.RelationshipType]
                 ],
                 errors: list[error.TermImportError]) -> None:
  """Implements the User Report generated after Term import.

  The user report contains information on errors that happened during import as
  well as aggregated data.
  Error messages contain:
    - The line and column in the CSV input file that generates the issue.
    - The display name of the term(s) and/or data assets involved in the error.
    - The operation that generated the issue.
    - A useful error message explaining the issue in detail.

  Errors per category should be also recorded and summarized to
  the user:
  - Successful term creations in the format
    (successful_term_imports / total_term_imports)

  Args:
    csv_terms: int
      Number of terms in the original CSV.
    terms_created: list[Term]
      List of all terms that were successfully created.
    relations_created: list[tuple[str, str, RelationshipType]]
      List of all relations successfully created represented as the
      source and destination assets and the relationship type.
    errors: list[TermImportError]
      List of all error instances that happened during term import.
  """
  logger = logging_utils.get_logger()
  if errors:
    logger.info("Import error report:")
    for err in errors:
      logger.error(err.to_string())

  if terms_created:
    logger.info("Terms successfully imported:")
    for term in sorted(terms_created, key=lambda t: t.display_name):
      logger.info(f"\t- {term.display_name}")

  if relations_created:
    logger.info("Relationships successfully imported:")
    for src, dst, relation_type in relations_created:
      logger.info(f"\t- {src} {relation_type.value} {dst}")

  logger.info("Statistics of Imported terms:")
  logger.info(f"Terms successfully created: {len(terms_created)}/"
              f"{csv_terms}.")
