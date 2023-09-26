"""Provides functionality of reading business glossary terms from a CSV file.

Typical usage example:
  terms, errors, lines_read = read_glossary_csv()
"""

import csv
import dataclasses
from typing import Any

import entry_type
import error
import parse_utils
import parser_types
import term as bg_term


"""Each attribute parser is represented as a tuple consisting of:
  field_name: Name of the field to parse.
  parser_function: Pointer to a parsing function for the field.
  is_optional_field: Boolean representing if the field is optional.
"""
_ATTRIBUTE_PARSERS: list[tuple[str, parser_types._ParseFn[Any], bool]] = [
    ("display_name", parse_utils.parse_term_str, False),
    ("description", parse_utils.parse_term_str, False),
    ("data_stewards", parse_utils.parse_term_data_stewards, True),
    ("tagged_assets", parse_utils.parse_list, True),
    ("synonyms", parse_utils.parse_list, True),
    ("relations", parse_utils.parse_list, True),
    ("belongs_to_category", parse_utils.parse_term_str, True),
]


_MAX_DISPLAY_NAME_LENGTH = 200
_NON_ALLOWED_DISPLAY_NAME_CHARACTERS = ("\n",)


@dataclasses.dataclass(frozen=True)
class TermEntry:
  line: int
  term: bg_term.Term

  # Allow unpacking as a tuple
  def __iter__(self):
    return iter((self.line, self.term))


def parse_glossary_csv(
    path: str,
) -> parser_types._ParserReturnType:
  """Reads CSV file containing business glossary terms.

  Args:
    path: Path of a CSV file to read.

  Returns:
    _ParserReturnType - a tuple of list of successfully parsed terms,
    a list of errors and the number of lines we read in the CSV.
  """

  terms = {}
  errors = []
  lines_read = 0

  # Set where we track terms that appeared previously in the glossary.
  # Duplicated terms will be recorded as an error.
  tracked_terms = set()
  try:
    with open(path) as csv_file:
      csv_reader = csv.reader(
          csv_file, delimiter=",", quotechar='"', skipinitialspace=True
      )
      for line_idx, record in enumerate(csv_reader):
        if not record:
          continue
        term, term_errors = parse_term(line_idx, record, tracked_terms)
        if term_errors:
          errors.extend(term_errors)
        else:
          terms[line_idx + 1] = term
        lines_read += 1
  except FileNotFoundError:
    errors.append(
        error.ParseError(
            entry_type.EntryType.TERM, message=f"{path} could not be found."
        )
    )

  return terms, errors, lines_read


def _validate_term(
    term: bg_term.Term, tracked_terms: set[str]
) -> parser_types._ParseErrors:
  """Validates a business glossary term.

  Performs the following tests:
  - The term is unique in the CSV
  - Display name is not empty
  - Description is not empty

  Args:
    term: Term
    tracked_terms: Set of terms seen so far in the CSV

  Returns:
    ParseErrors
  """
  errors = []

  # If the term display name is empty we record an error
  if not term.display_name:
    err = error.ParseError(
        entry_type.EntryType.TERM,
        message="The display name for the term is empty.",
        column=1,
    )
    errors.append(err)

  # If the term description is empty we record an error
  if not term.description:
    err = error.ParseError(
        entry_type.EntryType.TERM,
        message="The description for the term is empty.",
        column=2,
    )
    errors.append(err)

  if term.display_name:
    # If the term has appeared before in the CSV we record an error.
    if term.display_name.lower() in tracked_terms:
      err = error.ParseError(
          entry_type.EntryType.TERM,
          message="The term is duplicated in the CSV.",
          column=1,
          resources=[term.display_name],
      )
      errors.append(err)

    if len(term.display_name) > _MAX_DISPLAY_NAME_LENGTH:
      err = error.ParseError(
          entry_type.EntryType.TERM,
          message="The term's display name is too big.",
          column=1,
          resources=[term.display_name],
      )
      errors.append(err)

    for character in _NON_ALLOWED_DISPLAY_NAME_CHARACTERS:
      if character in term.display_name:
        err = error.ParseError(
            entry_type.EntryType.TERM,
            message="Unallowed character in display name.",
            column=1,
            resources=[term.display_name],
        )
        errors.append(err)

  return errors


def parse_term(
    line_idx: int, record: list[str], tracked_terms: set[str]
) -> parser_types._ParseResult[bg_term.Term]:
  """Parses a business glossary term.

  Args:
    line_idx: Index of the line where the term appears in the CSV.
    record: A list of term attributes in order conforming to the CSV schema.
    tracked_terms: Set of previously seen display names.

  Returns:
    A tuple of parsed term and a list of errors.
  """
  attributes = []
  errors = []

  for i, (attr_name, parse_fn, is_optional_field) in enumerate(
      _ATTRIBUTE_PARSERS
  ):
    if i >= len(record):
      # Add the default value to cover for the missing field
      default_value, _ = parse_fn("")  # pylint:disable=not-callable
      attributes.append(default_value)
      # If the field is not mandatory we can skip creating a ParseError
      if not is_optional_field:
        err = error.ParseError(
            entry_type.EntryType.TERM,
            message="Missing field",
            line=line_idx + 1,
            column=i + 1,
            record=record,
            resources=[attr_name],
        )
        errors.append(err)
      continue

    value, attr_errors = parse_fn(record[i])  # pylint:disable=not-callable
    attributes.append(value)
    for err in attr_errors:
      err.line = line_idx + 1
      err.column = i + 1
      err.record = record
      err.resources.append(attr_name)
    errors.extend(attr_errors)

  (
      display_name,
      description,
      data_stewards,
      tagged_assets,
      synonyms,
      related_terms,
      belongs_to_category,
      *_,
  ) = attributes

  term = bg_term.Term(
      display_name,
      description,
      data_stewards,
      tagged_assets,
      synonyms,
      related_terms,
      belongs_to_category,
  )

  validation_errors = _validate_term(term, tracked_terms)
  for err in validation_errors:
    err.line = line_idx + 1
    err.record = record
  if term.display_name:
    tracked_terms.add(term.display_name.lower())
  errors.extend(validation_errors)

  return term, errors
