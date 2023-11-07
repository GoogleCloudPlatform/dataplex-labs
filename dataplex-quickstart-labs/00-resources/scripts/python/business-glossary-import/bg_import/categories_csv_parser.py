"""Provides functionality of reading business glossary categories from a CSV file.

Typical usage example:
  categories, errors, lines_read = read_glossary_csv()
"""

import csv
import dataclasses
from typing import Any

import category as bg_category
import entry_type
import error
import parse_utils
import parser_types


"""
  Each attribute parser is represented as a tuple consisting of:
  field_name: Name of the field to parse.
  parser_function: Pointer to a parsing function for the field.
  is_optional_field: Boolean representing if the field is optional.
"""
_ATTRIBUTE_PARSERS: list[tuple[str, parser_types._ParseFn[Any], bool]] = [
    ("display_name", parse_utils.parse_category_str, False),
    ("description", parse_utils.parse_category_str, False),
    ("data_stewards", parse_utils.parse_category_data_stewards, True),
    ("belongs_to_category", parse_utils.parse_category_str, True),
]

_MAX_DISPLAY_NAME_LENGTH = 200
_NON_ALLOWED_DISPLAY_NAME_CHARACTERS = ("\n",)


@dataclasses.dataclass(frozen=True)
class CategoryEntry:
  line: int
  category: bg_category.Category

  # Allow unpacking as a tuple
  def __iter__(self):
    return iter((self.line, self.category))


def parse_glossary_csv(
    path: str,
) -> parser_types._ParserReturnType:
  """Reads CSV file containing business glossary categories.

  Args:
    path: Path of a CSV file to read.

  Returns:
    _ParserReturnType - a tuple of list of successfully parsed categories,
    a list of errors and the number of lines we read in the CSV file.
  """

  categories = {}
  errors = []
  lines_read = 0

  # Set where we track categories that appeared previously in the glossary.
  # Duplicated categories will be recorded as an error.
  tracked_categories = set()
  try:
    with open(path) as csv_file:
      csv_reader = csv.reader(
          csv_file, delimiter=",", quotechar='"', skipinitialspace=True
      )
      for line_idx, record in enumerate(csv_reader):
        if not record:
          continue
        category, category_errors = parse_category(
            line_idx, record, tracked_categories
        )
        if category_errors:
          errors.extend(category_errors)
        else:
          categories[line_idx + 1] = category
        lines_read += 1
  except FileNotFoundError:
    errors.append(
        error.ParseError(
            entry_type.EntryType.CATEGORY, message=f"{path} could not be found."
        )
    )

  return categories, errors, lines_read


def _validate_category(
    category: bg_category.Category, tracked_categories: set[str]
) -> parser_types._ParseErrors:
  """Validates a business glossary category.

  Performs the following tests:
  - The category is unique in the CSV file
  - Display name is not empty
  - Description is not empty

  Args:
    category: Category
    tracked_categories: Set of categories seen so far in the CSV file

  Returns:
    ParseErrors
  """
  errors = []

  # If the category display name is empty we record an error
  if not category.display_name:
    err = error.ParseError(
        entry_type.EntryType.CATEGORY,
        message="The display name for the category is empty.",
        column=1,
    )
    errors.append(err)

  # If the category description is empty we record an error
  if not category.description:
    err = error.ParseError(
        entry_type.EntryType.CATEGORY,
        message="The description for the category is empty.",
        column=2,
    )
    errors.append(err)

  if category.display_name:
    # If the category has appeared before in the CSV file we record an error.
    if category.display_name.lower() in tracked_categories:
      err = error.ParseError(
          entry_type.EntryType.CATEGORY,
          message="The category is duplicated in the CSV file.",
          column=1,
          resources=[category.display_name],
      )
      errors.append(err)

    if len(category.display_name) > _MAX_DISPLAY_NAME_LENGTH:
      err = error.ParseError(
          entry_type.EntryType.CATEGORY,
          message="The category's display name is too big.",
          column=1,
          resources=[category.display_name],
      )
      errors.append(err)

    for character in _NON_ALLOWED_DISPLAY_NAME_CHARACTERS:
      if character in category.display_name:
        err = error.ParseError(
            entry_type.EntryType.CATEGORY,
            message="Unallowed character in display name.",
            column=1,
            resources=[category.display_name],
        )
        errors.append(err)

  return errors


def parse_category(
    line_idx: int, record: list[str], tracked_categories: set[str]
) -> parser_types._ParseResult[bg_category.Category]:
  """Parses a business glossary category.

  Args:
    line_idx: Index of the line where the category appears in the CSV file.
    record: A list of category attributes in order conforming to the CSV file
      schema.
    tracked_categories: Set of previously seen display names.

  Returns:
    A tuple of parsed category and a list of errors.
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
            entry_type.EntryType.CATEGORY,
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
      belongs_to_category,
      *_,
  ) = attributes

  category = bg_category.Category(
      display_name,
      description,
      data_stewards,
      belongs_to_category,
  )

  validation_errors = _validate_category(category, tracked_categories)
  for err in validation_errors:
    err.line = line_idx + 1
    err.record = record
  if category.display_name:
    tracked_categories.add(category.display_name.lower())
  errors.extend(validation_errors)

  return category, errors
