"""Utility functions to parse each type of field in terms_csv_parser.py."""

import re
from typing import TypeVar

import entry_type as entry_type_lib
import error


_ParseErrors = list[error.ParseError]
_T = TypeVar("_T")
_ParseResult = tuple[_T, _ParseErrors]


def parse_category_str(s: str) -> _ParseResult[str | None]:
  return _parse_str(entry_type_lib.EntryType.CATEGORY, s)


def parse_term_str(s: str) -> _ParseResult[str | None]:
  return _parse_str(entry_type_lib.EntryType.TERM, s)


def _parse_str(
    entry_type: entry_type_lib.EntryType, s: str
) -> _ParseResult[str | None]:
  """Parses a single string.

  The parsed string might optionally be enclosed between double
  quotes (""), and we don't allow for line breaks and other control characters
  in the string.

  Args:
    entry_type: enum indicating parsed entry type (e.g. CATEGORY or TERM).
    s: input string.

  Returns:
    ParseResult with the parsed string, with any unnecessary spaces removed,
      if any, or None otherwise.
  """
  match = re.fullmatch(r'"[^"]*"|[^*]*', s)
  if match is None:
    return None, [error.ParseError(entry_type, f"Error parsing field {s}")]
  return match.group(0).strip('"').strip(), []


def parse_category_data_stewards(s: str) -> _ParseResult[list[str]]:
  return _parse_data_stewards(entry_type_lib.EntryType.CATEGORY, s)


def parse_term_data_stewards(s: str) -> _ParseResult[list[str]]:
  return _parse_data_stewards(entry_type_lib.EntryType.TERM, s)


def _parse_data_stewards(
    entry_type: entry_type_lib.EntryType, s: str
) -> _ParseResult[list[str]]:
  """Parses list of data stewards.

  Args:
    entry_type: enum indicating parsed entry type (e.g. CATEGORY or TERM).
    s: A string to parse.

  Returns:
    A tuple of list of parsed data stewards and a list of errors.
  """
  unfiltered, _ = parse_list(s)
  data_stewards = []
  errors = []
  for steward in unfiltered:
    if not steward:
      continue
    data_steward = _parse_data_steward(steward)
    if data_steward:
      data_stewards.append(data_steward)
    else:
      errors.append(
          error.ParseError(entry_type, f"Error parsing data steward {steward}")
      )

  return data_stewards, errors


def _parse_data_steward(s: str) -> str | None:
  """Parses a single data steward.

  Data stewards follows the pattern "Name <email>", where the name is optional.
  Args:
    s: Raw text containing a possible data steward.

  Returns:
    DataSteward | None.
  """
  match = re.fullmatch(r"\s*(?P<name>.*)<(?P<email>.+)>\s*", s)
  if not match:
    return None
  return match[0]


def parse_list(entities: str) -> _ParseResult[list[str]]:
  """Parses a list of strings separated by comma (,).

  Because the list of strings might contain items using the comma value
  themselves (such as display names), we use a regular expression to find
  matches for items that might appear delimited between quotes.

  Args:
    entities: A string containing a list of comma separated entities.

  Returns:
    ParseResult: List of matched entities.
  """
  pattern = r'(?:[^,"]|"(?:[^"])*")+'
  matches = re.findall(pattern, entities)
  matches = list(map(lambda x: str.strip(x.replace('"', "")), matches))
  return matches, []
