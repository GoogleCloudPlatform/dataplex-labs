"""Auxiliary classes to encapsulate errors.

Typical usage example:
  error = EntryImportError(
                          entry_type=EntryType.TERM,
                          line=1,
                          column=1,
                          resources=["Term 1", "Term 2"],
                          operation="create_synonym_relationship")
  print(error.to_string())
"""

import abc

import entry_type as entry_type_lib

_MAX_CHARS_PER_LINE = 120


class Error(abc.ABC):
  """Base class for Error.

  Attributes:
    entry_type: An enum containing the type of the record in the CSV file (e.g.
      TERM, CATEGORY).
    line: An integer containing the line of the record in the CSV file.
    column: An integer containing the column of the error in the CSV file.
    resources: A list of the resources (terms, FQNs, entries, etc) that caused
      the error.
    message: An optional string indicating a fix for the error.
    operation: An optional string indicating the operation that failed.
    record: A list of string containing each of the subfields in the line that
      caused the error.
  """

  def __init__(
      self,
      entry_type: entry_type_lib.EntryType,
      line: int,
      column: int,
      resources: list[str],
      message: str | None = None,
      record: list[str] | None = None,
  ):
    self.entry_type = entry_type
    self.line = line
    self.column = column
    self.resources = resources
    self.message = message
    self.operation = None
    self.record = record

  def __repr__(self) -> str:
    return self.to_string()

  def to_string(self) -> str:
    """Generates a string containing details on the error."""
    err = []
    err.append(f"{self.entry_type.value}")
    if self.line >= 1:
      err.append(f"Line {self.line}")
    if self.column >= 1:
      err.append(f"Column {self.column}")
    if err:
      err.append(":")
    if self.operation:
      err.append(
          f"Performing {self.operation}"
          f" on resource(s) [{(', ').join(self.resources)}]."
      )
    elif self.resources:
      err.append(f"On resource(s) [{(', ').join(self.resources)}].")
    if self.message is not None:
      err.append(f"{self.message}")

    error_msg = (" ").join(err)
    # Display the whole record line that contains the issue, and show the exact
    # part of it that needs to be fixed.
    if self.record is not None and self._is_column_field_set():
      error_msg = f"{error_msg}\n{self._add_record_information()}"

    return error_msg

  def _is_column_field_set(self) -> bool:
    return self.column >= 1

  def _add_record_information(self) -> str:
    """Generate an string with information about the record and its error.

    Returns:
      String containing the full record that generates an error and the location
      where the error exists signaled.
    """
    if not self.record:
      return ""

    # For empty fields, we increase the size to two empty characters to be
    # able to display where the issue is
    if not self.record[self.column - 1]:
      self.record[self.column - 1] = " " * 2
    join_record = ", ".join(self.record)
    # Number of space characters to add before the field with the issue
    chars_before_error = sum(
        [len(self.record[i]) for i in range(self.column - 1)]
    )
    chars_before_error += 2 * (self.column - 1)
    # Number of ^ chars to insert
    chars_error = len(self.record[self.column - 1])
    # Number of space characters to add after the field with the issue
    chars_after_error = len(join_record) - chars_before_error - chars_error

    error_mark = (
        f"{' ' * chars_before_error}"
        f"{'^' * chars_error}"
        f"{' ' * chars_after_error}"
    )

    # If the length of the join record is longer than _MAX_CHARS_PER_LINE
    # we split it into separate lines
    err = []
    for i in range(0, len(join_record), _MAX_CHARS_PER_LINE):
      err.append(
          (
              f"{join_record[i : (i + _MAX_CHARS_PER_LINE)]}\n"
              f"{error_mark[i : (i + _MAX_CHARS_PER_LINE)]}\n"
          )
      )
    return ("").join(err)


class ParseError(Error):
  """Initializes an instance of ParseError.

  ParseError objects are populated during the CSV file parsing and validation
  phase.
  """

  def __init__(
      self,
      entry_type: entry_type_lib.EntryType,
      message: str,
      line: int = -1,
      column: int = -1,
      resources: list[str] | None = None,
      record: list[str] | None = None,
  ):  # pylint: disable=useless-parent-delegation
    super().__init__(entry_type, line, column, resources or [], message, record)


class EntryImportError(Error):
  """Initializes an instance of ImportError.

  ImportError objects are populated during the term import phase.
  """

  def __init__(
      self,
      entry_type: entry_type_lib.EntryType,
      line: int,
      resources: list[str],
      message: str | None = None,
      operation: str | None = None,
      record: list[str] | None = None,
  ):
    assert len(resources) >= 1
    super().__init__(entry_type, line, -1, resources, message, record)
    self.operation = operation
