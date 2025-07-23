"""Enum for glossary entry type."""
import enum


@enum.unique
class EntryType(enum.Enum):
  """Entry type."""

  CATEGORY = "CATEGORY"
  TERM = "TERM"
