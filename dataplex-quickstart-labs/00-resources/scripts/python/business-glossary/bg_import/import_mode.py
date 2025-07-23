"""Enum for import mode."""
import enum


@enum.unique
class ImportMode(enum.Enum):
  """Import mode {strict, clear}."""

  STRICT = "strict"
  CLEAR = "clear"
