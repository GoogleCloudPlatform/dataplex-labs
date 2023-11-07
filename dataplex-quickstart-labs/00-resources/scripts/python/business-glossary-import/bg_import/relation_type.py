"""Enum for entries relationship type."""

import enum


class RelationshipType(enum.Enum):
  """Enum containing the types of relationships between terms."""

  SYNONYMOUS = 'is_synonymous_to'
  RELATED = 'is_related_to'
  DESCRIBED = 'is_described_by'
  BELONGS_TO = 'belongs_to'
