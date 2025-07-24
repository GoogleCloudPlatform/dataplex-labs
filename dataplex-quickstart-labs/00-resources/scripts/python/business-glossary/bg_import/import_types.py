"""Types for imported data."""

from typing import TypeVar

import category as bg_category
import entry_type as EntryType
import error
import relation_type
import term as bg_term

_T = TypeVar('_T')
_CreatedRelationship = tuple[str, str, _T]

_ImportResult = tuple[
  dict[EntryType.EntryType, list[bg_term.Term | bg_category.Category]],
  dict[
    EntryType.EntryType,
    list[_CreatedRelationship[relation_type.RelationshipType]],
  ],
  list[error.EntryImportError],
]
