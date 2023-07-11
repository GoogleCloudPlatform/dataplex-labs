"""Dataclass for the Term type.

A term represents an entry in a business glossary. Terms can describe assets,
 have other related terms, and have synonym terms.
"""

import random
import re
import string
from typing import Any


class Term:
  """Initializes an instance of Term.

  Attributes:
    display_name: A string indicating the display name for the term.
    description: A string containing a rich-text description of the term,
      encoded as plain text.
    data_stewards: A list of data stewards for this term.
    tagged_assets: A list of names for entries that are described by this
      term.
    synonyms: A list of display_name for terms that have a synonym relationship
      with this term.
    related_terms: A list of display_name for terms that have a related_to
      relationship with this term.
    term_id: A string containing a unique identifier for the term in DC
  """

  def __init__(
      self,
      display_name: str,
      description: str,
      data_stewards: list[str] | None = None,
      tagged_assets: list[str] | None = None,
      synonyms: list[str] | None = None,
      related_terms: list[str] | None = None,
      force_term_id: str | None = None
  ):
    self.display_name = display_name
    self.description = description
    self.data_stewards = data_stewards or []
    self.tagged_assets = tagged_assets or []
    self.synonyms = synonyms or []
    self.related_terms = related_terms or []
    self.term_id = force_term_id or self._generate_term_id()

  def __repr__(self):
    return (
        f"Term [{self.display_name} : {self.description} :"
        f" {self.data_stewards} : {self.tagged_assets} : {self.synonyms} :"
        f" {self.related_terms}]"
    )

  def _generate_term_id(self):
    """Unique glossary term ID."""
    if not self.display_name:
      return ""
    infix = re.sub(r"[^a-zA-Z0-9_]", "_", self.display_name).lower()
    prefix = "_" if infix[0] >= "0" and infix[0] <= "9" else ""
    suffix = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=7)
    )
    return f"{prefix}{infix}{suffix}"

  @classmethod
  def from_json(cls, entry: dict[str, Any]) -> ...:
    """Creates a term instance from a term entry in DataCatalog.

    Args:
      entry: Dictionary containing the term contents as returned by Data
        Catalog.

    Returns:
      Term.
    """

    def _get_term_id_from_resource_path(resource: str) -> str:
      return resource.split("/")[-1]

    # Parse entry UID, display_name, description - all of them are non-optional
    try:
      uid = _get_term_id_from_resource_path(entry["name"])
      display_name = entry["displayName"]
      description = entry["coreAspects"]["business_context"]["jsonContent"][
          "description"
      ]
    except KeyError:
      return None

    term = Term(display_name, description, force_term_id=uid)
    return term
