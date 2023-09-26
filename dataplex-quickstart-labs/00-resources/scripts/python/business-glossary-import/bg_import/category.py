"""Dataclass for the Category type.

A category represents an entry in a business glossary. Category can aggregate
terms and other categories into hierarchy via belongs_to relationship. Term and
category may belong to zero or one category. One category can aggregate many
terms and categories.

Typical usage example:
  stewards = ["John Doe<john@company.com>", "Lee<lee@company.com>"]
  category = Category("PII", "A Personally Identifiable Information.",
              data_stewards=stewards)
"""

from __future__ import annotations
import random
import re
import string
from typing import Any


class Category:
  """Initializes an instance of Category.

  Attributes:
    display_name: A string indicating the display name for the category.
    description: A string containing a rich-text description of the category,
      encoded as plain text.
    data_stewards: A list of strings representing data stewards for this
      category.
    belongs_to_category: A string indicating the display name of another
      category to which this category belongs to
    category_id: A string containing a unique identifier for the category in DC.
  """

  def __init__(
      self,
      display_name: str,
      description: str,
      data_stewards: list[str] | None = None,
      belongs_to_category: str | None = None,
      force_category_id: str | None = None,
  ):
    self.display_name = display_name
    self.description = description
    self.data_stewards = [] if data_stewards is None else data_stewards
    self.belongs_to_category = belongs_to_category
    self.category_id = force_category_id or self._generate_category_id()

  def __repr__(self):
    return (
        f"Category [{self.display_name} : {self.description} :"
        f" {self.data_stewards} : {self.belongs_to_category}]"
    )

  def _generate_category_id(self):
    """Unique glossary category ID."""
    if not self.display_name:
      return ""
    infix = re.sub(r"[^a-zA-Z0-9_]", "_", self.display_name).lower()
    prefix = "_" if infix[0].isdigit() else ""
    suffix = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=7)
    )
    return f"{prefix}{infix}{suffix}"

  @classmethod
  def from_dict(cls, entry: dict[str, Any]) -> Category | None:
    """Creates a category instance from a category entry in DataCatalog.

    Args:
      entry: Dictionary containing the category contents as returned by Data
        Catalog.

    Returns:
      Category.
    """

    def _get_category_id_from_resource_path(resource: str) -> str:
      return resource.split("/")[-1]

    # Parse entry UID, display_name, description - all of them are non-optional
    try:
      uid = _get_category_id_from_resource_path(entry["name"])
      display_name = entry["displayName"]
      description = entry["coreAspects"]["business_context"]["jsonContent"][
        "description"
      ]
    except KeyError:
      return None

    category = Category(display_name, description, force_category_id=uid)
    return category
