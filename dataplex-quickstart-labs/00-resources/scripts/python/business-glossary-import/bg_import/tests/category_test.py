import unittest

import category as bg_category


class CategoryTest(unittest.TestCase):

  def test_create_default_arguments(self):
    category = bg_category.Category("test-category-1", "description")
    self.assertEqual(category.display_name, "test-category-1")
    self.assertEqual(category.description, "description")
    self.assertEqual(len(category.data_stewards), 0)

  def test_create_with_stewards(self):
    data_steward_1 = "steward-name-1<steward-1@example.com>"
    data_steward_2 = "<steward-2@example.com>"
    category = bg_category.Category(
        "test-category-1",
        "description",
        data_stewards=[data_steward_1, data_steward_2],
    )
    self.assertEqual(len(category.data_stewards), 2)

  def test_create_with_invalid_parameters(self):
    """Should raise a TypeError exception due to the missing description."""
    with self.assertRaises(TypeError):
      bg_category.Category("test-category-1")

  def test_generates_id_based_on_display_name(self):
    category1 = bg_category.Category("category", "d1")
    self.assertEqual(category1.category_id[:8], "category")
    category2 = bg_category.Category("category CATEGORY", "d2")
    self.assertEqual(category2.category_id[:17], "category_category")
    category3 = bg_category.Category("123 CATEGORY", "d3")
    self.assertEqual(category3.category_id[:13], "_123_category")

  def test_create_with_belongs_to_category(self):
    category = bg_category.Category(
        "test-category-1",
        "description",
        [],
        belongs_to_category="law_protected_data",
    )
    self.assertEqual(len(category.data_stewards), 0)
    self.assertEqual(category.belongs_to_category, "law_protected_data")

  def test_mutable_fields_not_shared(self):
    category1 = bg_category.Category("test-category-1", "description")
    category2 = bg_category.Category("test-category-2", "description")
    category1.data_stewards.append("steward-1@example.com")
    self.assertEqual(len(category1.data_stewards), 1)
    self.assertEqual(len(category2.data_stewards), 0)

  def test_create_from_json(self):
    category_entry = {
        "name": "projects/123/locations/us/entryGroups/glossary_with_categories/entries/pii_data_xyz",
        "displayName": "PII Data",
        "coreAspects": {
            "business_context": {
                "jsonContent": {
                    "description": "Personally Identifiable Information Data"
                }
            }
        },
    }
    expected_category = bg_category.Category(
        "PII Data",
        "Personally Identifiable Information Data",
        force_category_id="pii_data_xyz",
    )

    actual_category = bg_category.Category.from_dict(category_entry)
    self.assertEqual(
        actual_category.display_name, expected_category.display_name
    )
    self.assertEqual(actual_category.description, expected_category.description)
    self.assertEqual(
        actual_category.category_id, expected_category.category_id
    )


if __name__ == "__main__":
  unittest.main()
