import unittest
from unittest import mock

from parameterized import parameterized
import entry_type
import categories_csv_parser as csv_parser


class CategoriesCsvReaderTest(unittest.TestCase):

  @parameterized.expand([
      ("category,description,name<email>,parent_category"),
      (""""category","description","name<email>",parent_category"""),
  ])
  def test_read_glossary_csv_single_line(self, content):
    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      categories, errors, lines_read = csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 0)
      self.assertEqual(categories[1].display_name, "category")
      self.assertEqual(categories[1].description, "description")
      self.assertEqual(categories[1].data_stewards, ["name<email>"])
      self.assertEqual(categories[1].belongs_to_category, "parent_category")
      self.assertEqual(lines_read, 1)

  def test_read_glossary_csv_multi_line(self):
    content = (
        '"category1","description1","<email1>","parent_category_1"\n'
        "category2,description2,,parent_category_2\n"
        'category3,description3,"name3<email3>,name33<email33>",parent_category_3'
    )

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      categories, errors, lines_read = csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 0)
      self.assertEqual(categories[1].display_name, "category1")
      self.assertEqual(categories[2].display_name, "category2")
      self.assertEqual(categories[3].display_name, "category3")
      self.assertEqual(categories[1].description, "description1")
      self.assertEqual(categories[2].description, "description2")
      self.assertEqual(categories[3].description, "description3")
      self.assertEqual(categories[1].data_stewards, ["<email1>"])
      self.assertEqual(len(categories[2].data_stewards), 0)
      self.assertEqual(
          categories[3].data_stewards,
          ["name3<email3>", "name33<email33>"],
      )
      self.assertEqual(categories[1].belongs_to_category, "parent_category_1")
      self.assertEqual(categories[2].belongs_to_category, "parent_category_2")
      self.assertEqual(categories[3].belongs_to_category, "parent_category_3")
      self.assertEqual(lines_read, 3)

  def test_read_glossary_csv_errors(self):
    content = (
        "category1,description1,<email1>,\n"
        "category_with_invalid_data_steward_format,description2,data"
        " steward,\n"
        "category_with_missing_fields,,\n"
        "category2,description2,<email2>,parent_category"
    )

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      categories, errors, lines_read = csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 2)
      # Steward is invalid
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.CATEGORY)
      self.assertEqual(errors[0].line, 2)
      self.assertEqual(errors[0].column, 3)
      # Description is missing
      self.assertEqual(errors[1].entry_type, entry_type.EntryType.CATEGORY)
      self.assertEqual(errors[1].line, 3)
      self.assertEqual(errors[1].column, 2)
      self.assertEqual(categories[1].display_name, "category1")
      self.assertEqual(categories[4].display_name, "category2")
      self.assertEqual(categories[1].description, "description1")
      self.assertEqual(categories[4].description, "description2")
      self.assertEqual(categories[1].data_stewards, ["<email1>"])
      self.assertEqual(categories[4].data_stewards, ["<email2>"])
      self.assertEqual(categories[4].belongs_to_category, "parent_category")
      self.assertEqual(lines_read, 4)

  def test_read_glossary_csv_empty_lines(self):
    content = "\n\n\n"
    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      categories, errors, lines_read = csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 0)
      self.assertEqual(len(categories), 0)
      self.assertEqual(lines_read, 0)

  def test_read_glossary_csv_duplicate_errors(self):
    content = """category 1,description1,<email1>,parent_category
Category 1,description2,,"""

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      _, errors, lines_read = csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 1)
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.CATEGORY)
      self.assertEqual(errors[0].line, 2)
      self.assertEqual(errors[0].column, 1)
      self.assertEqual(
          errors[0].message, "The category is duplicated in the CSV file."
      )
      self.assertEqual(lines_read, 2)

  def test_read_glossary_csv_empty_display_name(self):
    content = """ ,description1,<email1>,
\"   \",description2,,"""

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      _, errors, lines_read = csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 2)
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.CATEGORY)
      self.assertEqual(errors[0].line, 1)
      self.assertEqual(errors[0].column, 1)
      self.assertEqual(
          errors[0].message, "The display name for the category is empty."
      )
      self.assertEqual(errors[1].entry_type, entry_type.EntryType.CATEGORY)
      self.assertEqual(errors[1].line, 2)
      self.assertEqual(errors[1].column, 1)
      self.assertEqual(
          errors[1].message, "The display name for the category is empty."
      )
      self.assertEqual(lines_read, 2)

  def test_read_glossary_csv_large_display_name(self):
    name = "Test" * 51
    content = f"""{name},description1,<email@company.com>,,,"""

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      _, errors, lines_read = csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 1)
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.CATEGORY)
      self.assertEqual(errors[0].line, 1)
      self.assertEqual(errors[0].column, 1)
      self.assertEqual(
          errors[0].message, "The category's display name is too big."
      )
      self.assertEqual(lines_read, 1)

  def test_read_glossary_csv_non_allowed_character(self):
    content = """\"display\n name\",description1,<email@company.com>,,,"""

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      _, errors, lines_read = csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 1)
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.CATEGORY)
      self.assertEqual(errors[0].line, 1)
      self.assertEqual(errors[0].column, 1)
      self.assertEqual(
          errors[0].message, "Unallowed character in display name."
      )
      self.assertEqual(lines_read, 1)


if __name__ == "__main__":
  unittest.main()
