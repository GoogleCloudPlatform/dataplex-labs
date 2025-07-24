import unittest
from unittest import mock

import entry_type
import terms_csv_parser
from parameterized import parameterized


class TermsCsvReaderTest(unittest.TestCase):

  @parameterized.expand([
      ("term,description,name<email>,,,"),
      (""""term","description","name<email>",,,"""),
  ])
  def test_read_glossary_csv_single_line(self, content):
    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      terms, errors, lines_read = terms_csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 0)
      self.assertEqual(terms[1].display_name, "term")
      self.assertEqual(terms[1].description, "description")
      self.assertEqual(terms[1].data_stewards, ["name<email>"])
      self.assertEqual(lines_read, 1)

  def test_read_glossary_csv_multi_line(self):
    content = (
        '"term1","description1","<email1>",,,\n'
        "term2,description2,,,,,category2\n"
        'term3,description3,"name3<email3>, name33<email33>",,,,category3'
    )

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      terms, errors, lines_read = terms_csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 0)
      self.assertEqual(terms[1].display_name, "term1")
      self.assertEqual(terms[2].display_name, "term2")
      self.assertEqual(terms[3].display_name, "term3")
      self.assertEqual(terms[1].description, "description1")
      self.assertEqual(terms[2].description, "description2")
      self.assertEqual(terms[3].description, "description3")
      self.assertEqual(terms[1].data_stewards, ["<email1>"])
      self.assertEqual(len(terms[2].data_stewards), 0)
      self.assertEqual(
          terms[3].data_stewards,
          ["name3<email3>", "name33<email33>"],
      )
      self.assertEqual(terms[1].belongs_to_category, "")
      self.assertEqual(terms[2].belongs_to_category, "category2")
      self.assertEqual(terms[3].belongs_to_category, "category3")
      self.assertEqual(lines_read, 3)

  def test_read_glossary_csv_errors(self):
    content = (
        "term1,description1,<email1>,,,\n"
        "term_with_invalid_data_steward_format,description2,data steward,,,\n"
        "term_with_missing_fields,,,,\n"
        "term2,description2,<email2>,,,"
    )

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      terms, errors, lines_read = terms_csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 2)
      # Steward is invalid
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.TERM)
      self.assertEqual(errors[0].line, 2)
      self.assertEqual(errors[0].column, 3)
      # Description is missing
      self.assertEqual(errors[1].entry_type, entry_type.EntryType.TERM)
      self.assertEqual(errors[1].line, 3)
      self.assertEqual(errors[1].column, 2)
      self.assertEqual(terms[1].display_name, "term1")
      self.assertEqual(terms[4].display_name, "term2")
      self.assertEqual(terms[1].description, "description1")
      self.assertEqual(terms[4].description, "description2")
      self.assertEqual(terms[1].data_stewards, ["<email1>"])
      self.assertEqual(terms[4].data_stewards, ["<email2>"])
      self.assertEqual(lines_read, 4)

  def test_read_glossary_csv_empty_lines(self):
    content = "\n\n\n"
    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      terms, errors, lines_read = terms_csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 0)
      self.assertEqual(len(terms), 0)
      self.assertEqual(lines_read, 0)

  def test_read_glossary_csv_duplicate_errors(self):
    content = """term 1,description1,<email1>,,,
Term 1,description2,,,,"""

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      _, errors, lines_read = terms_csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 1)
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.TERM)
      self.assertEqual(errors[0].line, 2)
      self.assertEqual(errors[0].column, 1)
      self.assertEqual(
          errors[0].message, "The term is duplicated in the CSV file."
      )
      self.assertEqual(lines_read, 2)

  def test_read_glossary_csv_empty_display_name(self):
    content = """ ,description1,<email1>,,,
\"   \",description2,,,,"""

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      _, errors, lines_read = terms_csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 2)
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.TERM)
      self.assertEqual(errors[0].line, 1)
      self.assertEqual(errors[0].column, 1)
      self.assertEqual(
          errors[0].message, "The display name for the term is empty."
      )
      self.assertEqual(errors[1].entry_type, entry_type.EntryType.TERM)
      self.assertEqual(errors[1].line, 2)
      self.assertEqual(errors[1].column, 1)
      self.assertEqual(
          errors[1].message, "The display name for the term is empty."
      )
      self.assertEqual(lines_read, 2)

  def test_read_glossary_csv_large_display_name(self):
    name = "Test" * 51
    content = f"""{name},description1,<email@example.com>,,,"""

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      _, errors, lines_read = terms_csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 1)
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.TERM)
      self.assertEqual(errors[0].line, 1)
      self.assertEqual(errors[0].column, 1)
      self.assertEqual(errors[0].message, "The term's display name is too big.")
      self.assertEqual(lines_read, 1)

  def test_read_glossary_csv_non_allowed_character(self):
    content = """\"display\n name\",description1,<email@company.com>,,,"""

    with mock.patch("builtins.open", mock.mock_open(read_data=content)):
      _, errors, lines_read = terms_csv_parser.parse_glossary_csv("")
      self.assertEqual(len(errors), 1)
      self.assertEqual(errors[0].entry_type, entry_type.EntryType.TERM)
      self.assertEqual(errors[0].line, 1)
      self.assertEqual(errors[0].column, 1)
      self.assertEqual(
          errors[0].message, "Unallowed character in display name."
      )
      self.assertEqual(lines_read, 1)


if __name__ == "__main__":
  unittest.main()
