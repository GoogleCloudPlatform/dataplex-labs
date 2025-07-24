import unittest

from parameterized import parameterized
import entry_type as entry_type_lib
import parse_utils


class ParseUtilsTest(unittest.TestCase):

  @parameterized.expand([
      (entry_type_lib.EntryType.CATEGORY, "<email>", ["<email>"]),
      (entry_type_lib.EntryType.CATEGORY, "  name<email>  ", ["name<email>"]),
      (entry_type_lib.EntryType.CATEGORY, "Data Steward<steward@data.com>", ["Data Steward<steward@data.com>"]),
      (entry_type_lib.EntryType.TERM, "<email>", ["<email>"]),
      (entry_type_lib.EntryType.TERM, "  name<email>  ", ["name<email>"]),
      (entry_type_lib.EntryType.TERM, "Data Steward<steward@data.com>", ["Data Steward<steward@data.com>"]),
  ])
  def test_parse_data_stewards(
      self, entry_type: entry_type_lib.EntryType, text: str, expected: list[str]
  ):
    ds, errors = parse_utils._parse_data_stewards(entry_type, text)
    self.assertEqual(len(errors), 0)
    self.assertEqual(ds, expected)

  @parameterized.expand([
      ("<email>", ["<email>"]),
      ("  name<email>  ", ["name<email>"]),
      ("Data Steward<steward@data.com>",["Data Steward<steward@data.com>"]),
  ])
  def test_parse_category_data_stewards(self, text: str, expected: list[str]):
    ds, errors = parse_utils.parse_category_data_stewards(text)
    self.assertEqual(len(errors), 0)
    self.assertEqual(ds, expected)

  @parameterized.expand([
      ("<email>", ["<email>"]),
      ("  name<email>  ", ["name<email>"]),
      (
          "Data Steward<steward@data.com>",
          ["Data Steward<steward@data.com>"],
      ),
  ])
  def test_parse_terms_data_stewards(self, text: str, expected: list[str]):
    ds, errors = parse_utils.parse_term_data_stewards(text)
    self.assertEqual(len(errors), 0)
    self.assertEqual(ds, expected)

  @parameterized.expand([
      (entry_type_lib.EntryType.CATEGORY, ""),
      (entry_type_lib.EntryType.CATEGORY, " "),
      (entry_type_lib.EntryType.CATEGORY, "   "),
      (entry_type_lib.EntryType.CATEGORY, '" "'),
      (entry_type_lib.EntryType.CATEGORY, '"    "'),
      (entry_type_lib.EntryType.TERM, ""),
      (entry_type_lib.EntryType.TERM, " "),
      (entry_type_lib.EntryType.TERM, "   "),
      (entry_type_lib.EntryType.TERM, '" "'),
      (entry_type_lib.EntryType.TERM, '"    "'),
  ])
  def test_parse_empty_string(
      self, entry_type: entry_type_lib.EntryType, text: str
  ):
    ds, errors = parse_utils._parse_str(entry_type, text)
    self.assertEqual(len(ds), 0)
    self.assertEqual(len(errors), 0)

  @parameterized.expand([
      (""),
      (" "),
      ("   "),
      ('" "'),
      ('"    "'),
  ])
  def test_parse_category_string(self, text: str):
    ds, errors = parse_utils.parse_category_str(text)
    self.assertEqual(len(ds), 0)
    self.assertEqual(len(errors), 0)

  @parameterized.expand([
      (""),
      (" "),
      ("   "),
      ('" "'),
      ('"    "'),
  ])
  def test_parse_term_string(self, text: str):
    ds, errors = parse_utils.parse_term_str(text)
    self.assertEqual(len(ds), 0)
    self.assertEqual(len(errors), 0)

  @parameterized.expand([
      (entry_type_lib.EntryType.CATEGORY,),
      (entry_type_lib.EntryType.TERM,),
  ])
  def test_parse_data_stewards_2(self, entry_type: entry_type_lib.EntryType):
    ds, errors = parse_utils._parse_data_stewards(
        entry_type,
        "Data Steward I<steward1@data.com>, Data Steward II<steward2@data.com>,"
        " <steward3@data.com>",
    )

    self.assertEqual(len(errors), 0)
    self.assertEqual(
        ds,
        [
            "Data Steward I<steward1@data.com>",
            "Data Steward II<steward2@data.com>",
            "<steward3@data.com>",
        ],
    )

  @parameterized.expand([
      (entry_type_lib.EntryType.CATEGORY, ""),
      (entry_type_lib.EntryType.CATEGORY, "  "),
      (entry_type_lib.EntryType.CATEGORY, ","),
      (entry_type_lib.EntryType.CATEGORY, " ,  , "),
      (entry_type_lib.EntryType.TERM, ""),
      (entry_type_lib.EntryType.TERM, "  "),
      (entry_type_lib.EntryType.TERM, ","),
      (entry_type_lib.EntryType.TERM, " ,  , "),
  ])
  def test_parse_data_stewards_empty(
      self, entry_type: entry_type_lib.EntryType, text: str
  ):
    ds, errors = parse_utils._parse_data_stewards(entry_type, text)
    self.assertEqual(len(errors), 0)
    self.assertEqual(len(ds), 0)

  @parameterized.expand([
      (entry_type_lib.EntryType.CATEGORY,),
      (entry_type_lib.EntryType.TERM,),
  ])
  def test_parse_data_stewards_errors(
      self, entry_type: entry_type_lib.EntryType
  ):
    ds, errors = parse_utils._parse_data_stewards(
        entry_type, "no_email, ok<ok>, <not_closed, <trailing_char>x, <>"
    )
    self.assertEqual(len(errors), 4)
    self.assertEqual(ds, ["ok<ok>"])

  def test_parse_string_list(self):
    ret, errors = parse_utils.parse_list("term 1, term 2, term 3")
    self.assertEqual(len(errors), 0)
    self.assertEqual(ret, ["term 1", "term 2", "term 3"])

  def test_parse_string_list_with_delimiter_character(self):
    ret, errors = parse_utils.parse_list('term 1, term 2, term 3, "term, 4",')
    self.assertEqual(len(errors), 0)
    self.assertEqual(ret, ["term 1", "term 2", "term 3", "term, 4"])


if __name__ == "__main__":
  unittest.main()
