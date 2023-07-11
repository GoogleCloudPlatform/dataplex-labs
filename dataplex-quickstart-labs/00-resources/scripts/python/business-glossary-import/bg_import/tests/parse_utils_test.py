import unittest

from parameterized import parameterized
import parse_utils


class ParseUtilsTest(unittest.TestCase):

  @parameterized.expand([
      ("<email>", ["<email>"]),
      ("  name<email>  ", ["name<email>"]),
      ("Data Steward<steward@example.com>", ["Data Steward<steward@example.com>"]),
  ])
  def test_parse_data_stewards(self, text: str, expected: list[str]):
    ds, errors = parse_utils.parse_data_stewards(text)
    self.assertEqual(len(errors), 0)
    self.assertEqual(ds, expected)

  @parameterized.expand([
      (""),
      (" "),
      ("   "),
      ("\" \""),
      ("\"    \"")
  ])
  def test_parse_empty_string(self, text: str):
    ds, errors = parse_utils.parse_str(text)
    self.assertEqual(len(ds), 0)
    self.assertEqual(len(errors), 0)

  def test_parse_data_stewards_2(self):
    ds, errors = parse_utils.parse_data_stewards(
        "Data Steward I<steward1@example.com>, Data Steward II<steward2@example.com>,"
        " <steward3@example.com>"
    )

    self.assertEqual(len(errors), 0)
    self.assertEqual(
        ds,
        [
            "Data Steward I<steward1@example.com>",
            "Data Steward II<steward2@example.com>",
            "<steward3@example.com>",
        ],
    )

  @parameterized.expand([
      (""),
      ("  "),
      (","),
      (" ,  , "),
  ])
  def test_parse_data_stewards_empty(self, text: str):
    ds, errors = parse_utils.parse_data_stewards(text)
    self.assertEqual(len(errors), 0)
    self.assertEqual(len(ds), 0)

  def test_parse_data_stewards_errors(self):
    ds, errors = parse_utils.parse_data_stewards(
        "no_email, ok<ok>, <not_closed, <trailing_char>x, <>"
    )
    self.assertEqual(len(errors), 4)
    self.assertEqual(ds, ["ok<ok>"])

  def test_parse_string_list(self):
    ret, errors = parse_utils.parse_list(
        "term 1, term 2, term 3"
    )
    self.assertEqual(len(errors), 0)
    self.assertEqual(ret, ["term 1", "term 2", "term 3"])

  def test_parse_string_list_with_delimiter_character(self):
    ret, errors = parse_utils.parse_list(
        "term 1, term 2, term 3, \"term, 4\","
    )
    self.assertEqual(len(errors), 0)
    self.assertEqual(ret, ["term 1", "term 2", "term 3", "term, 4"])


if __name__ == "__main__":
  unittest.main()
