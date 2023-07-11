import unittest

import error


class ImportErrorTest(unittest.TestCase):

  def test_to_string_default(self):
    err = error.TermImportError(
        line=1,
        resources=["Resource 1", "Resource 2"],
        message=None,
        operation=None,
    )
    expected_message = (
        "Line 1 : On resource(s) [Resource 1, Resource 2]."
    )
    self.assertEqual(err.to_string(), expected_message)

  def test_to_string_operation(self):
    err = error.TermImportError(
        line=1,
        resources=["Resource 1"],
        message=None,
        operation="add_term",
    )
    expected_message = (
        "Line 1 : Performing add_term on resource(s) [Resource 1]."
    )
    self.assertEqual(err.to_string(), expected_message)

  def test_to_string_message(self):
    err = error.TermImportError(
        line=1,
        resources=["Resource 1"],
        message="Removing this resource will fix the error.",
        operation=None,
    )
    expected_message = (
        "Line 1 : On resource(s) [Resource 1]."
        " Removing this resource will fix the error."
    )
    self.assertEqual(err.to_string(), expected_message)

  def test_no_resources(self):
    with self.assertRaises(AssertionError):
      error.TermImportError(
          line=1,
          resources=[]
      )


if __name__ == "__main__":
  unittest.main()
