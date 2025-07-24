import unittest

import entry_type
import error

class EntryImportErrorTest(unittest.TestCase):

  def test_to_string_default(self):
    err = error.EntryImportError(
        entry_type.EntryType.TERM,
        line=1,
        resources=["Resource 1", "Resource 2"],
        message=None,
        operation=None,
    )
    expected_message = (
        "TERM Line 1 : On resource(s) [Resource 1, Resource 2]."
    )
    self.assertEqual(err.to_string(), expected_message)

  def test_to_string_operation(self):
    err = error.EntryImportError(
        entry_type.EntryType.TERM,
        line=1,
        resources=["Resource 1"],
        message=None,
        operation="add_term",
    )
    expected_message = (
        "TERM Line 1 : Performing add_term on resource(s) [Resource 1]."
    )
    self.assertEqual(err.to_string(), expected_message)

  def test_to_string_message(self):
    err = error.EntryImportError(
        entry_type.EntryType.TERM,
        line=1,
        resources=["Resource 1"],
        message="Removing this resource will fix the error.",
        operation=None,
    )
    expected_message = (
        "TERM Line 1 : On resource(s) [Resource 1]."
        " Removing this resource will fix the error."
    )
    self.assertEqual(err.to_string(), expected_message)

  def test_no_resources(self):
    with self.assertRaises(AssertionError):
      error.EntryImportError(
          entry_type.EntryType.TERM,
          line=1,
          resources=[]
      )


if __name__ == "__main__":
  unittest.main()
