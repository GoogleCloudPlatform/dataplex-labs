import unittest

import term as bg_term


class TermTest(unittest.TestCase):

  def test_create_default_arguments(self):
    term = bg_term.Term("test-term-1", "description")
    self.assertEqual(term.display_name, "test-term-1")
    self.assertEqual(term.description, "description")
    self.assertEqual(len(term.data_stewards), 0)

  def test_create_with_stewards(self):
    data_steward_1 = "steward-name-1<steward-1@example.com>"
    data_steward_2 = "<steward-2@example.com>"
    term = bg_term.Term(
        "test-term-1",
        "description",
        data_stewards=[data_steward_1, data_steward_2],
    )
    self.assertEqual(len(term.data_stewards), 2)

  def test_create_with_invalid_parameters(self):
    """Should raise a TypeError exception due to the missing description."""
    with self.assertRaises(TypeError):
      bg_term.Term("test-term-1")

  def test_generates_id_based_on_display_name(self):
    term1 = bg_term.Term("term", "d1")
    self.assertEqual(term1.term_id[:4], "term")
    term2 = bg_term.Term("term TERM", "d2")
    self.assertEqual(term2.term_id[:9], "term_term")
    term3 = bg_term.Term("123 term", "d3")
    self.assertEqual(term3.term_id[:9], "_123_term")

  def test_create_with_synonyms(self):
    term = bg_term.Term(
        "test-term-1",
        "description",
        [],
        synonyms=["test-term-2", "test-term-3"]
    )
    self.assertEqual(len(term.data_stewards), 0)
    self.assertEqual(len(term.related_terms), 0)
    self.assertEqual(len(term.synonyms), 2)

  def test_create_with_related_terms(self):
    term = bg_term.Term(
        "test-term-1",
        "description",
        [],
        related_terms=["test-term-2", "test-term-3"]
    )
    self.assertEqual(len(term.data_stewards), 0)
    self.assertEqual(len(term.synonyms), 0)
    self.assertEqual(len(term.related_terms), 2)

  def test_mutable_fields_not_shared(self):
    term1 = bg_term.Term("test-term-1",
                         "description")
    term2 = bg_term.Term("test-term-2",
                         "description")
    term1.data_stewards.append("steward-1@example.com")
    self.assertEqual(len(term1.data_stewards), 1)
    self.assertEqual(len(term2.data_stewards), 0)

  def test_create_from_json(self):
    term_entry = {
        "name": "projects/123/locations/us/entryGroups/glossary_with_terms/entries/pii_data_xyz",
        "displayName": "PII Data",
        "coreAspects": {
            "business_context": {
                "jsonContent": {
                    "description": "Personally Identifiable Information Data"
                }
            }
        },
    }
    expected_term = bg_term.Term(
        "PII Data",
        "Personally Identifiable Information Data",
        force_term_id="pii_data_xyz",
    )

    actual_term = bg_term.Term.from_dict(term_entry)
    self.assertEqual(
        actual_term.display_name, expected_term.display_name
    )
    self.assertEqual(actual_term.description, expected_term.description)
    self.assertEqual(
        actual_term.term_id, expected_term.term_id
    )

if __name__ == "__main__":
  unittest.main()
