import unittest
from unittest import mock

import glossary as dc_glossary
import glossary_identification
import term as bg_term
from tests.test_utils import mocks


class GlossaryTest(unittest.TestCase):

  def test_glossary_uid_returned(self):
    with mock.patch(
        "api_call_utils.requests.get",
        side_effect=mocks.mocked_get_api_response):
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_no_terms",
          "empty_glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      self.assertEqual(
          glossary._glossary_uid,
          "71372af7-bb1a-4020-aba8-223c57c366d2"
      )

  def test_glossary_not_found(self):
    with mock.patch(
        "api_call_utils.requests.get",
        side_effect=mocks.mocked_get_api_response):
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_no_glossary",
          "glossary_not_found"
      )
      with self.assertRaises(ValueError):
        dc_glossary.Glossary(glossary_id)

  def test_glossary_is_not_empty(self):
    with mock.patch(
        "api_call_utils.requests.get",
        side_effect=mocks.mocked_get_api_response):
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_terms",
          "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      self.assertFalse(glossary.is_glossary_empty())

  def test_glossary_is_empty(self):
    with mock.patch(
        "api_call_utils.requests.get",
        side_effect=mocks.mocked_get_api_response,
    ):
      glossary_id = glossary_identification.GlossaryId(
          "123", "us", "test_entry_group_with_no_terms", "empty_glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      is_empty = glossary.is_glossary_empty()
      self.assertTrue(is_empty)

  def test_glossary_term_is_created(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        ),
        mock.patch("requests.post") as post_mock,
    ):
      term = bg_term.Term("Term1", "Desc1")
      glossary_id = glossary_identification.GlossaryId(
          "123", "us", "test_entry_group_with_terms", "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._create_glossary_term(term)
      post_mock.assert_called_with(
          (
              "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
              "entryGroups/test_entry_group_with_terms/"
              f"entries?entry_id={term.term_id}"
          ),
          headers={
              "Content-Type": "application/json",
              "Authorization": "Bearer None",
              "X-Goog-User-Project": "123",
          },
          json={
              "entry_type": "glossary_term",
              "display_name": "Term1",
              "core_aspects": {
                  "business_context": {
                      "aspect_type": "business_context",
                      "json_content": {"description": "Desc1", "contacts": []},
                  }
              },
              "core_relationships": {
                  "relationship_type": "is_child_of",
                  "destination_entry_name": (
                      "projects/123/locations/us/"
                      "entryGroups/test_entry_group_with_terms/"
                      "entries/glossary_exists"
                  ),
              },
          },
      )

  def test_glossary_term_with_one_steward_is_created(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        ),
        mock.patch("requests.post") as post_mock,
    ):
      term = bg_term.Term(
          "Term1",
          "Desc1",
          ["name <email>"]
      )
      glossary_id = glossary_identification.GlossaryId(
          "123", "us", "test_entry_group_with_terms", "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._create_glossary_term(term)
      post_mock.assert_called_with(
          (
              "https://datacatalog.googleapis.com/v2/projects/123/"
              "locations/us/entryGroups/test_entry_group_with_terms/"
              f"entries?entry_id={term.term_id}"
          ),
          headers={
              "Content-Type": "application/json",
              "Authorization": "Bearer None",
              "X-Goog-User-Project": "123",
          },
          json={
              "entry_type": "glossary_term",
              "display_name": "Term1",
              "core_aspects": {
                  "business_context": {
                      "aspect_type": "business_context",
                      "json_content": {
                          "description": "Desc1",
                          "contacts": ["name <email>"]
                      },
                  }
              },
              "core_relationships": {
                  "relationship_type": "is_child_of",
                  "destination_entry_name": (
                      "projects/123/locations/us/"
                      "entryGroups/test_entry_group_with_terms/"
                      "entries/glossary_exists"
                  ),
              },
          },
      )

  def test_glossary_term_with_many_stewards_is_created(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        ),
        mock.patch("requests.post") as post_mock,
    ):
      term = bg_term.Term(
          "Term1",
          "Desc1",
          ["name <email>", "name2 <email2>"],
      )
      glossary_id = glossary_identification.GlossaryId(
          "123", "us", "test_entry_group_with_terms", "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._create_glossary_term(term)
      post_mock.assert_called_with(
          (
              "https://datacatalog.googleapis.com/v2/projects/123/"
              "locations/us/entryGroups/test_entry_group_with_terms/"
              f"entries?entry_id={term.term_id}"
          ),
          headers={
              "Content-Type": "application/json",
              "Authorization": "Bearer None",
              "X-Goog-User-Project": "123",
          },
          json={
              "entry_type": "glossary_term",
              "display_name": "Term1",
              "core_aspects": {
                  "business_context": {
                      "aspect_type": "business_context",
                      "json_content": {
                          "description": "Desc1",
                          "contacts": ["name <email>", "name2 <email2>"]
                      },
                  }
              },
              "core_relationships": {
                  "relationship_type": "is_child_of",
                  "destination_entry_name": (
                      "projects/123/locations/us/"
                      "entryGroups/test_entry_group_with_terms/"
                      "entries/glossary_exists"
                  ),
              },
          },
      )

  def test_glossary_synonym_relation_is_created(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        ),
        mock.patch("requests.post") as post_mock,
    ):
      term = bg_term.Term("Term 1", "Desc1", synonyms=["Term 2"])
      term2 = bg_term.Term("Term 2", "Desc2", synonyms=["Term 1"])
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_terms",
          "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._term_cache = {
          term.display_name: term,
          term2.display_name: term2
      }
      glossary._create_relationship(
          term.display_name,
          term.synonyms[0],
          dc_glossary.RelationshipType.SYNONYMOUS
      )
      dest_entry_name = (
          "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
          f"entries/{term2.term_id}"
      )
      post_mock.assert_called_with(
          (
              "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
              "entryGroups/test_entry_group_with_terms/"
              f"entries/{term.term_id}/relationships"
          ),
          headers={
              "Content-Type": "application/json",
              "Authorization": "Bearer None",
              "X-Goog-User-Project": "123",
          },
          json={
              "relationship_type":
                  dc_glossary.RelationshipType.SYNONYMOUS.value,
              "destination_entry_name": dest_entry_name,
          },
      )

  def test_glossary_related_to_relation_is_created(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        ),
        mock.patch("requests.post") as post_mock
    ):
      term = bg_term.Term("Term 1", "Desc1", related_terms=["Term 2"])
      term2 = bg_term.Term("Term 2", "Desc2", related_terms=["Term 1"])
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_terms",
          "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._term_cache = {
          term.display_name: term,
          term2.display_name: term2
      }
      glossary._create_relationship(
          term.display_name,
          term.related_terms[0],
          dc_glossary.RelationshipType.RELATED
      )
      dest_entry_name = (
          "projects/123/locations/us/"
          f"entryGroups/test_entry_group_with_terms/entries/{term2.term_id}"
      )
      post_mock.assert_called_with(
          (
              "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
              "entryGroups/test_entry_group_with_terms/"
              f"entries/{term.term_id}/relationships"
          ),
          headers={
              "Content-Type": "application/json",
              "Authorization": "Bearer None",
              "X-Goog-User-Project": "123",
          },
          json={
              "relationship_type": dc_glossary.RelationshipType.RELATED.value,
              "destination_entry_name": dest_entry_name,
          },
      )

  def test_glossary_unsuccessful_creation_returns_error(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        ),
        mock.patch(
            "api_call_utils.requests.post",
            side_effect=mocks.mocked_post_failed_api_response) as post_mock,
    ):
      term = bg_term.Term("Term 1", "Desc1", related_terms=["Term 2"])
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_terms",
          "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._term_cache = {term.display_name: term}
      ret = glossary._create_glossary_term(term)
      self.assertIsNone(ret["json"])
      self.assertIsNotNone(ret["error_msg"])
      post_mock.assert_called_with(
          (
              "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
              "entryGroups/test_entry_group_with_terms/"
              f"entries?entry_id={term.term_id}"
          ),
          headers={
              "Content-Type": "application/json",
              "Authorization": "Bearer None",
              "X-Goog-User-Project": "123",
          },
          json={
              "entry_type": "glossary_term",
              "display_name": "Term 1",
              "core_aspects": {
                  "business_context": {
                      "aspect_type": "business_context",
                      "json_content": {"description": "Desc1", "contacts": []},
                  }
              },
              "core_relationships": {
                  "relationship_type": "is_child_of",
                  "destination_entry_name": (
                      "projects/123/locations/us/"
                      "entryGroups/test_entry_group_with_terms/"
                      "entries/glossary_exists"
                  ),
              },
          },
      )

  def test_glossary_described_by_relation_is_created(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        ),
        mock.patch(
            "api_call_utils.requests.post",
            side_effect=mocks.mocked_post_failed_api_response) as post_mock,
    ):
      term = bg_term.Term("Term 1", "Desc1")
      asset_name = (
          "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
          "entries/test_asset"
      )
      dest_entry_name = (
          "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
          f"entries/{term.term_id}"
      )
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_terms",
          "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._term_cache = {term.display_name: term}

      glossary._create_relationship(
          asset_name,
          term.display_name,
          dc_glossary.RelationshipType.DESCRIBED
      )

      post_mock.assert_called_with(
          (
              "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
              "entryGroups/test_entry_group_with_terms/"
              "entries/test_asset/relationships"
          ),
          headers={
              "Content-Type": "application/json",
              "Authorization": "Bearer None",
              "X-Goog-User-Project": "123",
          },
          json={
              "relationship_type": dc_glossary.RelationshipType.DESCRIBED.value,
              "destination_entry_name": dest_entry_name,
          },
      )

  def test_glossary_described_by_relation_with_column_is_created(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        ),
        mock.patch(
            "api_call_utils.requests.post",
            side_effect=mocks.mocked_post_failed_api_response) as post_mock,
    ):
      term = bg_term.Term("Term 1", "Desc1")
      asset_name = (
          "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
          "entries/test_asset:subcolumn"
      )
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_terms",
          "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._term_cache = {term.display_name: term}
      glossary._create_relationship(
          asset_name,
          term.display_name,
          dc_glossary.RelationshipType.DESCRIBED
      )
      dest_entry_name = (
          "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
          f"entries/{term.term_id}"
      )
      post_mock.assert_called_with(
          (
              "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
              "entryGroups/test_entry_group_with_terms/entries/test_asset/relationships"
          ),
          headers={
              "Content-Type": "application/json",
              "Authorization": "Bearer None",
              "X-Goog-User-Project": "123",
          },
          json={
              "relationship_type": dc_glossary.RelationshipType.DESCRIBED.value,
              "destination_entry_name": dest_entry_name,
              "source_column": "subcolumn",
          },
      )

  def test_glossary_self_relation_error(self):
    with (
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        )
    ):
      term = bg_term.Term("Term 1", "Desc1")
      glossary_id = glossary_identification.GlossaryId(
          "123",
          "us",
          "test_entry_group_with_terms",
          "glossary_exists"
      )
      glossary = dc_glossary.Glossary(glossary_id)
      glossary._term_cache = {term.display_name: term}
      err = glossary._create_relationship(
          term.display_name,
          term.display_name,
          dc_glossary.RelationshipType.RELATED
      )
      self.assertIsNotNone(err)
      self.assertEqual(err.resources, [term.display_name, term.display_name])
      self.assertEqual(
          err.operation,
          f"create_{dc_glossary.RelationshipType.RELATED.value}_relationship"
      )

  def test_glossary_parse_entry_path_with_source_column(self):
    asset_name = (
        "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
        "entries/test_asset:subcolumn"
    )
    entry, source_column = dc_glossary.Glossary._parse_entry_path(asset_name)
    self.assertEqual(
        entry,
        (
            "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
            "entries/test_asset"
        )
    )
    self.assertEqual(source_column, "subcolumn")

  def test_glossary_parse_entry_path_without_source_column(self):
    asset_name = (
        "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
        "entries/test_asset"
    )
    entry, source_column = dc_glossary.Glossary._parse_entry_path(asset_name)
    self.assertEqual(entry, asset_name)
    self.assertIsNone(source_column)

  def test_glossary_parse_entry_path_no_entry(self):
    asset_name = (
        "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
        "entries/"
    )
    entry, source_column = dc_glossary.Glossary._parse_entry_path(asset_name)
    self.assertIsNone(entry)
    self.assertIsNone(source_column)

  def test_glossary_parse_entry_path_wrong_path(self):
    asset_name = "some_path"
    entry, source_column = dc_glossary.Glossary._parse_entry_path(asset_name)
    self.assertIsNone(entry)
    self.assertIsNone(source_column)

  def test_glossary_parse_entry_source_column_with_multiple_colons(self):
    asset_name = (
        "projects/123/locations/us/entryGroups/"
        "test_entry_group_with_terms/entries/test_asset:subcolumn:with:colons"
    )
    entry, source_column = dc_glossary.Glossary._parse_entry_path(asset_name)
    self.assertEqual(
        entry,
        (
            "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
            "entries/test_asset"
        )
    )
    self.assertEqual(source_column, "subcolumn:with:colons")


if __name__ == "__main__":
  unittest.main()
