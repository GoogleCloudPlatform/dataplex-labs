import unittest
from unittest import mock

import category as bg_category
import entry_type as EntryType
import error
import glossary as dc_glossary
import glossary_identification
import relation_type
import term as bg_term
import user_report
import utils
from tests.test_utils import mocks


class GlossaryTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.enterContext(
        mock.patch(
            "api_call_utils.requests.get",
            side_effect=mocks.mocked_get_api_response,
        )
    )
    self.post_mock = self.enterContext(mock.patch("requests.post"))

  def test_glossary_uid_returned(self):
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_no_terms", "empty_glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    self.assertEqual(
        glossary._glossary_uid, "71372af7-bb1a-4020-aba8-223c57c366d2"
    )

  def test_glossary_not_found(self):
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_no_glossary", "glossary_not_found"
    )
    with self.assertRaises(ValueError):
      dc_glossary.Glossary(glossary_id)

  def test_glossary_print_report_and_exit_on_categories_import_error(self):
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    category1 = bg_category.Category("Category1", "Desc Category1")
    term1 = bg_term.Term("Term1", "Desc Term1")
    glossary._category_cache = {category1.display_name: category1}
    glossary._term_cache = {term1.display_name: term1}

    expected_category_import_error = error.EntryImportError(
        EntryType.EntryType.CATEGORY,
        1,
        [category1.display_name],
        "Some error message",
    )
    expected_imported_categories = []
    expected_not_imported_belongs_to_relations = []
    expected_categories_import_errors = [expected_category_import_error]
    expected_import_glossary_categories_ret = (
        expected_imported_categories,
        expected_not_imported_belongs_to_relations,
        expected_categories_import_errors,
    )
    expected_imported_terms = []
    expected_imported_relations_term_to_term = set()
    expected_terms_import_errors = []
    expected_import_glossary_terms_ret = (
        expected_imported_terms,
        expected_imported_relations_term_to_term,
        expected_terms_import_errors,
    )
    mock_import_glossary_categories = self.enterContext(
        mock.patch.object(
            dc_glossary.Glossary,
            "_import_glossary_categories",
            return_value=expected_import_glossary_categories_ret,
        )
    )
    self.enterContext(
        mock.patch.object(
            dc_glossary.Glossary,
            "_import_glossary_terms",
            return_value=expected_import_glossary_terms_ret,
        )
    )
    mock_print_report_for_erroneous_categories_import = self.enterContext(
        mock.patch.object(
            user_report, "print_report_for_erroneous_categories_import"
        )
    )
    mock_end_program_execution = self.enterContext(
        mock.patch.object(utils, "end_program_execution")
    )

    categories = {1: category1}
    terms = {1: term1}
    glossary.import_glossary(terms, categories)

    mock_import_glossary_categories.assert_called_once_with(categories)
    mock_print_report_for_erroneous_categories_import.assert_called_once_with(
        expected_imported_categories, expected_categories_import_errors
    )
    mock_end_program_execution.assert_called_once()

  def test_glossary_clear_terms_and_categories(self):
    mock_paralellize = self.enterContext(
        mock.patch.object(dc_glossary.Glossary, "_parallelize", return_valu=[])
    )
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    category1 = bg_category.Category(
        "Category1", "Desc Category1", force_category_id="Category1_xyz"
    )
    category2 = bg_category.Category(
        "Category2", "Desc Category2", force_category_id="Category2_xyz"
    )
    term1 = bg_term.Term("Term 1", "Desc Term1", force_term_id="Term1_xyz")
    term2 = bg_term.Term("Term 1", "Desc Term1", force_term_id="Term2_xyz")
    glossary._category_cache = {
        "Category1": category1,
        "Category2": category2,
    }
    glossary._term_cache = {
        "Term1": term1,
        "Term2": term2,
    }
    expected_tasks = [
        ("Term1_xyz",),
        ("Term2_xyz",),
        ("Category1_xyz",),
        ("Category2_xyz",),
    ]

    glossary.clear_glossary()

    mock_paralellize.assert_called_once_with(
        glossary._remove_glossary_entry, expected_tasks
    )

  def test_glossary_remove_entry(self):
    delete_mock = self.enterContext(mock.patch("requests.delete"))
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    category1 = bg_category.Category(
        "Category1", "Desc Category1", force_category_id="Category1_xyz"
    )
    glossary._category_cache = {
        "Category1": category1,
    }
    glossary._remove_glossary_entry(category1.category_id)

    delete_mock.assert_called_with(
        (
            "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
            "entryGroups/test_entry_group_with_categories_and_terms/"
            f"entries/{category1.category_id}"
        ),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer None",
            "X-Goog-User-Project": "123",
        },
        json=None,
    )

  def test_glossary_is_not_empty(self):
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    self.assertFalse(glossary.is_glossary_empty())

  def test_glossary_with_terms_is_not_empty(self):
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    self.assertFalse(glossary.is_glossary_empty())

  def test_glossary_with_categories_is_not_empty(self):
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_categories", "glossary_not_empty"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    self.assertFalse(glossary.is_glossary_empty())

  def test_glossary_is_empty(self):
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_no_terms", "empty_glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    is_empty = glossary.is_glossary_empty()
    self.assertTrue(is_empty)

  def test_glossary_term_is_created(self):
    term = bg_term.Term("Term1", "Desc1")
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._create_glossary_entry(term)
    self.post_mock.assert_called_with(
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
    term = bg_term.Term("Term1", "Desc1", ["name <email>"])
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._create_glossary_entry(term)
    self.post_mock.assert_called_with(
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
                        "contacts": ["name <email>"],
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
    term = bg_term.Term(
        "Term1",
        "Desc1",
        ["name <email>", "name2 <email2>"],
    )
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._create_glossary_entry(term)
    self.post_mock.assert_called_with(
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
                        "contacts": ["name <email>", "name2 <email2>"],
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

  def test_glossary_category_is_created(self):
    category = bg_category.Category("Category1", "Desc1")
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_categories", "glossary_not_empty"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._create_glossary_entry(category)
    self.post_mock.assert_called_with(
        (
            "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
            "entryGroups/test_entry_group_with_categories/"
            f"entries?entry_id={category.category_id}"
        ),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer None",
            "X-Goog-User-Project": "123",
        },
        json={
            "entry_type": "glossary_category",
            "display_name": "Category1",
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
                    "entryGroups/test_entry_group_with_categories/"
                    "entries/glossary_not_empty"
                ),
            },
        },
    )

  def test_glossary_category_with_one_steward_is_created(self):
    category = bg_category.Category("Category1", "Desc1", ["name <email>"])
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_categories", "glossary_not_empty"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._create_glossary_entry(category)
    self.post_mock.assert_called_with(
        (
            "https://datacatalog.googleapis.com/v2/projects/123/"
            "locations/us/entryGroups/test_entry_group_with_categories/"
            f"entries?entry_id={category.category_id}"
        ),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer None",
            "X-Goog-User-Project": "123",
        },
        json={
            "entry_type": "glossary_category",
            "display_name": "Category1",
            "core_aspects": {
                "business_context": {
                    "aspect_type": "business_context",
                    "json_content": {
                        "description": "Desc1",
                        "contacts": ["name <email>"],
                    },
                }
            },
            "core_relationships": {
                "relationship_type": "is_child_of",
                "destination_entry_name": (
                    "projects/123/locations/us/"
                    "entryGroups/test_entry_group_with_categories/"
                    "entries/glossary_not_empty"
                ),
            },
        },
    )

  def test_glossary_category_with_many_stewards_is_created(self):
    category = bg_category.Category(
        "Category1",
        "Desc1",
        ["name <email>", "name2 <email2>"],
    )
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_categories", "glossary_not_empty"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._create_glossary_entry(category)
    self.post_mock.assert_called_with(
        (
            "https://datacatalog.googleapis.com/v2/projects/123/"
            "locations/us/entryGroups/test_entry_group_with_categories/"
            f"entries?entry_id={category.category_id}"
        ),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer None",
            "X-Goog-User-Project": "123",
        },
        json={
            "entry_type": "glossary_category",
            "display_name": "Category1",
            "core_aspects": {
                "business_context": {
                    "aspect_type": "business_context",
                    "json_content": {
                        "description": "Desc1",
                        "contacts": ["name <email>", "name2 <email2>"],
                    },
                }
            },
            "core_relationships": {
                "relationship_type": "is_child_of",
                "destination_entry_name": (
                    "projects/123/locations/us/"
                    "entryGroups/test_entry_group_with_categories/"
                    "entries/glossary_not_empty"
                ),
            },
        },
    )

  def test_glossary_realation_belongs_to_is_created(self):
    term = bg_term.Term("Term 1", "Desc Term", belongs_to_category="Category 1")
    category = bg_category.Category("Category 1", "Desc Category")
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {term.display_name: term}
    glossary._category_cache = {category.display_name: category}
    glossary._create_relationship(
        term.display_name,
        EntryType.EntryType.TERM,
        term.belongs_to_category,
        EntryType.EntryType.CATEGORY,
        relation_type.RelationshipType.BELONGS_TO,
    )
    dest_entry_name = (
        "projects/123/locations/us/entryGroups/test_entry_group_with_categories_and_terms/"
        f"entries/{category.category_id}"
    )
    self.post_mock.assert_called_with(
        (
            "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
            "entryGroups/test_entry_group_with_categories_and_terms/"
            f"entries/{term.term_id}/relationships"
        ),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer None",
            "X-Goog-User-Project": "123",
        },
        json={
            "relationship_type": (
                relation_type.RelationshipType.BELONGS_TO.value
            ),
            "destination_entry_name": dest_entry_name,
        },
    )

  def test_glossary_relation_asset_is_described_by_term_is_created_even_though_asset_entry_not_in_internal_cache(
      self,
  ):
    term = bg_term.Term("Term 1", "Desc Term", force_term_id="term_id")
    asset = "projects/asset_project/locations/us-central1/entryGroups/asset-group/entries/bg_fileset:field1"
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {term.display_name: term}
    glossary._category_cache = {}
    glossary._create_relationship(
        asset,
        EntryType.EntryType.TERM,
        term.display_name,
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.DESCRIBED,
    )
    dest_entry_name = (
        "projects/123/locations/us/entryGroups/test_entry_group_with_categories_and_terms/"
        f"entries/{term.term_id}"
    )
    self.post_mock.assert_called_with(
        (
            "https://datacatalog.googleapis.com/v2/projects/asset_project/"
            "locations/us-central1/entryGroups/asset-group/"
            "entries/bg_fileset/relationships"
        ),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer None",
            "X-Goog-User-Project": "123",
        },
        json={
            "relationship_type": relation_type.RelationshipType.DESCRIBED.value,
            "destination_entry_name": dest_entry_name,
            "source_column": "field1",
        },
    )

  def test_glossary_invalidate_relation_described_when_src_entry_type_is_other_than_term(
      self,
  ):
    term1 = bg_term.Term("Term 1", "Desc1")
    category1 = bg_category.Category("Category 1", "Desc1")
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._category_cache = {
        category1.display_name: category1,
    }
    glossary._term_cache = {term1.display_name: term1}

    err = glossary._create_relationship(
        category1.display_name,
        EntryType.EntryType.CATEGORY,
        term1.display_name,
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.DESCRIBED,
    )

    self.assertIsNotNone(err)
    self.assertEqual(
        err.resources, [category1.display_name, term1.display_name]
    )
    self.assertEqual(
        err.operation,
        f"create_{relation_type.RelationshipType.DESCRIBED.value}_relationship_validation",
    )

    # We should not call post when is_described_by source is not a term
    self.post_mock.assert_not_called()

  def test_glossary_invalidate_relation_described_when_dst_entry_type_is_other_than_term(
      self,
  ):
    term1 = bg_term.Term("Term 1", "Desc1")
    category1 = bg_category.Category("Category 1", "Desc1")
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._category_cache = {
        category1.display_name: category1,
    }
    glossary._term_cache = {term1.display_name: term1}

    err = glossary._create_relationship(
        term1.display_name,
        EntryType.EntryType.TERM,
        category1.display_name,
        EntryType.EntryType.CATEGORY,
        relation_type.RelationshipType.DESCRIBED,
    )

    self.assertIsNotNone(err)
    self.assertEqual(
        err.resources, [term1.display_name, category1.display_name]
    )
    self.assertEqual(
        err.operation,
        f"create_{relation_type.RelationshipType.DESCRIBED.value}_relationship_validation",
    )

    # We should not call post when is_described_by destination is not a term
    self.post_mock.assert_not_called()

  def test_glossary_invalidate_relation_when_source_entry_is_not_in_cache(self):
    term1 = bg_term.Term(
        "Term 1",
        "Desc1",
    )
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {
        term1.display_name: term1,
    }
    glossary._create_relationship(
        "non_existent_entry",
        EntryType.EntryType.TERM,
        term1.display_name,
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.SYNONYMOUS,
    )
    # We should not call post when source entry is not in cache
    self.post_mock.assert_not_called()

  def test_glossary_invalidate_relation_when_destination_entry_is_not_in_cache(
      self,
  ):
    non_existent_entry = "non_existent_entry"
    term1 = bg_term.Term(
        "Term 1",
        "Desc1",
    )
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {
        term1.display_name: term1,
    }
    err = glossary._create_relationship(
        term1.display_name,
        EntryType.EntryType.TERM,
        non_existent_entry,
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.SYNONYMOUS,
    )

    self.assertIsNotNone(err)
    self.assertEqual(err.resources, [term1.display_name, non_existent_entry])
    self.assertEqual(
        err.operation,
        f"create_{relation_type.RelationshipType.SYNONYMOUS.value}_relationship_validation",
    )
    # We should not call post when destination entry is not in cache
    self.post_mock.assert_not_called()

  def test_glossary_invalidate_relation_belongs_to_non_category_destination(
      self,
  ):
    term1 = bg_term.Term(
        "Term 1",
        "Desc1",
        belongs_to_category="Term 2",
    )
    term2 = bg_term.Term("Term 2", "Desc2")
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {
        term1.display_name: term1,
        term2.display_name: term2,
    }
    glossary._create_relationship(
        term1.display_name,
        EntryType.EntryType.TERM,
        term1.belongs_to_category,
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.BELONGS_TO,
    )
    # We should not call post when belongs_to destination is not a category
    self.post_mock.assert_not_called()

  def test_glossary_invalidate_relation_category_balongs_to_category_with_same_display_name(
      self,
  ):
    category1 = bg_category.Category(
        "Category 1",
        "Desc1",
        belongs_to_category="Category 1",
    )
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._category_cache = {
        category1.display_name: category1,
    }
    err = glossary._create_relationship(
        category1.display_name,
        EntryType.EntryType.CATEGORY,
        category1.belongs_to_category,
        EntryType.EntryType.CATEGORY,
        relation_type.RelationshipType.BELONGS_TO,
    )
    self.assertIsNotNone(err.message)
    self.post_mock.assert_not_called()

  def test_glossary_invalidate_relation_with_missing_destination(self):
    category1 = bg_category.Category(
        "Category 1",
        "Desc1",
        belongs_to_category="Missing Category",
    )
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._category_cache = {
        category1.display_name: category1,
    }
    err = glossary._create_relationship(
        category1.display_name,
        EntryType.EntryType.CATEGORY,
        category1.belongs_to_category,
        EntryType.EntryType.CATEGORY,
        relation_type.RelationshipType.BELONGS_TO,
    )
    self.assertIsNotNone(err.message)
    self.post_mock.assert_not_called()

  def test_glossary_invalidate_relation_with_missing_source(self):
    term1 = bg_term.Term(
        "Term 1",
        "Desc1",
        synonyms=["Term 2"],
    )
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {
        term1.display_name: term1,
    }
    err = glossary._create_relationship(
        term1.display_name,
        EntryType.EntryType.TERM,
        "Term 2",
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.SYNONYMOUS,
    )
    self.assertIsNotNone(err.message)
    self.post_mock.assert_not_called()

  def test_glossary_term_and_category_with_same_name_allowed(self):
    term1 = bg_term.Term(
        "PII",
        "Term description",
        belongs_to_category="PII",
        force_term_id="PII_term",
    )
    category1 = bg_category.Category(
        "PII",
        "Category description",
        force_category_id="PII_category",
    )
    glossary_id = glossary_identification.GlossaryId(
        "123",
        "us",
        "test_entry_group_with_categories_and_terms",
        "glossary_not_empty",
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {term1.display_name: term1}
    glossary._category_cache = {category1.display_name: category1}
    glossary._create_relationship(
        term1.display_name,
        EntryType.EntryType.TERM,
        term1.belongs_to_category,
        EntryType.EntryType.CATEGORY,
        relation_type.RelationshipType.BELONGS_TO,
    )
    self.post_mock.assert_called_with(
        (
            "https://datacatalog.googleapis.com/v2/projects/123/locations/us/"
            "entryGroups/test_entry_group_with_categories_and_terms/"
            f"entries/{term1.term_id}/relationships"
        ),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer None",
            "X-Goog-User-Project": "123",
        },
        json={
            "relationship_type": "belongs_to",
            "destination_entry_name": f"projects/123/locations/us/entryGroups/test_entry_group_with_categories_and_terms/entries/{category1.category_id}",
        },
    )

  def test_glossary_synonym_relation_is_created(self):
    term = bg_term.Term("Term 1", "Desc1", synonyms=["Term 2"])
    term2 = bg_term.Term("Term 2", "Desc2", synonyms=["Term 1"])
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {
        term.display_name: term,
        term2.display_name: term2,
    }
    glossary._create_relationship(
        term.display_name,
        EntryType.EntryType.TERM,
        term.synonyms[0],
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.SYNONYMOUS,
    )
    dest_entry_name = (
        "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
        f"entries/{term2.term_id}"
    )
    self.post_mock.assert_called_with(
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
            "relationship_type": (
                relation_type.RelationshipType.SYNONYMOUS.value
            ),
            "destination_entry_name": dest_entry_name,
        },
    )

  def test_glossary_relation_related_to_is_created(self):
    term = bg_term.Term("Term 1", "Desc1", related_terms=["Term 2"])
    term2 = bg_term.Term("Term 2", "Desc2", related_terms=["Term 1"])
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {
        term.display_name: term,
        term2.display_name: term2,
    }
    glossary._create_relationship(
        term.display_name,
        EntryType.EntryType.TERM,
        term.related_terms[0],
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.RELATED,
    )
    dest_entry_name = (
        "projects/123/locations/us/"
        f"entryGroups/test_entry_group_with_terms/entries/{term2.term_id}"
    )
    self.post_mock.assert_called_with(
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
            "relationship_type": relation_type.RelationshipType.RELATED.value,
            "destination_entry_name": dest_entry_name,
        },
    )

  def test_glossary_unsuccessful_creation_returns_error(self):
    self.post_mock = self.enterContext(
        mock.patch(
            "api_call_utils.requests.post",
            side_effect=mocks.mocked_post_failed_api_response,
        )
    )
    term = bg_term.Term("Term 1", "Desc1", related_terms=["Term 2"])
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {term.display_name: term}
    ret = glossary._create_glossary_entry(term)
    self.assertIsNone(ret["json"])
    self.assertIsNotNone(ret["error_msg"])
    self.post_mock.assert_called_with(
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
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {term.display_name: term}

    glossary._create_relationship(
        asset_name,
        EntryType.EntryType.TERM,
        term.display_name,
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.DESCRIBED,
    )

    self.post_mock.assert_called_with(
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
            "relationship_type": relation_type.RelationshipType.DESCRIBED.value,
            "destination_entry_name": dest_entry_name,
        },
    )

  def test_glossary_described_by_relation_with_column_is_created(self):
    term = bg_term.Term("Term 1", "Desc1")
    asset_name = (
        "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
        "entries/test_asset:subcolumn"
    )
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {term.display_name: term}
    glossary._create_relationship(
        asset_name,
        EntryType.EntryType.TERM,
        term.display_name,
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.DESCRIBED,
    )
    dest_entry_name = (
        "projects/123/locations/us/entryGroups/test_entry_group_with_terms/"
        f"entries/{term.term_id}"
    )
    self.post_mock.assert_called_with(
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
            "relationship_type": relation_type.RelationshipType.DESCRIBED.value,
            "destination_entry_name": dest_entry_name,
            "source_column": "subcolumn",
        },
    )

  def test_glossary_self_relation_error(self):
    term = bg_term.Term("Term 1", "Desc1")
    glossary_id = glossary_identification.GlossaryId(
        "123", "us", "test_entry_group_with_terms", "glossary_exists"
    )
    glossary = dc_glossary.Glossary(glossary_id)
    glossary._term_cache = {term.display_name: term}
    err = glossary._create_relationship(
        term.display_name,
        EntryType.EntryType.TERM,
        term.display_name,
        EntryType.EntryType.TERM,
        relation_type.RelationshipType.RELATED,
    )
    self.assertIsNotNone(err)
    self.assertEqual(err.resources, [term.display_name, term.display_name])
    self.assertEqual(
        err.operation,
        f"create_{relation_type.RelationshipType.RELATED.value}_relationship_validation",
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
        ),
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
        ),
    )
    self.assertEqual(source_column, "subcolumn:with:colons")


if __name__ == "__main__":
  unittest.main()
