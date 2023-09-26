"""Functions to get the state of glossary."""

import multiprocessing
import re
from typing import Any, Iterable, TypeVar

import api_call_utils
import category as bg_category
import entry_type as entry_type_lib
import error
import glossary_identification
import import_types
import logging_utils
import relation_type
import requests
import term as bg_term
import user_report
import utils


logger = logging_utils.get_logger()


class Glossary:
  """Instance of a Business Glossary.

  Contains helper methods to transparently handle several REST operations on the
  target glossary.
  """

  def __init__(self, config: glossary_identification.GlossaryId):
    self._config = config
    self._glossary_endpoint = Glossary._configure_endpoint_url(self._config)
    self._category_cache: dict[str, bg_category.Category] = {}
    self._term_cache: dict[str, bg_term.Term] = {}
    self._glossary_uid = None
    # Load UID of the target glossary
    self._load_glossary_uid()
    # Store a mapping from display names to Term entries existing in DC
    self._populate_caches()

  @classmethod
  def _configure_endpoint_url(
      cls,
      config: glossary_identification.GlossaryId
  ) -> str:
    """Setup destination glossary resource URL.

    Args:
      config: Glossary Configuration

    Returns:
      string
    """
    return (
        f'https://datacatalog.googleapis.com/v2/projects'
        f'/{config.project_id}/locations/{config.location}'
        f'/entryGroups/{config.entry_group}'
    )

  def _load_glossary_uid(self) -> None:
    """Fetch glossary UID for the requested glossary."""
    endpoint = f'{self._glossary_endpoint}/entries/{self._config.glossary_id}'

    response = api_call_utils.fetch_api_response(
        requests.get, endpoint, self._config.project_id
    )

    if not response['error_msg'] and 'entryUid' in response['json']:
      self._glossary_uid = response['json']['entryUid']
      return
    logger.error(response['error_msg'])
    raise ValueError("Provided glossary information doesn't exist.")

  def is_glossary_empty(self) -> bool:
    """Verify if targeted glossary is empty."""

    has_categories = bool(self._category_cache)
    has_terms = bool(self._term_cache)
    if not has_categories and not has_terms:
      logger.info(
          f'Glossary with ID: {self._config.glossary_id} does not have any'
          ' categories nor terms.'
      )
      return True

    if has_categories:
      logger.info(
          f'Glossary with ID: {self._config.glossary_id} already has some'
          ' categories.'
      )

    if has_terms:
      logger.info(
          f'Glossary with ID: {self._config.glossary_id} already has some'
          ' terms.'
      )
    return False

  def _populate_caches(self):
    """Populates internal caches of terms and categories existing in the target glossary."""
    endpoint = f'{self._glossary_endpoint}/entries?view=FULL'
    keep_reading, page_token = True, None
    while keep_reading:
      if page_token:
        endpoint_url = f'{endpoint}&pageToken={page_token}'
      else:
        endpoint_url = endpoint
      response = api_call_utils.fetch_api_response(
          requests.get, endpoint_url, self._config.project_id
      )

      if response['error_msg']:
        logger.error(response['error_msg'])
        return

      if 'entries' not in response['json']:
        return

      # Check if we need to read another page
      page_token = response['json'].get('nextPageToken', None)
      if not page_token:
        keep_reading = False

      self._populate_term_cache(response)
      self._populate_category_cache(response)

  def _populate_term_cache(self, response: dict[str, Any]) -> None:
    """Populates an internal cache of terms existing in the target glossary."""
    for entry in response['json'].get('entries'):
      if entry['entryType'] != 'glossary_term':
        continue
      term = bg_term.Term.from_dict(entry)
      if term:
        self._term_cache[term.display_name] = term
      else:
        logger.warning(f'Could not import term from {entry}')

  def _populate_category_cache(self, response: dict[str, Any]) -> None:
    """Populates an internal cache of categories existing in the target glossary."""
    for entry in response['json'].get('entries'):
      if entry['entryType'] != 'glossary_category':
        continue
      category = bg_category.Category.from_dict(entry)
      if category:
        self._category_cache[category.display_name] = category
      else:
        logger.warning(f'Could not import category from {entry}')

  def _create_glossary_entry(
      self, entry: bg_term.Term | bg_category.Category
  ) -> dict[str, Any]:
    """Create new entry (term or category) in target glossary.

    Args:
      entry: entry data object - Term or Category.

    Returns:
      Dictionary with response and response code.
    """
    endpoint = (
        f'{self._glossary_endpoint}/entries?entry_id={entry.category_id}'
        if isinstance(entry, bg_category.Category)
        else f'{self._glossary_endpoint}/entries?entry_id={entry.term_id}'
    )

    entry_type = (
        'glossary_category'
        if isinstance(entry, bg_category.Category)
        else 'glossary_term'
    )

    dest_entry_name = (
        f'{self._glossary_endpoint}/entries/{self._config.glossary_id}'.replace(
            'https://datacatalog.googleapis.com/v2/', ''
        )
    )

    return api_call_utils.fetch_api_response(
        requests.post,
        endpoint,
        self._config.project_id,
        {
            'entry_type': entry_type,
            'display_name': entry.display_name,
            'core_aspects': {
                'business_context': {
                    'aspect_type': 'business_context',
                    'json_content': {
                        'description': entry.description,
                        'contacts': entry.data_stewards,
                    },
                }
            },
            'core_relationships': {
                'relationship_type': 'is_child_of',
                'destination_entry_name': dest_entry_name,
            },
        },
    )

  def _create_glossary_categories(
      self, categories: dict[int, bg_category.Category]
  ) -> Iterable[Any]:
    """Create new categories in the target glossary.

    Args:
      categories: Dictionary mapping from lines in the csv to categories

    Returns:
      Iterable containing errors
    """
    tasks = [(category,) for category in categories.values()]

    return Glossary._parallelize(self._create_glossary_entry, tasks)

  def _create_glossary_terms(
      self, terms: dict[int, bg_term.Term]
  ) -> Iterable[Any]:
    """Create new terms in the target glossary.

    Args:
      terms: Dictionary mapping from lines in the csv to terms

    Returns:
      Iterable containing errors
    """
    tasks = [(term,) for term in terms.values()]

    return Glossary._parallelize(self._create_glossary_entry, tasks)

  def _get_entry_from_cache(
      self, display_name: str, entry_type: entry_type_lib.EntryType
  ) -> bg_term.Term | bg_category.Category | None:
    """Returns entry (term or category) based on display_name and entry_type.

    Args:
      display_name: string indicating display_name of the entry
      entry_type: EntryType enum indicating entry type

    Returns:
    """
    if entry_type == entry_type_lib.EntryType.CATEGORY:
      return self._category_cache.get(display_name)
    elif entry_type == entry_type_lib.EntryType.TERM:
      return self._term_cache.get(display_name)
    return None

  def _get_entry_id_from_cache(
      self, display_name: str, entry_type: entry_type_lib.EntryType
  ) -> str | None:
    entry = self._get_entry_from_cache(display_name, entry_type)
    if isinstance(entry, bg_term.Term):
      return entry.term_id
    elif isinstance(entry, bg_category.Category):
      return entry.category_id
    return None

  def _is_relationship_valid(
      self,
      src_display_name: str,
      src_type: entry_type_lib.EntryType,
      dst_display_name: str,
      dst_type: entry_type_lib.EntryType,
      relationship_type: relation_type.RelationshipType,
  ) -> tuple[bool, str | None]:
    """Check if both terms in a relationship exist in the cache of terms.

    Args:
      src_display_name: Display name of the first entry of the relationship.
      src_type: EntryType of the source entry.
      dst_display_name: Display name of the second entry of the relationship.
      dst_type: EntryType of the destination entry.
      relationship_type: RELATED, SYNONYMOUS, DESCRIBED or BELONGS_TO.

    Returns:
      A boolean value specifying if the relationship was created.
      An optional error message containing the reason why the relationship is
        not valid.
    """
    src_entry = self._get_entry_from_cache(src_display_name, src_type)
    dst_entry = self._get_entry_from_cache(dst_display_name, dst_type)

    # Described is a relation between asset (not present in the internal cache)
    # and term. We want to check validity of this special relation as first.
    if (
        relationship_type == relation_type.RelationshipType.DESCRIBED
        and src_type == entry_type_lib.EntryType.TERM
        and dst_type == entry_type_lib.EntryType.TERM
        and dst_entry
    ):
      return True, None
    if relationship_type == relation_type.RelationshipType.DESCRIBED and (
        src_type != entry_type_lib.EntryType.TERM
        or dst_type != entry_type_lib.EntryType.TERM
    ):
      err = (
          f'Won\'t be able to create a "{relationship_type.value}" relation'
          f' between "{src_display_name}" and "{dst_display_name}" because'
          f' "{src_type}" is not a term or "{dst_type}" is not a term.'
      )
      return False, err
    if src_display_name == dst_display_name and src_type == dst_type:
      err = (
          f'Won\'t be able to create a "{relationship_type.value}" relation'
          f' between "{src_display_name}" and itself.'
      )
      return False, err
    if not src_entry:
      err = (
          f'Won\'t be able to create a "{relationship_type.value}" relation'
          f' between "{src_display_name}" and "{dst_display_name}" because'
          f' "{src_display_name}" doesn\'t exist in the CSV.'
      )
      return False, err
    elif not dst_entry:
      err = (
          f'Won\'t be able to create a "{relationship_type.value}" relation'
          f' between "{src_display_name}" and "{dst_display_name}" because'
          f' "{dst_display_name}" doesn\'t exist in the CSV.'
      )
      return False, err
    elif (
        relationship_type == relation_type.RelationshipType.BELONGS_TO
        and dst_type != entry_type_lib.EntryType.CATEGORY
    ):
      err = (
          f'Won\'t be able to create a "{relationship_type.value}" relation'
          f' between "{src_display_name}" and "{dst_display_name}" because'
          f' "{dst_display_name}" is not a category.'
      )
      return False, err
    return True, None

  @classmethod
  def _parse_entry_path(cls, entry_path) -> tuple[str | None, str | None]:
    """Parses an entry path containin an optional source column.

    Args:
      entry_path: Path to an entry in the format:
        'projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry}[:{source_column}]}'

    Returns:
      Path of the entry if any.
      Source column in the entry if any.
    """
    pattern = (
        r'projects/(?P<projectId>[^/]+)/locations/(?P<location>[^/]+)/entryGroups/(?P<entryGroup>[^/]+)/entries/(?P<entryId>[^:/]+)(:{1}(?P<sourceColumn>[^/]+))?'
    )
    match = re.fullmatch(pattern, entry_path)
    if match is None:
      return None, None

    entry = (
        f'projects/{match.group("projectId")}/'
        f'locations/{match.group("location")}/'
        f'entryGroups/{match.group("entryGroup")}/'
        f'entries/{match.group("entryId")}'
    )
    source_column = match.group('sourceColumn') or None
    return entry, source_column

  def _create_relationship(
      self,
      src_display_name: str,
      src_type: entry_type_lib.EntryType,
      dst_display_name: str,
      dst_type: entry_type_lib.EntryType,
      relationship_type: relation_type.RelationshipType,
  ) -> error.EntryImportError | None:
    """Create a relationship between two entries in DC Business Glossary.

    Args:
      src_display_name: display name of the source end of the relationship.
      src_type: EntryType of the source end.
      dst_display_name: display name of the destination end of the relationship.
      dst_type: EntryType of the destination end.
      relationship_type: RELATED, SYNONYMOUS, DESCRIBED or BELONGS_TO.

    Returns:
      An error, if any.
    """

    valid, error_msg = self._is_relationship_valid(
        src_display_name,
        src_type,
        dst_display_name,
        dst_type,
        relationship_type,
    )
    if not valid:
      return error.EntryImportError(
          src_type,
          -1,
          [src_display_name, dst_display_name],
          error_msg,
          operation=f'create_{relationship_type.value}_relationship_validation',
      )

    if relationship_type == relation_type.RelationshipType.DESCRIBED:
      # If the asset has a field or column specified we extract it
      # e.g. for projects/123/locations/us-central1/entryGroups/abc/
      # entries/fileset:field1,
      # we want to split the asset
      # [projects/123/locations/us-central1/entryGroups/abc/entries/fileset]
      # from the subfield [field1]
      entry, source_column = Glossary._parse_entry_path(src_display_name)
      if entry is None:
        return error.EntryImportError(
            src_type,
            -1,
            [src_display_name],
            (
                'Resource does not conform with the expected '
                '"projects/{project_id}/locations/{location}/entryGroups/'
                '{entry_group}/entries/{entry_id}[:{source_column}]" pattern.'
            ),
            operation=f'create_{relationship_type.value}_relationship',
        )

      # For assets described by a term, we use the asset name after extracting
      # the field or column (if any)
      endpoint = f'https://datacatalog.googleapis.com/v2/{entry}/relationships'
    else:
      # For other entries, we use the internal id
      src_entry_id = self._get_entry_id_from_cache(src_display_name, src_type)
      if not src_entry_id:
        return error.EntryImportError(
            src_type,
            -1,
            [src_display_name, dst_display_name],
            f'Source entry {src_display_name} not found.',
            operation=f'create_{relationship_type.value}_relationship',
        )
      endpoint = (
          f'{self._glossary_endpoint}/entries/{src_entry_id}/relationships'
      )
      # Source column is not used
      source_column = None

    dst_entry_id = self._get_entry_id_from_cache(dst_display_name, dst_type)
    if not dst_entry_id:
      return error.EntryImportError(
          src_type,
          -1,
          [src_display_name, dst_display_name],
          f'Destination entry {dst_display_name} not found.',
          operation=f'create_{relationship_type.value}_relationship',
      )
    dest_entry_name = (
        f'{self._glossary_endpoint}/entries/{dst_entry_id}'
    ).replace('https://datacatalog.googleapis.com/v2/', '')

    # JSON content of the request
    request_body = {
        'relationship_type': relationship_type.value,
        'destination_entry_name': dest_entry_name,
    }

    # If a field or column in the endpoint was specified for a is_described_by
    # relationship, we express it by using the source_column field of the
    # payload
    if (
        relationship_type == relation_type.RelationshipType.DESCRIBED
        and source_column
    ):
      request_body['source_column'] = source_column

    ret = api_call_utils.fetch_api_response(
        requests.post,
        endpoint,
        self._config.project_id,
        request_body,
    )

    err = ret['error_msg']
    if err:
      return error.EntryImportError(
          src_type,
          -1,
          [src_display_name, dst_display_name],
          message=err,
          operation=f'create_{relationship_type.value}_relationship',
      )

  def _create_relationships(
      self,
      src_type: entry_type_lib.EntryType,
      dst_type: entry_type_lib.EntryType,
      related_entries: set[tuple[str, str]],
      relationship_type: relation_type.RelationshipType,
  ) -> tuple[
    list[import_types._CreatedRelationship[relation_type.RelationshipType]],
    list[error.EntryImportError],
  ]:
    """Create a relationship between two entries in DC Business Glossary.

    Args:
      src_type: EntryType of source entries
      dst_type: EntryType of destination entries
      related_entries: Set of tuples containing the entries to create the
        relationship for
      relationship_type: SYNONYMOUS, RELATED, DESCRIBED or BELONGS_TO

    Returns:
      List of EntryImportError
    """
    errors: list[error.EntryImportError] = []
    successful_relations: list[
      import_types._CreatedRelationship[relation_type.RelationshipType]
    ] = []
    if not related_entries:
      return successful_relations, errors

    logger.info(
        f'Adding {relationship_type.value} relations between'
        f' {src_type.value} and {dst_type.value} entries...'
    )
    tasks = [
        (src, src_type, dst, dst_type, relationship_type)
        for src, dst in related_entries
    ]

    ret = Glossary._parallelize(self._create_relationship, tasks)

    for task, err in zip(tasks, ret):
      if err:
        errors.append(err)
      else:
        src, _, dst, _, _ = task
        successful_relation = (src, dst, relationship_type)
        successful_relations.append(successful_relation)

    return successful_relations, errors

  @classmethod
  def _parallelize(cls, task, params):
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    results = pool.starmap(task, params)
    return results

  def import_glossary(
      self,
      terms: dict[int, bg_term.Term] | None,
      categories: dict[int, bg_category.Category] | None,
  ) -> import_types._ImportResult:
    """Imports categories, terms and relationships to Data Catalog Business Glossary.

    Args:
      terms: dictionary indicating Term object for related line number
      categories: dictionary indicating Category object for related line number

    Returns:
      A tuple consisting of:
      * dictionary mapping EntryType to list of imported Entries (Terms or
        Categories)
      * dictionary mapping EntryType to list of imported relations
      * lit of import errors
    """
    imported_entries = {}
    imported_relations = {
        entry_type_lib.EntryType.TERM: [],
        entry_type_lib.EntryType.CATEGORY: [],
    }
    import_errors = []
    not_imported_category_belongs_to_category_relations = set()

    # Import categories if they were parsed
    if categories is not None:
      (
          imported_categories,
          not_imported_belongs_to_relations,
          categories_import_errors,
      ) = self._import_glossary_categories(categories)
      imported_entries[entry_type_lib.EntryType.CATEGORY] = imported_categories
      import_errors.extend(categories_import_errors)
      not_imported_category_belongs_to_category_relations.update(
          not_imported_belongs_to_relations
      )
      if categories_import_errors:
        error_log_suffix = ' No terms were imported.' if terms else ''
        logger.error(
            'Errors occurred during categories import.%s', error_log_suffix
        )
        user_report.print_report_for_erronous_categories_import(
            imported_categories, categories_import_errors
        )
        utils.end_program_execution()

    # Import terms if they were parsed
    if terms is not None:
      (
          imported_terms,
          imported_relations_term_to_term,
          terms_import_errors,
      ) = self._import_glossary_terms(terms)
      imported_entries[entry_type_lib.EntryType.TERM] = imported_terms
      imported_relations[entry_type_lib.EntryType.TERM].extend(
          imported_relations_term_to_term
      )
      import_errors.extend(terms_import_errors)

    # Import category belongs_to category relations as second (due to hierarhcy
    # limit)
    (
        imported_relations_category_to_category,
        category_to_category_relations_import_error,
    ) = self._create_relationships(
        src_type=entry_type_lib.EntryType.CATEGORY,
        dst_type=entry_type_lib.EntryType.CATEGORY,
        related_entries=not_imported_category_belongs_to_category_relations,
        relationship_type=relation_type.RelationshipType.BELONGS_TO,
    )
    imported_relations[entry_type_lib.EntryType.CATEGORY].extend(
        imported_relations_category_to_category
    )
    import_errors.extend(category_to_category_relations_import_error)

    return (imported_entries, imported_relations, import_errors)

  def _import_glossary_categories(
      self, categories: dict[int, bg_category.Category]
  ) -> tuple[
    list[bg_category.Category],
    set[tuple[str, str]],
    list[error.EntryImportError],
  ]:
    """Imports categories into Data Catalog Business Glossary.

    Args:
      categories: List of categories to add to the glossary.

    Returns:
      A tuple containing:
      * a list of successfully imported categories
      * a set of unimported category belongs_to category relations,
      * a list of import errors
    """
    category_import_errors: list[error.EntryImportError] = []
    # We want to import category belongs_to category relations later.
    # Due to hierarchy height limit we allow terms to create belongs_to
    # relationships first.
    not_imported_belongs_to_relations = set()

    # Create category entries
    ret = self._create_glossary_categories(categories)

    # Gather category creation results and prepare relationships
    for elem_order, response in zip(categories.items(), ret):
      line_num, category = elem_order
      err = response['error_msg']

      if err:
        new_error = error.EntryImportError(
            entry_type_lib.EntryType.CATEGORY,
            line_num,
            [category.display_name],
            message=err,
            operation='add_new_category',
        )
        category_import_errors.append(new_error)
      else:
        # Populate internal category cache
        self._category_cache[category.display_name] = category

        # Add belongs to category relations to create later
        if category.belongs_to_category:
          not_imported_belongs_to_relations.add(
              (category.display_name, category.belongs_to_category)
          )

    return (
        list(self._category_cache.values()),
        not_imported_belongs_to_relations,
        category_import_errors,
    )

  def _import_glossary_terms(
      self, terms: dict[int, bg_term.Term]
  ) -> tuple[
    list[bg_term.Term],
    list[import_types._CreatedRelationship[relation_type.RelationshipType]],
    list[error.EntryImportError],
  ]:
    """Imports terms into Data Catalog Business Glossary.

    Args:
      terms: List of terms to add to the glossary.

    Returns:
      A tuple containing:
      * a list of successfully imported terms
      * a list of imported relations,
      * a list of import errors
    """
    term_import_errors: list[error.EntryImportError] = []
    imported_relations = []
    synonym_relations = set()
    related_term_relations = set()
    tagged_asset_relations = set()
    belongs_to_relations = set()

    # Create term entries
    ret = self._create_glossary_terms(terms)

    # Gather term creation results and prepare relationships
    for elem_order, response in zip(terms.items(), ret):
      line_num, term = elem_order
      err = response['error_msg']

      if err:
        new_error = error.EntryImportError(
            entry_type_lib.EntryType.TERM,
            line_num,
            [term.display_name],
            message=err,
            operation='add_new_term',
        )
        term_import_errors.append(new_error)
      else:
        # Populate internal term cache
        self._term_cache[term.display_name] = term
        # Add synonym relations to create
        for dst in term.synonyms:
          # Sort terms by name to make sure the relationship is only created
          # once.
          first, second = min(term.display_name, dst), max(
              term.display_name, dst
          )
          synonym_relations.add((first, second))

        # Add related term relations to create
        for dst in term.related_terms:
          # Sort terms by name to make sure the relationship is only created
          # once.
          first, second = min(term.display_name, dst), max(
              term.display_name, dst
          )
          related_term_relations.add((first, second))

        # Add tagged asset relations to create
        for src in term.tagged_assets:
          # Sort terms by name to make sure the relationship is only created
          # once.
          tagged_asset_relations.add((src, term.display_name))

        # Add belongs to category relations to create
        if term.belongs_to_category:
          belongs_to_relations.add(
              (term.display_name, term.belongs_to_category)
          )

    tasks = [
        (
            entry_type_lib.EntryType.TERM,
            entry_type_lib.EntryType.TERM,
            synonym_relations,
            relation_type.RelationshipType.SYNONYMOUS,
        ),
        (
            entry_type_lib.EntryType.TERM,
            entry_type_lib.EntryType.TERM,
            related_term_relations,
            relation_type.RelationshipType.RELATED,
        ),
        (
            entry_type_lib.EntryType.TERM,
            entry_type_lib.EntryType.TERM,
            tagged_asset_relations,
            relation_type.RelationshipType.DESCRIBED,
        ),
        (
            entry_type_lib.EntryType.TERM,
            entry_type_lib.EntryType.CATEGORY,
            belongs_to_relations,
            relation_type.RelationshipType.BELONGS_TO,
        ),
    ]
    for src_type, dst_type, relations, rel_type in tasks:
      created_relationships, errors = self._create_relationships(
          src_type=src_type,
          dst_type=dst_type,
          related_entries=relations,
          relationship_type=rel_type,
      )
      imported_relations.extend(created_relationships)
      term_import_errors.extend(errors)

    return (
        list(self._term_cache.values()),
        imported_relations,
        term_import_errors,
    )

  def _remove_glossary_entry(self, entry_id: str) -> dict[str, Any]:
    """Remove entry in target glossary.

    Args:
      entry_id: Entry id in the target glossary.

    Returns:
      Dictionary with response and response code.
    """

    endpoint = f'{self._glossary_endpoint}/entries/{entry_id}'

    return api_call_utils.fetch_api_response(
        requests.delete, endpoint, self._config.project_id
    )

  def clear_glossary(self) -> bool:
    """Remove existing terms and categories from a Data Catalog Business Glossary.

    Args:
      None.

    Returns:
      A boolean indicating if the operation succeeded.
    """
    logger.info(
        'Clearing the existing terms and categories in the target glossary.'
    )
    tasks = []
    for term in self._term_cache.values():
      tasks.append((term.term_id,))
    for category in self._category_cache.values():
      tasks.append((category.category_id,))

    ret = Glossary._parallelize(self._remove_glossary_entry, tasks)
    for response in ret:
      err = response['error_msg']

      if err:
        logger.error(
            'Could not delete entry (term or catgory) from the target glossary.'
        )
        return False

    # Refresh term cache
    self._term_cache = {}
    self._category_cache = {}

    return True
