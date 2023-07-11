"""Functions to get the state of glossary."""

import enum
import multiprocessing
import re
from typing import Any, Iterable, TypeVar

import api_call_utils
import error
import glossary_identification
import logging_utils
import requests
import term as bg_term


_T = TypeVar('_T')
_CreatedRelationship = tuple[str, str, _T]

logger = logging_utils.get_logger()


class RelationshipType(enum.Enum):
  """Enum containing the types of relationships between terms.
  """
  SYNONYMOUS = 'is_synonymous_to'
  RELATED = 'is_related_to'
  DESCRIBED = 'is_described_by'


class Glossary:
  """Instance of a Business Glossary.

  Contains helper methods to transparently handle several REST operations on the
  target glossary.
  """

  def __init__(self, config: glossary_identification.GlossaryId):
    self._config = config
    self._glossary_endpoint = Glossary._configure_endpoint_url(self._config)
    self._term_cache: dict[str, bg_term.Term] = {}
    self._glossary_uid = None
    # Load UID of the target glossary
    self._load_glossary_uid()
    # Store a mapping from display names to Term entries existing in DC
    self._populate_term_cache()

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

    if not self._term_cache:
      logger.info(
          f'Glossary with ID: {self._config.glossary_id} does not have any'
          ' terms.'
      )
      return True

    logger.info(
        f'Glossary with ID: {self._config.glossary_id} already has some terms.'
    )
    return False

  def _populate_term_cache(self) -> None:
    """Populates an internal cache of terms existing in the target glossary."""
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

      # Read terms in the current page
      for entry in response['json'].get('entries'):
        if entry['entryType'] != 'glossary_term':
          continue
        term = bg_term.Term.from_json(entry)
        if term:
          self._term_cache[term.display_name] = term
        else:
          logger.warning(f'Could not import term from {entry}')

  def _create_glossary_term(self, term: bg_term.Term) -> dict[str, Any]:
    """Create new term in target glossary.

    Args:
      term: Term data object.

    Returns:
      Dictionary with response and response code.
    """

    endpoint = f'{self._glossary_endpoint}/entries?entry_id={term.term_id}'

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
            'entry_type': 'glossary_term',
            'display_name': term.display_name,
            'core_aspects': {
                'business_context': {
                    'aspect_type': 'business_context',
                    'json_content': {
                        'description': term.description,
                        'contacts': term.data_stewards,
                    },
                }
            },
            'core_relationships': {
                'relationship_type': 'is_child_of',
                'destination_entry_name': dest_entry_name,
            },
        },
    )

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

    return Glossary._parallelize(self._create_glossary_term, tasks)

  def _is_relationship_valid(
      self, src: str, dst: str, relationship_type: RelationshipType
  ) -> tuple[bool, str | None]:
    """Check if both terms in a relationship exist in the cache of terms.

    Args:
      src: First term of the relationship.
      dst: Second term of the relationship.
      relationship_type: RelationshipType.

    Returns:
      A boolean value specifying if the relationship was created.
      An optional error message containing the reason why the relationship is
        not valid.
    """
    if src == dst:
      err = (
          f'Won\'t be able to create a "{relationship_type.value}" relation'
          f' between "{src}" and itself.'
      )
      return False, err
    if (
        relationship_type != RelationshipType.DESCRIBED
        and src not in self._term_cache
    ):
      err = (
          f'Won\'t be able to create a "{relationship_type.value}" relation'
          f' between "{src}" and "{dst}" because "{src}" doesn\'t exist in the'
          ' CSV.'
      )
      return False, err
    elif dst not in self._term_cache:
      err = (
          f'Won\'t be able to create a "{relationship_type.value}" relation'
          f' between "{src}" and "{dst}" because "{dst}" doesn\'t exist in the'
          ' CSV.'
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
      src: str,
      dst: str,
      relationship_type: RelationshipType
  ) -> error.TermImportError | None:
    """Create a relationship between two terms in DC Business Glossary.

    Args:
      src: source end of the relationship.
      dst: destination end of the relationship.
      relationship_type: RELATED, SYNONYMOUS or DESCRIBED.
    Returns:
      An error, if any.
    """

    valid, error_msg = self._is_relationship_valid(
        src, dst, relationship_type
    )
    if not valid:
      return error.TermImportError(
          -1,
          [src, dst],
          error_msg,
          operation=f'create_{relationship_type.value}_relationship',
      )

    if relationship_type == RelationshipType.DESCRIBED:
      # If the asset has a field or column specified we extract it
      # e.g. for projects/123/locations/us-central1/entryGroups/abc/
      # entries/fileset:field1,
      # we want to split the asset
      # [projects/123/locations/us-central1/entryGroups/abc/entries/fileset]
      # from the subfield [field1]
      entry, source_column = Glossary._parse_entry_path(src)
      if entry is None:
        return error.TermImportError(
            -1,
            [src],
            (
                'Resource does not conform with the expected '
                '"projects/{project_id}/locations/{location}/entryGroups/'
                '{entry_group}/entries/{entry_id}[:{source_column}]" pattern.'
            ),
            operation=f'create_{relationship_type.value}_relationship',
        )

      # For assets described by a term, we use the asset name after extracting
      # the field or column (if any)
      endpoint = (
          'https://datacatalog.googleapis.com/'
          f'v2/{entry}/relationships'
      )
    else:
      # For other terms, we use the internal term_id
      endpoint = f'{self._glossary_endpoint}/entries/{self._term_cache[src].term_id}/relationships'
      # Source column is not used
      source_column = None

    dest_entry_name = (
        f'{self._glossary_endpoint}/entries/'
        f'{self._term_cache[dst].term_id}'
    ).replace('https://datacatalog.googleapis.com/v2/', '')

    # JSON content of the request
    request_body = {
        'relationship_type': relationship_type.value,
        'destination_entry_name': dest_entry_name,
    }

    # If a field or column in the endpoint was specified for a is_described_by
    # relationship, we express it by using the source_column field of the
    # payload
    if relationship_type == RelationshipType.DESCRIBED and source_column:
      request_body['source_column'] = source_column

    ret = api_call_utils.fetch_api_response(
        requests.post,
        endpoint,
        self._config.project_id,
        request_body,
    )

    err = ret['error_msg']
    if err:
      return error.TermImportError(
          -1,
          [src, dst],
          message=err,
          operation=f'create_{relationship_type.value}_relationship',
      )

  def _create_relationships(
      self,
      related_terms: set[tuple[str, str]],
      relationship_type: RelationshipType
  ) -> tuple[
      list[_CreatedRelationship[RelationshipType]],
      list[error.TermImportError]
    ]:
    """Create a relationship between two terms in DC Business Glossary.

    Args:
      related_terms: Set of tuples containing the terms to create the
        relationship for
      relationship_type: RelationshipType.SYNONYMOUS, RelationshipType.RELATED
        or RelasionshipType.DESCRIBED
    Returns:
      List of TermImportError
    """
    errors: list[error.TermImportError] = []
    successful_relations: list[_CreatedRelationship[RelationshipType]] = []
    if not related_terms:
      return successful_relations, errors

    logger.info(f'Adding {relationship_type.value} relations between terms...')
    tasks = [(src, dst, relationship_type,) for src, dst in related_terms]

    ret = Glossary._parallelize(self._create_relationship, tasks)

    for task, err in zip(tasks, ret):
      if err:
        errors.append(err)
      else:
        successful_relations.append(task)

    return successful_relations, errors

  @classmethod
  def _parallelize(cls, task, params):
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    results = pool.starmap(task, params)
    return results

  def import_glossary(
      self, terms: dict[int, bg_term.Term]
  ) -> tuple[
      list[bg_term.Term],
      list[_CreatedRelationship[RelationshipType]],
      list[error.TermImportError]
    ]:
    """Imports terms into Data Catalog Business Glossary.

    Args:
      terms: List of terms to add to the glossary.

    Returns:
      A list of successfully imported terms, and a list of import errors.
    """
    term_import_errors: list[error.TermImportError] = []
    imported_relations = []
    synonym_relations = set()
    related_term_relations = set()
    tagged_asset_relations = set()

    # Create term entries
    ret = self._create_glossary_terms(terms)

    # Gather term creation results and prepare relationships
    for elem_order, response in zip(terms.items(), ret):
      line_num, term = elem_order
      err = response['error_msg']

      if err:
        new_error = error.TermImportError(
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

    tasks = [
        (synonym_relations, RelationshipType.SYNONYMOUS),
        (related_term_relations, RelationshipType.RELATED),
        (tagged_asset_relations, RelationshipType.DESCRIBED),
    ]
    for relations, rel_type in tasks:
      created_relationships, errors = self._create_relationships(
          relations,
          rel_type
      )
      imported_relations.extend(created_relationships)
      term_import_errors.extend(errors)

    return (
        list(self._term_cache.values()),
        imported_relations,
        term_import_errors,
    )

  def _remove_glossary_term(self, term_id: str) -> dict[str, Any]:
    """Remove term in target glossary.

    Args:
      term_id: Term id in the target glossary.

    Returns:
      Dictionary with response and response code.
    """

    endpoint = f'{self._glossary_endpoint}/entries/{term_id}'

    return api_call_utils.fetch_api_response(
        requests.delete, endpoint, self._config.project_id
    )

  def clear_glossary(self) -> bool:
    """Remove existing terms from a Data Catalog Business Glossary.

    Args:
      None.

    Returns:
      A boolean indicating if the operation succeeded.
    """
    logger.info('Clearing the existing terms in the target glossary.')
    tasks = []
    for term in self._term_cache.values():
      tasks.append((term.term_id,))

    ret = Glossary._parallelize(self._remove_glossary_term, tasks)
    for response in ret:
      err = response['error_msg']

      if err:
        logger.error(
            'Could not delete term from the target glossary.'
        )
        return False

    # Refresh term cache
    self._term_cache = {}

    return True
