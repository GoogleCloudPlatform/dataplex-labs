"""This script is used to export the data from a Data Catalog glossary to CSV files - one for categories and one for terms.

Categories CSV file contains the following columns:
  - category_display_name: The display name of the category.
  - description:  Plain text or rich text encoded as plain text description for
  the category
  - steward:  List of data stewards for the current category, with each steward
  separated by a comma
  - belongs_to_category: Display name of a category to which the category
  belongs


Terms CSV file contains the following columns:
  - term_display_name: Unique name for the entry term
  - description: Plain text or rich text encoded as plain text description for
  the term.
  - steward: List of data stewards for the current term, with each steward
  separated by a comma
  - tagged_assets: List of assets tagged with the term, with each asset
  separated by a comma (not implemented yet)
  - synonyms: List of terms that have a synonym relation with the current term,
  with each term separated by a comma
  - related_terms: List of terms that have a related-to relation with the
  current term, with each term separated by a comma
  - belongs_to_category: Display name of a category to which the term belong
"""


import csv
import os
import requests
import sys
from typing import Any, List, Dict
import glossary as dc_glossary
import glossary_identification
import api_call_utils
import logging_utils
import utils

logger = logging_utils.get_logger()
DATACATALOG_BASE_URL = 'https://datacatalog.googleapis.com/v2'
csv.field_size_limit(sys.maxsize)

# Assuming fetch_api_response is defined in api_call_utils
from api_call_utils import fetch_api_response

def fetch_entries(project: str, location: str, entry_group: str) -> List[Dict[str, Any]]:
    """Fetches all entries in the glossary.

  Args:
      project: The Google Cloud Project ID.
      location: The location of the glossary.
      entry_group: The entry group of the glossary.

  Returns:
      A list of dictionaries containing the entries.
  """
    entries = []
    get_full_entry_url = (
      DATACATALOG_BASE_URL
      + f'/projects/{project}/locations/{location}/entryGroups/{entry_group}/entries?view=FULL')
    keep_reading, page_token = True, None

    while keep_reading:
        if page_token:
            endpoint_url = f'{get_full_entry_url}&pageToken={page_token}'
        else:
            endpoint_url = get_full_entry_url

        response = api_call_utils.fetch_api_response(
            requests.get, endpoint_url, project
        )

        if response['error_msg']:
            raise ValueError(response['error_msg'])

        if 'entries' in response['json']:
            entries.extend(response['json']['entries'])

        page_token = response['json'].get('nextPageToken', None)
        if not page_token:
         keep_reading = False

    return entries


def fetch_entry_info(entry_name: str, project: str) -> Dict[str, Any]:
  """Fetches details for a specific entry from the Data Catalog.

  Args:
      entry_name: The full resource name of the entry.
      project: The Google Cloud Project ID.

  Returns:
      A dictionary containing the entry details.
  """
  fetch_entry_info_url = DATACATALOG_BASE_URL + f'/{entry_name}'

  response = api_call_utils.fetch_api_response(
      requests.get, fetch_entry_info_url, project
  )
  if response['error_msg']:
    raise ValueError(response['error_msg'])
  return response['json']


def fetch_relationships(entry_name: str, project: str) -> List[Dict[str, Any]]:
  """Fetches relationships for a specific entry from the Data Catalog.

  Args:
      entry_name: The full resource name of the entry.
      project: The Google Cloud Project ID.

  Returns:
      A list of dictionaries containing the relationships.
  """
  fetch_relationships_url = DATACATALOG_BASE_URL + f'/{entry_name}/relationships'
  response = api_call_utils.fetch_api_response(
      requests.get, fetch_relationships_url, project
  )
  if response['error_msg']:
    raise ValueError(response['error_msg'])
  return response['json'].get('relationships', [])


def get_entry_display_name(entry_name: str, project: str) -> str:
  fetch_display_name_url = DATACATALOG_BASE_URL + f'/{entry_name}'
  response = api_call_utils.fetch_api_response(
      requests.get, fetch_display_name_url, project
  )
  if response['error_msg']:
    raise ValueError(response['error_msg'])
  return response['json'].get('displayName', '')


def export_glossary_entries(
    entries: List[Dict[str, Any]],
    categories_csv: str,
    terms_csv: str,
    project: str,
):
  """Exports the glossary entries to a CSV file.

  Args:
      entries: The list of entries to export.
      categories_csv: The path to the CSV file to export the categories data.
      terms_csv: The path to the CSV file to export the terms data.
      project: The Google Cloud Project ID.
  """
  categories_fields = [
      'category_display_name',
      'description',
      'steward',
      'belongs_to_category',
  ]
  terms_fields = [
      'term_display_name',
      'description',
      'steward',
      'tagged_assets',
      'synonyms',
      'related_terms',
      'belongs_to_category',
  ]

  with (
      open(categories_csv, mode='w', newline='') as categories_file,
      open(terms_csv, mode='w', newline='') as terms_file,
  ):
    categories_writer = csv.DictWriter(
        categories_file, fieldnames=categories_fields, quoting=csv.QUOTE_ALL
    )
    terms_writer = csv.DictWriter(
        terms_file, fieldnames=terms_fields, quoting=csv.QUOTE_ALL
    )

    for entry in entries:
      entry_info = fetch_entry_info(entry['name'], project)
      entry_type = entry_info.get('entryType', '')
      display_name = entry_info.get('displayName', '')

      # Initialize core aspects and json content
      core_aspects = entry_info.get('coreAspects', {})
      business_context = core_aspects.get('business_context', {})
      business_context = business_context.get('jsonContent', {})

      # Extract description and stewards
      description = business_context.get('description', '')
      stewards = ', '.join(business_context.get('contacts', []))

      # Fetch relationships
      relationships = fetch_relationships(entry_info['name'], project)
      belongs_to_category = ''
      synonyms = ''
      related_terms = ''

      for rel in relationships:
        if rel['relationshipType'] == 'belongs_to':
          belongs_to_category = get_entry_display_name(
              rel['destinationEntryName'], project
          )
        elif rel['relationshipType'] == 'is_synonymous_to':
          synonyms += (
              get_entry_display_name(rel['destinationEntryName'], project)
              + ', '
          )
        elif rel['relationshipType'] == 'is_related_to':
          related_terms += (
              get_entry_display_name(rel['destinationEntryName'], project)
              + ', '
          )

      synonyms = synonyms.rstrip(', ')
      related_terms = related_terms.rstrip(', ')

      if entry_type == 'glossary_term':
        terms_writer.writerow({
            'term_display_name': display_name,
            'description': description,
            'steward': stewards,
            'tagged_assets': '',
            'synonyms': synonyms,
            'related_terms': related_terms,
            'belongs_to_category': belongs_to_category,
        })
      elif entry_type == 'glossary_category':
        categories_writer.writerow({
            'category_display_name': display_name,
            'description': description,
            'steward': stewards,
            'belongs_to_category': belongs_to_category,
        })


def main():
  args = utils.get_export_arguments()
  utils.validate_export_args(args)

  try:
    dc_glossary.Glossary(
        glossary_identification.GlossaryId(
            project_id=args.project,
            location=args.location,
            entry_group=args.group,
            glossary_id=args.glossary,
        )
    )
  except ValueError as e:
    logger.error(
        "Can't proceed with export. Please select a valid glossary.", e
    )
    sys.exit(1)
  entries = fetch_entries(args.project, args.location, args.group)
  export_glossary_entries(
      entries, args.categories_csv, args.terms_csv, args.project
  )


if __name__ == '__main__':
  main()
