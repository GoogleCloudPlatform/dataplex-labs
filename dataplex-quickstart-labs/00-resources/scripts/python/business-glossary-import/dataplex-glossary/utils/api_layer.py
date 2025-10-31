"""
API Layer for Dataplex Glossary Operations.

This module provides a common interface for interacting with the Dataplex API
for glossary-related operations. It's used by both export and import utilities.
"""

# Standard library imports
import os
import sys
from typing import Dict, List, Optional

# Third-party imports
import requests
from google.auth import default
from googleapiclient.discovery import build

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Local imports - use relative imports from utils directory
from . import api_call_utils, business_glossary_utils, logging_utils
from .api_call_utils import fetch_api_response
from .constants import (
    CLOUD_RESOURCE_MANAGER_BASE_URL,
    DATAPLEX_BASE_URL,
    DATAPLEX_ENTRY_PATTERN,
    PROJECT_PATTERN,
    SPREADSHEET_URL_PATTERN,
)
from .error import *

logger = logging_utils.get_logger()


def authenticate_dataplex() -> build:
    """Authenticates with the Dataplex API using Application Default Credentials.
    
    Returns:
        A Dataplex API service object.
    
    Raises:
        google.auth.exceptions.DefaultCredentialsError: If credentials cannot be found.
        DataplexAPIError: If there is an error during authentication.
    """
    try:
        dataplex_scopes = ['https://www.googleapis.com/auth/cloud-platform']
        dataplex_creds, _ = default(scopes=dataplex_scopes)
        return build('dataplex', 'v1', credentials=dataplex_creds, cache_discovery=False)
    except Exception as e:
        logger.error(f"Error during Dataplex authentication: {e}")
        raise DataplexAPIError(f"Error during Dataplex authentication: {e}")

def authenticate_sheets() -> build:
    """Authenticates with the Google Sheets API using Application Default Credentials.
    
    Returns:
        A Google Sheets API service object.
    
    Raises:
        google.auth.exceptions.DefaultCredentialsError: If credentials cannot be found.
        SheetsAPIError: If there is an error during authentication.
    """
    try:
        sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        sheets_creds, _ = default(scopes=sheets_scopes)
        return build('sheets', 'v4', credentials=sheets_creds)
    except Exception as e:
        logger.error(f"Error during Sheets authentication: {e}")
        raise SheetsAPIError(f"Error during Sheets authentication: {e}")

def list_glossary_categories(dataplex_service: build, glossary_name: str) -> List[Dict]:
    """Lists categories from a Dataplex glossary.
    
    Args:
        dataplex_service: The Dataplex API service object.
        glossary_name: The full glossary name.
    
    Returns:
        A list of glossary categories.
    
    Raises:
      DataplexAPIError: If there is an error during the API call.
      NoCategoriesFoundError: If no categories are found.
    """
    try:
        request = dataplex_service.projects().locations().glossaries().categories().list(
            parent=glossary_name, pageSize=1000)

        categories = []
        while request:
            response = request.execute()
            if 'categories' in response:
                categories.extend(response['categories'])
            request = dataplex_service.projects().locations().glossaries().categories().list_next(request, response)
        if not categories:
            logger.warning(f"No categories found for {glossary_name}")
            raise NoCategoriesFoundError(f"No categories found for {glossary_name}")
        return categories
    except Exception as e:
        logger.error(f"Error while listing glossary categories: {e}")
        raise DataplexAPIError(f"Error while listing glossary categories: {e}")


def list_glossary_terms(dataplex_service: build, glossary_name: str) -> List[Dict]:
    """Lists terms from a Dataplex glossary, handling pagination.

    Args:
        dataplex_service: The Dataplex API service object.
        glossary_name: The full glossary name.

    Returns:
        A list of glossary terms.

    Raises:
      DataplexAPIError: If there is an error during the API call.
    """
    try:
        terms = []
        page_token = None
        while True:
            request = dataplex_service.projects().locations().glossaries().terms().list(
                parent=glossary_name, pageSize=1000, pageToken=page_token
            )
            response = request.execute()

            if 'terms' in response:
                terms.extend(response['terms'])
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        
        if not terms:
            logger.warning(f"No terms found for {glossary_name}")
        return terms
    except Exception as e:
        logger.error(f"Error while listing glossary terms for {glossary_name}: {e}", exc_info=True)
        raise DataplexAPIError(f"Error while listing glossary terms for {glossary_name}: {e}")

def lookup_entry_links_for_term(entry_id: str, user_project: str) -> Optional[List[Dict]]:
    """Looks up EntryLinks for a given glossary term.

    Args:
        entry_id: The full entry ID for the term.
        user_project: The project ID to bill for the API call.

    Returns:
        A list of EntryLinks or None if none are found.

    Raises:
        DataplexAPIError: If there is an error during the API call.
    """
    try:
        # Extract project and location from entry ID
        match = DATAPLEX_ENTRY_PATTERN.match(entry_id)
        if not match:
            logger.error(f"Invalid entry ID format: {entry_id}")
            raise InvalidEntryIdFormatError(f"Invalid entry ID format: {entry_id}")
        
        project_id, location_id = match.group('project_id'), match.group('location_id')
        
        # Construct the API URL
        url = build_entry_lookup_url(entry_id, project_id, location_id)
               
        response = fetch_api_response(
            method=requests.get,
            url=url,
            project_id=user_project
        )
        
        if response.get('error_msg'):
            logger.error(f"Error looking up entry links: {response['error_msg']}")
            
        data = response.get('json', {})
        entry_links = data.get('entryLinks', [])
        return entry_links if entry_links else None
        
    except Exception as e:
        logger.error(f"Error while looking up entry links: {e}")
        return None

def build_entry_lookup_url(entry_id, project_id, location_id):
    return f"{DATAPLEX_BASE_URL}/projects/{project_id}/locations/{location_id}:lookupEntryLinks?entry={entry_id}"

def lookup_entry(dataplex_service: build, entry_id: str, project_location_name: str) -> Optional[Dict]:
    """Looks up an entry using the Dataplex API.

    Args:
        dataplex_service: The Dataplex API service object.
        entry_id: The entry ID to look up.

    Returns:
        The entry response as a dictionary, or None if an error occurs.

    Raises:
        DataplexAPIError: If there is an error during the API call.
    """
    try:
        request = dataplex_service.projects().locations().lookupEntry(name=project_location_name, entry=entry_id, view="ALL")
        response = request.execute()
        return response
    except Exception as e:
        logger.error(f"Error while looking up entry {entry_id}: {e}")
    

def get_spreadsheet_id(spreadsheet_url: str) -> str:
    """Extracts the spreadsheet ID from the URL.
    
    Args:
        spreadsheet_url: The URL of the Google Sheet.
    
    Returns:
        The spreadsheet ID.
    
    Raises:
        InvalidSpreadsheetURLError: If the spreadsheet URL is invalid.
    """
    match = SPREADSHEET_URL_PATTERN.match(spreadsheet_url)
    if not match:
        logger.error(f"Invalid spreadsheet URL: {spreadsheet_url}")
        raise InvalidSpreadsheetURLError(f"Invalid spreadsheet URL: {spreadsheet_url}")
    return match.group('spreadsheet_id')

def read_from_sheet(sheets_service: build, spreadsheet_id: str, range_name: str = 'A:Z') -> List[List[str]]:
    """Reads data from a Google Sheet.
    
    Args:
        sheets_service: The Google Sheets API service object.
        spreadsheet_id: The ID of the spreadsheet.
        range_name: The A1 notation of the range to retrieve (default: 'A:Z').
    
    Returns:
        A list of rows, where each row is a list of values.
    
    Raises:
        SheetsAPIError: If there is an error during the read operation.
    """
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        values = result.get('values', [])
        logger.info(f"Read {len(values)} rows from spreadsheet: {spreadsheet_id}")
        return values
    except Exception as e:
        logger.error(f"Error while reading from spreadsheet: {e}")
        raise SheetsAPIError(f"Error while reading from spreadsheet: {e}")

def write_to_sheet(sheets_service: build, spreadsheet_id: str, data: List[List[str]], range_name: str = 'A1') -> None:
    """Writes data to a Google Sheet.
    
    Args:
        sheets_service: The Google Sheets API service object.
        spreadsheet_id: The ID of the spreadsheet.
        data: The data to write to the sheet (list of rows).
        range_name: The A1 notation of the starting cell (default: 'A1').
    
    Raises:
        SheetsAPIError: If there is an error during the write operation.
    """
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body={'values': data}
        ).execute()
        logger.info(f"Data written to spreadsheet: {spreadsheet_id}")
    except Exception as e:
        logger.error(f"Error while writing to spreadsheet: {e}")
        raise SheetsAPIError(f"Error while writing to spreadsheet: {e}")


def _get_project_url(project_id: str) -> str:
    """Builds the Cloud Resource Manager project URL."""
    return f"{CLOUD_RESOURCE_MANAGER_BASE_URL}/projects/{project_id}"


def _fetch_project_info(project_id: str, user_project: str) -> dict:
    """Calls the Cloud Resource Manager API and returns the project JSON payload."""
    url = _get_project_url(project_id)
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    if response["error_msg"]:
        logger.error(f"Failed to fetch project info for '{project_id}': {response['error_msg']}")
        sys.exit(1)
    return response.get("json", {})


def _extract_project_number_from_info(project_info: dict) -> str:
    """Extracts the numeric project number from the project info 'name' field."""
    name = project_info.get("name", "")
    match = PROJECT_PATTERN.search(name)
    if match:
        return match.group('project_number')
    logger.error("Project number not found in project info.")
    sys.exit(1)


def get_project_number(project_id: str, user_project: str) -> str:
    """Fetches the project number from the project ID (composed from smaller helpers)."""
    project_info = _fetch_project_info(project_id, user_project)
    return _extract_project_number_from_info(project_info)

