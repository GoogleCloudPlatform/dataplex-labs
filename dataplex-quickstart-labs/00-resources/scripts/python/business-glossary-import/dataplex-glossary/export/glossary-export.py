import logging
import os
import sys
from typing import List

from googleapiclient.discovery import build

# Configure Python path for module imports
curr_dir = os.path.dirname(os.path.abspath(__file__))
import_dir = os.path.dirname(curr_dir)  # import directory
glossary_dir = os.path.dirname(import_dir)  # dataplex-glossary
base_dir = os.path.dirname(glossary_dir)  # business-glossary-import

# Add all necessary paths
sys.path.append(import_dir)  # For import local modules
sys.path.append(glossary_dir)  # For glossary modules
sys.path.append(base_dir)  # For migration modules

from utils import api_layer, error
from utils.constants import (
    CATEGORY_NAME_PATTERN,
    GLOSSARY_URL_PATTERN,
    SPREADSHEET_URL_PATTERN,
    TERM_NAME_PATTERN,
)

# --- Configure logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Expected format for the glossary URL (for error messages)
EXPECTED_GLOSSARY_URL_FORMAT = "any_url_containing/dp-glossaries/projects/<project_id>/locations/<location_id>/glossaries/<glossary_id>"

ID_COLUMN = "id"
PARENT_COLUMN = "parent"
DISPLAY_NAME_COLUMN = "display_name"
DESCRIPTION_COLUMN = "description"
OVERVIEW_COLUMN = "overview"
TYPE_COLUMN = "type"
CONTACT1_EMAIL_COLUMN = "contact1_email"
CONTACT1_NAME_COLUMN = "contact1_name"
CONTACT2_EMAIL_COLUMN = "contact2_email"
CONTACT2_NAME_COLUMN = "contact2_name"
LABEL1_KEY_COLUMN = "label1_key"
LABEL1_VALUE_COLUMN = "label1_value"
LABEL2_KEY_COLUMN = "label2_key"
LABEL2_VALUE_COLUMN = "label2_value"
OVERVIEW_ASPECT_ID = "655216118709.global.overview"
CONTACTS_ASPECT_ID = "655216118709.global.contacts"

SHEET_HEADERS = [ID_COLUMN, PARENT_COLUMN, DISPLAY_NAME_COLUMN, DESCRIPTION_COLUMN, OVERVIEW_COLUMN, TYPE_COLUMN, CONTACT1_EMAIL_COLUMN, CONTACT1_NAME_COLUMN, CONTACT2_EMAIL_COLUMN, CONTACT2_NAME_COLUMN, LABEL1_KEY_COLUMN, LABEL1_VALUE_COLUMN, LABEL2_KEY_COLUMN, LABEL2_VALUE_COLUMN]

# --- Helper Functions ---
def _get_spreadsheet_id(spreadsheet_url: str) -> str:
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
        logging.error(f"Invalid spreadsheet URL: {spreadsheet_url}")
        raise error.InvalidSpreadsheetURLError(f"Invalid spreadsheet URL: {spreadsheet_url}")
    return match.group('spreadsheet_id')


def _generate_entry_name_from_category_name(category_name: str) -> str:
    """Generates an entryId from a category name.
    
    Args:
        category_name: The full category name.
        
    Returns:
        The generated entryId.
        
    Raises:
        InvalidCategoryNameError: If the category name format is invalid.
    """
    match = CATEGORY_NAME_PATTERN.match(category_name)
    if not match:
        logging.error(f"Invalid category name format: {category_name}")
        raise error.InvalidCategoryNameError(f"Invalid category name format: {category_name}")
    
    project_id = match.group('project_id')
    location_id = match.group('location_id')
    glossary_id = match.group('glossary_id')
    category_id = match.group('category_id')
    return f"projects/{project_id}/locations/{location_id}/entryGroups/@dataplex/entries/projects/{project_id}/locations/{location_id}/glossaries/{glossary_id}/categories/{category_id}"

def _generate_entry_name_from_term_name(term_name: str) -> str:
    """Generates an entryId from a term name.
    
    Args:
        term_name: The full term name.
        
    Returns:
        The generated entryId.
        
    Raises:
        InvalidTermNameError: If the term name format is invalid.
    """
    match = TERM_NAME_PATTERN.match(term_name)
    if not match:
        logging.error(f"Invalid term name format: {term_name}")
        raise error.InvalidTermNameError(f"Invalid term name format: {term_name}")
    
    project_id = match.group('project_id')
    location_id = match.group('location_id')
    glossary_id = match.group('glossary_id')
    term_id = match.group('term_id')
    
    return f"projects/{project_id}/locations/{location_id}/entryGroups/@dataplex/entries/projects/{project_id}/locations/{location_id}/glossaries/{glossary_id}/terms/{term_id}"

def _get_sheet_row_for_category(category, entry):
    """Generates a sheet row from category and entry.
    
    Args:
        category: A Category.
        entry: An Entry representing a Category.
      
    Returns:
          an object containing values for the following keys
        ["id", "parent", "display_name", "description", "overview", "type", "contact1_email", "contact1_name", "contact2_email", "contact2_name", "label1_key", "label1_value", "label2_key", "label2_value"]
    """
    match = CATEGORY_NAME_PATTERN.match(category.get("name"))
    if not match:
        logging.error(f"Invalid category name format: {category.get('name')}")
        raise error.InvalidCategoryNameError(f"Invalid category name format: {category.get('name')}")
  
    project_id = match.group('project_id')
    location_id = match.group('location_id')
    glossary_id = match.group('glossary_id')
    category_id = match.group('category_id')
    
    sheet_data = {}
  
    # append id
    sheet_data["id"] = category_id

    # append parent id. if the parent is glossary then keep the parent empty
    if category.get("parent") == f"projects/{project_id}/locations/{location_id}/glossaries/{glossary_id}":
        sheet_data["parent"] = ""
    else:
        parent_match = CATEGORY_NAME_PATTERN.match(category.get("parent"))
        if not parent_match:
            logging.error(f"Invalid parent: {category.get("parent")}")
        else:
            parent_category_id = parent_match.group("category_id")
            sheet_data["parent"] = parent_category_id

    # append display name
    sheet_data["display_name"] = category.get("displayName", "")

    # append description
    sheet_data["description"] = category.get("description", "")

    simplified_aspects = _get_aspects_from_entry(entry)

    # append overview
    if simplified_aspects["overview"]:
        sheet_data["overview"] = simplified_aspects["overview"]
  
    # append type
    sheet_data["type"] = "CATEGORY"

    # append contacts
    if simplified_aspects["identities"]:
        index = 1
        identities = simplified_aspects["identities"]
        for identity in identities:
            if (identity["name"] or identity["id"]):
                sheet_data[f"contact{index}_email"] = identity["id"]
                sheet_data[f"contact{index}_name"] = identity["name"]
                index += 1

    # append labels
    index=1
    for key, value in category.get("labels", {}).items():
        sheet_data[f"label{index}_key"] = key
        sheet_data[f"label{index}_value"] = value
        index += 1

    return sheet_data

def _get_sheet_row_for_term(term, entry):
    """Generates a sheet row from term and entry.

    Args:
        term: A Term.
        entry: An Entry representing a Term.      
    Returns:
        an object containing values for the following keys
        ["id", "parent", "display_name", "description", "overview", "type", "contact1_email", "contact1_name", "contact2_email", "contact2_name", "label1_key", "label1_value", "label2_key", "label2_value"]
    """
    sheet_data = {}
    match = TERM_NAME_PATTERN.match(term.get("name"))
    if not match:
        logging.error(f"Invalid term name format: {term.get('name')}")
        raise error.InvalidTermNameError(f"Invalid term name format: {term.get('name')}")
    
    project_id = match.group('project_id')
    location_id = match.group('location_id')
    glossary_id = match.group('glossary_id')
    term_id = match.group('term_id')
  
    # append id
    sheet_data["id"] = term_id

    # append parent id. if the parent is glossary then keep the parent empty
    if term.get("parent") == f"projects/{project_id}/locations/{location_id}/glossaries/{glossary_id}":
        sheet_data["parent"] = ""
    else:
        parent_match = CATEGORY_NAME_PATTERN.match(term.get("parent"))
        if not parent_match:
            logging.error(f"Invalid parent: {term.get("parent")}")
        else:
            category_id = parent_match.group('category_id')
            sheet_data["parent"] = category_id

    # append display name
    sheet_data["display_name"] = term.get("displayName", "")

    # append description
    sheet_data["description"] = term.get("description", "")

    simplified_aspects = _get_aspects_from_entry(entry)

    # append overview
    if simplified_aspects["overview"]:
        sheet_data["overview"] = simplified_aspects["overview"]
  
    # append type
    sheet_data["type"] = "TERM"

    # append contacts
    if simplified_aspects["identities"]:
        index = 1
        identities = simplified_aspects["identities"]
        for identity in identities:
            if (identity["name"] or identity["id"]):
                sheet_data[f"contact{index}_email"] = identity["id"]
                sheet_data[f"contact{index}_name"] = identity["name"]
                index += 1

    # append labels
    index=1
    for key, value in term.get("labels", {}).items():
        sheet_data[f"label{index}_key"] = key
        sheet_data[f"label{index}_value"] = value
        index += 1

    return sheet_data


def _get_aspects_from_entry(entry):
    """
        Extracts simplified aspects from an entry.
        This function simplifies the aspects of an entry to include only the overview and identities.
        Args:
            entry: An Entry object.
        Returns:
            A dictionary with simplified aspects containing "overview" and "identities".
    """
    simplified_aspects = {"overview": "", "identities": []}
    aspects_map = entry["aspects"]
    # append overview
    if aspects_map and aspects_map.get(OVERVIEW_ASPECT_ID):
        overview = aspects_map[OVERVIEW_ASPECT_ID]
        if overview.get("data") and overview.get("data").get("content"):
            content = overview.get("data").get("content")
            simplified_aspects["overview"] = content
    # append contacts
    if aspects_map and aspects_map.get(CONTACTS_ASPECT_ID):
        contacts = aspects_map[CONTACTS_ASPECT_ID]
        if contacts.get("data") and contacts.get("data").get("identities"):
            simplified_aspects["identities"] = contacts.get("data").get("identities")
    return simplified_aspects

def _write_to_sheet(sheets_service: build, spreadsheet_id: str, data: List[List[str]]) -> None:
    """Writes data to a Google Sheet.
    
    Args:
        sheets_service: The Google Sheets API service object.
        spreadsheet_id: The ID of the spreadsheet.
        data: The data to write to the sheet.
    
    Raises:
        SheetsAPIError: If there is an error during the write operation.
    """
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='USER_ENTERED',
            body={'values': data}
        ).execute()
        logging.info(f"Data written to spreadsheet: {spreadsheet_id}")
    except Exception as e:
        logging.error(f"Error while writing to spreadsheet: {e}")
        raise error.SheetsAPIError(f"Error while writing to spreadsheet: {e}")

# --- Main Function ---
def list_and_write_glossary_taxonomy(spreadsheet_url: str, glossary_name: str) -> None:
    """
    Orchestrates the process of listing Dataplex glossary categories and writing them to a Google Sheet.
    
    Args:
        spreadsheet_url: The URL of the Google Sheet.
        glossary_name: The full glossary name.
    
    Raises:
        Any of the custom exceptions or exceptions from the google libraries.
    """
    try:
        logging.info(f"Starting process for glossary: {glossary_name}, sheet: {spreadsheet_url}")
        # 1. Get Spreadsheet ID
        spreadsheet_id = _get_spreadsheet_id(spreadsheet_url)
        
        # 2. Authenticate to APIs
        dataplex_service = api_layer.authenticate_dataplex()
        sheets_service = api_layer.authenticate_sheets()

        # 3. List glossary categories
        categories = api_layer.list_glossary_categories(dataplex_service, glossary_name)

        # 4. List glossary terms
        terms = api_layer.list_glossary_terms(dataplex_service, glossary_name)

        entry_names_map = {}
        # 5. Get Category and Terms.
        for category in categories:
            category_name = category.get("name")
            if not category_name:
                logging.error("Category name is missing. Skipping this category.")
                continue
            entry_names_map[category_name] = _generate_entry_name_from_category_name(category_name)
        for term in terms:
            term_name = term.get("name")
            if not term_name:
                logging.error("Term name is missing. Skipping this term.")
                continue
            entry_names_map[term_name] = _generate_entry_name_from_term_name(term_name)

        # 6. Lookup entries and store results
        entry_id_to_entry_map = {}
        for entry_id, entry_name in entry_names_map.items():
            try:
                entry_response = api_layer.lookup_entry(dataplex_service, entry_name)
                if entry_response:
                    entry_id_to_entry_map[entry_id] = entry_response
            except error.DataplexAPIError as e:
                logging.warning(f"Skipping entry {entry_name} due to lookup error: {e}")
                continue

        sheet_data = [SHEET_HEADERS]
        # 7. Add Categories data in the sheet
        for category in categories:
            category_entry = entry_id_to_entry_map.get(category.get("name"))
            if not category_entry:
                logging.warning(f"No entry found for category: {category.get('name', '')}. Skipping this category.")
                continue
            category_row_data = _get_sheet_row_for_category(category, category_entry)
            category_row = []
            for header in SHEET_HEADERS:
                if category_row_data.get(header):
                    category_row.append(category_row_data[header])
                else:
                    category_row.append("")
            sheet_data.append(category_row)

        # 8. Add Terms data in the sheet
        for term in terms:
          term_entry = entry_id_to_entry_map.get(term.get("name"))
          if not term_entry:
            logging.warning(f"No entry found for term: {term.get('name', '')}. Skipping this term.")
            continue
          term_row_data = _get_sheet_row_for_term(term, term_entry)
          term_row = []
          for header in SHEET_HEADERS:
            if term_row_data.get(header):
              term_row.append(term_row_data[header])
            else:
              term_row.append("")
          sheet_data.append(term_row)
        
        # 9. Write data to the sheet
        _write_to_sheet(sheets_service, spreadsheet_id, sheet_data)

        logging.info("Process completed successfully.")

    except (error.InvalidSpreadsheetURLError, error.InvalidGlossaryNameError, error.DataplexAPIError, error.SheetsAPIError, error.NoCategoriesFoundError) as e:
        logging.error(f"Operation failed: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    sheet_url = input("Enter the Google Sheet URL: ")
    glossary_url = input("Enter the glossary URL: ")
    sheet_url = sheet_url.strip()
    glossary_url = glossary_url.strip()
    
    if not sheet_url or not glossary_url:
        print("Both Google Sheet URL and glossary URL are required.")
        exit(1)
    
    # Validate inputs
    if not sheet_url.startswith("https://docs.google.com/spreadsheets/d/"):
        print("Invalid Google Sheet URL. Please provide a valid URL. It should start with 'https://docs.google.com/spreadsheets/d/'.")
        exit(1)
    
    match = GLOSSARY_URL_PATTERN.match(glossary_url)
    if not match:
        print(f"Invalid glossary URL format. Expected url format: {EXPECTED_GLOSSARY_URL_FORMAT}, but got: {glossary_url}")
        exit(1)
    project_id = match.group("project_id")
    location_id = match.group("location_id")
    glossary_id = match.group("glossary_id")
    glossary_name = f"projects/{project_id}/locations/{location_id}/glossaries/{glossary_id}"
    
    list_and_write_glossary_taxonomy(sheet_url, glossary_name)