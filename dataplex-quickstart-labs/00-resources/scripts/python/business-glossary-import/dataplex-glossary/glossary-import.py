import gspread
import json
from google.auth import default
from google.cloud import storage
import os
import re
import datetime
from typing import List
import argparse
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth

# Regex pattern for the glossary URL, allowing any valid URL
GLOSSARY_URL_PATTERN = re.compile(r".*dp-glossaries/projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/glossaries/(?P<glossary_id>[^/?#]+).*")

# Expected format for the glossary URL (for error messages)
EXPECTED_GLOSSARY_URL_FORMAT = "any_url_containing/dp-glossaries/projects/<project_id>/locations/<location_id>/glossaries/<glossary_id>"

EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

TERM_TYPE = "TERM"
CATEGORY_TYPE = "CATEGORY"

# Allowed types
ALLOWED_TYPES = {TERM_TYPE, CATEGORY_TYPE}

# Regex pattern for the name format (term_id or category_id)
ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")

# Regex pattern for the parent format (category_id)
PARENT_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")

ID_COLUMN = "id"  # A constant for the name column name
DISPLAY_NAME_COLUMN_NAME = "display_name"  # A constant for the display name column name
DESCRIPTION_COLUMN_NAME = "description"  # A constant for the description column name
PARENT_COLUMN_NAME = "parent"  # A constant for the parent column name
OVERVIEW_COLUMN_NAME = "overview"  # A constant for the overview column name
CONTACT1_EMAIL_COLUMN_NAME = "contact1_email"  # A constant for the contact1 email column name
CONTACT1_NAME_COLUMN_NAME = "contact1_name"  # A constant for the contact1 name column name
CONTACT2_EMAIL_COLUMN_NAME = "contact2_email"  # A constant for the contact2 email column name
CONTACT2_NAME_COLUMN_NAME = "contact2_name"  # A constant for the contact2 name column name
TYPE_COLUMN_NAME = "type"  # A constant for the type column name
LABEL1_KEY_COLUMN_NAME = "label1_key"  # A constant for the label1 key column name
LABEL1_VALUE_COLUMN_NAME = "label1_value"  # A constant for label1 value column name
LABEL2_KEY_COLUMN_NAME = "label2_key"  # A constant for the label2 key column name
LABEL2_VALUE_COLUMN_NAME = "label2_value"  # A constant for the label2 value column name

# Allowed headers for the Google Sheet
# These headers are used to validate the sheet structure
# and ensure that the required fields are present.
# They also help in mapping the columns to the corresponding fields in the Dataplex Glossary
# entry.
ALLOWED_HEADERS = {
    ID_COLUMN,
    PARENT_COLUMN_NAME,
    DISPLAY_NAME_COLUMN_NAME,
    DESCRIPTION_COLUMN_NAME,
    OVERVIEW_COLUMN_NAME,
    TYPE_COLUMN_NAME,
    CONTACT1_EMAIL_COLUMN_NAME,
    CONTACT1_NAME_COLUMN_NAME,
    CONTACT2_EMAIL_COLUMN_NAME,
    CONTACT2_NAME_COLUMN_NAME,
    LABEL1_KEY_COLUMN_NAME,
    LABEL1_VALUE_COLUMN_NAME,
    LABEL2_KEY_COLUMN_NAME,
    LABEL2_VALUE_COLUMN_NAME,
}

# GENERATED COLUMNS
ENTRY_NAME_COLUMN = "ENTRY_NAME_COLUMN"
PARENT_ENTRY_COLUMN_NAME="PARENT_ENTRY_COLUMN_NAME"
ANCESTORS = "ANCESTORS"

ENTRY_GROUP_ID = "@dataplex" # Added a constant for the entry group id
MAX_DEPTH = 4 # Max depth allowed for a node.
MAX_NUM_CATEGORIES = 200 # Max number of categories allowed in the glossary
MAX_NUM_TERMS = 5000 # Max number of terms allowed in the glossary

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

class InvalidGlossaryURLError(Exception):
    """Custom exception for invalid glossary URL."""
    def __init__(self, message):
        super().__init__(message)

class InvalidTypeException(Exception):
    """Custom exception for invalid type."""
    def __init__(self, message):
        super().__init__(message)

class InvalidNameException(Exception):
    """Custom exception for invalid name."""
    def __init__(self, message):
        super().__init__(message)

class InvalidParentException(Exception):
    """Custom exception for invalid parent."""
    def __init__(self, message):
        super().__init__(message)

class ParentNotFoundException(Exception):
    """Custom exception for parent not found."""
    def __init__(self, message):
      super().__init__(message)

class InvalidContactException(Exception):
    """Custom exception for invalid contact."""
    def __init__(self, message):
      super().__init__(message)

class InvalidLabelException(Exception):
    """Custom exception for invalid label."""
    def __init__(self, message):
      super().__init__(message)

class InvalidDepthException(Exception):
    """Custom exception for invalid depth."""
    def __init__(self, message):
      super().__init__(message)

class InvalidHeaderException(Exception):
    """Custom exception for invalid sheet header."""
    def __init__(self, message):
      super().__init__(message)

class ValidationException(Exception):
    """Custom exception for misc validations."""
    def __init__(self, message):
      super().__init__(message)


class SheetProcessor:
    """
    Handles reading, validating, and processing data from a Google Sheet.
    """

    def __init__(self, sheet_url, glossary_url, creds):
        self.sheet_url = sheet_url
        self.glossary_url = glossary_url
        self.creds = creds
        self.project_id = None
        self.location_id = None
        self.glossary_id = None
        self.project_location_base = None
        self.category_names = {}
        self._extract_glossary_ids()
        
    def _extract_glossary_ids(self):
        """Extracts project, location, and glossary IDs from the glossary URL."""
        match = GLOSSARY_URL_PATTERN.match(self.glossary_url)
        if not match:
            raise InvalidGlossaryURLError(
                f"Invalid glossary URL format. Expected: {EXPECTED_GLOSSARY_URL_FORMAT}, "
                f"but got: {self.glossary_url}"
            )
        self.project_id = match.group("project_id")
        self.location_id = match.group("location_id")
        self.glossary_id = match.group("glossary_id")
        self.project_location_base = f"projects/{self.project_id}/locations/{self.location_id}"
        self.entry_group_name = f"{self.project_location_base}/entryGroups/{ENTRY_GROUP_ID}"
        self.base_parent = f"{self.entry_group_name}/entries/{self.project_location_base}/glossaries/{self.glossary_id}"
        

    def _validate_id(self, id, row_num):
        """Validates the name field against the regex pattern.

        Args:
            id: The name field to validate.
            row_num: The row number.

        Raises:
            InvalidNameException: If the name is invalid.
        """
        if not id:
            raise InvalidNameException(f"Missing 'id' value in row {row_num}")
        if not ID_PATTERN.match(id):
             raise InvalidNameException(f"Invalid 'id' format in row {row_num}. Id should contain only lowercase letters, numbers, or hyphens and should start with a lowercase letter. Actual value: {id}")
        if len(id) > 63:
            raise InvalidNameException(f"Invalid 'id' length in row {row_num}. Id should be less than or equal to 63 characters. Actual value: {id}")

    def _validate_display_name(self, display_name, row_num):
        """Validates the display_name field.

        Args:
            display_name: The display_name field to validate.
            row_num: The row number.

        Raises:
            ValidationException: If the name is invalid.
        """
        if display_name and len(display_name) > 256:
            raise ValidationException(f"Invalid '{DISPLAY_NAME_COLUMN_NAME}' length in row {row_num}. Display name should be less than or equal to 256 characters. Actual value: {display_name}")

    def _validate_description(self, description, row_num):
        """Validates the description field.

        Args:
            description: The description field to validate.
            row_num: The row number.

        Raises:
            ValidationException: If the name is invalid.
        """
        if description and len(description) > 1024:
            raise ValidationException(f"Invalid '{DESCRIPTION_COLUMN_NAME}' length in row {row_num}. Description should be less than or equal to 1024 characters. Actual value: {description}")

    def _validate_overview(self, overview, row_num):
        """Validates the overview field.

        Args:
            overview: The overview field to validate.
            row_num: The row number.

        Raises:
            ValidationException: If the name is invalid.
        """
        if overview and len(overview) > 120000:
            raise ValidationException(f"Invalid '{OVERVIEW_COLUMN_NAME}' length in row {row_num}. Overview should be less than or equal to 120KB. Actual value: {overview}")

    def _validate_type(self, type_value, row_num):
        """Validate that the type is one of the allowed types
        Args:
          type_value: The type value to validate
          row_num: The row number

        Raises:
          InvalidTypeException: If the type is not valid.
        """
        if not type_value:
          raise InvalidTypeException(
              f"Missing '{TYPE_COLUMN_NAME}' value in row {row_num}. "
              f"Please make sure the row contains a value for the '{TYPE_COLUMN_NAME}' column. "
              f"Valid values are: {', '.join(ALLOWED_TYPES)}"
          )
        if type_value not in ALLOWED_TYPES:
            raise InvalidTypeException(f"Invalid '{TYPE_COLUMN_NAME}' value in row {row_num}. Expected one of: {', '.join(ALLOWED_TYPES)}, Actual value: {type_value}")

    def _validate_parent(self, parent, row_data, row_num):
        """Validates the parent field against the regex pattern.

        Args:
            parent: The parent field to validate.
            row_num: The row number.

        Raises:
            InvalidParentException: If the parent is invalid.
        """
        if parent and not PARENT_PATTERN.match(parent):
            raise InvalidParentException(f"Invalid '{PARENT_COLUMN_NAME}' format in row {row_num}. Parent should contain only lowercase letters, numbers, or hyphens and should start with a lowercase letter. Actual value: {parent}")

    def _validate_contacts(self, row_data, row_num):
        """Validates the contacts.

        Args:
            row_data: row data which contains the contacts.
            row_num: The row number.

        Raises:
            InvalidContactException: If the contact is invalid.
        """
        name1 = row_data[CONTACT1_NAME_COLUMN_NAME]
        email1 = row_data[CONTACT1_EMAIL_COLUMN_NAME]
        if name1 and not email1:
            raise InvalidContactException(f"Invalid '{CONTACT1_EMAIL_COLUMN_NAME}' format in row {row_num}. Please provide an email id along with name. '{CONTACT1_NAME_COLUMN_NAME}' is: {name1}, while '{CONTACT1_EMAIL_COLUMN_NAME}' is empty")
        if email1 and not EMAIL_PATTERN.match(email1):
            raise InvalidContactException(f"Invalid '{CONTACT1_EMAIL_COLUMN_NAME}' format in row {row_num}. Please provide a valid email id. Actual value: {email1}")

        email2 = row_data[CONTACT2_EMAIL_COLUMN_NAME]
        name2 = row_data[CONTACT2_NAME_COLUMN_NAME]
        if name2 and not email2:
            raise InvalidContactException(f"Invalid '{CONTACT2_EMAIL_COLUMN_NAME}' format in row {row_num}. Please provide an email id along with name. '{CONTACT2_NAME_COLUMN_NAME}' is: {name2}, while '{CONTACT2_EMAIL_COLUMN_NAME}' is empty")
        if email2 and not EMAIL_PATTERN.match(email2):
            raise InvalidContactException(f"Invalid '{CONTACT2_EMAIL_COLUMN_NAME}' format in row {row_num}. Please provide a valid email id. Actual value: {email2}")
    
    def _validate_labels(self, row_data, row_num):
        """Validates the labels.

        Args:
            row_data: row data which contains the labels.
            row_num: The row number.

        Raises:
            InvalidLabelException: If the label is invalid.
        """
        label1_key = row_data[LABEL1_KEY_COLUMN_NAME]
        label1_value = row_data[LABEL1_VALUE_COLUMN_NAME]
        if (label1_key and not label1_value) or (label1_value and not label1_key):
            raise InvalidLabelException(f"Invalid Label in row {row_num}. Please provide both key and value for label. '{LABEL1_KEY_COLUMN_NAME}' is: {label1_key}, while '{LABEL1_VALUE_COLUMN_NAME}' is {label1_value}")
        if label1_key and len(label1_key) > 128:
            raise InvalidLabelException(f"Invalid Label key length in row {row_num}. Label key should be less than or equal to 128 characters. Actual value: {label1_key}")
        if label1_value and len(label1_value) > 128:
            raise InvalidLabelException(f"Invalid Label value length in row {row_num}. Label value should be less than or equal to 128 characters. Actual value: {label1_value}")
        
        label2_key = row_data[LABEL2_KEY_COLUMN_NAME]
        label2_value = row_data[LABEL2_VALUE_COLUMN_NAME]
        if (label2_key and not label2_value) or (label2_value and not label2_key):
            raise InvalidLabelException(f"Invalid Label format in row {row_num}. Please provide both key and value for label. '{LABEL2_KEY_COLUMN_NAME}' is: {label2_key}, while '{LABEL2_VALUE_COLUMN_NAME}' is {label2_value}")
        if label2_key and len(label2_key) > 128:
            raise InvalidLabelException(f"Invalid Label key length in row {row_num}. Label key should be less than or equal to 128 characters. Actual value: {label2_key}")
        if label2_value and len(label2_value) > 128:
            raise InvalidLabelException(f"Invalid Label value length in row {row_num}. Label value should be less than or equal to 128 characters. Actual value: {label2_value}")
        

    def _generate_full_name(self, name, type_value):
        """Generates the full name based on the type and the glossary URL."""
        if type_value == TERM_TYPE:
            return f"{self.base_parent}/terms/{name}"
        elif type_value == CATEGORY_TYPE:
            return f"{self.base_parent}/categories/{name}"
        return None

    def _generate_full_parent(self, parent):
        """Generates the full parent based on the parent and the glossary URL."""
        if not parent:
            return self.base_parent
        if parent and parent not in self.category_names:
            raise ParentNotFoundException(f"Parent {parent} not found in sheet. Please make sure that the category exists in the sheet.")
        return f"{self.base_parent}/categories/{parent}"

    def _generate_ancestors(self, row_data_list):
        # Convert the list of dicts to a dict with name as key
        row_data = {item[ENTRY_NAME_COLUMN]: item for item in row_data_list}

        # Create a copy to avoid modifying the original dict.
        row_data_copy = row_data.copy()
        root_entry_name = self.base_parent 

        # Find all nodes that are marked as ROOT.
        root_nodes = [node for node, data in row_data_copy.items() if data[PARENT_ENTRY_COLUMN_NAME] == root_entry_name]

        # Create a dictionary to store paths. 
        root_to_node_path_map = {}

        # Function to perform Depth-First Search (DFS) to validate the tree and generate paths
        def dfs(node_entry_name, current_path):
            if node_entry_name in root_to_node_path_map:
                print(f"Error: Cycle detected at node {node_entry_name}")
                return False

            root_to_node_path_map[node_entry_name] = current_path + [node_entry_name]

            for other_node, other_data in row_data_copy.items():
                if other_data[PARENT_ENTRY_COLUMN_NAME] == node_entry_name: # run dfs for all immediate children of current node
                    if not dfs(other_node, root_to_node_path_map[node_entry_name]):
                        return False
            return True

        # Start DFS for all direct child of Glossary
        for node_entry_name in root_nodes:
            if not dfs(node_entry_name, [root_entry_name]):
                return None


        # Check if all nodes were visited
        if len(root_to_node_path_map) != len(row_data_copy):
            print(f"Error: Not all nodes were visited. Missing nodes {set(row_data_copy.keys()) - set(root_to_node_path_map.keys())}")
            return None

        ancestors_map = {}

        # populate ancestors object
        for node_name, root_to_node_path in root_to_node_path_map.items():
            if node_name not in ancestors_map:
                ancestors_map[node_name] = []
            if len(root_to_node_path) > MAX_DEPTH:
                raise InvalidDepthException(f"Invalid depth for hierarchy {root_to_node_path}. Max depth allowed in glossary hierarchy is {MAX_DEPTH}.")
            for parent_node_name in root_to_node_path:
                if parent_node_name != node_name:
                    ancestor = {}
                    ancestor["name"] =parent_node_name
                    if parent_node_name in row_data_copy:
                        ancestor["type"] = "projects/dataplex-types/locations/global/entryTypes/glossary-category"
                    else:
                        ancestor["type"] = "projects/dataplex-types/locations/global/entryTypes/glossary"
                    ancestors_map[node_name] = ancestors_map[node_name] + [ancestor]
        return ancestors_map


    def _convert_to_import_item(self, row_data, ancestors):
        entry_type = ""
        resource = ""
        aspects = {}
        identities = []
        labels = {}
        if row_data[CONTACT1_EMAIL_COLUMN_NAME]:
            identities.append({"role":"steward","name":row_data[CONTACT1_NAME_COLUMN_NAME],"id":row_data[CONTACT1_EMAIL_COLUMN_NAME]})
        if row_data[CONTACT2_EMAIL_COLUMN_NAME]:
            identities.append({"role":"steward","name":row_data[CONTACT2_NAME_COLUMN_NAME],"id":row_data[CONTACT2_EMAIL_COLUMN_NAME]})
        
        if row_data[LABEL1_KEY_COLUMN_NAME] and row_data[LABEL1_VALUE_COLUMN_NAME]:
            labels[row_data[LABEL1_KEY_COLUMN_NAME]] = row_data[LABEL1_VALUE_COLUMN_NAME]
        if row_data[LABEL2_KEY_COLUMN_NAME] and row_data[LABEL2_VALUE_COLUMN_NAME]:
            labels[row_data[LABEL2_KEY_COLUMN_NAME]] = row_data[LABEL2_VALUE_COLUMN_NAME]
        
        if row_data["type"] == TERM_TYPE:
            entry_type = "projects/dataplex-types/locations/global/entryTypes/glossary-term"
            resource = f"{self.project_location_base}/glossaries/{self.glossary_id}/terms/{row_data[ID_COLUMN]}"
            aspects = {
                "dataplex-types.global.glossary-term-aspect": {"data": {}}, 
                "dataplex-types.global.overview": {"data": {"content": row_data[OVERVIEW_COLUMN_NAME]}},
                "dataplex-types.global.contacts": {"data": {"identities": identities}}
            }
        elif row_data["type"] == CATEGORY_TYPE:
            entry_type = "projects/dataplex-types/locations/global/entryTypes/glossary-category"
            resource = f"{self.project_location_base}/glossaries/{self.glossary_id}/categories/{row_data[ID_COLUMN]}"
            aspects = {
                "dataplex-types.global.glossary-category-aspect": {"data": {}}, 
                "dataplex-types.global.overview": {"data": {"content": row_data[OVERVIEW_COLUMN_NAME]}},
                "dataplex-types.global.contacts": {"data": {"identities": identities}}
            }
        else:
            raise ValueError(f"Invalid type: {row_data['type']}, expected TERM or CATEGORY")

        import_item = {
            "entry": {
                "name": row_data[ENTRY_NAME_COLUMN],
                "entryType": entry_type,
                "parentEntry": self.base_parent,
                "aspects": aspects,
                "entrySource": {
                    "resource": resource,
                    "displayName": row_data[DISPLAY_NAME_COLUMN_NAME],
                    "description": row_data[DESCRIPTION_COLUMN_NAME],
                    "ancestors": ancestors,
                    "labels": labels
                },
            },
            "entryLink": None,
        }

        return import_item



    def read_and_validate_data(self):
        gc = gspread.authorize(self.creds)
        sheet = gc.open_by_url(self.sheet_url).sheet1
        data = sheet.get_all_values()
        if not data:
            print("No data found in the sheet.")
            return False, []

        headers = data[0]
        valid_rows = []
        is_dump_valid = True
        dump_entries = []
        num_terms = 0

        # Trim whitespace
        headers = [header.strip() for header in headers]

        if set(headers) != ALLOWED_HEADERS:
            print(f"Invalid sheet. The first row of the sheet should be headers conatin exactly : {ALLOWED_HEADERS}. Actual headers: {headers}")
            is_dump_valid = False
            return is_dump_valid, dump_entries

        # First, populate category_names
        for row_num, row in enumerate(data[1:], start=2):
            row_data = dict(zip(headers, row))
            # Trim whitespace
            for key, value in row_data.items():
                if isinstance(value, str):
                    row_data[key] = value.strip()
            name = row_data.get(ID_COLUMN)
            type_value = row_data.get(TYPE_COLUMN_NAME)
            if name and type_value == CATEGORY_TYPE:
              self.category_names[name] = name

        if len(self.category_names) > MAX_NUM_CATEGORIES:
            print(f"Invalid sheet. The number of categories exceeds the maximum allowed limit of {MAX_NUM_CATEGORIES}. Actual number of categories: {len(self.category_names)}")
            is_dump_valid = False
        
        for row_num, row in enumerate(data[1:], start=2):
            row_data = dict(zip(headers, row))
            # Trim whitespace
            for key, value in row_data.items():
                if isinstance(value, str):
                    row_data[key] = value.strip()
            id = row_data.get(ID_COLUMN)
            type_value = row_data.get(TYPE_COLUMN_NAME)
            parent = row_data.get(PARENT_COLUMN_NAME)
            if type_value == TERM_TYPE:
                num_terms += 1
                if num_terms > MAX_NUM_TERMS:
                    print(f"Invalid sheet. The number of terms exceeds the maximum allowed limit of {MAX_NUM_TERMS}. Actual number of terms: {num_terms}")
                    is_dump_valid = False
            try:
                self._validate_id(id, row_num)
                self._validate_type(type_value, row_num)
                self._validate_parent(parent, row_data, row_num)
                self._validate_contacts(row_data, row_num)
                self._validate_display_name(row_data.get(DISPLAY_NAME_COLUMN_NAME), row_num)
                self._validate_description(row_data.get(DESCRIPTION_COLUMN_NAME), row_num)
                self._validate_overview(row_data.get(OVERVIEW_COLUMN_NAME), row_num)
                self._validate_labels(row_data, row_num)
                row_data[ENTRY_NAME_COLUMN] = self._generate_full_name(id, type_value)
                row_data[PARENT_ENTRY_COLUMN_NAME] = self._generate_full_parent(parent)
                valid_rows.append(row_data)
            except Exception as e:
                is_dump_valid = False
                print(f"Invalid data: {e}")
        
        ancestors_map = self._generate_ancestors(valid_rows)
        
        if not ancestors_map:
            is_dump_valid = False
            return is_dump_valid, dump_entries
        
        for row_data in valid_rows:
            entry_name = row_data[ENTRY_NAME_COLUMN]
            if entry_name in ancestors_map:
                dump_entries.append(self._convert_to_import_item(row_data, ancestors_map[entry_name]))

        return is_dump_valid, dump_entries


def write_json_to_file(output_file_path, data):
    """Writes JSON data to a file, one object per line."""
    with open(output_file_path, 'w') as outfile:
        for row_data in data:
            json.dump(row_data, outfile)
            outfile.write('\n')
    print(f"Successfully saved to {output_file_path}")



def delete_all_bucket_objects(bucket):
    """Deletes all blobs in the given bucket."""
    blobs = list(bucket.list_blobs())
    if not blobs:
        print(f"Bucket {bucket.name} is already empty.")
        return
    print(f"Deleting {len(blobs)} objects from bucket {bucket.name}...")
    # delete_blobs can take a list of blob objects
    bucket.delete_blobs(blobs)
    print(f"Successfully deleted {len(blobs)} objects from bucket {bucket.name}.")


def upload_to_gcs(creds, bucket_id, file_path):
    """
    Deletes all objects in the GCS bucket and then uploads a new file.
    """
    storage_client = storage.Client(credentials=creds)
    bucket = storage_client.get_bucket(bucket_id)
    # 1. Delete all contents of the GCS bucket
    print(f"Preparing to empty bucket: {bucket_id}")
    delete_all_bucket_objects(bucket)
    # 2. Upload the new file
    gcs_file_name = f"output_{TIMESTAMP}.json"
    blob = bucket.blob(gcs_file_name)
    print(f"Uploading {file_path} to gs://{bucket_id}/{gcs_file_name}...")
    blob.upload_from_filename(file_path)
    print(f"File {file_path} uploaded to gs://{bucket_id}/{gcs_file_name}")
    # 3. Remove the local file
    try:
        os.remove(file_path)
        print(f"Local file {file_path} removed")
    except OSError as e:
        print(f"Error removing local file {file_path}: {e}")



def process_sheet_to_json_and_upload(sheet_url, glossary_url, output_file_path, bucket_id):
    """
    Orchestrates reading, validation, processing, and uploading data.
    """
    try:
        # Use ADC to get credentials
        creds, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/spreadsheets.readonly'])

        sheet_processor = SheetProcessor(sheet_url, glossary_url, creds)
        is_dump_valid, dump_entries = sheet_processor.read_and_validate_data()

        if not dump_entries:
          print("No valid rows found.")
          return False

        if not is_dump_valid:
            return False

        write_json_to_file(output_file_path, dump_entries)

        if bucket_id:
            upload_to_gcs(creds, bucket_id, output_file_path)
        else:
            print("Skipping GCS upload, bucket_id not provided")

        return True 
    except gspread.exceptions.APIError as e:
        print(f"Unable to open sheet: {e}")
        return False
    except InvalidGlossaryURLError as e:
        print(f"Invalid glossary URL error: {e}")
        return False

def create_dataplex_metadata_job(
    project_id,
    location_id,
    job_id,
    bucket_id,
    glossary_name
):
    try:
        credentials, _ = google.auth.default()
        service = build('dataplex', 'v1', credentials=credentials)
        parent = f"projects/{project_id}/locations/{location_id}"
        metadata_job_body = {
            "type": "IMPORT",
            "import_spec": {
                "log_level": "DEBUG",
                "source_storage_uri": f"gs://{bucket_id}/",
                "entry_sync_mode":"FULL",
                "aspect_sync_mode":"INCREMENTAL",
                "scope":{
                    "glossaries": f"{glossary_name}"
                }
            }
        }

        print(f"Creating Metadata Job in: {parent}")
        print(f"Job ID: {job_id}")
        print(f"Request body: {json.dumps(metadata_job_body, indent=2)}")

        request = service.projects().locations().metadataJobs().create(
            parent=parent,
            metadataJobId=job_id,
            body=metadata_job_body
        )
        operation = request.execute()
        return operation

    except HttpError as err:
        print(f"HTTP Error: {err}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def poll_operation(operation_name, poll_interval=10, max_polls=60):
    """Polls a Long Running Operation until it's done."""
    credentials, _ = google.auth.default()
    service = build('dataplex', 'v1', credentials=credentials)
    for i in range(max_polls):
        try:
            op = service.projects().locations().operations().get(name=operation_name).execute()
            if op.get('done'):
                print("Operation finished:")
                print(json.dumps(op, indent=2))
                return op
            print(f"Operation not done yet, polling again in {poll_interval} seconds...")
            time.sleep(poll_interval)
        except HttpError as err:
            print(f"Error polling operation: {err}")
            return None
    print("Warning: Operation polling timed out.")
    return None

if __name__ == "__main__":
    sheet_url = input("Enter the Google Sheet URL: ")
    glossary_url = input("Enter the glossary URL: ")
    bucket_id = input("Enter the GCS bucket ID (or leave empty to skip upload): ")
    sheet_url = sheet_url.strip()
    glossary_url = glossary_url.strip()
    bucket_id = bucket_id.strip() if bucket_id else None
    
    if not sheet_url or not glossary_url:
        print("Both Google Sheet URL and glossary URL are required.")
        exit(1)
    
    # Validate inputs
    if not sheet_url.startswith("https://docs.google.com/spreadsheets/d/"):
        print("Invalid Google Sheet URL. Please provide a valid URL.")
        exit(1)
    
    match = GLOSSARY_URL_PATTERN.match(glossary_url)
    if not match:
        print(f"Invalid glossary URL format. Expected url format: {EXPECTED_GLOSSARY_URL_FORMAT}, but got: {glossary_url}")
        exit(1)
    project_id = match.group("project_id")
    location_id = match.group("location_id")
    glossary_id = match.group("glossary_id")
    glossary_name = f"projects/{project_id}/locations/{location_id}/glossaries/{glossary_id}"
    output_file = f"{glossary_id}-{TIMESTAMP}-exported.json"
    
    is_successful = process_sheet_to_json_and_upload(sheet_url, glossary_url, output_file, bucket_id)

    if is_successful and bucket_id:
        job_id = f"{glossary_id}-{TIMESTAMP}"
        operation = create_dataplex_metadata_job(project_id, location_id, job_id, bucket_id, glossary_name)
        if operation:
            print("\nCreate Metadata Job Operation initiated:")
            print(json.dumps(operation, indent=2))
            poll_operation(operation['name'])