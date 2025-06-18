# Overview

`glossary_import` is a utility that performs bulk import and export of categories and terms into a Dataplex business glossary using google sheets. To achieve the import functionality, the google sheet is provided as input. The sheet is parsed and validated and then the data from the sheet is converted into an import file compatible with CreateMetadataJob API of dataplex. The converted file is uploaded into a GCS bucket provided by the user. Once the upload to GCS bucket is successful, the script calls CreateMetadataJob API of dataplex to start the import job. At the end the result from the api is printed on the terminal.

## Usage

### Prerequisites
```
pip3 install -r bg_import/csv_import/requirements.txt
```

### Import
```
python3 bg_import/csv_import/glossary_import.py 
```
  
### Access token

The below command is required to access the sheet, upload file to GCS and call CreateMetadataJob() API
```
gcloud auth application-default login --scopes="https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets.readonly"
```

## Sheets file schema
The first line of the sheet should contain the following header

`id, parent, display_name, description	overview, contact1_email, contact1_name, contact2_email, contact2_name, type`

Where:

*   `id` (required): Unique id for the term/category in the glossary.
*   `parent` (optional): The id parent for this term/category. If the paren is not provided then this will become direct child of the Glossary. the id provided should be present in this sheet and it should be an id of a category.
*   `display_name` (required): The display name of the term/category.
*   `description` (optional): A brief description of the term/category.
*   `overview` (optional): A rich text description of the term/category. It can contain html tags.
*   `contact1_email` (optional): Email id of the data steward for this term/category.
*   `contact1_name` (optional): Name of the data steward for this term/category.
*   `contact2_email` (optional): Email id of another data steward for this term/category.
*   `contact2_name` (optional): Name of another data steward for this term/category.
*   `type` (required): Describes whether this row represents a TERM or a CATEGORY. Only valid values for this is TERM or CATEGORY
 