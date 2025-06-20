# Overview

`glossary_import` is a utility that performs bulk export of categories and terms from Dataplex business glossary into google sheets. To the google sheet and glossary name are provided as input.

## Usage

### Prerequisites
```
pip3 install -r requirements.txt
```

### Access token

The below command is required to access the sheet and call Dataplex APIs
```
gcloud auth application-default login --scopes="https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets"
```

### Export
```
python3 glossary-export.py 
```

## Sheets file schema
The first line of the sheet contains the following header

`id, parent, display_name, description	overview, type, contact1_email, contact1_name, contact2_email, contact2_name, label1_key, label1_value, label2_key, label2_value`

Where:

*   `id` (required): Unique id for the term/category in the glossary.
*   `parent` (optional): The id parent for this term/category. If the paren is not provided then this will become direct child of the Glossary. the id provided should be present in this sheet and it should be an id of a category.
*   `display_name` (required): The display name of the term/category.
*   `description` (optional): A brief description of the term/category.
*   `overview` (optional): A rich text description of the term/category. It can contain html tags.
*   `type` (required): Describes whether this row represents a TERM or a CATEGORY. Only valid values for this is TERM or CATEGORY
*   `contact1_email` (optional): Email id of the data steward for this term/category.
*   `contact1_name` (optional): Name of the data steward for this term/category.
*   `contact2_email` (optional): Email id of another data steward for this term/category.
*   `contact2_name` (optional): Name of another data steward for this term/category.
*   `label1_key` (optional): Label1's key.
*   `label1_value` (optional): Label1's value.
*   `label2_key` (optional): Label2's key.
*   `label2_value` (optional): Label2's value.