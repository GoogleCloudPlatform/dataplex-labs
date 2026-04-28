# Import Utilities

This directory contains import utilities for Dataplex business glossary data.

## Prerequisites

Before running the scripts, ensure your local environment and Google Cloud project are correctly configured.

### 1. Install Dependencies

```bash
pip3 install -r requirements.txt
```

### 2. Required APIs

Ensure the following APIs are enabled in the Google Cloud project you will use:

- Dataplex API
- Cloud Resource Manager API
- Cloud Storage API
- Google Sheets API

You can enable them by visiting the APIs & Services dashboard in the Google Cloud Console, or run:

```bash
gcloud services enable dataplex.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable sheets.googleapis.com
```

### 3. GCS Buckets

For the import operation, create one or more empty Google Cloud Storage buckets. The script uses these buckets as a staging area for the import files.

Grant the following roles to the Dataplex service account (`service-PROJECT_NUMBER@gcp-sa-dataplex.iam.gserviceaccount.com`) on each GCS bucket:

| Role | Description |
|---|---|
| Storage Object Creator (`roles/storage.objectCreator`) | Allows creating objects. |
| Storage Object Admin (`roles/storage.objectAdmin`) | Full control over objects. |

### 4. Google Sheets Setup

Prepare a Google Sheet that will be used for the import:

Share the Google Sheet with the service account (`SA_EMAIL`) as a **Viewer** so it has read access.

*   **Glossary Import**: The sheet should contain the following header row:
    `id, parent, display_name, description, overview, type, contact1_email, contact1_name, contact2_email, contact2_name, label1_key, label1_value, label2_key, label2_value`
*   **EntryLinks Import**: The sheet should contain the following columns in the header row:
    *   `entry_link_type` - Type of link: `definition`, `related`, or `synonym`
    *   `source_entry` - Full entry name of the source (e.g., `projects/PROJECT/locations/LOCATION/entryGroups/ENTRY_GROUP/entries/ENTRY_ID`)
    *   `target_entry` - Full entry name of the target
    *   `source_path` - (Optional) Column/field path for definition links (e.g., `Schema.column_name`)

### Authentication

Use the following commands to authenticate yourself first, then create Application Default Credentials (ADC) by impersonating the service account used by the scripts.

The service account must have the required IAM roles, and your user account must have `roles/iam.serviceAccountTokenCreator` on that service account.

```bash
# Authenticate your user account
gcloud auth login

# Then impersonate the service account for ADC
SA_EMAIL="<service-account-emailid>"
gcloud auth application-default login \
  --impersonate-service-account="${SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/spreadsheets.readonly"
```

---

## 1. Glossary Import

Performs bulk import of categories and terms into a Dataplex business glossary from Google Sheets. The sheet is parsed and validated, then converted into an import file compatible with the Dataplex CreateMetadataJob API. The converted file is uploaded to a GCS bucket provided by the user. Once the upload is successful, the script calls CreateMetadataJob to start the import job and prints the result on the terminal.

### Usage

```bash
python3 glossary-import.py
```

### Sheets file schema (Glossary)

The first line of the sheet should contain the following header:

`id, parent, display_name, description, overview, type, contact1_email, contact1_name, contact2_email, contact2_name, label1_key, label1_value, label2_key, label2_value`

Where:

*   `id` (required): Unique id for the term/category in the glossary.
*   `parent` (optional): The id of the parent for this term/category. If not provided, it becomes a direct child of the glossary. The id should be present in this sheet and should be an id of a category.
*   `display_name` (required): The display name of the term/category.
*   `description` (optional): A brief description of the term/category.
*   `overview` (optional): A rich text description of the term/category. It can contain HTML tags.
*   `type` (required): Whether this row represents a TERM or a CATEGORY. Only valid values are TERM or CATEGORY.
*   `contact1_email` (optional): Email id of the data steward for this term/category.
*   `contact1_name` (optional): Name of the data steward for this term/category.
*   `contact2_email` (optional): Email id of another data steward for this term/category.
*   `contact2_name` (optional): Name of another data steward for this term/category.
*   `label1_key` (optional): Label1's key.
*   `label1_value` (optional): Label1's value.
*   `label2_key` (optional): Label2's key.
*   `label2_value` (optional): Label2's value.

---

## 2. EntryLinks Import

Imports EntryLinks into Dataplex from Google Sheets. The script reads entry link definitions from the spreadsheet, validates that referenced entries exist in Dataplex, groups them by entry group and link type, creates import JSON files, uploads them to GCS buckets, and triggers Dataplex CreateMetadataJob import jobs.

### Usage

```bash
python3 entrylinks-import.py \
  --spreadsheet-url <SPREADSHEET_URL> \
  --buckets <BUCKET_LIST> \
  --user-project <PROJECT_ID>
```

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--spreadsheet-url` | Yes | Google Sheets URL containing EntryLinks to import |
| `--buckets` | Yes | Comma-separated list of GCS bucket IDs for staging import files (e.g. `bucket-1,bucket-2`) |
| `--user-project` | Yes | Project ID to use for billing and API quota (e.g. `my-project-id`) |

### Example

```bash
python3 entrylinks-import.py \
  --spreadsheet-url "https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit#gid=0" \
  --buckets "my-staging-bucket-1,my-staging-bucket-2" \
  --user-project "my-billing-project"
```

### Sheets file schema (EntryLinks)

The first row of the sheet should contain the following header:

`entry_link_type, source_entry, target_entry, source_path`

Where:

*   `entry_link_type` (required): Type of EntryLink. Valid values: `definition`, `synonym`, `related`.
*   `source_entry` (required): Full Dataplex entry resource path for the source (e.g. `projects/my-project/locations/us/entryGroups/@bigquery/entries/my-entry`).
*   `target_entry` (required): Full Dataplex entry resource path for the target.
*   `source_path` (optional): Path within the source entry (e.g. a BigQuery column path like `Schema.Field1`). Used for definition entrylinks.
