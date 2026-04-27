# Export Utilities

This directory contains export utilities for Dataplex business glossary data.

## Prerequisites

Before running the scripts, ensure your local environment and Google Cloud project are correctly configured.

### 1. Install Dependencies

```bash
pip3 install -r requirements.txt
```

### 2. Service Account Setup

To access Google Sheets we would need to run the script using a Service Account.

**Identify/Create the Service Account:**

1. Go to the Google Cloud Console > IAM & Admin > Service Accounts.
2. You can use an existing service account or create a new one (e.g., `my-script-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com`). Let's call this `SA_EMAIL`.

**Grant Your User the "Service Account Token Creator" Role:**

This is the crucial step that allows your account to impersonate the service account.

1. Stay on the "Service Accounts" page.
2. Find the service account you identified in Step 1. Click on it.
3. Go to the tab **"Principals with access"**.
4. Click the **"Grant Access"** button.
5. In the **"New principals"** field, enter your Google user account: `my-user1@google.com`.
6. In the **"Role"** dropdown, search for and select **"Service Account Token Creator"** (`roles/iam.serviceAccountTokenCreator`).
7. Click **"Save"**.

**Grant the following roles to the service account on the project:**

| Role |
|---|
| Dataplex Administrator |
| Dataplex Catalogue Admin |
| Dataplex Catalogue Editor |

### 3. Required APIs

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

### 4. Google Sheets Setup

Create a Google Sheet that will be used for the export:

*   **Glossary Export**: Create an empty Google Sheet (or use an existing one). The script will write to the first sheet.
*   **EntryLinks Export**: Create an empty Google Sheet (or use an existing one). The script will write to the first sheet.

### Authentication

The below command is required to access Google Sheets and call Dataplex APIs:
```bash
gcloud auth application-default login --scopes="https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets"
```

---

## 1. Glossary Export

Exports categories and terms from a Dataplex business glossary into Google Sheets.

### Usage

```bash
python3 glossary-export.py
```

### Sheets file schema (Glossary)

The first line of the sheet contains the following header:

`id, parent, display_name, description, overview, type, contact1_email, contact1_name, contact2_email, contact2_name, label1_key, label1_value, label2_key, label2_value`

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

---

## 2. EntryLinks Export

Exports EntryLinks from Dataplex glossary terms into Google Sheets. For each term in the glossary, the script queries all relevant regional endpoints (or fans out to all endpoints for global glossaries), collects entry links, deduplicates symmetric links (synonym/related), and writes the results to the specified sheet.

### Usage

```bash
python3 entrylinks-export.py \
  --glossary-url <GLOSSARY_URL> \
  --spreadsheet-url <SPREADSHEET_URL> \
  --user-project <PROJECT_ID>
```

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--glossary-url` | Yes | Dataplex Glossary URL to export EntryLinks from |
| `--spreadsheet-url` | Yes | Google Sheets URL to write the exported EntryLinks data |
| `--user-project` | Yes | Project ID to use for billing and API quota (e.g. `my-project-id`) |

### Example

```bash
python3 entrylinks-export.py \
  --glossary-url "https://console.cloud.google.com/dataplex/glossaries/my-glossary?project=my-project&location=us-central1" \
  --spreadsheet-url "https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit#gid=0" \
  --user-project "my-billing-project"
```

### Sheets file schema (EntryLinks)

The first row of the sheet contains the following header:

`entry_link_type, source_entry, target_entry, source_path`

Where:

*   `entry_link_type` (required): Type of EntryLink. Valid values: `definition`, `synonym`, `related`.
*   `source_entry` (required): Full Dataplex entry resource path for the source (e.g. `projects/my-project/locations/us/entryGroups/@bigquery/entries/my-entry`).
*   `target_entry` (required): Full Dataplex entry resource path for the target.
*   `source_path` (optional): Path within the source entry (e.g. a BigQuery column path like `Schema.Field1`). Populated for definition entrylinks.
