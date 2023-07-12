# Overview

`bg_import` is a utility that performs bulk import of terms into a Data Catalog business
glossary from a CSV file. To achieve that the CSV file is parsed and validated.
The resulting list of terms is then added into the target glossary via Data
Catalog API. If any errors occur at any stage of the process then an error
report is printed and import continues or completely stops depending on input flags.

Business Glossary API is currently on private preview, and it needs to be
enabled on the project for it to be used.

## Usage

```
python3 bg_import/business_glossary_import.py <csv file>
  --project=<project_id>
  --group=<entry_group_id>
  --glossary=<glossary_id>
  --location=<location_code>
  [--import-mode={strict,clear}]
  [--strict-parsing]
  [-h]
```

Run `python3 bg_import/business_glossary_import.py -h` for description of individual arguments.

### Access token

For the utility to be able to access Data Catalog API an access token has to be
provided via `GCLOUD_ACCESS_TOKEN` environment variable. E.g.:

```
export GCLOUD_ACCESS_TOKEN=$(gcloud auth print-access-token)
```

## CSV file schema

The source CSV file shall adhere to RFC4180 format. Each record in the file
represents a single term with the following values:

`term_display_name,description,steward,tagged_assets,synonyms,related_terms`

Where:

*   `term_display_name` (required): Unique name for the entry term.
*   `description` (required): Plain text or rich text encoded as plain text
    description for the term.
*   `steward` (optional): List of data stewards for the current term, with each
    steward separated by a comma (`,`). E.g.: `Data
    Steward1<steward1@example.com>, Data teward2<steward2@example.com>"`
*   `tagged_assets` (optional): List of asset names for assets explained by the
    current term, with each asset separated by a comma (`,`). If a specific
    field of the asset needs to be explained by the current term, and not the
    asset as a whole, the field can be indicated by separating it from the
    asset name with a colon (:) eg. `asset_name:field`
*   `synonyms` (optional): List of terms that have a synonym relation with the
    current term, with each term separated by a comma (`,`)
*   `related_terms` (optional): List of terms that have a related-to relation
    with the current term, with each term separated by a comma (`,`)

In the case where a list of items inside a field contains the delimiter value
comma (,) the field has to be escaped by using double quotes (" "). e.g. `term 1,
"Term 1, a description", "Data Steward1<steward1@example.com>, Data
teward2<steward2@example.com>",,,`
