# Overview

`bg_import` is a utility that performs bulk import of categories and terms into
a Data Catalog business glossary from CSV files. To achieve that, the CSV files - one for
categories and one for terms - are parsed and validated. The resulting list of
categories and terms are then added into the target glossary via Data Catalog
API. If any errors occur at any stage of the process then an error report is
printed and import continues or completely stops depending on input flags.

Business Glossary API is currently on private preview, and it needs to be
enabled on the project for it to be used.

## Usage

```
python3 bg_import/business_glossary_import.py <terms csv file legacy>
  --project=<project_id>
  --group=<entry_group_id>
  --glossary=<glossary_id>
  --location=<location_code>
  [--import-mode={strict,clear}]
  [--categories-csv=<categories csv file>]
  [--terms-csv={terms csv file}]
  [-h]
```

Currently `strict` and `clear` import mode are supported. The default
mode is `strict`. \
Provide a terms CSV file by using `--terms-csv` argument, `terms csv file legacy`
is deprecated. \
Run `python3 bg_import/business_glossary_import.py -h` for description of
individual arguments.

### Access token

For the utility to be able to access Data Catalog API an access token has to be
provided via `GCLOUD_ACCESS_TOKEN` environment variable. E.g.:

```
export GCLOUD_ACCESS_TOKEN=$(gcloud auth print-access-token)
```

## CSV file schema

The source CSV files shall adhere to RFC4180 format.

However, currently, we do not allow a header line and all records should be
encoded in UTF-8 charset. We expect values to be separated by comma (`,`)
character.

### Categories CSV schema

Each record in the categories CSV file represents a single category with the
following values:

`category_display_name,description,steward,belongs_to_category`

Where:

*   `category_display_name` (required): Unique name for the entry category.
*   `description` (required): Plain text or rich text encoded as plain text
    description for the category.
*   `steward` (optional): List of data stewards for the current category, with
    each steward separated by a comma (`,`). E.g.: `Data
    Steward1<steward1@example.com>, Data teward2<steward2@example.com>"`
*   `belongs_to_category` (optional): Display name of a category to which the
    category belongs

### Terms CSV schema

Each record in the terms CSV file represents a single category with the
following values:

`term_display_name,description,steward,tagged_assets,synonyms,related_terms,belongs_to_category`

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
    asset as a whole, the field can be indicated by separating it from the asset
    name with a colon (:) eg. `asset_name:field`
*   `synonyms` (optional): List of terms that have a synonym relation with the
    current term, with each term separated by a comma (`,`)
*   `related_terms` (optional): List of terms that have a related-to relation
    with the current term, with each term separated by a comma (`,`)
*   `belongs_to_category` (optional): Display name of a category to which the
    term belongs

In the case where a list of items inside a field contains the delimiter value
comma (,) the field has to be escaped by using double quotes (" "). e.g. term 1,
"Term 1, a description", "Data Steward1<steward1@example.com>, Data
teward2<steward2@example.com>",,,
