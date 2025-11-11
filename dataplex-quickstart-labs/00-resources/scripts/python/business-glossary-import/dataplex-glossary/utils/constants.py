"""Constants used by the Business Glossary Migration Tool."""

import re

# --- URLs ---
DATACATALOG_BASE_URL = "https://datacatalog.googleapis.com/v2"
DATAPLEX_BASE_URL = "https://dataplex.googleapis.com/v1"
SEARCH_BASE_URL = "https://datacatalog.googleapis.com/v1/catalog:search"
CLOUD_RESOURCE_MANAGER_BASE_URL = "https://cloudresourcemanager.googleapis.com/v3"

# --- Dataplex Entry Group Constants ---
DATAPLEX_SYSTEM_ENTRY_GROUP = "@dataplex"
BIGQUERY_SYSTEM_ENTRY_GROUP = "@bigquery"

# --- Regex Patterns ---
# Glossary and Term Patterns
GLOSSARY_URL_PATTERN = re.compile(r".*dp-glossaries/projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/glossaries/(?P<glossary_id>[^/?#]+).*")
GLOSSARY_NAME_PATTERN = re.compile(r"projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/glossaries/(?P<glossary_id>[^/]+)")
TERM_NAME_PATTERN = re.compile(r"projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/glossaries/(?P<glossary_id>[^/]+)/terms/(?P<term_id>[^/]+)")
CATEGORY_NAME_PATTERN = re.compile(r"projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/glossaries/(?P<glossary_id>[^/?#]+)/categories/(?P<category_id>[^/]+)")

# Entry Patterns
ENTRY_NAME_PATTERN = re.compile(r"projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/entryGroups/(?P<entry_group>[^/]+)/entries/(?P<entry_id>.*)")
DATAPLEX_ENTRY_PATTERN = re.compile(r"projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/entryGroups/@dataplex/entries/.*")

# EntryLink Patterns
ENTRYLINK_NAME_PATTERN = re.compile(r"projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/entryGroups/(?P<entry_group>[^/]+)/entryLinks/(?P<entrylink_id>[^/]+)")
ENTRYLINK_TYPE_PATTERN = re.compile(r"projects/655216118709/locations/global/entryLinkTypes/(?P<link_type>[^/]+)")

# Source Entry Pattern (for extracting project/location/entryGroup from full entry paths)
SOURCE_ENTRY_PATTERN = re.compile(r"projects/(?P<project_id>[^/]+)/locations/(?P<location_id>[^/]+)/entryGroups/(?P<entry_group>[^/]+)/entries/")

# Google Sheets Pattern
SPREADSHEET_URL_PATTERN = re.compile(r"https://docs\.google\.com/spreadsheets/d/(?P<spreadsheet_id>[^/]+)")

# Validation Patterns
EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
PARENT_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
LABEL_PATTERN = re.compile(r"^[a-z0-9_-]+$")

# Project Pattern
PROJECT_PATTERN = re.compile(r"projects/(?P<project_number>\d+)")


# --- Dataplex Constants ---
# Dataplex Aspects
ASPECT_CONTACTS = "contacts"
ASPECT_OVERVIEW = "overview"

# Dataplex Link Types
DP_LINK_TYPE_DEFINITION = "definition"
DP_LINK_TYPE_RELATED = "related"
DP_LINK_TYPE_SYNONYM = "synonym"

# Entry Reference Types
ENTRY_REFERENCE_TYPE_SOURCE = "SOURCE"
ENTRY_REFERENCE_TYPE_TARGET = "TARGET"

# Dataplex Entry Types / Aspect Prefixes
DP_TYPE_GLOSSARY_CATEGORY = "glossary-category"
DP_TYPE_GLOSSARY_TERM = "glossary-term"
DP_TYPE_GLOSSARY = "glossary"
ASPECT_TYPE_CATEGORY = "glossary-category-aspect"
ASPECT_TYPE_TERM = "glossary-term-aspect"

# --- General Constants ---
CATEGORIES = "categories"
TERMS = "terms"
MAX_DESC_SIZE_BYTES = 120 * 1024
MAX_WORKERS = 10
PAGE_SIZE = 1000
PROJECT_NUMBER = "655216118709"

# -- BACKOFF Constants ---
MAX_ATTEMPTS = 10
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 300

# --- Filesystem Constants ---
LOGS_DIRECTORY = "logs"
SUMMARY_DIRECTORY = "summary"

MAX_BUCKETS = 20
MAX_POLLS = 12*12  # 12 hours
POLL_INTERVAL_MINUTES = 5
QUEUED_TIMEOUT_MINUTES = 10

LINK_TYPES = {
    DP_LINK_TYPE_DEFINITION: 'projects/dataplex-types/locations/global/entryLinkTypes/definition',
    DP_LINK_TYPE_SYNONYM: 'projects/dataplex-types/locations/global/entryLinkTypes/synonym',
    DP_LINK_TYPE_RELATED: 'projects/dataplex-types/locations/global/entryLinkTypes/related'
}