"""Constants used by the Business Glossary Migration Tool."""

# --- URLs ---
DATACATALOG_BASE_URL = "https://datacatalog.googleapis.com/v2"
DATAPLEX_BASE_URL = "https://staging-dataplex.sandbox.googleapis.com/v1"
SEARCH_BASE_URL = "https://datacatalog.googleapis.com/v1/catalog:search"
CLOUD_RESOURCE_MANAGER_BASE_URL = "https://cloudresourcemanager.googleapis.com/v3"

# --- Data Catalog Constants ---
# Data Catalog Relationship Types
DC_RELATIONSHIP_TYPE_BELONGS_TO = "belongs_to"
DC_RELATIONSHIP_TYPE_DESCRIBED_BY = "is_described_by"
DC_RELATIONSHIP_TYPE_RELATED = "is_related_to"
DC_RELATIONSHIP_TYPE_SYNONYMOUS = "is_synonymous_to"

# Data Catalog Entry Types
DC_TYPE_GLOSSARY_CATEGORY = "glossary_category"
DC_TYPE_GLOSSARY_TERM = "glossary_term"
DC_TYPE_GLOSSARY = "glossary"

# --- Dataplex Constants ---
# Dataplex Aspects
ASPECT_CONTACTS = "contacts"
ASPECT_OVERVIEW = "overview"

# Dataplex Link Types
DP_LINK_TYPE_DEFINITION = "definition"
DP_LINK_TYPE_RELATED = "related"
DP_LINK_TYPE_SYNONYM = "synonym"

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
PROJECT_NUMBER = "418487367933"
ROLE_STEWARD = "steward"
MAX_FOLDERS = 15

# -- BACKOFF Constants ---
MAX_ATTEMPTS = 10
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 300

# --- Filesystem Constants ---
EXPORTED_FILES_DIRECTORY = "exported_files"
IMPORTED_GLOSSARIES_DIRECTORY = "imported_glossaries"
IMPORTED_ENTRYLINKS_DIRECTORY = "imported_entrylinks"
ENTRYLINKS_DIRECTORY = "pending_entrylinks"
GLOSSARIES_DIRECTORY = "pending_glossaries"
UNGROUPED_ENTRYLINKS_DIRECTORY = "ungrouped_entrylinks"
LOGS_DIRECTORY = "logs"
SUMMARY_DIRECTORY = "summary"
MIGRATION_FOLDER_PREFIX = "migration_folder_"

MAX_BUCKETS = 20
MAX_POLLS = 12*12  # 12 hours
POLL_INTERVAL_MINUTES = 5
QUEUED_TIMEOUT_MINUTES = 10

# --- Mapping Constants ---
LINK_TYPE_MAP = {
    DC_RELATIONSHIP_TYPE_SYNONYMOUS: DP_LINK_TYPE_SYNONYM,
    DC_RELATIONSHIP_TYPE_RELATED: DP_LINK_TYPE_RELATED,
    DC_RELATIONSHIP_TYPE_DESCRIBED_BY: DP_LINK_TYPE_DEFINITION
}

TYPE_MAP = {
    DC_TYPE_GLOSSARY_CATEGORY: DP_TYPE_GLOSSARY_CATEGORY,
    DC_TYPE_GLOSSARY_TERM: DP_TYPE_GLOSSARY_TERM,
    DC_TYPE_GLOSSARY: DP_TYPE_GLOSSARY
}
