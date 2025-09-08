import re
from typing import Tuple, List, Optional
import logging_utils
from file_utils import *
from migration_utils import normalize_id

logger = logging_utils.get_logger()

def build_import_spec_base(gcs_bucket: str) -> dict:
    return {
        "log_level": "DEBUG",
        "source_storage_uri": f"gs://{gcs_bucket}/",
        "entry_sync_mode": "FULL",
        "aspect_sync_mode": "INCREMENTAL"
    }

def extract_job_location_from_entry_group(entry_group: str) -> str:
    if not entry_group:
        return "global"
    match = re.search(r'locations/([^/]+)', entry_group)
    return match.group(1) if match else "global"

def extract_glossary_id_from_synonym_related_filename(filename: str) -> str:
    match = re.search(r'entrylinks_related_synonyms_(.*?)\.json', filename)
    return match.group(1) if match else "unknown"

def get_link_type(file_path: str) -> str | None:
    data = read_first_json_line(file_path)
    if data:
        return data.get("entryLink", {}).get("entryLinkType", "")
    return None


def extract_scopes_from_entry_references(data: dict) -> set:
    scopes = set()
    entry_references = data.get("entryLink", {}).get("entryReferences", [])
    if len(entry_references) > 0:
        name = entry_references[0].get("name", "")
        match = re.search(r"(projects/[^/]+)", name)
        if match:
            scopes.add(match.group(1))
    return scopes


def get_project_scopes_from_all_lines(file_path: str) -> set:
    scopes = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                data = parse_json_line(line)
                if data:
                    scopes.update(extract_scopes_from_entry_references(data))
    except IOError:
        pass
    return scopes


def get_project_scope_from_first_line(file_path: str) -> set:
    scopes = set()
    data = read_first_json_line(file_path)
    if data:
        scopes.update(extract_scopes_from_entry_references(data))
    return scopes

def build_related_synonym_referenced_entry_scopes(file_path: str, main_project_id: str) -> List[str]:
    scopes = get_project_scopes_from_all_lines(file_path)
    scopes.add(f"projects/{main_project_id}")
    return list(scopes)

def build_defintion_referenced_entry_scopes(file_path: str, main_project_id: str) -> List[str]:
    scopes = get_project_scope_from_first_line(file_path)
    scopes.add(f"projects/{main_project_id}")
    return list(scopes)

def build_glossary_payload(filename: str, project_id: str, import_spec_base: dict) -> Tuple[str, dict, str]:
    glossary_id = filename.replace("glossary_", "").replace(".json", "").replace("_", "-")
    job_id_prefix = f"glossary-{glossary_id}"
    job_location = "global"
    payload = {
        "type": "IMPORT",
        "import_spec": {
            **import_spec_base,
            "scope": {
                "glossaries": [f"projects/{project_id}/locations/global/glossaries/{glossary_id}"]
            }
        }
    }
    logger.debug(f"build_glossary_payload input: filename={filename}, project_id={project_id}, import_spec_base={import_spec_base} | output: {job_id_prefix}, {payload}, {job_location}")
    return job_id_prefix, payload, job_location


def build_definition_entrylink_payload(file_path: str, filename: str, project_id: str,import_spec_base: dict) -> Tuple[str, dict, str]:
    entry_group = get_entry_group(file_path)
    job_location = extract_job_location_from_entry_group(entry_group)
    job_id_prefix = normalize_id(filename)
    referenced_scopes = build_defintion_referenced_entry_scopes(file_path, project_id)
    payload = {
        "type": "IMPORT",
        "import_spec": {
            **import_spec_base,
            "scope": {
                "entry_groups": [entry_group],
                "entry_link_types": ["projects/dataplex-types/locations/global/entryLinkTypes/definition"],
                "referenced_entry_scopes": referenced_scopes
            }
        }
    }
    logger.debug(f"build_definition_entrylink_payload input: file_path={file_path}, filename={filename}, import_spec_base={import_spec_base}, referenced_scopes={referenced_scopes} | output: {job_id_prefix}, {payload}, {job_location}")
    return job_id_prefix, payload, job_location


def build_synonym_related_entrylink_payload(file_path: str, filename: str, project_id: str, import_spec_base: dict) -> Tuple[str, dict, str]:
    glossary_id = extract_glossary_id_from_synonym_related_filename(filename)
    referenced_scopes = build_related_synonym_referenced_entry_scopes(file_path, project_id)
    job_id_prefix = f"entrylinks-synonym-related-{glossary_id}"
    job_location = "global"
    payload = {
        "type": "IMPORT",
        "import_spec": {
            **import_spec_base,
            "scope": {
                "entry_groups": [f"projects/{project_id}/locations/global/entryGroups/@dataplex"],
                "entry_link_types": [
                    "projects/dataplex-types/locations/global/entryLinkTypes/synonym",
                    "projects/dataplex-types/locations/global/entryLinkTypes/related"
                ],
                "referenced_entry_scopes": referenced_scopes
            }
        }
    }
    logger.debug(f"build_synonym_related_entrylink_payload input: filename={filename}, project_id={project_id}, import_spec_base={import_spec_base}, referenced_scopes={referenced_scopes} | output: {job_id_prefix}, {payload}, {job_location}")
    return job_id_prefix, payload, job_location


def build_entrylink_payload(file_path: str, filename: str, project_id: str, import_spec_base: dict) -> Tuple[Optional[str], Optional[dict], Optional[str]]:
    """
    Wrapper for choosing definition vs synonyms/related.
    This function expects callers will pass the get_link_type and get_referenced_scopes functions (to avoid circular imports).
    """
    link_type = get_link_type(file_path)
    if not link_type:
        logger.warning(f"Cannot determine link type for {filename}. Skipping.")
        return None, None, None

    if "definition" in link_type:
        return build_definition_entrylink_payload(file_path, filename, project_id, import_spec_base)
    else:
        return build_synonym_related_entrylink_payload(file_path, filename, project_id, import_spec_base)


def build_payload(file_path: str, filename: str, project_id: str, gcs_bucket: str):
    import_spec_base = build_import_spec_base(gcs_bucket)
    if filename.startswith("glossary_"):
        return build_glossary_payload(filename, project_id, import_spec_base)
    elif filename.startswith("entrylinks_"):
        return build_entrylink_payload(file_path, filename, project_id, import_spec_base)
    else:
        logger.warning(f"Unknown file type: {filename}. Skipping.")
        return None, None, None

