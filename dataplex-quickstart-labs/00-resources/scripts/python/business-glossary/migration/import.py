import os
import sys
import json
import re
import uuid
import time
import httplib2
import google_auth_httplib2
import google.auth
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from utils import logging_utils


logger = logging_utils.get_logger()

BASE_DIR = os.path.join(os.getcwd(), "migration/Exported_Files")
GLOSSARIES_DIR = os.path.join(BASE_DIR, "Glossaries")
ENTRYLINKS_DIR = os.path.join(BASE_DIR, "EntryLinks")
MAX_BUCKETS = 20
POLL_INTERVAL_MINUTES = 5

def get_dataplex_service():
    """Builds and returns an authenticated Dataplex service object."""
    credentials, _ = google.auth.default()
    http_client = httplib2.Http(timeout=300)
    authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http_client)
    service = build('dataplex', 'v1', http=authorized_http, cache_discovery=False)
    return service


def poll_metadata_job(service, project_id: str, location: str, job_id: str) -> bool:
    """Polls the metadataJob itself until it completes or fails."""
    logger.info(f"Polling status for job '{job_id}' every {POLL_INTERVAL_MINUTES} minutes...")
    poll_interval_seconds = POLL_INTERVAL_MINUTES * 60
    max_polls = 10
    queued_timeout_seconds = 600
    first_queued_time = None
    job_path = f"projects/{project_id}/locations/{location}/metadataJobs/{job_id}"

    for i in range(max_polls):
        time.sleep(poll_interval_seconds)
        try:
            job = service.projects().locations().metadataJobs().get(name=job_path).execute()
            state = job.get("status", {}).get("state")
            if state == "SUCCEEDED" or state == "SUCCEEDED_WITH_ERRORS":
                logger.info(f"Job '{job_id}' SUCCEEDED.")
                return True
            if state == "FAILED":
                error_message = job.get("status", {}).get("message", "No error message provided.")
                logger.error(f"Job '{job_id}' FAILED. Reason: {error_message}")
                return False
            if state == "QUEUED":
                if first_queued_time is None:
                    first_queued_time = time.time()
                queued_duration = time.time() - first_queued_time
                if queued_duration > queued_timeout_seconds:
                    logger.warning(f"Job '{job_id}' was queued for over 10 minutes, treating as failed.")
                    return False
                logger.info(f"Job '{job_id}' is QUEUED. Continuing to wait... (check {i+1}/{max_polls})")
            elif state == "RUNNING":
                first_queued_time = None
                logger.info(f"Job '{job_id}' is RUNNING. Continuing to wait... (check {i+1}/{max_polls})")
            else:
                logger.warning(f"Job '{job_id}' is in an unknown state: {state}. Continuing to wait...")
        except HttpError as err:
            logger.error(f"Error polling job '{job_id}': {err}")
            return False
    logger.warning(f"Polling for job '{job_id}' timed out.")
    return False


def create_and_monitor_job(service, project_id: str, location: str, payload: dict, job_id_prefix: str) -> bool:
    safe_prefix = re.sub(r'[^a-z0-9-]', '-', job_id_prefix.lower()).strip('-')
    truncated_prefix = safe_prefix[:50]
    job_id = f"{truncated_prefix}-{uuid.uuid4().hex[:8]}"
    parent = f"projects/{project_id}/locations/{location}"
    try:
        service.projects().locations().metadataJobs().create(parent=parent, metadataJobId=job_id, body=payload).execute()
        logger.info(f"Job '{job_id}' submitted successfully.")
        return poll_metadata_job(service, project_id, location, job_id)
    except HttpError as e:
        logger.error(f"Failed to create job '{job_id}': {e}")
        return False
    except TimeoutError:
        logger.error(f"Network timeout while trying to create job '{job_id}'. Check connection.")
        return False


def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)


def delete_all_bucket_objects(bucket_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    if not blobs:
        return
    try:
        bucket.delete_blobs(blobs)
    except Exception as e:
        logger.error(f"Failed to delete objects from bucket {bucket_name}: {e}")


def get_referenced_scopes_from_file(file_path: str, main_project_id: str) -> list:
    project_scopes = set()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                references = data.get("entryLink", {}).get("entryReferences", [])
                for reference in references:
                    ref_name = reference.get("name", "")
                    match = re.search(r"(projects/[^/]+)", ref_name)
                    if match:
                        project_scopes.add(match.group(1))
            except json.JSONDecodeError:
                logger.warning(f"Could not parse JSON line in {file_path}: {line.strip()}")
                continue
    logger.info(project_scopes)
    project_scopes.add(f"projects/{main_project_id}")
    return list(project_scopes)


def get_entry_group_from_file_content(file_path: str) -> str or None:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if not first_line:
                return None
            data = json.loads(first_line)
            entry_link_name = data["entryLink"]["name"]
            entry_group = entry_link_name.split('/entryLinks/')[0]
            return entry_group
    except (IOError, json.JSONDecodeError, KeyError) as e:
        logger.error(f"Could not extract entry group from {file_path}: {e}")
        return None

def get_link_type_from_file(file_path: str) -> str or None:
    """Reads the first line of a file to determine the entryLinkType."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if not first_line:
                return None
            data = json.loads(first_line)
            return data.get("entryLink", {}).get("entryLinkType", "")
    except (IOError, json.JSONDecodeError):
        logger.warning(f"Could not read or parse link type from {os.path.basename(file_path)}.")
        return None

def process_file(file_path: str, project_id: str, gcs_bucket: str) -> bool:
    service = get_dataplex_service()
    filename = os.path.basename(file_path)
    job_location = "global"
    job_id_prefix = "generic-import"
    payload = {}

    import_spec_base = {
        "log_level": "DEBUG",
        "source_storage_uri": f"gs://{gcs_bucket}/",
        "entry_sync_mode": "FULL",
        "aspect_sync_mode": "INCREMENTAL"
    }
    if filename.startswith("glossary_"):
        glossary_id_raw = filename.replace("glossary_", "").replace(".json", "")
        glossary_id = glossary_id_raw.replace("_", "-")
        job_id_prefix = f"glossary-{glossary_id}"
        payload = {"type": "IMPORT", "import_spec": { **import_spec_base, "scope": { "glossaries": [f"projects/{project_id}/locations/global/glossaries/{glossary_id}"] } }}
    elif filename.startswith("entrylinks_"):
        # Determine link type from file content, NOT filename
        link_type = get_link_type_from_file(file_path)
        if not link_type:
            logger.warning(f"Could not determine link type from content of {filename}. Skipping.")
            return False

        referenced_entry_scopes = get_referenced_scopes_from_file(file_path, project_id)

        if "definition" in link_type:
            entry_group = get_entry_group_from_file_content(file_path)
            if entry_group:
                match = re.search(r'locations/([^/]+)', entry_group)
                if match:
                    job_location = match.group(1)
                glossary_id_match = re.search(r'entrylinks_definition_(.*?)_', filename)
                glossary_id = glossary_id_match.group(1) if glossary_id_match else "unknown"
                entry_group_name = entry_group.split('/')[-1]
                job_id_prefix = f"entrylinks-definition-{glossary_id}-{entry_group_name}"
                payload = {"type": "IMPORT", "import_spec": { **import_spec_base, "scope": { "entry_groups": [entry_group], "entry_link_types": ["projects/dataplex-types/locations/global/entryLinkTypes/definition"], "referenced_entry_scopes": referenced_entry_scopes } }}
        
        elif "related" in link_type or "synonym" in link_type:
            glossary_id_match = re.search(r'entrylinks_related_synonyms_(.*?)\.json', filename)
            glossary_id = glossary_id_match.group(1) if glossary_id_match else "unknown"
            job_id_prefix = f"entrylinks-synonym-related-{glossary_id}"
            payload = {"type": "IMPORT", "import_spec": { **import_spec_base, "scope": { "entry_groups": [f"projects/{project_id}/locations/global/entryGroups/@dataplex"], "entry_link_types": [ "projects/dataplex-types/locations/global/entryLinkTypes/synonym", "projects/dataplex-types/locations/global/entryLinkTypes/related" ], "referenced_entry_scopes": referenced_entry_scopes } }}
    
    if not payload:
        logger.warning(f"Could not determine import type for file {filename}. Skipping.")
        return False
    try:
        delete_all_bucket_objects(gcs_bucket)
        upload_to_gcs(gcs_bucket, file_path, filename)
        job_success = create_and_monitor_job(service, project_id, job_location, payload, job_id_prefix)
        if job_success:
            os.remove(file_path)
        return job_success
    except Exception as e:
        logger.error(f"Critical error processing file {file_path}: {e}", exc_info=True)
        return False


def get_files_from_dir(directory: str) -> list:
    if not os.path.isdir(directory):
        return []
    return sorted([os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.json')])


def check_entrylink_dependency(file_path: str) -> bool:
    """
    Checks if an entrylink file's parent glossary was successfully imported
    by verifying its corresponding glossary file has been deleted.
    """
    filename = os.path.basename(file_path)
    parent_glossary_id = None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if not first_line:
                return False
            
            data = json.loads(first_line)
            link_data = data.get("entryLink", {})
            link_type = link_data.get("entryLinkType", "")
            refs = link_data.get("entryReferences", [])
            
            ref_to_check = None
            
            if "definition" in link_type and len(refs) > 1:
                ref_to_check = refs[1].get("name", "")  # The 2nd ref is the glossary term
            elif ("related" in link_type or "synonym" in link_type) and len(refs) > 0:
                ref_to_check = refs[0].get("name", "")  # The 1st ref is the source term
            
            if ref_to_check:
                match = re.search(r'glossaries/([^/]+)', ref_to_check)
                if match:
                    parent_glossary_id = match.group(1)

    except (IOError, json.JSONDecodeError):
        logger.warning(f"Could not read or parse dependency info from {filename}.")
        return False

    if not parent_glossary_id:
        logger.warning(f"Could not determine parent glossary for {filename}.")
        return False

    # Construct the expected path of the parent glossary's source file
    glossary_filename = f"glossary_{parent_glossary_id}.json"
    glossary_file_path = os.path.join(GLOSSARIES_DIR, glossary_filename)
    # If the glossary file STILL EXISTS, its import was not successful.
    if os.path.exists(glossary_file_path):
        return False

    # If the file does NOT exist, it was successfully imported and deleted.
    return True


def main(project_id: str, buckets: list):
    """Main function to run the import process."""
    phases = {
        "Glossaries": get_files_from_dir(GLOSSARIES_DIR),
        "EntryLinks": get_files_from_dir(ENTRYLINKS_DIR)
    }

    for phase_name, files in phases.items():
        if not files:
            logger.info(f"No files found in {phase_name} folder. Skipping phase.")
            continue
        
        logger.info("="*50)
        logger.info(f"Starting Import Phase: {phase_name.upper()} ({len(files)} files)")
        logger.info("="*50)
        
        files_to_process = files
        # --- UPDATED: Dependency check now uses the file existence method ---
        if phase_name == "EntryLinks":
            logger.info("Checking EntryLink file dependencies against successfully imported glossaries...")
            files_to_process = [f for f in files if check_entrylink_dependency(f)]

            if not files_to_process:
                logger.info("No EntryLink files with successfully imported parent glossaries were found.")
                continue
        
        is_parallel = True
        max_workers = len(buckets) if is_parallel else 1
        bucket_cycler = cycle(buckets)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = [(f, project_id, next(bucket_cycler)) for f in files_to_process]
            results = list(executor.map(lambda p: process_file(*p), tasks))
            successful_imports = sum(1 for r in results if r)

            logger.info(f"--- Phase {phase_name} Complete: {successful_imports}/{len(files_to_process)} files imported successfully. ---")
            if successful_imports < len(files_to_process):
                logger.warning("Some files failed to import. They remain in the 'Exported_Files' directory.")
                logger.warning("Please review the errors above and re-run the script to retry importing the remaining files.")


if __name__ == "__main__":
    project_id = input("Enter the GCP project ID to import into (e.g., my-gcp-project): ").strip()
    buckets_str = input(f"Enter a comma-separated list of GCS bucket names (max {MAX_BUCKETS}): ").strip()
    buckets = [b.strip() for b in buckets_str.split(',') if b.strip()][:MAX_BUCKETS]

    if not project_id or not buckets:
        logger.error("Project ID and at least one GCS bucket are required.")
        sys.exit(1)
        
    main(project_id, buckets)