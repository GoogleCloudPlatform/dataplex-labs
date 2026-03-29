# Standard library imports
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
from typing import Dict, List

# Local imports
from utils import dataplex_dao, file_utils, gcs_dao, import_utils, logging_utils, payloads
from utils.constants import (
    DP_LINK_TYPE_RELATED,
    DP_LINK_TYPE_SYNONYM,
    ENTRYLINK_NAME_PATTERN,
)

logger = logging_utils.get_logger()


def create_import_json_files(grouped_entrylinks: Dict[str, Dict[str, List[Dict]]], archive_dir: str) -> List[str]:
    """
    Create JSON files from grouped entrylinks for import.
    
    Args:
        grouped_entrylinks: Dictionary grouped by link_type and entry_group
        archive_dir: Directory path where JSON files should be created
        
    Returns:
        List of file paths to the created JSON files
    """
    import_files = []
    
    for link_type, entry_groups in grouped_entrylinks.items():
        for group_key, group_entrylinks in entry_groups.items():
            # group_key is already in project_id_location_entryGroup format
            filename = f"entrylinks_{link_type}_{group_key}.json"
            json_file = file_utils.write_entrylinks_to_file(group_entrylinks, archive_dir, filename)
            import_files.append(json_file)
            logger.debug(f"Created import file: {json_file} with {len(group_entrylinks)} entrylinks")
    
    return import_files


def get_referenced_scopes(file_path: str, main_project_id: str) -> list:
    link_type = file_utils.get_link_type(file_path)
    if link_type and (DP_LINK_TYPE_RELATED in link_type or DP_LINK_TYPE_SYNONYM in link_type):
        scopes = file_utils.get_project_scopes_from_all_lines(file_path)
    else:
        scopes = file_utils.get_project_scope_from_first_line(file_path)
    scopes.add(f"projects/{main_project_id}")
    return list(scopes)

def extract_project_id_from_entrylink(entry_link_json: Dict) -> str:
    """Extract project ID from the EntryLink name itself."""
    try:
        # Extract from EntryLink name: projects/{project_id}/locations/{location}/entryGroups/{entryGroup}/entryLinks/{id}
        entrylink_name = entry_link_json['entryLink']['name']
        match = ENTRYLINK_NAME_PATTERN.match(entrylink_name)
        if not match:
            logger.error(f"Invalid EntryLink name format: {entrylink_name}")
            return None
        return match.group('project_id')
    except (KeyError, IndexError) as e:
        logger.error(f"Failed to extract project ID from EntryLink name: {e}")
        return None


def process_import_file(file_path: str, gcs_bucket: str) -> bool:
    """
    Processes a single glossary or entrylink file:
    - Builds payload
    - Uploads file to GCS (via prepare_gcs_bucket)
    - Creates & monitors Dataplex job
    - Moves local file to imported folder on success (or removes empty files)
    """
    filename = os.path.basename(file_path)

    if file_utils.is_file_empty(file_path):
        logger.info(f"File {filename} is empty. Skipping the import job.")
        file_utils.move_file_to_imported_folder(file_path)
        return True

    logger.debug(f"Processing file: {filename}")

    service = dataplex_dao.get_dataplex_service()
    first_entry = file_utils.read_first_json_line(file_path)
    project_id = import_utils.extract_project_id_from_entrylink(first_entry)
    job_id, payload, job_location = payloads.build_payload(file_path, project_id, gcs_bucket)
    if not payload or not job_id or not job_location:
        return False

    try:
        # Upload file to GCS first; only continue if upload succeeded
        upload_status = gcs_dao.prepare_gcs_bucket(gcs_bucket, file_path, filename)
        if not upload_status:
            logger.error(f"Failed to prepare GCS bucket '{gcs_bucket}' for file '{filename}'. Skipping import.")
            return False
        job_success = dataplex_dao.create_and_monitor_job(service, project_id, job_location, payload, job_id)
        if job_success:
            file_utils.move_file_to_imported_folder(file_path)
        return job_success
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        logger.debug(f"Error processing file {file_path}: {e}", exc_info=True)
        return False


def _process_files_for_bucket(files_for_bucket: List[str], bucket: str) -> List[bool]:
    """Worker: processes all files assigned to a bucket sequentially."""
    results = []
    for f in files_for_bucket:
        try:
            result = import_utils.process_import_file(f, bucket)
        except Exception as e:
            logger.error(f"Error processing file {f} in bucket {bucket}: {e}")
            logger.debug(f"Error processing file {f} in bucket {bucket}: {e}", exc_info=True)
            result = False
        results.append(result)
    return results


def run_import_files(files: List[str], buckets: List[str]) -> List[bool]:
    """
    Distribute files round-robin to buckets, start one worker per bucket and process sequentially
    to guarantee no two threads operate on the same bucket.
    """
    if not buckets:
        logger.error("No buckets provided to run_import_files.")
        return [False] * len(files)

    bucket_file_map: Dict[str, List[str]] = {b: [] for b in buckets}
    cycler = cycle(buckets)
    for f in files:
        b = next(cycler)
        bucket_file_map[b].append(f)

    results: List[bool] = []
    with ThreadPoolExecutor(max_workers=len(buckets)) as executor:
        future_map = {
            executor.submit(_process_files_for_bucket, bucket_file_map[bucket], bucket): bucket
            for bucket in buckets
        }
        for future in as_completed(future_map):
            bucket_results = future.result() or []
            results.extend(bucket_results)
    return results
