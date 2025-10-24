from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
from typing import Dict, List
from utils import logging_utils
from file_utils import *
from gcs_dao import prepare_gcs_bucket
from dataplex_dao import get_dataplex_service, create_and_monitor_job
from payloads import *
import os
from file_utils import *
logger = logging_utils.get_logger()


def get_referenced_scopes(file_path: str, main_project_id: str) -> list:
    link_type = get_link_type(file_path)
    if link_type and ("related" in link_type or "synonym" in link_type):
        scopes = get_project_scopes_from_all_lines(file_path)
    else:
        scopes = get_project_scope_from_first_line(file_path)
    scopes.add(f"projects/{main_project_id}")
    return list(scopes)


def process_import_file(file_path: str, project_id: str, gcs_bucket: str) -> bool:
    """
    Processes a single glossary or entrylink file:
    - Builds payload
    - Uploads file to GCS (via prepare_gcs_bucket)
    - Creates & monitors Dataplex job
    - Moves local file to imported folder on success (or removes empty files)
    """
    filename = os.path.basename(file_path)

    if is_file_empty(file_path):
        logger.info(f"File {filename} is empty. Skipping the import job.")
        move_file_to_imported_folder(file_path)
        return True

    logger.debug(f"Processing file: {filename}")

    service = get_dataplex_service()

    job_id, payload, job_location = build_payload(file_path, project_id, gcs_bucket)
    if not payload or not job_id or not job_location:
        return False

    try:
        # Upload file to GCS first; only continue if upload succeeded
        upload_status = prepare_gcs_bucket(gcs_bucket, file_path, filename)
        if not upload_status:
            logger.error(f"Failed to prepare GCS bucket '{gcs_bucket}' for file '{filename}'. Skipping import.")
            return False
        job_success = create_and_monitor_job(service, project_id, job_location, payload, job_id)
        if job_success:
            move_file_to_imported_folder(file_path)
        return job_success
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        logger.debug(f"Error processing file {file_path}: {e}", exc_info=True)
        return False


def _process_files_for_bucket(files_for_bucket: List[str], project_id: str, bucket: str) -> List[bool]:
    """Worker: processes all files assigned to a bucket sequentially."""
    results = []
    for f in files_for_bucket:
        try:
            result = process_import_file(f, project_id, bucket)
        except Exception as e:
            logger.error(f"Error processing file {f} in bucket {bucket}: {e}")
            logger.debug(f"Error processing file {f} in bucket {bucket}: {e}", exc_info=True)
            result = False
        results.append(result)
    return results


def run_import_files(files: List[str], project_id: str, buckets: List[str]) -> List[bool]:
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
            executor.submit(_process_files_for_bucket, bucket_file_map[bucket], project_id, bucket): bucket
            for bucket in buckets
        }
        for future in as_completed(future_map):
            bucket_results = future.result() or []
            results.extend(bucket_results)
    return results


def filter_files_for_phases(phase_name: str, exported_files: List[str]) -> List[str]:
    if phase_name == "EntryLinks":
        logger.debug(f"{phase_name} and exported files: {exported_files}")
        filtered_entry_links_files = [file_path for file_path in exported_files if check_entrylink_dependency(file_path)]
        if not filtered_entry_links_files:
            logger.info("No EntryLink files with imported parent glossaries. Skipping phase.")
        return filtered_entry_links_files
    return exported_files


def process_phase(phase_name: str, files: List[str], project_id: str, buckets: List[str]):
    if not files:
        logger.info(f"No files found in {phase_name} folder. Skipping phase.")
        return

    logger.info("="*50)
    logger.info(f"Starting Import Phase: {phase_name.upper()} ({len(files)} files)")
    logger.info("="*50)

    files_to_process = filter_files_for_phases(phase_name, files)

    results = run_import_files(files_to_process, project_id, buckets)
    successful_imports = sum(1 for r in results if r)

    logger.info(f"--- Phase {phase_name} Complete: {successful_imports}/{len(files)} files imported successfully. ---")
    if successful_imports < len(files_to_process):
        logger.warning("Some files failed to import. Retry remaining files later.")


def import_status():
    if not get_file_paths_from_directory(GLOSSARIES_DIRECTORY_PATH) and not get_file_paths_from_directory(ENTRYLINKS_DIRECTORY_PATH):
        return True
    return False


def main(project_id: str, buckets: List[str]):
    phases = {
        "Glossaries": get_file_paths_from_directory(GLOSSARIES_DIRECTORY_PATH),
        "EntryLinks": get_file_paths_from_directory(ENTRYLINKS_DIRECTORY_PATH),
    }
    for phase_name, files in phases.items():
        process_phase(phase_name, files, project_id, buckets)
    return import_status()
