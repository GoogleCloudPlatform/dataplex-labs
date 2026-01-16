from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
import logging_utils
from file_utils import *
from gcs_dao import prepare_gcs_bucket, ensure_folders_exist
from dataplex_dao import get_dataplex_service, create_and_monitor_job
from payloads import *
from constants import MIGRATION_FOLDER_PREFIX, MAX_FOLDERS
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


def process_import_file(file_path: str, project_id: str, gcs_bucket: str, folder_name: str) -> bool:
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

    job_id, payload, job_location = build_payload(file_path, project_id, gcs_bucket, folder_name)
    if not payload or not job_id or not job_location:
        return False

    try:
        # Upload file to GCS first; only continue if upload succeeded
        upload_status = prepare_gcs_bucket(gcs_bucket, folder_name, file_path, filename)
        logger.debug(f"Upload status for file '{filename}': {upload_status} to bucket '{gcs_bucket}' in folder '{folder_name}'")
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


def run_import_files(files: List[str], project_id: str, buckets: List[str]) -> List[bool]:
    """Runs import for multiple files using ThreadPoolExecutor for concurrency."""
    if not files:
        return []
    if not buckets:
        logger.error("No buckets provided to run_import_files.")
        return [False] * len(files)

    bucket = buckets[0]
    if len(buckets) > 1:
        logger.warning(f"Multiple buckets provided; using '{bucket}' with folder-based uploads.")

    folder_names = [f"{MIGRATION_FOLDER_PREFIX}{idx+1}" for idx in range(min(MAX_FOLDERS, len(files)))]
    if not ensure_folders_exist(bucket, folder_names):
        logger.error(f"Unable to ensure migration folders exist in bucket '{bucket}'.")
        return [False] * len(files)

    assignments = list(zip(files, folder_names))
    logger.info(f"Starting import of {len(files)} files into bucket '{bucket}' using {len(folder_names)} folders.")

    results: List[bool] = []
    max_workers = min(MAX_FOLDERS, len(assignments))
    import_files_with_threads(project_id, bucket, assignments, results, max_workers)  # Re-raise to propagate the interrupt
            
    return results

def import_files_with_threads(project_id, bucket, assignments, results, max_workers):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_import_file, file_path, project_id, bucket, folder_name): (file_path, folder_name)
            for file_path, folder_name in assignments
        }
        
        try:
            for future in as_completed(future_map):
                file_path, folder_name = future_map[future]
                try:
                    result = future.result()
                except Exception as e:
                    logger.error(f"Error processing file {file_path} in bucket {bucket} folder {folder_name}: {e}")
                    logger.debug(f"Error processing file {file_path} in bucket {bucket} folder {folder_name}: {e}", exc_info=True)
                    result = False
                results.append(result)
        except KeyboardInterrupt:
            logger.warning("\n*** Import interrupted by user (Ctrl+C) ***")
            logger.info("Cancelling pending imports...")
            
            # Cancel all pending futures
            for future in future_map.keys():
                if not future.done():
                    future.cancel()
            
            # Shutdown without waiting for threads
            executor.shutdown(wait=False)
            logger.info("Import process terminated by user")
            raise


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
