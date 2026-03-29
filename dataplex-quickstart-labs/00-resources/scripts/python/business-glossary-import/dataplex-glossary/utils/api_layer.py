"""API Layer for Dataplex Glossary Operations."""

import os
import sys
import time
import random
import socket
from typing import Dict, List, Optional, Callable, TypeVar

import requests
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import httplib2

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from . import api_call_utils, logging_utils
from .api_call_utils import fetch_api_response, is_transient_error, _get_friendly_error_message, _format_retry_wait_time
from .constants import (
    CLOUD_RESOURCE_MANAGER_BASE_URL,
    DATAPLEX_BASE_URL,
    CATALOG_ENTRY_PATTERN,
    INITIAL_BACKOFF_SECONDS,
    LOCATION_TYPE_GLOBAL,
    LOCATION_TYPE_MULTI_REGIONAL,
    LOCATION_TYPE_REGIONAL,
    MAX_BACKOFF_SECONDS,
    MAX_RETRY_DURATION_SECONDS,
    MULTI_REGIONAL_LOCATIONS,
    PAGE_SIZE,
    PROJECT_PATTERN,
)
from .error import *

logger = logging_utils.get_logger()

_locations_cache: Dict[str, List[str]] = {}
_default_project: Optional[str] = None
T = TypeVar('T')


def get_default_project() -> Optional[str]:
    """Get the default project from ADC credentials."""
    global _default_project
    if _default_project is None:
        try:
            logger.debug("Fetching default project from Application Default Credentials (ADC)...")
            _, project = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            _default_project = project
            logger.debug(f"Default project from ADC: {project}")
        except Exception as e:
            logger.debug(f"Failed to get default project from ADC: {e}")
    return _default_project


def _is_retryable_google_api_error(api_error: Exception) -> bool:
    """Check if a Google API client error is retryable."""
    if is_transient_error(api_error):
        return True
    
    if isinstance(api_error, (socket.timeout, socket.error, TimeoutError, 
                          httplib2.ServerNotFoundError, httplib2.RelativeURIError)):
        return True
    
    if isinstance(api_error, HttpError):
        http_status = api_error.resp.status if hasattr(api_error, 'resp') else 0
        return http_status >= 500 or http_status == 429
    
    error_message_lower = str(api_error).lower()
    timeout_patterns = ['timed out', 'timeout', 'deadline exceeded']
    return any(pattern in error_message_lower for pattern in timeout_patterns)


def _execute_with_retry(
    operation: Callable[[], T],
    operation_name: str = "API call"
) -> T:
    """Execute an operation with exponential backoff retry for transient errors."""
    retry_start_time = time.time()
    current_backoff = INITIAL_BACKOFF_SECONDS
    attempt_count = 0
    last_exception = None
    
    while True:
        attempt_count += 1
        elapsed_seconds = time.time() - retry_start_time
        remaining_seconds = MAX_RETRY_DURATION_SECONDS - elapsed_seconds
        
        try:
            return operation()
        except Exception as retry_error:
            last_exception = retry_error
            elapsed_seconds = time.time() - retry_start_time
            remaining_seconds = MAX_RETRY_DURATION_SECONDS - elapsed_seconds
            
            if not _is_retryable_google_api_error(retry_error) or remaining_seconds <= 0:
                if remaining_seconds <= 0 and _is_retryable_google_api_error(retry_error):
                    logger.error(
                        "Network connectivity issue persists after retrying for %s. "
                        "Please check your internet connection.",
                        _format_retry_wait_time(elapsed_seconds)
                    )
                raise
            
            logger.warning(
                "%s failed (attempt %d): %s. Retrying in %s... "
                "(will retry for up to %s more)",
                operation_name,
                attempt_count,
                _get_friendly_error_message(retry_error),
                _format_retry_wait_time(current_backoff),
                _format_retry_wait_time(remaining_seconds)
            )
            
            time.sleep(current_backoff + random.uniform(0, 0.5))
            current_backoff = min(current_backoff * 2, MAX_BACKOFF_SECONDS)


def initialize_locations_cache(user_project: str) -> List[str]:
    """Pre-fetch and cache the list of supported locations.
    
    Call this function once at the start of batch operations to avoid
    repeated API calls for each term.
    
    Args:
        user_project: The project ID to fetch locations for.
    
    Returns:
        List of standard region IDs (excludes global, us, eu).
    """
    all_locations = list_supported_locations(user_project)
    standard_regions = get_standard_regions(all_locations)
    logger.debug(f"Initialized locations cache with {len(standard_regions)} queryable regions")
    return standard_regions


def authenticate_dataplex() -> build:
    """Authenticate with Dataplex API."""
    global _default_project
    try:
        logger.debug("Authenticating with Dataplex API using Application Default Credentials...")
        creds, project = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        _default_project = project
        logger.debug(f"Authenticated successfully. Default project: {project}")
        service = build('dataplex', 'v1', credentials=creds, cache_discovery=False)
        logger.debug("Dataplex API service built successfully")
        return service
    except Exception as e:
        logger.error(f"Dataplex auth error: {e}")
        raise DataplexAPIError(f"Dataplex auth error: {e}")


def list_glossary_categories(dataplex_service: build, glossary_name: str) -> List[Dict]:
    """Lists categories from a Dataplex glossary with retry support."""
    all_categories = []
    logger.debug(f"[LIST CATEGORIES] Request: dataplex.glossaries.categories.list(parent={glossary_name})")
    category_request = dataplex_service.projects().locations().glossaries().categories().list(
        parent=glossary_name, pageSize=1000)

    page_number = 0
    while category_request:
        page_number += 1
        def fetch_categories_page():
            return category_request.execute()
        
        try:
            page_response = _execute_with_retry(
                fetch_categories_page,
                f"List glossary categories for {glossary_name}"
            )
        except Exception as category_error:
            logger.error(f"Error while listing glossary categories: {category_error}")
            raise DataplexAPIError(f"Error while listing glossary categories: {category_error}")
        
        page_categories = page_response.get('categories', [])
        if page_categories:
            all_categories.extend(page_categories)
            logger.debug(f"[LIST CATEGORIES] Response page {page_number}: {len(page_categories)} categories")
        
        category_request = dataplex_service.projects().locations().glossaries().categories().list_next(
            category_request, page_response
        )
    
    logger.debug(f"[LIST CATEGORIES] Total: {len(all_categories)} categories for {glossary_name}")
    if not all_categories:
        logger.warning(f"No categories found for {glossary_name}")
        raise NoCategoriesFoundError(f"No categories found for {glossary_name}")
    return all_categories


def list_glossary_terms(dataplex_service: build, glossary_name: str) -> List[Dict]:
    """Lists terms from a Dataplex glossary with pagination support."""
    all_terms = []
    next_page_token = None
    logger.debug(f"[LIST TERMS] Request: dataplex.glossaries.terms.list(parent={glossary_name})")
    
    page_number = 0
    while True:
        page_number += 1
        
        def fetch_terms_page():
            nonlocal next_page_token
            terms_request = dataplex_service.projects().locations().glossaries().terms().list(
                parent=glossary_name, pageSize=1000, pageToken=next_page_token
            )
            return terms_request.execute()
        
        try:
            page_response = _execute_with_retry(
                fetch_terms_page, 
                f"List glossary terms for {glossary_name}"
            )
        except Exception as terms_error:
            logger.error(f"Error while listing glossary terms for {glossary_name}: {terms_error}")
            raise DataplexAPIError(f"Error while listing glossary terms for {glossary_name}: {terms_error}")

        page_terms = page_response.get('terms', [])
        if page_terms:
            all_terms.extend(page_terms)
            logger.debug(f"[LIST TERMS] Response page {page_number}: {len(page_terms)} terms")
        
        next_page_token = page_response.get('nextPageToken')
        if not next_page_token:
            break
    
    logger.debug(f"[LIST TERMS] Total: {len(all_terms)} terms for {glossary_name}")
    if not all_terms:
        logger.warning(f"No terms found for {glossary_name}")
    return all_terms

def _parse_entry_id_components(entry_id: str) -> tuple:
    """Parse entry ID to extract project and location."""
    entry_id_match = CATALOG_ENTRY_PATTERN.match(entry_id)
    if not entry_id_match:
        logger.error(f"Invalid entry ID format: {entry_id}")
        raise InvalidEntryIdFormatError(f"Invalid entry ID format: {entry_id}")
    return entry_id_match.group('project_id'), entry_id_match.group('location_id')


def _fetch_entry_links_page(
    entry_id: str, 
    project_id: str, 
    location_id: str, 
    billing_project: str, 
    page_token: str = None
) -> tuple:
    """Fetch a single page of entry links. Returns (entry_links, next_page_token, error_msg)."""
    lookup_url = build_entry_lookup_url(entry_id, project_id, location_id, page_token=page_token)
    
    api_response = fetch_api_response(
        method=requests.get,
        url=lookup_url,
        project_id=billing_project
    )
    
    if api_response.get('error_msg'):
        return [], None, api_response['error_msg']
    
    response_data = api_response.get('json', {})
    page_entry_links = response_data.get('entryLinks', [])
    next_page_token = response_data.get('nextPageToken')
    
    return page_entry_links, next_page_token, None


def lookup_entry_links_for_term(
    entry_id: str, 
    billing_project: str,
    location: Optional[str] = None
) -> Optional[List[Dict]]:
    """Looks up EntryLinks for a glossary term with pagination."""
    try:
        project_id, entry_location = _parse_entry_id_components(entry_id)
        target_location = location if location else entry_location
        
        logger.debug(f"[LOOKUP ENTRY LINKS] Request: entry_id={entry_id}, project={project_id}, location={target_location}")
        
        all_entry_links = []
        current_page_token = None
        
        while True:
            page_links, next_token, error_message = _fetch_entry_links_page(
                entry_id, project_id, target_location, billing_project, current_page_token
            )
            
            if error_message:
                logger.debug(f"lookupEntryLinks at {target_location}: {error_message}")
                break
            
            if page_links:
                all_entry_links.extend(page_links)
            
            current_page_token = next_token
            if not current_page_token:
                break
            
            logger.debug(f"Fetching next page of entry links for {entry_id} at {target_location}")
        
        logger.debug(f"[LOOKUP ENTRY LINKS] Response: found {len(all_entry_links)} entry links for {entry_id}")
        return all_entry_links if all_entry_links else None
        
    except Exception as lookup_error:
        logger.debug(f"[LOOKUP ENTRY LINKS] Error while looking up entrylinks for {entry_id}: {lookup_error}")
        logger.error(f"Error while looking up entry links: {lookup_error}")
        return None


def build_entry_lookup_url(
    entry_id: str, 
    project_id: str, 
    location_id: str, 
    page_size: int = PAGE_SIZE,
    page_token: Optional[str] = None
) -> str:
    """Builds the lookupEntryLinks API URL with pagination parameters.
    
    Args:
        entry_id: The full entry ID to lookup.
        project_id: The project ID.
        location_id: The location ID.
        page_size: Maximum number of results per page (default: PAGE_SIZE).
        page_token: Token for fetching the next page of results.
    
    Returns:
        The constructed API URL.
    """
    url = f"{DATAPLEX_BASE_URL}/projects/{project_id}/locations/{location_id}:lookupEntryLinks?entry={entry_id}&pageSize={page_size}"
    if page_token:
        url += f"&pageToken={page_token}"
    logger.debug(f"Built lookupEntryLinks URL with location={location_id}")
    return url

def lookup_entry(dataplex_service: build, entry_id: str, project_location_name: str) -> Optional[Dict]:
    """Looks up an entry using the Dataplex API.

    Args:
        dataplex_service: The Dataplex API service object.
        entry_id: The entry ID to look up.

    Returns:
        The entry response as a dictionary, or None if an error occurs.

    Raises:
        DataplexAPIError: If there is an error during the API call.
    """
    try:
        logger.debug(f"[LOOKUP ENTRY] Request: entry_id={entry_id}, location={project_location_name}")
        request = dataplex_service.projects().locations().lookupEntry(name=project_location_name, entry=entry_id, view="ALL")
        response = request.execute()
        logger.debug(f"[LOOKUP ENTRY] Response: found entry with name={response.get('name', 'N/A')}")
        return response
    except Exception as e:
        logger.debug(f"[LOOKUP ENTRY] Failed for {entry_id}: {e}")
        return None


def _get_project_url(project_id: str) -> str:
    """Builds the Cloud Resource Manager project URL."""
    return f"{CLOUD_RESOURCE_MANAGER_BASE_URL}/projects/{project_id}"


def _fetch_project_info(project_id: str, user_project: str) -> dict:
    """Calls the Cloud Resource Manager API and returns the project JSON payload."""
    url = _get_project_url(project_id)
    logger.debug(f"[FETCH PROJECT INFO] Request: project_id={project_id}, billing_project={user_project}")
    response = api_call_utils.fetch_api_response(requests.get, url, user_project)
    if response["error_msg"]:
        logger.error(f"Failed to fetch project info for '{project_id}': {response['error_msg']}")
        sys.exit(1)
    logger.debug(f"[FETCH PROJECT INFO] Response: received project info for {project_id}")
    return response.get("json", {})


def _extract_project_number_from_info(project_info: dict) -> str:
    """Extracts the numeric project number from the project info 'name' field."""
    name = project_info.get("name", "")
    match = PROJECT_PATTERN.search(name)
    if match:
        return match.group('project_number')
    logger.error("Project number not found in project info.")
    sys.exit(1)


def get_project_number(project_id: str, user_project: str) -> str:
    """Fetches the project number from the project ID (composed from smaller helpers)."""
    project_info = _fetch_project_info(project_id, user_project)
    return _extract_project_number_from_info(project_info)


def _is_location_cached(billing_project: str, force_refresh: bool) -> bool:
    """Check if locations are cached for the project."""
    return not force_refresh and billing_project in _locations_cache


def _extract_location_ids(locations_response: List[Dict]) -> List[str]:
    """Extract location IDs from API response."""
    return [loc.get('locationId') for loc in locations_response if loc.get('locationId')]


def list_supported_locations(billing_project: str, dataplex_service=None, force_refresh: bool = False) -> List[str]:
    """Lists all supported Dataplex locations for a project with caching."""
    global _locations_cache
    
    if _is_location_cached(billing_project, force_refresh):
        cached_count = len(_locations_cache[billing_project])
        logger.debug(f"[LIST LOCATIONS] Using cached locations for project {billing_project} ({cached_count} locations)")
        return _locations_cache[billing_project]
    
    if dataplex_service is None:
        dataplex_service = authenticate_dataplex()
    
    try:
        all_location_ids = []
        parent_resource = f"projects/{billing_project}"
        logger.debug(f"[LIST LOCATIONS] Request: dataplex.projects.locations.list(name={parent_resource})")
        locations_request = dataplex_service.projects().locations().list(name=parent_resource)
        
        page_number = 0
        while locations_request:
            page_number += 1
            def fetch_locations_page():
                return locations_request.execute()
            
            page_response = _execute_with_retry(fetch_locations_page, f"List locations for {billing_project}")
            
            page_location_ids = _extract_location_ids(page_response.get('locations', []))
            all_location_ids.extend(page_location_ids)
            
            logger.debug(f"[LIST LOCATIONS] Response page {page_number}: {page_location_ids}")
            
            locations_request = dataplex_service.projects().locations().list_next(locations_request, page_response)
        
        _locations_cache[billing_project] = all_location_ids
        logger.debug(f"[LIST LOCATIONS] Total: {len(all_location_ids)} locations cached for {billing_project}")
        return all_location_ids
        
    except Exception as locations_error:
        logger.error(f"Error while listing supported locations: {locations_error}")
        raise DataplexAPIError(f"Error while listing supported locations: {locations_error}")


def classify_location(location: str) -> str:
    """Classify location type: regional, global, or multi-regional."""
    location_lower = location.lower()
    if location_lower == "global":
        return LOCATION_TYPE_GLOBAL
    elif location_lower in MULTI_REGIONAL_LOCATIONS:
        return LOCATION_TYPE_MULTI_REGIONAL
    return LOCATION_TYPE_REGIONAL


def get_standard_regions(all_locations: List[str]) -> List[str]:
    """Filter locations to only standard regions (excludes global, us, eu)."""
    excluded = {"global"} | MULTI_REGIONAL_LOCATIONS
    return [loc for loc in all_locations if loc.lower() not in excluded]


def resolve_regions_to_query(location: str, user_project: str) -> List[str]:
    """Resolve location to regions for entry link queries.
    
    Multi-regional 'us'/'eu' query only their endpoint, not regional ones.
    Global queries all endpoints.
    """
    location_type = classify_location(location)
    
    if location_type == LOCATION_TYPE_REGIONAL:
        return [location]
    elif location_type == LOCATION_TYPE_MULTI_REGIONAL:
        return [location.lower()]
    elif location_type == LOCATION_TYPE_GLOBAL:
        all_locations = list_supported_locations(user_project)
        return ['global', 'us', 'eu'] + get_standard_regions(all_locations)
    return [location]