"""
Business Glossary Utility Functions

Common utility functions for working with Dataplex Glossary resources.
"""

# Standard library imports
import re
import uuid

# Local imports
from utils.constants import (
    DATAPLEX_SYSTEM_ENTRY_GROUP,
    GLOSSARY_NAME_PATTERN,
    GLOSSARY_URL_PATTERN,
    TERM_NAME_PATTERN,
)
from utils.error import InvalidTermNameError


def extract_glossary_name(url: str) -> str:
    """
    Extract the glossary resource name from a Dataplex URL or resource name.
    
    Args:
        url: Either a full Dataplex glossary URL or a glossary resource name
        
    Returns:
        The glossary resource name in format: projects/{project}/locations/{location}/glossaries/{glossary}
        
    Raises:
        ValueError: If the URL/resource name format is invalid
    """
    # First try to match as a URL
    match = GLOSSARY_URL_PATTERN.match(url)
    if match:
        project_id = match.group('project_id')
        location_id = match.group('location_id')
        glossary_id = match.group('glossary_id')
        return f"projects/{project_id}/locations/{location_id}/glossaries/{glossary_id}"
    
    # Try to match as a direct resource name
    glossary_name = url.strip().rstrip('/')
    match = GLOSSARY_NAME_PATTERN.match(glossary_name)
    if match:
        return glossary_name
    
    raise ValueError(
        f"Could not extract glossary resource from URL: {url}. "
        f"Expected format: 'projects/{{project}}/locations/{{location}}/glossaries/{{glossary}}' "
        f"or a URL containing this pattern."
    )


def generate_entry_name_from_term_name(term_name: str) -> str:
    """
    Generates a Dataplex entry ID from a glossary term name.
    
    Args:
        term_name: The full term name in format:
                   projects/{project}/locations/{location}/glossaries/{glossary}/terms/{term}
    Returns:
        The generated entry ID in format:
        projects/{project}/locations/{location}/entryGroups/@dataplex/entries/projects/{project}/locations/{location}/glossaries/{glossary}/terms/{term}
    """
    match = TERM_NAME_PATTERN.match(term_name)
    if not match:
        raise InvalidTermNameError(f"Invalid term name format: {term_name}")
    
    project_id = match.group('project_id')
    location_id = match.group('location_id')
    glossary_id = match.group('glossary_id')
    term_id = match.group('term_id')
    
    return (
        f"projects/{project_id}/locations/{location_id}/entryGroups/{DATAPLEX_SYSTEM_ENTRY_GROUP}/entries/"
        f"projects/{project_id}/locations/{location_id}/glossaries/{glossary_id}/terms/{term_id}"
    )


def normalize_id(name: str) -> str:
    """
    Converts a string to a valid Dataplex ID (lowercase, numbers, hyphens), starting with a letter.
    
    Args:
        name: The string to normalize
        
    Returns:
        A normalized ID suitable for Dataplex (lowercase, numbers, hyphens, starts with letter)
        
    Example:
        >>> normalize_id("My Special ID!")
        'my-special-id'
        >>> normalize_id("123-start-with-number")
        'g123-start-with-number'
    """
    if not name:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    # Ensure starts with a letter
    if not normalized or not normalized[0].isalpha():
        normalized = "g" + normalized
    return normalized


def get_entry_link_id() -> str:
    """
    Generate a unique entry link ID that starts with a lowercase letter 
    and contains only lowercase letters and numbers.
    
    Returns:
        A unique entry link ID in format: g{uuid_hex}
        
    Example:
        >>> id = get_entry_link_id()
        >>> id.startswith('g')
        True
        >>> len(id)
        33
    """
    entrylink_id = 'g' + uuid.uuid4().hex
    return entrylink_id

