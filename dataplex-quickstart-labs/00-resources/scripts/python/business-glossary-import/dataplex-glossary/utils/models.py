"""
Data models for Dataplex Glossary Import/Export operations.

These dataclasses provide type-safe representations of Dataplex resources
and eliminate the need for fragile dictionary access patterns.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class EntryReference:
    """Represents a source or target reference within an EntryLink."""
    name: str
    path: str = ""
    type: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EntryReference':
        """Create an EntryReference from a dictionary."""
        return cls(
            name=data.get('name', ''),
            path=data.get('path', ''),
            type=data.get('type')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        result = {'name': self.name}
        if self.path:
            result['path'] = self.path
        if self.type:
            result['type'] = self.type
        return result


@dataclass
class EntryLinkData:
    """Represents the inner entryLink data structure."""
    name: str
    entryLinkType: str
    entryReferences: List[EntryReference] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EntryLinkData':
        """Create EntryLinkData from a dictionary."""
        refs = [EntryReference.from_dict(ref) for ref in data.get('entryReferences', [])]
        return cls(
            name=data.get('name', ''),
            entryLinkType=data.get('entryLinkType', ''),
            entryReferences=refs
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'name': self.name,
            'entryLinkType': self.entryLinkType,
            'entryReferences': [ref.to_dict() for ref in self.entryReferences]
        }


@dataclass
class EntryLink:
    """Represents a complete EntryLink for import/export operations."""
    entryLink: EntryLinkData
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EntryLink':
        """Create an EntryLink from a dictionary."""
        entry_link_data = EntryLinkData.from_dict(data.get('entryLink', {}))
        return cls(entryLink=entry_link_data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {'entryLink': self.entryLink.to_dict()}


@dataclass
class SpreadsheetRow:
    """Represents a row from the EntryLink import spreadsheet."""
    entry_link_type: str
    source_entry: str
    target_entry: str
    source_path: str = ""
    
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'SpreadsheetRow':
        """Create a SpreadsheetRow from a dictionary."""
        return cls(
            entry_link_type=data.get('entry_link_type', ''),
            source_entry=data.get('source_entry', ''),
            target_entry=data.get('target_entry', ''),
            source_path=data.get('source_path', '')
        )
