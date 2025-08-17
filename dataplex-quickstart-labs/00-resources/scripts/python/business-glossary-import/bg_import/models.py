# models.py
"""
Data models for the Business Glossary export process.
"""
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any

@dataclass
class Ancestor:
    """Represents a single ancestor in the glossary hierarchy."""
    name: str
    type: str

@dataclass
class EntrySource:
    """Represents the source information for a glossary entry."""
    resource: str
    displayName: str
    description: str
    ancestors: List[Ancestor] = field(default_factory=list)

@dataclass
class GlossaryEntry:
    """Represents a complete glossary term or category for export."""
    name: str
    entryType: str
    aspects: Dict[str, Any]
    parentEntry: str
    entrySource: EntrySource

    def to_dict(self) -> Dict[str, Any]:
        """Recursively converts the object to a dictionary for JSON serialization."""
        return {"entry": asdict(self)}

@dataclass
class EntryReference:
    """Represents a source or target reference within an EntryLink."""
    name: str
    path: Optional[str] = ""
    type: Optional[str] = None

@dataclass
class EntryLink:
    """Represents a complete entry link (relationship) for export."""
    name: str
    entryLinkType: str
    entryReferences: List[EntryReference] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Recursively converts the object to a dictionary for JSON serialization."""
        return {"entryLink": asdict(self)}