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
class GlossaryEntry: #CHECK - GLOSSRAY TAXONOMY ENTRY - reduce dicts and use this
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
        
@dataclass
class Context:
    """Represents the configuration for glossary export."""
    user_project: str
    org_ids: List[str]
    dataplex_entry_group: str
    project: str
    location_id: str
    entry_group_id: str
    dc_glossary_id: str
    dp_glossary_id: str
    display_name: str = ""


@dataclass
class CoreAspects:
    """Core aspect associated with a glossary entry."""
    description: str = ""                                   # Optional description
    contacts: List[str] = field(default_factory=list)        # Steward or contact list

@dataclass
class GlossaryTaxonomyEntry:
    """Represents a glossary entry (term or category) in taxonomy."""
    name: str = ""                                         # Full resource name
    entryType: str = ""                                     # e.g. "TERM" / "CATEGORY"
    uid: str = ""                                        # Unique identifier
    displayName: str = ""                                   # Human-readable name
    description: str = ""                                   # Optional description
    coreAspects: CoreAspects = field(default_factory=CoreAspects)
    

@dataclass
class GlossaryTaxonomyRelationship:
    """Represents a relationship between glossary entries."""
    name: str = ""                                          # Full resource name
    sourceEntryName: str = ""                                 # ID of source entry
    destinationEntryName: str = ""                                 # ID of target entry
    relationshipType: str = ""                              # e.g. "is_related_to", "is_synonymous_to"
    parentGlossaryEntryName: str = ""                              # Parent entry name if applicable

@dataclass
class DcEntryRelationship:
    """Represents a relationship between Data Catalog entries."""
    name: str = ""                                          # Full resource name
    sourceColumn: str = ""               # Source entry object
    destinationEntryName: str = ""          # Destination entry object
    relationshipType: str = ""                              # e.g. "is_related_to", "is_synonymous_to"

@dataclass
class SearchEntryResult:
    """Represents the result of searching for an entry in Dataplex."""
    relativeResourceName: str = ""                          # Full Dataplex resource path
    linkedResource: str = ""                                # Linked resource path