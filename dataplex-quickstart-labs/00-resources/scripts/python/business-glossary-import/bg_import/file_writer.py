# file_writer.py
"""
Handles writing lists of transformed data objects to JSON files.
"""
import json
import os
from typing import List, Union, Dict
import logging_utils
from models import GlossaryEntry, EntryLink

logger = logging_utils.get_logger()

def write_jsonl_file(export_list: List[Union[GlossaryEntry, EntryLink]], filename: str):
    """Writes a list of dataclass objects to a JSONL file."""
    output_dir = os.path.join(os.getcwd(), "Exported_Files")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    filepath = os.path.join(output_dir, filename)
    
    valid_items = [item for item in export_list if item]
    
    with open(filepath, "w", encoding="utf-8") as f:
        for item in valid_items:
            f.write(json.dumps(item.to_dict()) + "\n")
    
    logger.info(f"Successfully exported {len(valid_items)} items to {filepath}")

def write_grouped_jsonl_files(config: dict, grouped_links: Dict[str, List[EntryLink]]):
    """Writes grouped links into separate files based on their group key."""
    for ple_key, links in grouped_links.items():
        filename = f"term_entry_links_{config['normalized_glossary']}_{ple_key}.jsonl"
        write_jsonl_file(links, filename)