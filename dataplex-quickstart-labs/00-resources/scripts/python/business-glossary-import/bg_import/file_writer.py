# file_writer.py
"""
Handles writing lists of transformed data objects to JSON files.
"""
import json
import os
from typing import List, Union, Dict, Any
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

def write_files(
    config: Dict[str, Any],
    glossary_data: List[GlossaryEntry],
    term_term_data: List[EntryLink],
    term_entry_data: Dict[str, List[EntryLink]]
):
    """Writes the transformed data objects to their respective files."""
    logger.info("Step 4: Writing transformed data to files...")
    write_jsonl_file(glossary_data, f"glossary_{config['normalized_glossary']}.jsonl")
    write_jsonl_file(term_term_data, f"term_term_links_{config['normalized_glossary']}.jsonl")
    write_grouped_jsonl_files(config, term_entry_data)