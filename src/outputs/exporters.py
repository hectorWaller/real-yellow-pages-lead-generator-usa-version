thonimport csv
import json
import logging
import os
from typing import Any, Dict, Iterable, List

logger = logging.getLogger("outputs.exporters")

def _ensure_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def save_to_json(records: Iterable[Dict[str, Any]], filepath: str) -> None:
    """
    Save an iterable of records to a JSON file.
    """
    data: List[Dict[str, Any]] = list(records)
    _ensure_dir(filepath)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d records to JSON file %s", len(data), filepath)

def save_to_csv(records: Iterable[Dict[str, Any]], filepath: str) -> None:
    """
    Save an iterable of records to a CSV file.
    """
    data: List[Dict[str, Any]] = list(records)
    if not data:
        logger.warning("No records to write to CSV file %s", filepath)
        return

    _ensure_dir(filepath)

    # Use keys from the first record as header
    fieldnames = sorted(data[0].keys())
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    logger.info("Saved %d records to CSV file %s", len(data), filepath)