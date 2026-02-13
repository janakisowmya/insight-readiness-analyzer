from __future__ import annotations
from typing import Any, Dict

def validate_profile(prof: Dict[str, Any]) -> None:
    """
    Validates a data profile dictionary.
    """
    if not isinstance(prof, dict):
        raise ValueError("Profile must be a dictionary")

    # Metadata
    meta = prof.get("metadata")
    if not isinstance(meta, dict):
        raise ValueError("Profile missing 'metadata' section")
    req_meta = {"dataset_name", "timestamp", "row_count", "col_count"}
    missing_meta = req_meta - meta.keys()
    if missing_meta:
        raise ValueError(f"Profile metadata missing keys: {missing_meta}")

    # Readiness
    readiness = prof.get("readiness")
    if not isinstance(readiness, dict):
        raise ValueError("Profile missing 'readiness' section")
    if "score" not in readiness or "breakdown" not in readiness:
        raise ValueError("Readiness section missing 'score' or 'breakdown'")

    # Columns
    columns = prof.get("columns")
    if not isinstance(columns, dict):
        raise ValueError("Profile missing 'columns' section")
    
    # Check at least one column entry if not empty
    if columns:
        first_col = next(iter(columns.values()))
        if not isinstance(first_col, dict):
            raise ValueError("Column entry must be a dictionary")
