from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

def validate_audit_record(record: Dict[str, Any]) -> None:
    """
    Validates a single audit JSONL record.
    Supports: detailed events, summary_aggregate, and job_summary.
    """
    if not isinstance(record, dict):
        raise ValueError("Audit record must be a dictionary")

    # Determine record type
    rec_type = record.get("type", "detailed")

    if rec_type == "detailed":
        _validate_detailed(record)
    elif rec_type == "summary_aggregate":
        _validate_summary(record)
    elif rec_type == "job_summary":
        _validate_footer(record)
    else:
        raise ValueError(f"Unknown audit record type: {rec_type}")

def _validate_detailed(r: Dict[str, Any]) -> None:
    required = {"timestamp", "event_type", "reason", "policy_section"}
    missing = required - r.keys()
    if missing:
        raise ValueError(f"Detailed audit record missing keys: {missing}")

def _validate_summary(r: Dict[str, Any]) -> None:
    required = {"type", "event_type", "column", "reason", "count", "samples"}
    missing = required - r.keys()
    if missing:
        raise ValueError(f"Summary aggregate record missing keys: {missing}")
    if not isinstance(r.get("samples"), list):
        raise ValueError("Summary 'samples' must be a list")

def _validate_footer(r: Dict[str, Any]) -> None:
    required = {"type", "detail", "total_events", "event_type_counts", "unique_columns_touched", "job_context"}
    missing = required - r.keys()
    if missing:
        raise ValueError(f"Job summary footer missing keys: {missing}")
    if not isinstance(r.get("event_type_counts"), dict):
        raise ValueError("Job summary 'event_type_counts' must be a dict")
    if not isinstance(r.get("job_context"), dict):
        raise ValueError("Job summary 'job_context' must be a dict")

def validate_audit_log(path: str | Path) -> None:
    """
    Validates an entire audit JSONL file.
    Ensures footer is the last line and all records are valid.
    """
    import json
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError("Audit log is empty")
    
    num_lines = len(lines)
    for i in range(num_lines - 1):
        rec = json.loads(lines[i])
        validate_audit_record(rec)
        if rec.get("type") == "job_summary":
            raise ValueError(f"job_summary found at line {i+1}, before the last line")
            
    last_rec = json.loads(lines[-1])
    validate_audit_record(last_rec)
    if last_rec.get("type") != "job_summary":
        raise ValueError("Last line must be job_summary")
