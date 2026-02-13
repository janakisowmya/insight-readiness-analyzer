from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import numpy as np
import pytest
from ira.correction.pipeline import run_correction_pipeline
from ira.reporting.audit import AuditLogger

def test_parsing_on_failure_null(tmp_path: Path):
    """Verify on_failure='null' produces NaN/None and logs failure."""
    df = pd.DataFrame({
        "_row_id": [1],
        "amount": ["abc"]
    })
    policy = {
        "roles": {"critical_columns": ["_row_id"]},
        "parsing": {
            "column_types": {"amount": "numeric"},
            "numeric": {"on_failure": "null"}
        }
    }
    
    audit_path = tmp_path / "audit_null.jsonl"
    logger = AuditLogger(audit_path, detail="detailed")
    try:
        out_df = run_correction_pipeline(df, policy, audit=logger)
    finally:
        logger.close()
        
    assert pd.isna(out_df.loc[0, "amount"])
    
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    # Check for parsing_failure event
    events = [json.loads(line) for line in lines[:-1]]
    failure = next(e for e in events if e["event_type"] == "parsing_failure")
    assert failure["column"] == "amount"
    assert failure["reason"] == "invalid_numeric"
    assert failure["policy_section"] == "parsing.numeric"

def test_parsing_on_failure_keep_raw(tmp_path: Path):
    """Verify on_failure='keep_raw' preserves raw string and logs failure."""
    df = pd.DataFrame({
        "_row_id": [1],
        "amount": ["abc"]
    })
    policy = {
        "roles": {"critical_columns": ["_row_id"]},
        "parsing": {
            "column_types": {"amount": "numeric"},
            "numeric": {"on_failure": "keep_raw"}
        }
    }
    
    audit_path = tmp_path / "audit_keep.jsonl"
    logger = AuditLogger(audit_path, detail="detailed")
    try:
        out_df = run_correction_pipeline(df, policy, audit=logger)
    finally:
        logger.close()
        
    assert out_df.loc[0, "amount"] == "abc"
    
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    events = [json.loads(line) for line in lines[:-1]]
    assert any(e["event_type"] == "parsing_failure" for e in events)

def test_parsing_on_failure_drop_row(tmp_path: Path):
    """Verify on_failure='drop_row' removes row and logs both failure and drop."""
    df = pd.DataFrame({
        "_row_id": [1, 2],
        "amount": ["abc", "123"]
    })
    policy = {
        "roles": {"critical_columns": ["_row_id"]},
        "parsing": {
            "column_types": {"amount": "numeric"},
            "numeric": {"on_failure": "drop_row"}
        }
    }
    
    audit_path = tmp_path / "audit_drop.jsonl"
    logger = AuditLogger(audit_path, detail="detailed")
    try:
        out_df = run_correction_pipeline(df, policy, audit=logger)
    finally:
        logger.close()
        
    assert len(out_df) == 1
    assert out_df.loc[1, "amount"] == 123.0 # index 1 is original row 2
    
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    events = [json.loads(line) for line in lines[:-1]]
    
    # Must have parsing_failure
    assert any(e["event_type"] == "parsing_failure" and e["row_id"] == 1 for e in events)
    # Must have row_dropped
    drop_event = next(e for e in events if e["event_type"] == "row_dropped")
    assert drop_event["row_id"] == 1
    assert drop_event["reason"] == "parsing_on_failure_drop_row"
    assert drop_event["policy_section"] == "parsing.on_failure.drop_row"
