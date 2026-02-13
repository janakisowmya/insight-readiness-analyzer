from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import pytest
from ira.correction.pipeline import run_correction_pipeline
from ira.reporting.audit import AuditLogger

def test_audit_footer_determinism(tmp_path: Path):
    """Verify audit footer timestamps match fixed clock accurately."""
    df = pd.DataFrame({"_row_id": [1], "val": [" x "]})
    fixed_time = "2023-01-01T12:00:00Z"
    clock = lambda: fixed_time
    
    policy = {
        "roles": {"critical_columns": ["_row_id"]},
        "standardization": {"global_trim_whitespace": True}
    }
    
    audit_path = tmp_path / "audit_footer.jsonl"
    logger = AuditLogger(audit_path)
    try:
        run_correction_pipeline(df, policy, audit=logger, clock=clock)
    finally:
        logger.close()
        
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    footer = json.loads(lines[-1])
    assert footer["timestamp_start"] == fixed_time
    assert footer["timestamp_end"] == fixed_time
    assert footer["total_events"] > 0

def test_summary_aggregate_ordering(tmp_path: Path):
    """Verify summary_aggregate records are written in sorted order by key."""
    # Create changes in non-sorted order if possible
    df = pd.DataFrame({
        "_row_id": [1, 2],
        "z_col": [" a ", " b "],
        "a_col": [" a ", " b "]
    })
    
    policy = {
        "roles": {"critical_columns": ["_row_id"]},
        "standardization": {"global_trim_whitespace": True}
    }
    
    audit_path = tmp_path / "audit_order.jsonl"
    # Summary mode is default
    logger = AuditLogger(audit_path, detail="summary")
    try:
        run_correction_pipeline(df, policy, audit=logger)
    finally:
        logger.close()
        
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    # Type should be 'summary_aggregate'
    aggregates = [json.loads(line) for line in lines[:-1]]
    
    # Get (column, reason) pairs
    keys = [(a["column"], a["reason"]) for a in aggregates]
    assert keys == sorted(keys), f"Aggregates not sorted: {keys}"

def test_output_df_equality(tmp_path: Path):
    """Verify DataFrame output is identical across identical runs with same clock."""
    df = pd.DataFrame({
        "_row_id": [1, 2, 3],
        "amount": ["$10.00", "20.50", None],
        "val": [" x ", " y ", " z "]
    })
    
    policy = {
        "roles": {"critical_columns": ["_row_id"]},
        "standardization": {"global_trim_whitespace": True},
        "parsing": {"column_types": {"amount": "numeric"}},
        "missing_data": {
            "enabled": True,
            "imputation": {"numeric": {"default": "mean"}}
        }
    }
    
    fixed_time = "2023-01-01T12:00:00Z"
    clock = lambda: fixed_time
    
    # Run 1
    df1 = run_correction_pipeline(df.copy(), policy, clock=clock)
    
    # Run 2
    df2 = run_correction_pipeline(df.copy(), policy, clock=clock)
    
    pd.testing.assert_frame_equal(df1, df2)
