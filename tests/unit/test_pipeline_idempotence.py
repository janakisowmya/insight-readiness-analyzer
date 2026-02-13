from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from ira.correction.pipeline import run_correction_pipeline
from ira.reporting.audit import AuditLogger

def test_pipeline_idempotence(tmp_path: Path):
    df = pd.DataFrame({
        "_row_id": [1, 2],
        "val": [" abc ", "def"]
    })
    policy = {
        "standardization": {"global_trim_whitespace": True},
        "missing_data": {"enabled": False},
        "roles": {"critical_columns": ["_row_id"]}
    }
    
    # Run 1
    df1 = run_correction_pipeline(df, policy)
    
    # Run 2 on top of df1
    audit_path = tmp_path / "audit_idempotence.jsonl"
    logger = AuditLogger(audit_path)
    try:
        df2 = run_correction_pipeline(df1, policy, audit=logger)
    finally:
        logger.close()
    
    pd.testing.assert_frame_equal(df1, df2)
    
    # Load footer and check total_events
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    footer = json.loads(lines[-1])
    assert footer["total_events"] == 0

def test_pipeline_idempotence_with_types(tmp_path: Path):
    # Test that once types are coerced, they remain stable
    df = pd.DataFrame({
        "_row_id": [1, 2],
        "amount": ["$1,200.50", "300"]
    })
    policy = {
        "parsing": {
            "column_types": {"amount": "numeric"},
            "numeric": {"allow_currency_symbols": True}
        },
        "missing_data": {"enabled": False},
        "roles": {"critical_columns": ["_row_id"]}
    }
    
    df1 = run_correction_pipeline(df, policy)
    
    audit_path = tmp_path / "audit_types_idempotence.jsonl"
    logger = AuditLogger(audit_path)
    try:
        df2 = run_correction_pipeline(df1, policy, audit=logger)
    finally:
        logger.close()
    
    pd.testing.assert_frame_equal(df1, df2)
    
    footer = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[-1])
    assert footer["total_events"] == 0
