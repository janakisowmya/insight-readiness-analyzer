from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import pytest
from ira.correction.pipeline import run_correction_pipeline
from ira.reporting.audit import AuditLogger

def test_protected_columns_parsing_standardization(tmp_path: Path):
    """Verify protected columns are NOT modified even if explicitly configured in policy."""
    df = pd.DataFrame({
        "_row_id": [1],
        "secret_id": ["  messy_123  "]
    })
    
    # Policy that tries to both trim and parse secret_id
    policy = {
        "roles": {
            "critical_columns": ["_row_id"],
            "protected_columns": ["secret_id"]
        },
        "standardization": {
            "global_trim_whitespace": True
        },
        "parsing": {
            "column_types": {"secret_id": "numeric"},
            "numeric": {"on_failure": "null"}
        }
    }
    
    audit_path = tmp_path / "audit_protected_parsing.jsonl"
    logger = AuditLogger(audit_path)
    try:
        out_df = run_correction_pipeline(df, policy, audit=logger)
    finally:
        logger.close()
        
    # Must remain exactly as input
    assert out_df.loc[0, "secret_id"] == "  messy_123  "
    
    # Audit must show 0 touches
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    footer = json.loads(lines[-1])
    assert footer["total_events"] == 0
    assert footer["unique_columns_touched"] == 0

def test_protected_columns_imputation(tmp_path: Path):
    """Verify protected columns are excluded from imputation even if missing."""
    df = pd.DataFrame({
        "_row_id": [1, 2],
        "secret_note": [None, "existing"]
    })
    
    policy = {
        "roles": {
            "critical_columns": ["_row_id"],
            "protected_columns": ["secret_note"]
        },
        "missing_data": {
            "enabled": True,
            "imputation": {
                "categorical": {
                    "default": "constant",
                    "constants": {"secret_note": "FILL_ME"}
                }
            }
        }
    }
    
    audit_path = tmp_path / "audit_protected_impute.jsonl"
    logger = AuditLogger(audit_path)
    try:
        out_df = run_correction_pipeline(df, policy, audit=logger)
    finally:
        logger.close()
        
    # Must stay None
    assert pd.isna(out_df.loc[0, "secret_note"])
    
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    footer = json.loads(lines[-1])
    assert footer["total_events"] == 0
