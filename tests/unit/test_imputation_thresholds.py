from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import pytest
from ira.correction.pipeline import run_correction_pipeline
from ira.reporting.audit import AuditLogger

def test_imputation_threshold_skip_logging(tmp_path: Path):
    """Verify imputation_skipped_threshold event is logged when threshold exceeded."""
    df = pd.DataFrame({
        "_row_id": [1, 2, 3, 4],
        "val": [10.0, None, None, 40.0]  # 50% missing
    })
    
    # Policy with 25% threshold (will be exceeded by 50% missing)
    policy = {
        "roles": {"critical_columns": ["_row_id"]},
        "parsing": {"column_types": {"val": "numeric"}},
        "missing_data": {
            "enabled": True,
            "imputation": {
                "numeric": {
                    "default": "mean",
                    "allow_if_missing_pct_leq": 0.25
                }
            }
        }
    }
    
    audit_path = tmp_path / "audit_threshold.jsonl"
    logger = AuditLogger(audit_path, detail="detailed")
    try:
        out_df = run_correction_pipeline(df, policy, audit=logger)
    finally:
        logger.close()
        
    # Data should remain missing
    assert out_df["val"].isna().sum() == 2
    
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    events = [json.loads(line) for line in lines[:-1]]
    
    skip_event = next(e for e in events if e["event_type"] == "imputation_skipped_threshold")
    assert skip_event["column"] == "val"
    assert "gt_0.25" in skip_event["reason"]
    assert skip_event["policy_section"] == "missing_data.imputation.numeric"
