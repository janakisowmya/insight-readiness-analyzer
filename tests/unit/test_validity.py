import pytest
import pandas as pd
import numpy as np
from ira.correction.validity import apply_validity
from ira.reporting.audit import AuditLogger

def test_validity_non_negative():
    df = pd.DataFrame({"val": [1, -5, 10]})
    policy = {
        "validity_rules": {
            "non_negative_columns": ["val"],
            "on_violation": "null"
        }
    }
    res = apply_validity(df, policy)
    assert pd.notna(res.loc[0, "val"])
    assert pd.isna(res.loc[1, "val"])
    assert pd.notna(res.loc[2, "val"])

def test_validity_range_min_max():
    df = pd.DataFrame({"val": [10, 50, 200]})
    policy = {
        "validity_rules": {
            "ranges": {"val": {"min": 20, "max": 100}},
            "on_violation": "null"
        }
    }
    res = apply_validity(df, policy)
    # 10 < 20 -> null
    # 200 > 100 -> null
    assert pd.isna(res.loc[0, "val"])
    assert res.loc[1, "val"] == 50
    assert pd.isna(res.loc[2, "val"])

def test_validity_range_min_only():
    df = pd.DataFrame({"val": [10, 50]})
    policy = {
        "validity_rules": {
            "ranges": {"val": {"min": 20}},
            "on_violation": "null"
        }
    }
    res = apply_validity(df, policy)
    assert pd.isna(res.loc[0, "val"])
    assert res.loc[1, "val"] == 50

def test_validity_regex_drop():
    df = pd.DataFrame({"email": ["a@b.com", "bad-email", "c@d.org"]})
    policy = {
        "validity_rules": {
            "regex": {"email": r"^[^@]+@[^@]+\.[^@]+$"},
            "on_violation": "drop_row"
        }
    }
    res = apply_validity(df, policy)
    assert len(res) == 2
    assert "bad-email" not in res["email"].values
    assert "a@b.com" in res["email"].values

def test_validity_allowed_values():
    df = pd.DataFrame({"status": ["active", "inactive", "unknown", np.nan]})
    policy = {
        "validity_rules": {
            "allowed_values": {"status": ["active", "inactive"]},
            "on_violation": "null"
        }
    }
    res = apply_validity(df, policy)
    assert res.loc[0, "status"] == "active"
    assert res.loc[1, "status"] == "inactive"
    assert pd.isna(res.loc[2, "status"])
    assert pd.isna(res.loc[3, "status"])

def test_validity_audit_log(tmp_path):
    audit_file = tmp_path / "validity_audit.jsonl"
    logger = AuditLogger(audit_file, detail="detailed")
    
    df = pd.DataFrame({"val": [-10, 10]})
    policy = {
        "validity_rules": {
            "non_negative_columns": ["val"],
            "on_violation": "null"
        }
    }
    
    apply_validity(df, policy, audit=logger)
    logger.close()
    
    with open(audit_file) as f:
        lines = f.readlines()
        
    # Check for validity event
    # We expect one event for the column violation aggregate or detail?
    # Our implementation logs one aggregate event per column/violation type in detailed mode?
    # Actually wait, apply_validity calls audit.log() once per column violation group.
    
    # Detailed mode writes line by line too?
    # audit.log() calls _aggregate AND _write_line if detail="detailed".
    
    found = False
    for line in lines:
        if "validity_null" in line and "non_negative" in line:
            found = True
            break
            
    assert found
