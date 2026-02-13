from __future__ import annotations

import json
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import pytest

from ira.correction.standardize import apply_standardization
from ira.reporting.audit import AuditLogger


def fixed_clock() -> str:
    return datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()


@pytest.fixture
def base_df():
    return pd.DataFrame({
        "_row_id": pd.Series([10, 20, 30], dtype="int64"),
        "name": ["  Alice  ", "  bob  ", "Charlie"],
        "city": ["New York", "london", "Paris\x00"],
        "status": ["active", "inactive", "pending"],
        "protected": ["  keep  ", "ME", "safe"],
    })


def test_protected_columns_untouched(base_df):
    """Protected columns should never be modified."""
    pol = {
        "roles": {"protected_columns": ["protected"]},
        "standardization": {
            "global_trim_whitespace": True,
            "global_collapse_whitespace": True,
            "strip_nonprinting": True,
            "casefold": "lower",
        }
    }
    out = apply_standardization(base_df.copy(), pol, audit=None, clock=fixed_clock)
    
    # Protected column unchanged
    assert out.at[0, "protected"] == "  keep  "
    assert out.at[1, "protected"] == "ME"
    assert out.at[2, "protected"] == "safe"
    
    # Other columns modified
    assert out.at[0, "name"] == "alice"


def test_strip_nonprinting_before_whitespace():
    """Verify strip_nonprinting runs before whitespace trim/collapse."""
    # Scenario: " \x00 "
    # If strip runs first: "   " -> trim resulting in ""
    # This verifies that control characters are removed before we clean up spaces.
    df = pd.DataFrame({
        "_row_id": [1],
        "text": [" \x00 "]
    })
    pol = {
        "standardization": {
            "strip_nonprinting": True,
            "global_trim_whitespace": True,
        }
    }
    out = apply_standardization(df.copy(), pol, audit=None, clock=fixed_clock)
    assert out.at[0, "text"] == ""

    # Scenario: "A\x00 B"
    # strip -> "A B" -> collapse -> "A B"
    df2 = pd.DataFrame({
        "_row_id": [1],
        "text": ["A\x1F B"] # \x1F is a control char
    })
    pol2 = {
        "standardization": {
            "strip_nonprinting": True,
            "global_collapse_whitespace": True,
        }
    }
    out2 = apply_standardization(df2.copy(), pol2, audit=None, clock=fixed_clock)
    assert out2.at[0, "text"] == "A B"


def test_nan_preservation():
    """Verify that NaNs are preserved and not converted to strings."""
    df = pd.DataFrame({
        "_row_id": [1, 2],
        "val": ["  hello  ", None]
    })
    pol = {
        "standardization": {
            "global_trim_whitespace": True,
            "casefold": "upper"
        }
    }
    out = apply_standardization(df.copy(), pol, audit=None, clock=fixed_clock)
    
    assert out.at[0, "val"] == "HELLO"
    assert pd.isna(out.at[1, "val"])
    # Crucially, it shouldn't be the string "None" or "nan"
    assert out.at[1, "val"] is None or np.isnan(out.at[1, "val"])


def test_mappings_work():
    """Verify per-column value mappings."""
    df = pd.DataFrame({
        "_row_id": [1, 2, 3],
        "status": ["active", "inactive", "pending"]
    })
    pol = {
        "standardization": {
            "mappings": {
                "status": {"inactive": "Archived", "pending": "Waiting"}
            }
        }
    }
    out = apply_standardization(df.copy(), pol, audit=None, clock=fixed_clock)
    
    assert out.at[0, "status"] == "active"
    assert out.at[1, "status"] == "Archived"
    assert out.at[2, "status"] == "Waiting"


def test_casefold_variants(base_df):
    """Test all casefold options."""
    for cf in ["lower", "upper", "title"]:
        pol = {"standardization": {"casefold": cf}}
        out = apply_standardization(base_df.copy(), pol, audit=None, clock=fixed_clock)
        if cf == "lower":
            assert out.at[1, "city"] == "london"
        elif cf == "upper":
            assert out.at[1, "city"] == "LONDON"
        elif cf == "title":
            assert out.at[1, "city"] == "London"


def test_audit_logs_correct_row_id(base_df, tmp_path):
    """Test that audit logs use row_id correctly (handling numpy scalars)."""
    audit_path = tmp_path / "audit.jsonl"
    logger = AuditLogger(audit_path, detail="detailed")
    
    pol = {
        "standardization": {
            "global_trim_whitespace": True
        }
    }
    
    # base_df has int64 row_ids
    apply_standardization(base_df.copy(), pol, audit=logger, clock=fixed_clock)
    logger.close()
    
    content = audit_path.read_text(encoding="utf-8").strip()
    logs = [json.loads(line) for line in content.splitlines()]
    
    # Check that row_id 10 (numpy int64) was logged as a plain int. Filter out non-event records (like job_summary).
    alice_log = [l for l in logs if "column" in l and l["column"] == "name" and l["row_id"] == 10]
    assert len(alice_log) == 1
    assert alice_log[0]["row_id"] == 10
    assert isinstance(alice_log[0]["row_id"], int)


def test_collapse_works():
    """Test whitespace collapse functionality."""
    df = pd.DataFrame({
        "_row_id": [1],
        "text": ["multiple    spaces\t\ttabs"]
    })
    pol = {
        "standardization": {
            "global_collapse_whitespace": True,
        }
    }
    out = apply_standardization(df.copy(), pol, audit=None, clock=fixed_clock)
    assert out.at[0, "text"] == "multiple spaces tabs"
