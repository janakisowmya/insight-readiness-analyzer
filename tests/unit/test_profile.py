from __future__ import annotations

import pandas as pd
import numpy as np
import pytest
from ira.profiling.profile import create_profile


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "_row_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "", "NA", "Eve"],
        "age": [25, 30, np.nan, 40, "mixed"],
        "score": [10.5, 20.0, 10.5, 20.0, 10.5], # 2 dupes: (10.5, 20.0) matches (10.5, 20.0) -> wait
        "status": ["active", "active", "active", "active", "active"],
    })


@pytest.fixture
def base_policy():
    return {
        "dataset": {"name": "test_dataset"},
        "reproducibility": {"policy_hash": "hash123"},
        "roles": {
            "critical_columns": ["name"],
            "protected_columns": ["_row_id"],
        },
        "parsing": {
            "column_types": {
                "age": "integer",
                "score": "float"
            }
        }
    }


def test_missingness_metrics(sample_df, base_policy):
    """Verify null_missing_pct vs effective_missing_pct."""
    # Add a more diverse set of missing values
    df = pd.DataFrame({
        "_row_id": range(6),
        "name": ["Alice", "Bob", "", "NA", "null", "n/a"]
    })
    profile = create_profile(df, base_policy)
    
    # name: 6 rows, all 6 are effective missing tokens (Alice/Bob are not missing)
    # wait: ["Alice", "Bob", "", "NA", "null", "n/a"]
    # Non-missing: Alice, Bob (2)
    # Missing: "", "NA", "null", "n/a" (4)
    # null_missing_pct = 0.0
    # effective_missing_pct = 4/6 = 0.6667
    
    name_stats = profile["columns"]["name"]
    assert name_stats["null_missing_pct"] == 0.0
    assert name_stats["effective_missing_pct"] == 0.6667
    
    # Verify readiness score exists
    assert "readiness" in profile


def test_numeric_percent_alignment():
    """Verify numeric parseability aligns with endswith('%') logic."""
    df = pd.DataFrame({
        "_row_id": [1, 2, 3],
        "pct": ["100%", "50 %", "contains % 50"] # only 100% and 50 % should be matched if endswith is used
    })
    policy = {
        "dataset": {"name": "test"},
        "parsing": {"column_types": {"pct": "numeric"}}
    }
    profile = create_profile(df, policy)
    
    # "100%" -> ends with %
    # "50 %" -> ends with % (after strip)
    # "contains % 50" -> does NOT end with % -> invalid
    assert profile["columns"]["pct"]["invalid_count"] == 1


def test_exact_duplicates_exclude_row_id():
    """Verify exact row duplicates exclude _row_id."""
    df = pd.DataFrame({
        "_row_id": [1, 2, 3],
        "val": ["A", "A", "B"]
    })
    policy = {"dataset": {"name": "test"}}
    profile = create_profile(df, policy)
    
    # Rows 1 and 2 are duplicates if we ignore _row_id
    assert profile["duplicates"]["exact_row_dupe_count"] == 1


def test_pk_duplicates(base_policy):
    """Verify PK duplicates if defined in policy."""
    df = pd.DataFrame({
        "_row_id": [1, 2, 3],
        "id": ["ID1", "ID1", "ID2"],
        "val": ["A", "B", "C"]
    })
    pol = base_policy.copy()
    pol["dataset"]["primary_key"] = {"columns": ["id"]}
    
    profile = create_profile(df, pol)
    assert profile["duplicates"]["pk_dupe_count"] == 1


def test_invalid_count_for_typed_columns(sample_df, base_policy):
    """Verify invalid_count and is_parseable only for typed columns."""
    profile = create_profile(sample_df, base_policy)
    
    # age is typed as 'integer', has "mixed" which is not an integer
    # [25, 30, nan, 40, "mixed"] -> "mixed" fails
    age_stats = profile["columns"]["age"]
    assert age_stats["invalid_count"] == 1
    assert age_stats["is_parseable"] is False
    assert age_stats["invalid_type_pct"] == 0.2
    
    # name is untyped
    name_stats = profile["columns"]["name"]
    assert name_stats["is_parseable"] is None
    assert name_stats["invalid_count"] is None


def test_mixed_type_detection():
    """Verify mixed-type flag behavior (threshold < 0.95)."""
    # Majority type bucket covers 2/3 = 0.66 < 0.95
    df = pd.DataFrame({
        "_row_id": [1, 2, 3, 4, 5],
        "mixed_col": [1, "str", 2, "NA", None] # non-missing: [1, "str", 2]
    })
    policy = {"dataset": {"name": "test"}}
    profile = create_profile(df, policy)
    
    assert profile["columns"]["mixed_col"]["is_mixed_type"] is True
    
    # Mostly one type
    df2 = pd.DataFrame({
        "_row_id": range(100),
        "mostly_int": [i for i in range(96)] + ["str"] * 4
    })
    profile2 = create_profile(df2, policy)
    assert profile2["columns"]["mostly_int"]["is_mixed_type"] is False # 96/100 >= 0.95


def test_metadata_reproducibility(base_policy):
    """Verify metadata includes policy_hash and deterministic timestamp."""
    df = pd.DataFrame({"_row_id": [1], "v": [1]})
    fixed_time = "2025-01-01T12:00:00+00:00"
    profile = create_profile(df, base_policy, clock=lambda: fixed_time)
    
    assert profile["metadata"]["policy_hash"] == "hash123"
    assert profile["metadata"]["dataset_name"] == "test_dataset"
    assert profile["metadata"]["timestamp"] == fixed_time
    assert profile["metadata"]["row_count"] == 1
    assert profile["metadata"]["col_count"] == 2


def test_mixed_type_sampling_excludes_missing():
    """Verify mixed-type sampling excludes effective-missing tokens."""
    df = pd.DataFrame({
        "_row_id": [1, 2, 3, 4],
        "col": [1, None, "nan", "NA"] # Only one non-missing: [1]
    })
    policy = {"dataset": {"name": "test"}}
    profile = create_profile(df, policy)
    
    # Only one non-missing value, so it's not mixed
    assert profile["columns"]["col"]["is_mixed_type"] is False
