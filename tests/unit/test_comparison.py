import pandas as pd
import pytest
from ira.profiling.comparison import create_profile_with_transformation

def fixed_clock():
    return "2026-02-08T16:00:00Z"

def test_comparison_output_structure():
    # Dataset with easy improvements: "1,200" string mapping to numeric
    data = {
        "_row_id": [1, 2],
        "val": ["100", "200"] 
    }
    df = pd.DataFrame(data)
    
    # Policy with explicit types so validity improves
    policy = {
        "dataset": {"name": "test"},
        "parsing": {
            "column_types": {"val": "numeric"}
        },
        "missing_data": {"enabled": False}
    }
    
    # Run profiling WITH comparison
    prof = create_profile_with_transformation(df, policy, clock=fixed_clock)
    
    readiness = prof.get("readiness", {})
    assert "transformation" in readiness
    
    trans = readiness["transformation"]
    assert "raw" in trans
    assert "optimized" in trans
    assert "delta" in trans
    
    # Scores should be the same here because raw dataset already looks like numeric strings
    # But let's verify keys
    assert "score" in trans["raw"]
    assert "breakdown" in trans["raw"]
    assert "score" in trans["optimized"]
    assert "score" in trans["delta"]

def test_comparison_readiness_improvement():
    # Dataset where raw is "bad" and optimized is "good"
    # Raw: many "null" strings (effective missing)
    # Optimized: imputation fills them
    data = {
        "_row_id": [1, 2, 3, 4],
        "val": ["10", "null", "null", "40"]
    }
    df = pd.DataFrame(data)
    
    policy = {
        "dataset": {"name": "test"},
        "parsing": {
            "column_types": {"val": "numeric"}
        },
        "missing_data": {
            "enabled": True,
            "imputation": {
                "numeric": {
                    "default": "mean",
                    "allow_if_missing_pct_leq": 1.0
                }
            }
        }
    }
    
    prof = create_profile_with_transformation(df, policy, clock=fixed_clock)
    
    raw_score = prof["readiness"]["transformation"]["raw"]["score"]
    opt_score = prof["readiness"]["transformation"]["optimized"]["score"]
    delta = prof["readiness"]["transformation"]["delta"]["score"]
    
    # Imputation should improve completeness
    assert opt_score > raw_score
    assert delta > 0
    assert delta == pytest.approx(opt_score - raw_score)

def test_comparison_delta_breakdown():
    data = {
        "_row_id": [1, 2],
        "val": ["10", "null"]
    }
    df = pd.DataFrame(data)
    policy = {
        "dataset": {"name": "test"},
        "parsing": {"column_types": {"val": "numeric"}},
        "missing_data": {
            "enabled": True,
            "imputation": {"numeric": {"default": "constant", "constants": {"val": 0}}}
        }
    }
    
    prof = create_profile_with_transformation(df, policy, clock=fixed_clock)
    delta_breakdown = prof["readiness"]["transformation"]["delta"]["breakdown"]
    
    # Completeness should have positive delta
    assert delta_breakdown["completeness"] > 0
