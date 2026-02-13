import pandas as pd
import pytest
import numpy as np
from ira.correction.pipeline import run_correction_pipeline

def test_pipeline_ordering_standardization_before_parsing():
    # Scenario: "High" needs to be mapped to "3" (standardization)
    # then parsed as numeric (parsing)
    data = {
        "_row_id": [1, 2],
        "score": ["High", "1"]
    }
    df = pd.DataFrame(data)
    
    policy = {
        "dataset": {"name": "test"},
        "roles": {
            "standardize_columns": ["score"]
        },
        "standardization": {
            "mappings": {
                "score": {"High": "3"}
            }
        },
        "parsing": {
            "column_types": {
                "score": "numeric"
            },
            "numeric": {
                "on_failure": "null"
            }
        },
        "missing_data": {"enabled": False}
    }
    
    # Run pipeline
    out = run_correction_pipeline(df, policy)
    
    # If standardization ran first: "High" -> "3" -> 3.0
    # If parsing ran first: "High" -> NaN
    assert out.loc[out["_row_id"] == 1, "score"].iloc[0] == 3.0
    assert out.loc[out["_row_id"] == 2, "score"].iloc[0] == 1.0


def test_pipeline_ordering_parsing_before_imputation():
    # Scenario: Missing value should be imputed with the mean of parsed values
    data = {
        "_row_id": [1, 2, 3],
        "val": ["10", "20", "null"]
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
    
    out = run_correction_pipeline(df, policy)
    
    # mean of 10 and 20 is 15
    assert out.loc[out["_row_id"] == 3, "val"].iloc[0] == 15.0


def test_pipeline_immutability(df_example):
    # Ensure input is not mutated
    policy = {"missing_data": {"enabled": False}, "parsing": {}, "roles": {}}
    df_orig = df_example.copy()
    
    _ = run_correction_pipeline(df_example, policy)
    
    pd.testing.assert_frame_equal(df_example, df_orig)

@pytest.fixture
def df_example():
    return pd.DataFrame({
        "_row_id": [1],
        "a": [1]
    })
