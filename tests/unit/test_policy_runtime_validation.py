from __future__ import annotations
import pandas as pd
from ira.correction.policy_runtime_validation import validate_policy_against_df

def test_runtime_validation_success():
    df = pd.DataFrame({"id": [1], "val": [10]})
    policy = {
        "roles": {"critical_columns": ["id"]},
        "missing_data": {"imputation": {"numeric": {"allow_if_missing_pct_leq": 0.5}}}
    }
    errors, warnings = validate_policy_against_df(policy, df)
    assert not errors
    assert not warnings

def test_runtime_validation_missing_critical():
    df = pd.DataFrame({"val": [10]})
    policy = {"roles": {"critical_columns": ["id"]}}
    errors, warnings = validate_policy_against_df(policy, df)
    assert "Critical columns missing" in errors[0]

def test_runtime_validation_bad_threshold():
    df = pd.DataFrame({"val": [10]})
    policy = {
        "missing_data": {"imputation": {"numeric": {"allow_if_missing_pct_leq": 1.5}}}
    }
    errors, warnings = validate_policy_against_df(policy, df)
    assert "must be between 0 and 1" in errors[0]

def test_runtime_validation_warnings():
    df = pd.DataFrame({"id": [1]})
    policy = {
        "roles": {"critical_columns": ["id"]},
        "missing_data": {"drop_if_missing_critical": False},
        "dataset": {"primary_key": {"columns": ["missing_pk"]}}
    }
    errors, warnings = validate_policy_against_df(policy, df)
    assert not errors
    assert len(warnings) == 2
