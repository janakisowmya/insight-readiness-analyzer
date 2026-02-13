from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ira.correction.missing import drop_critical_missing, apply_imputation
from ira.reporting.audit import AuditLogger


def fixed_clock() -> str:
    return datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()


@pytest.fixture()
def df():
    return pd.DataFrame(
        {
            "_row_id": [1, 2, 3, 4],
            "x_num": [1.0, np.nan, 3.0, np.nan],
            "y_cat": ["a", None, "a", "b"],
            "z_dt": [pd.Timestamp("2026-01-01"), pd.NaT, pd.Timestamp("2026-01-03"), pd.NaT],
        }
    )


@pytest.fixture()
def policy():
    return {
        "roles": {
            "critical_columns": ["x_num"],
        },
        "parsing": {
            "column_types": {
                "x_num": "numeric",
                "y_cat": "categorical",
                "z_dt": "datetime",
            }
        },
        "missing_data": {
            "enabled": True,
            "drop_if_missing_critical": True,
            "imputation": {
                "numeric": {
                    "default": "mean",
                    "constants": {},
                    "allow_if_missing_pct_leq": 0.60,
                },
                "categorical": {
                    "default": "mode",
                    "constants": {},
                    "allow_if_missing_pct_leq": 0.60,
                },
                "datetime": {
                    "default": "constant",
                    "constants": {"z_dt": "2026-02-01"},
                    "allow_if_missing_pct_leq": 0.60,
                },
            },
        },
    }


def test_drop_critical_missing_drops_rows(df, policy):
    out = drop_critical_missing(df.copy(), policy, audit=None, clock=fixed_clock)
    # x_num missing in row_id 2 and 4 -> dropped
    assert set(out["_row_id"]) == {1, 3}


def test_impute_mean_numeric(df, policy):
    # first: do not drop rows (turn off)
    pol = dict(policy)
    pol["missing_data"] = dict(policy["missing_data"])
    pol["missing_data"]["drop_if_missing_critical"] = False

    out = apply_imputation(df.copy(), pol, audit=None, clock=fixed_clock)

    # mean of present x_num = (1 + 3)/2 = 2
    assert out.loc[out["_row_id"] == 2, "x_num"].iloc[0] == pytest.approx(2.0)
    assert out.loc[out["_row_id"] == 4, "x_num"].iloc[0] == pytest.approx(2.0)


def test_impute_mode_categorical(df, policy):
    pol = dict(policy)
    pol["missing_data"] = dict(policy["missing_data"])
    pol["missing_data"]["drop_if_missing_critical"] = False

    out = apply_imputation(df.copy(), pol, audit=None, clock=fixed_clock)
    # y_cat mode among non-missing is "a" (two a's vs one b)
    assert out.loc[out["_row_id"] == 2, "y_cat"].iloc[0] == "a"


def test_impute_constant_datetime(df, policy):
    pol = dict(policy)
    pol["missing_data"] = dict(policy["missing_data"])
    pol["missing_data"]["drop_if_missing_critical"] = False

    out = apply_imputation(df.copy(), pol, audit=None, clock=fixed_clock)
    assert str(out.loc[out["_row_id"] == 2, "z_dt"].iloc[0].date()) == "2026-02-01"
    assert str(out.loc[out["_row_id"] == 4, "z_dt"].iloc[0].date()) == "2026-02-01"


def test_threshold_blocks_imputation(df, policy):
    # make x_num too sparse threshold (missing 50% here). set threshold lower than 0.5
    pol = dict(policy)
    pol["missing_data"] = dict(policy["missing_data"])
    pol["missing_data"]["drop_if_missing_critical"] = False
    pol["missing_data"]["imputation"] = dict(policy["missing_data"]["imputation"])
    pol["missing_data"]["imputation"]["numeric"] = dict(policy["missing_data"]["imputation"]["numeric"])
    pol["missing_data"]["imputation"]["numeric"]["allow_if_missing_pct_leq"] = 0.40

    out = apply_imputation(df.copy(), pol, audit=None, clock=fixed_clock)

    # should remain NaN because imputation blocked
    assert pd.isna(out.loc[out["_row_id"] == 2, "x_num"].iloc[0])
    assert pd.isna(out.loc[out["_row_id"] == 4, "x_num"].iloc[0])


def test_audit_logs_every_fill(df, policy, tmp_path: Path):
    pol = dict(policy)
    pol["missing_data"] = dict(policy["missing_data"])
    pol["missing_data"]["drop_if_missing_critical"] = False

    audit_path = tmp_path / "audit.jsonl"
    logger = AuditLogger(audit_path, detail="detailed")

    out = apply_imputation(df.copy(), pol, audit=logger, clock=fixed_clock)
    logger.close()

    content = audit_path.read_text(encoding="utf-8").strip()
    assert content

    logs = [json.loads(line) for line in content.splitlines()]

    # We expect fills for: x_num row 2, x_num row 4, y_cat row 2, z_dt row 2, z_dt row 4 => 5 events
    fill_events = [l for l in logs if l.get("event_type") == "imputation_fill"]
    assert len(fill_events) == 5

    # Ensure row_id uses _row_id values, not index
    assert set(l["row_id"] for l in fill_events) == {2, 4}


def test_imputation_immutability(df, policy):
    # Ensure apply_imputation does not mutate input df
    df_orig = df.copy()
    pol = dict(policy)
    pol["missing_data"]["drop_if_missing_critical"] = False
    
    out = apply_imputation(df, pol, audit=None)
    
    assert not out.equals(df_orig) # should be changed
    pd.testing.assert_frame_equal(df, df_orig) # original should be untouched


def test_impute_mode_ignores_effective_missing(policy):
    # Create df where "null" is most frequent but should be ignored
    data = {
        "_row_id": [1, 2, 3, 4, 5],
        "cat_col": ["a", "null", "null", "b", "null"] # "null" is mode of strings
    }
    df = pd.DataFrame(data)
    
    pol = dict(policy)
    pol["parsing"]["column_types"]["cat_col"] = "categorical"
    pol["missing_data"]["imputation"]["categorical"]["default"] = "mode"
    
    # Impute missing "null" values
    out = apply_imputation(df, pol, audit=None)
    
    # Non-"null" values are "a" and "b". Tie-break chooses "a" usually.
    # The point is it should NOT be "null".
    val = out.loc[out["_row_id"] == 2, "cat_col"].iloc[0]
    assert val in ("a", "b")
    assert val != "null"
