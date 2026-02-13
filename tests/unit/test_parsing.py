from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from ira.correction.parsing import apply_parsing
from ira.reporting.audit import AuditLogger


def fixed_clock() -> str:
    return datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()


@pytest.fixture()
def base_df():
    return pd.DataFrame(
        {
            "_row_id": [1, 2, 3, 4],
            "price": ["$1,200.50", "bad", "", "€10"],
            "rate": ["20%", "0.5%", "bad%", None],
            "flag": ["Yes", "No", "maybe", ""],
            "dt": ["2026-01-01", "01/02/2026", "bad", ""],
            "protected_id": ["A", "B", "C", "D"],
        }
    )


@pytest.fixture()
def policy_null_failures():
    return {
        "roles": {"protected_columns": ["protected_id"]},
        "parsing": {
            "column_types": {
                "price": "numeric",
                "rate": "numeric",
                "flag": "boolean",
                "dt": "datetime",
                "protected_id": "numeric",  # should NOT be parsed due to protected
            },
            "numeric": {
                "allow_commas": True,
                "allow_currency_symbols": True,
                "allow_percent_symbol": True,
                "percent_scale": "0_1",
                "on_failure": "null",
            },
            "boolean": {
                "true_values": ["yes", "y", "1", "true"],
                "false_values": ["no", "n", "0", "false"],
                "on_failure": "null",
            },
            "datetime": {
                "allowed_formats": ["%Y-%m-%d"],
                "dayfirst": False,
                "yearfirst": False,
                "on_failure": "null",
            },
        },
    }


def test_apply_parsing_respects_protected(base_df, policy_null_failures):
    out = apply_parsing(base_df.copy(), policy_null_failures, audit=None, clock=fixed_clock)
    assert list(out["protected_id"]) == ["A", "B", "C", "D"]


def test_numeric_parsing_and_currency(base_df, policy_null_failures):
    out = apply_parsing(base_df.copy(), policy_null_failures, audit=None, clock=fixed_clock)
    assert out.loc[out["_row_id"] == 1, "price"].iloc[0] == pytest.approx(1200.50)
    assert pd.isna(out.loc[out["_row_id"] == 2, "price"].iloc[0])
    assert pd.isna(out.loc[out["_row_id"] == 3, "price"].iloc[0])
    assert out.loc[out["_row_id"] == 4, "price"].iloc[0] == pytest.approx(10.0)


def test_percent_scale_0_1(base_df, policy_null_failures):
    out = apply_parsing(base_df.copy(), policy_null_failures, audit=None, clock=fixed_clock)
    assert out.loc[out["_row_id"] == 1, "rate"].iloc[0] == pytest.approx(0.2)
    assert out.loc[out["_row_id"] == 2, "rate"].iloc[0] == pytest.approx(0.005)
    assert pd.isna(out.loc[out["_row_id"] == 3, "rate"].iloc[0])


def test_boolean_parsing(base_df, policy_null_failures):
    out = apply_parsing(base_df.copy(), policy_null_failures, audit=None, clock=fixed_clock)
    assert out.loc[out["_row_id"] == 1, "flag"].iloc[0] == True
    assert out.loc[out["_row_id"] == 2, "flag"].iloc[0] == False
    assert pd.isna(out.loc[out["_row_id"] == 3, "flag"].iloc[0])


def test_datetime_allowed_formats(base_df, policy_null_failures):
    out = apply_parsing(base_df.copy(), policy_null_failures, audit=None, clock=fixed_clock)
    assert str(out.loc[out["_row_id"] == 1, "dt"].iloc[0].date()) == "2026-01-01"
    assert pd.notna(out.loc[out["_row_id"] == 2, "dt"].iloc[0])  # fallback parse
    assert pd.isna(out.loc[out["_row_id"] == 3, "dt"].iloc[0])


def test_drop_row_behavior(base_df, policy_null_failures):
    pol = dict(policy_null_failures)
    pol["parsing"] = dict(policy_null_failures["parsing"])
    pol["parsing"]["numeric"] = dict(policy_null_failures["parsing"]["numeric"])
    pol["parsing"]["numeric"]["on_failure"] = "drop_row"

    out = apply_parsing(base_df.copy(), pol, audit=None, clock=fixed_clock)
    assert 2 not in set(out["_row_id"])  # row_id 2 had "bad" price -> dropped


def test_audit_logging_with_row_id(base_df, policy_null_failures, tmp_path):
    audit_path = tmp_path / "audit.jsonl"
    logger = AuditLogger(audit_path, detail="detailed")

    apply_parsing(base_df.copy(), policy_null_failures, audit=logger, clock=fixed_clock)
    logger.close()

    content = audit_path.read_text(encoding="utf-8").strip()
    assert content, "Audit log empty"

    logs = [json.loads(line) for line in content.splitlines()]
    price_logs = [l for l in logs if l.get("column") == "price" and l.get("reason") == "invalid_numeric"]

    assert len(price_logs) == 1
    assert price_logs[0]["row_id"] == 2  # must be _row_id, not dataframe index
    assert price_logs[0]["timestamp"] == fixed_clock()
