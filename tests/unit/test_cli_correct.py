from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ira.cli import main


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.write_text(df.to_csv(index=False), encoding="utf-8")


def _write_yaml(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_cli_correct_happy_path_generates_csv_and_audit(tmp_path: Path) -> None:
    # Input dataset
    df = pd.DataFrame(
        {
            "_row_id": [1, 2, 3, 4],
            "x_num": ["1", " ", "3", "null"],  # needs standardize + parse + impute
            "y_cat": ["a", "nan", "a", "b"],   # needs impute mode for row 2
        }
    )
    in_csv = tmp_path / "in.csv"
    out_csv = tmp_path / "out.csv"
    audit_jsonl = tmp_path / "audit.jsonl"
    policy_yaml = tmp_path / "policy.yaml"

    _write_csv(in_csv, df)

    # Policy: parse x_num as numeric, critical column so drop is configurable, and impute enabled
    _write_yaml(
        policy_yaml,
        """
dataset:
  name: test
roles:
  critical_columns: ["x_num"]
  protected_columns: []
standardization:
  strip_nonprinting: true
  global_trim_whitespace: true
  global_collapse_whitespace: true
  casefold: none
  mappings: {}
parsing:
  column_types:
    x_num: numeric
missing_data:
  enabled: true
  drop_if_missing_critical: false
  imputation:
    numeric:
      default: mean
      allow_if_missing_pct_leq: 1.0
      constants: {}
    categorical:
      default: mode
      allow_if_missing_pct_leq: 1.0
      constants: {}
    datetime:
      default: none
      allow_if_missing_pct_leq: 1.0
      constants: {}
""".lstrip(),
    )

    rc = main(
        [
            "correct",
            "--input",
            str(in_csv),
            "--policy",
            str(policy_yaml),
            "--out",
            str(out_csv),
            "--audit",
            str(audit_jsonl),
            "--audit-detail",
            "detailed",
            "--clock",
            "2026-02-08T16:00:00+00:00",
            "--quiet",
        ]
    )
    assert rc == 0
    assert out_csv.exists()
    assert audit_jsonl.exists()

    out = pd.read_csv(out_csv)

    # x_num should be numeric after parsing; missing rows should be imputed with mean of present numeric values (1 and 3 => 2)
    # rows 2 and 4 were effectively missing in x_num, so should be filled to ~2.0
    assert float(out.loc[out["_row_id"] == 2, "x_num"].iloc[0]) == 2.0
    assert float(out.loc[out["_row_id"] == 4, "x_num"].iloc[0]) == 2.0

    # y_cat: row 2 was "nan" token => mode among non-missing is "a"
    assert out.loc[out["_row_id"] == 2, "y_cat"].iloc[0] == "a"

    # Audit file should have content
    lines = audit_jsonl.read_text(encoding="utf-8").strip().splitlines()
    assert lines, "audit JSONL should not be empty"

    # Must be valid JSON per line
    for line in lines:
        json.loads(line)

    # Last record must be job_summary
    last = json.loads(lines[-1])
    assert last["type"] == "job_summary"
    # x_num standardization cleanup or imputation occurred
    assert last["total_events"] > 0


def test_cli_correct_summary_vs_detailed(tmp_path: Path) -> None:
    df = pd.DataFrame({"_row_id": [1, 2], "x_num": ["1", "null"]})
    in_csv = tmp_path / "in.csv"
    out_csv_a = tmp_path / "out_a.csv"
    out_csv_b = tmp_path / "out_b.csv"
    audit_a = tmp_path / "a.jsonl"
    audit_b = tmp_path / "b.jsonl"
    policy_yaml = tmp_path / "policy.yaml"

    _write_csv(in_csv, df)

    _write_yaml(
        policy_yaml,
        """
dataset:
  name: test
roles:
  critical_columns: []
  protected_columns: []
parsing:
  column_types:
    x_num: numeric
missing_data:
  enabled: true
  drop_if_missing_critical: false
  imputation:
    numeric:
      default: mean
      allow_if_missing_pct_leq: 1.0
      constants: {}
""".lstrip(),
    )

    # summary
    rc1 = main(
        [
            "correct",
            "--input",
            str(in_csv),
            "--policy",
            str(policy_yaml),
            "--out",
            str(out_csv_a),
            "--audit",
            str(audit_a),
            "--audit-detail",
            "summary",
            "--quiet",
        ]
    )
    assert rc1 == 0

    # detailed
    rc2 = main(
        [
            "correct",
            "--input",
            str(in_csv),
            "--policy",
            str(policy_yaml),
            "--out",
            str(out_csv_b),
            "--audit",
            str(audit_b),
            "--audit-detail",
            "detailed",
            "--quiet",
        ]
    )
    assert rc2 == 0

    # detailed should usually produce >= number of records than summary
    a_lines = audit_a.read_text(encoding="utf-8").strip().splitlines()
    b_lines = audit_b.read_text(encoding="utf-8").strip().splitlines()

    assert len(b_lines) >= len(a_lines)
