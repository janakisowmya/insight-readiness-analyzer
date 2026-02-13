import json
from pathlib import Path
import pandas as pd
from ira.cli import main
from ira.contracts.report_contract import validate_report
from ira.contracts.audit_contract import validate_audit_record

def test_cli_correct_artifact_completeness(tmp_path: Path):
    in_csv = tmp_path / "input.csv"
    policy_yaml = tmp_path / "policy.yaml"
    out_csv = tmp_path / "out.csv"
    audit_log = tmp_path / "audit.jsonl"
    report_json = tmp_path / "report.json"
    report_md = tmp_path / "report.md"
    
    in_csv.write_text("_row_id,customer_id,val\n1,C001, 10 \n2,C002,20\n", encoding="utf-8")
    policy_yaml.write_text("""
dataset: {name: test_complete}
roles:
  critical_columns: [_row_id]
  protected_columns: [customer_id]
standardization:
  global_trim_whitespace: true
""", encoding="utf-8")

    result = main([
        "correct",
        "--input", str(in_csv),
        "--policy", str(policy_yaml),
        "--out", str(out_csv),
        "--audit", str(audit_log),
        "--report", str(report_json),
        "--report-md", str(report_md),
        "--quiet"
    ])
    
    # Validate CSV Preservation
    out_df = pd.read_csv(out_csv)
    assert "_row_id" in out_df.columns
    assert list(out_df["_row_id"]) == [1, 2] # Order preserved
    
    # Validate Report JSON
    report_data = json.loads(report_json.read_text())
    validate_report(report_data)
    
    # Validate Audit JSONL (Check full log contract)
    from ira.contracts.audit_contract import validate_audit_log
    validate_audit_log(audit_log)
    
    # Validate Markdown (Headers)
    md_text = report_md.read_text()
    assert "# Correction Report" in md_text
    assert "## Job Summary" in md_text
    assert "## Readiness Impact" in md_text
    assert "## Actions" in md_text
    assert "## Policy Compliance" in md_text
