import json
import pytest
from pathlib import Path
from ira.reporting.report import build_correction_report, generate_markdown_report

def test_report_generation_basic(tmp_path: Path):
    # Mock data
    prof_raw = {
        "metadata": {"dataset_name": "test", "row_count": 100, "policy_hash": "abc", "timestamp": "2023-01-01T10:00:00Z"},
        "readiness": {"score": 50.0, "breakdown": {"validity": 50.0}},
        "columns": {
            "col1": {"effective_missing_pct": 0.20},
            "col2": {"effective_missing_pct": 0.40}
        }
    }
    prof_opt = {
        "metadata": {"dataset_name": "test", "row_count": 90, "policy_hash": "abc", "timestamp": "2023-01-01T10:05:00Z"},
        "readiness": {"score": 80.0, "breakdown": {"validity": 80.0}},
        "columns": {
            "col1": {"effective_missing_pct": 0.0}
        }
    }
    
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        json.dumps({"event_type": "correction", "column": "col1", "reason": "test"}) + "\n" +
        json.dumps({"event_type": "row_dropped", "column": "__row__", "reason": "critical_missing"}) + "\n" +
        json.dumps({
            "event_type": "imputation_skipped_threshold",
            "column": "col2",
            "reason": "missing_pct_0.4000_gt_0.3",
            "policy_section": "missing_data.imputation.numeric"
        }) + "\n" +
        json.dumps({"type": "job_summary", "total_events": 3}) + "\n",
        encoding="utf-8"
    )
    
    policy = {
        "roles": {"protected_columns": ["id"]},
        "parsing": {"column_types": {"col1": "numeric"}},
        "missing_data": {
            "imputation": {
                "numeric": {"allow_if_missing_pct_leq": 0.15}
            }
        }
    }
    
    report = build_correction_report(prof_raw, prof_opt, audit_path, policy)
    
    # Assertions
    assert report["metadata"]["rows_raw"] == 100
    assert report["metadata"]["rows_optimized"] == 90
    assert report["readiness"]["delta"]["score"] == 30.0
    assert report["actions"]["row_drops"]["total"] == 1
    assert report["compliance"]["protected_columns"]["status"] == "pass"
    
    # Check skipped imputation (from explicit events in Phase 8.1+)
    skipped = report["compliance"]["skipped_imputation_columns"]
    assert len(skipped) == 1
    assert skipped[0]["column"] == "col2"
    assert skipped[0]["missing_pct"] == 0.4
    assert skipped[0]["threshold"] == 0.3

def test_report_protected_violation(tmp_path: Path):
    prof = {"metadata": {}, "readiness": {}, "columns": {}}
    audit_path = tmp_path / "audit.jsonl"
    # Event on a protected column
    audit_path.write_text(
        json.dumps({"event_type": "correction", "column": "secret", "reason": "modified"}) + "\n"
    )
    
    policy = {"roles": {"protected_columns": ["secret"]}}
    
    report = build_correction_report(prof, prof, audit_path, policy)
    assert report["compliance"]["protected_columns"]["status"] == "fail"
    assert len(report["compliance"]["protected_columns"]["violations"]) == 1

def test_markdown_report_contains_keys():
    report = {
        "metadata": {"dataset_name": "MyData", "rows_raw": 10, "rows_optimized": 8},
        "readiness": {"raw": {"score": 10}, "optimized": {"score": 90}, "delta": {"score": 80}},
        "actions": {
            "event_counts": {"correction": 5},
            "top_impacted_columns": [{"column": "colA", "count": 5}]
        },
        "compliance": {"protected_columns": {"status": "pass"}}
    }
    md = generate_markdown_report(report)
    assert "# Correction Report" in md
    assert "MyData" in md
    assert "Raw score: **10**" in md
    assert "Optimized score: **90**" in md
    assert "colA" in md
