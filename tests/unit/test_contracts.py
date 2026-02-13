from __future__ import annotations
import pytest
from ira.contracts.audit_contract import validate_audit_record
from ira.contracts.profile_contract import validate_profile
from ira.contracts.report_contract import validate_report

def test_audit_contract_detailed():
    # Valid
    validate_audit_record({
        "timestamp": "2023-01-01T10:00:00Z",
        "event_type": "change",
        "reason": "trim",
        "policy_section": "std"
    })
    # Invalid type
    with pytest.raises(ValueError, match="Unknown audit record type"):
        validate_audit_record({"type": "invalid"})
    # Missing key
    with pytest.raises(ValueError, match="Detailed audit record missing keys"):
        validate_audit_record({"timestamp": "..."})

def test_audit_contract_summary():
    # Valid
    validate_audit_record({
        "type": "summary_aggregate",
        "event_type": "change",
        "column": "col1",
        "reason": "trim",
        "count": 5,
        "samples": []
    })
    # Missing count
    with pytest.raises(ValueError, match="Summary aggregate record missing keys"):
        validate_audit_record({
            "type": "summary_aggregate",
            "event_type": "change",
            "column": "col1",
            "reason": "trim",
            "samples": []
        })

def test_audit_contract_footer():
    # Valid
    validate_audit_record({
        "type": "job_summary",
        "detail": "summary",
        "total_events": 10,
        "event_type_counts": {"change": 10},
        "unique_columns_touched": 1,
        "job_context": {"dataset": "test"}
    })

def test_profile_contract():
    valid_prof = {
        "metadata": {
            "dataset_name": "test",
            "timestamp": "2023-01-01T10:00:00Z",
            "row_count": 100,
            "col_count": 5
        },
        "readiness": {
            "score": 85.0,
            "breakdown": {"validity": 85.0}
        },
        "columns": {
            "col1": {"effective_missing_pct": 0.0}
        }
    }
    validate_profile(valid_prof)
    
    with pytest.raises(ValueError, match="Profile missing 'metadata' section"):
        validate_profile({})

def test_report_contract():
    valid_report = {
        "metadata": {
            "rows_raw": 100,
            "rows_optimized": 95
        },
        "readiness": {
            "raw": {"score": 50},
            "optimized": {"score": 80},
            "delta": {"score": 30}
        },
        "actions": {
            "event_counts": {"drop": 5},
            "row_drops": {"total": 5}
        },
        "compliance": {
            "protected_columns": {"status": "pass"}
        }
    }
    validate_report(valid_report)
    
    with pytest.raises(ValueError, match="Report missing 'actions' section"):
        validate_report({
            "metadata": {"rows_raw": 1, "rows_optimized": 1},
            "readiness": {"raw": {}, "optimized": {}, "delta": {}}
        })
