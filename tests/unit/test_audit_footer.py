from __future__ import annotations
import json
from pathlib import Path
from ira.reporting.audit import AuditLogger

def test_audit_summary_footer(tmp_path: Path):
    audit_path = tmp_path / "audit.jsonl"
    logger = AuditLogger(audit_path, detail="summary")
    logger.set_context(dataset_name="test_ds", rows_in=10, rows_out=9)
    
    logger.log({
        "timestamp": "2023-01-01T10:00:00Z",
        "event_type": "change",
        "reason": "trim",
        "column": "col1",
        "policy_section": "std"
    })
    logger.close()
    
    lines = audit_path.read_text().splitlines()
    assert len(lines) == 2
    
    # Line 1: Summary Aggregate
    rec1 = json.loads(lines[0])
    assert rec1["type"] == "summary_aggregate"
    assert rec1["count"] == 1
    
    # Line 2: Job Summary Footer
    rec2 = json.loads(lines[1])
    assert rec2["type"] == "job_summary"
    assert rec2["total_events"] == 1
    ctx = rec2["job_context"]
    assert ctx["dataset_name"] == "test_ds"
    assert ctx["rows_in"] == 10
    assert ctx["rows_out"] == 9

def test_audit_detailed_footer(tmp_path: Path):
    audit_path = tmp_path / "audit.jsonl"
    logger = AuditLogger(audit_path, detail="detailed")
    
    logger.log({
        "timestamp": "2023-01-01T10:00:00Z",
        "event_type": "change",
        "reason": "trim",
        "column": "col1",
        "policy_section": "std"
    })
    logger.close()
    
    lines = audit_path.read_text().splitlines()
    assert len(lines) == 2
    
    # Line 1: Detailed Event
    rec1 = json.loads(lines[0])
    assert "type" not in rec1 or rec1["type"] == "detailed"
    
    # Line 2: Job Summary Footer
    rec2 = json.loads(lines[1])
    assert rec2["type"] == "job_summary"
    assert rec2["total_events"] == 1
