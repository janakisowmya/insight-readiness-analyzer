import json
import pytest
from pathlib import Path
from ira.reporting.audit import AuditLogger

def test_audit_logger_detailed_creates_jsonl(tmp_path: Path):
    log_path = tmp_path / "audit.jsonl"
    logger = AuditLogger(log_path, detail="detailed")
    
    event1 = {
        "timestamp": "2023-01-01T10:00:00Z",
        "event_type": "correction",
        "reason": "whitespace",
        "policy_section": "standardization",
        "column": "name",
        "old_value": "  foo  ",
        "new_value": "foo"
    }
    logger.log(event1)
    logger.close()
    
    assert log_path.exists()
    lines = log_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    
    loaded = json.loads(lines[0])
    assert loaded["event_type"] == "correction"
    assert loaded["old_value"] == "  foo  "

def test_audit_logger_required_fields(tmp_path: Path):
    logger = AuditLogger(tmp_path / "audit.jsonl")
    
    # Missing timestamp
    with pytest.raises(ValueError, match="Missing required audit fields"):
        logger.log({"event_type": "foo", "reason": "bar", "policy_section": "baz"})
        
    logger.close()

def test_audit_logger_determinism(tmp_path: Path):
    log_path = tmp_path / "audit.jsonl"
    logger = AuditLogger(log_path, detail="detailed")
    
    # Intentionally mixed order
    event = {
        "reason": "r",
        "timestamp": "t",
        "policy_section": "p",
        "event_type": "e"
    }
    logger.log(event)
    logger.close()
    
    lines = log_path.read_text(encoding="utf-8").strip().split("\n")
    # verify strictly no spaces after separators
    expected = '{"event_type":"e","policy_section":"p","reason":"r","timestamp":"t"}'
    assert lines[0] == expected
    assert json.loads(lines[1])["type"] == "job_summary"

def test_audit_logger_summary_mode(tmp_path: Path):
    log_path = tmp_path / "summary.jsonl"
    logger = AuditLogger(log_path, detail="summary")
    
    # Log 30 events for same group
    for i in range(30):
        logger.log_value_change(
            event_type="cleanup",
            row_id=i,
            column="col1",
            old_value="bad",
            new_value="good",
            reason="testing",
            policy_section="test",
            timestamp=f"2023-01-01T10:00:{i:02d}Z"
        )
        
    logger.close()
    
    lines = log_path.read_text(encoding="utf-8").strip().split("\n")
    # Should have 1 aggregate line
    assert len(lines) == 2
    
    agg = json.loads(lines[0])
    assert agg["type"] == "summary_aggregate"
    assert agg["count"] == 30
    assert len(agg["samples"]) == 25  # Capped at 25
    assert agg["samples"][0]["row_id"] == 0

def test_audit_logger_close_idempotent(tmp_path: Path):
    logger = AuditLogger(tmp_path / "audit.jsonl")
    logger.close()
    logger.close() # Should not raise
