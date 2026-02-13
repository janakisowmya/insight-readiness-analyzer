from __future__ import annotations
from typing import Any, Dict

def validate_report(report: Dict[str, Any]) -> None:
    """
    Validates a correction report dictionary.
    """
    if not isinstance(report, dict):
        raise ValueError("Report must be a dictionary")

    # Metadata
    meta = report.get("metadata")
    if not isinstance(meta, dict):
        raise ValueError("Report missing 'metadata' section")
    req_meta = {"rows_raw", "rows_optimized"}
    missing_meta = req_meta - meta.keys()
    if missing_meta:
        raise ValueError(f"Report metadata missing keys: {missing_meta}")

    # Readiness
    readiness = report.get("readiness")
    if not isinstance(readiness, dict):
        raise ValueError("Report missing 'readiness' section")
    req_readiness = {"raw", "optimized", "delta"}
    missing_readiness = req_readiness - readiness.keys()
    if missing_readiness:
        raise ValueError(f"Report readiness missing keys: {missing_readiness}")

    # Actions
    actions = report.get("actions")
    if not isinstance(actions, dict):
        raise ValueError("Report missing 'actions' section")
    req_actions = {"event_counts", "row_drops"}
    missing_actions = req_actions - actions.keys()
    if missing_actions:
        raise ValueError(f"Report actions missing keys: {missing_actions}")

    # Compliance
    compliance = report.get("compliance")
    if not isinstance(compliance, dict):
        raise ValueError("Report missing 'compliance' section")
    comp_protected = compliance.get("protected_columns")
    if not isinstance(comp_protected, dict) or "status" not in comp_protected:
        raise ValueError("Compliance missing 'protected_columns.status'")
