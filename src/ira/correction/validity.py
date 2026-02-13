from __future__ import annotations

import re
import numpy as np
import pandas as pd
from typing import Any, Dict, Optional, Callable

from ira.reporting.audit import AuditLogger


def apply_validity(
    df: pd.DataFrame,
    policy: Dict[str, Any],
    audit: Optional[AuditLogger] = None,
    *,
    clock: Optional[Callable[[], str]] = None,
) -> pd.DataFrame:
    """
    Enforce validity rules defined in the policy:
    - Numeric ranges (min/max)
    - Allowed values (categorical)
    - Regex patterns
    - Non-negative columns
    
    Handles violations according to 'on_violation': flag, null, or drop_row.
    """
    rules = policy.get("validity_rules", {}) or {}
    if not rules.get("enabled", True):
        return df

    ranges = rules.get("ranges", {}) or {}
    allowed_values = rules.get("allowed_values", {}) or {}
    regex_patterns = rules.get("regex", {}) or {}
    non_negative = set(rules.get("non_negative_columns", []) or [])
    default_action = rules.get("on_violation", "flag")

    # Track rows to drop
    rows_to_drop = pd.Series(False, index=df.index)

    # 1. Non-negative columns
    for col in non_negative:
        if col not in df.columns:
            continue
        
        # Only check numeric columns
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
            
        mask = df[col] < 0
        if mask.any():
            _handle_violation(
                df, mask, col, "non_negative", 
                "Value must be >= 0", default_action, 
                rows_to_drop, audit
            )

    # 2. Ranges (Numeric)
    for col, limits in ranges.items():
        if col not in df.columns:
            continue
            
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
            
        mn, mx = limits.get("min"), limits.get("max")
        
        if mn is not None:
            mask = df[col] < mn
            if mask.any():
                _handle_violation(
                    df, mask, col, "range_min", 
                    f"Value < {mn}", default_action, 
                    rows_to_drop, audit
                )
                
        if mx is not None:
            mask = df[col] > mx
            if mask.any():
                _handle_violation(
                    df, mask, col, "range_max", 
                    f"Value > {mx}", default_action, 
                    rows_to_drop, audit
                )

    # 3. Allowed Values
    for col, allowed in allowed_values.items():
        if col not in df.columns:
            continue
            
        # Skip nulls (handled by missing_data section)
        not_null = df[col].notna()
        # Check membership
        is_bad = ~df.loc[not_null, col].isin(allowed)
        
        full_mask = pd.Series(False, index=df.index)
        full_mask.loc[not_null] = is_bad
        
        if full_mask.any():
            _handle_violation(
                df, full_mask, col, "allowed_values", 
                f"Value not in allowed list", default_action, 
                rows_to_drop, audit
            )

    # 4. Regex
    for col, pattern in regex_patterns.items():
        if col not in df.columns:
            continue
            
        # Convert to string for regex check
        s_str = df[col].astype(str)
        # Skip NaNs if they are actual nulls, but astype(str) makes 'nan'
        # Use underlying notna check
        not_null = df[col].notna()
        
        # Vectorized match
        matches = s_str.loc[not_null].str.match(pattern)
        is_bad = ~matches
        
        full_mask = pd.Series(False, index=df.index)
        full_mask.loc[not_null] = is_bad
        
        if full_mask.any():
            _handle_violation(
                df, full_mask, col, "regex_mismatch", 
                f"Value does not match pattern '{pattern}'", default_action, 
                rows_to_drop, audit, clock
            )

    # Apply drops
    if rows_to_drop.any():
        drop_count = rows_to_drop.sum()
        if audit:
            ts = clock() if clock else None
            audit.log({
                "timestamp": ts,
                "event_type": "validity_drop",
                "reason": f"Dropped {drop_count} rows due to validity violations",
                "policy_section": "validity_rules",
                "count": int(drop_count)
            })
        df = df[~rows_to_drop].copy()

    return df


def _handle_violation(
    df: pd.DataFrame,
    mask: pd.Series,
    col: str,
    reason_code: str,
    message: str,
    action: str,
    rows_to_drop: pd.Series,
    audit: Optional[AuditLogger],
    clock: Optional[Callable[[], str]] = None,
):
    """Refactored handler for all validity violation types."""
    count = mask.sum()
    if count == 0:
        return

    if audit:
        ts = clock() if clock else None
        audit.log({
            "timestamp": ts,
            "event_type": f"validity_{action}",
            "column": col,
            "reason": f"{reason_code}: {message} ({count} rows)",
            "policy_section": "validity_rules",
            "violation_count": int(count)
        })

    if action == "drop_row":
        rows_to_drop |= mask
        
    elif action == "null":
        # Nulify the values
        df.loc[mask, col] = np.nan
        
    elif action == "flag":
        # In a real system we might add a metadata column
        # For now, we just log (audit) which is done above
        pass
