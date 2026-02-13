from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

import pandas as pd
import numpy as np

from ira.reporting.audit import AuditLogger


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_row_id(df: pd.DataFrame) -> None:
    if "_row_id" not in df.columns:
        raise ValueError("DataFrame must contain a '_row_id' column for stable auditing.")


def _is_string_like(s: pd.Series) -> bool:
    """Check if series is object or string dtype."""
    return pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)


def _audit_change(
    audit: Optional[AuditLogger],
    *,
    column: str,
    row_ids: pd.Series,
    old_s: pd.Series,
    new_s: pd.Series,
    reason: str,
    policy_section: str,
    clock: Callable[[], str],
) -> None:
    """Audit changes between old and new series, logging only changed values."""
    if audit is None:
        return

    # Efficient comparison using sentinel for NaN equality
    sentinel = object()
    mask = old_s.fillna(sentinel) != new_s.fillna(sentinel)
    
    if not mask.any():
        return
        
    changed_indices = mask[mask].index
    ts = clock()
    
    for idx in changed_indices:
        # Get row_id as a JSON-safe scalar
        rid_val = row_ids.at[idx]
        if isinstance(rid_val, np.generic):
            rid_val = rid_val.item()

        old_val = old_s.at[idx]
        new_val = new_s.at[idx]
        
        audit.log_value_change(
            event_type="standardization_change",
            row_id=rid_val,
            column=column,
            old_value=str(old_val) if pd.notna(old_val) else None,
            new_value=str(new_val) if pd.notna(new_val) else None,
            reason=reason,
            policy_section=policy_section,
            timestamp=ts,
        )


def trim_whitespace(s: pd.Series) -> pd.Series:
    """Trim leading/trailing whitespace ONLY on string values, preserving others and NaNs."""
    mask = s.map(lambda x: isinstance(x, str))
    if not mask.any():
        return s
    result = s.copy()
    result.loc[mask] = s.loc[mask].str.strip()
    return result


def collapse_whitespace(s: pd.Series) -> pd.Series:
    """Collapse multiple whitespace ONLY on string values, preserving NaNs."""
    mask = s.map(lambda x: isinstance(x, str))
    if not mask.any():
        return s
    result = s.copy()
    result.loc[mask] = s.loc[mask].str.replace(r'\s+', ' ', regex=True)
    return result


def strip_nonprinting(s: pd.Series) -> pd.Series:
    """Remove non-printing control characters, zero-width chars, RTL/LTR marks,
    NULL bytes, and normalize NBSP to space. Also applies NFKC normalization
    to convert full-width characters to ASCII equivalents."""
    import unicodedata

    mask = s.map(lambda x: isinstance(x, str))
    if not mask.any():
        return s
    result = s.copy()
    # 1. NFKC normalization: full-width → ASCII (e.g., ＄→$, １→1)
    result.loc[mask] = result.loc[mask].map(
        lambda x: unicodedata.normalize("NFKC", x) if isinstance(x, str) else x
    )
    # 2. Remove C0/C1 control chars, NULL bytes, zero-width chars, RTL/LTR marks, BOM
    pat = (
        r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F'   # C0 controls + DEL
        r'\u200B-\u200F'                         # zero-width space, ZWNJ, ZWJ, LRM, RLM
        r'\u2028\u2029'                          # line/paragraph separator
        r'\uFEFF'                                # BOM / zero-width no-break space
        r'\u2060\u2061\u2062\u2063\u2064'        # invisible operators
        r']'
    )
    result.loc[mask] = result.loc[mask].str.replace(pat, "", regex=True)
    # 3. NBSP (U+00A0) and thin space (U+2009) → regular space
    result.loc[mask] = result.loc[mask].str.replace('\u00a0', ' ', regex=False)
    result.loc[mask] = result.loc[mask].str.replace('\u2009', ' ', regex=False)
    return result


def apply_casefold(s: pd.Series, casefold: str) -> pd.Series:
    """Apply case normalization ONLY on string values, preserving NaNs."""
    if casefold == "none":
        return s
    
    mask = s.map(lambda x: isinstance(x, str))
    if not mask.any():
        return s
    
    result = s.copy()
    if casefold == "lower":
        result.loc[mask] = s.loc[mask].str.lower()
    elif casefold == "upper":
        result.loc[mask] = s.loc[mask].str.upper()
    elif casefold == "title":
        result.loc[mask] = s.loc[mask].str.title()
    
    return result


def apply_mappings(s: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
    """Apply value mappings only on non-null values."""
    if not mapping:
        return s
    
    mask = s.notna()
    result = s.copy()
    result.loc[mask] = s.loc[mask].replace(mapping)
    return result


def apply_standardization(
    df: pd.DataFrame,
    policy: Dict[str, Any],
    audit: Optional[AuditLogger] = None,
    *,
    clock: Optional[Callable[[], str]] = None,
) -> pd.DataFrame:
    """
    Apply standardization rules according to policy schema.
    Returns a NEW DataFrame (immutability lock).
    
    Order of operations:
    1. Strip non-printing
    2. Trim whitespace
    3. Collapse whitespace
    4. Global casefold
    5. Per-column mappings
    """
    _require_row_id(df)
    
    # Immutability lock
    df = df.copy()
    
    clk = clock or _utc_iso_now
    row_ids = df["_row_id"]

    roles = policy.get("roles", {}) or {}
    protected = set(roles.get("protected_columns", []) or [])
    
    std_cfg = policy.get("standardization", {}) or {}
    
    # 1. Strip non-printing characters
    if std_cfg.get("strip_nonprinting", False):
        for col in df.columns:
            if col in protected or not _is_string_like(df[col]):
                continue
            old_s = df[col]
            new_s = strip_nonprinting(old_s)
            
            if not old_s.equals(new_s):
                _audit_change(
                    audit,
                    column=col,
                    row_ids=row_ids,
                    old_s=old_s,
                    new_s=new_s,
                    reason="strip_nonprinting",
                    policy_section="standardization.strip_nonprinting",
                    clock=clk,
                )
                df[col] = new_s

    # 2. Global trim whitespace
    if std_cfg.get("global_trim_whitespace", False):
        for col in df.columns:
            if col in protected or not _is_string_like(df[col]):
                continue
            old_s = df[col]
            new_s = trim_whitespace(old_s)
            
            if not old_s.equals(new_s):
                _audit_change(
                    audit,
                    column=col,
                    row_ids=row_ids,
                    old_s=old_s,
                    new_s=new_s,
                    reason="trim_whitespace",
                    policy_section="standardization.global_trim_whitespace",
                    clock=clk,
                )
                df[col] = new_s

    # 3. Global collapse whitespace
    if std_cfg.get("global_collapse_whitespace", False):
        for col in df.columns:
            if col in protected or not _is_string_like(df[col]):
                continue
            old_s = df[col]
            new_s = collapse_whitespace(old_s)
            
            if not old_s.equals(new_s):
                _audit_change(
                    audit,
                    column=col,
                    row_ids=row_ids,
                    old_s=old_s,
                    new_s=new_s,
                    reason="collapse_whitespace",
                    policy_section="standardization.global_collapse_whitespace",
                    clock=clk,
                )
                df[col] = new_s

    # 4. Global casefold
    casefold = std_cfg.get("casefold", "none")
    if isinstance(casefold, str) and casefold != "none":
        for col in df.columns:
            if col in protected or not _is_string_like(df[col]):
                continue
            old_s = df[col]
            new_s = apply_casefold(old_s, casefold)
            
            if not old_s.equals(new_s):
                _audit_change(
                    audit,
                    column=col,
                    row_ids=row_ids,
                    old_s=old_s,
                    new_s=new_s,
                    reason=f"casefold_{casefold}",
                    policy_section="standardization.casefold",
                    clock=clk,
                )
                df[col] = new_s

    # 5. Per-column value mappings
    mappings = std_cfg.get("mappings", {})
    if isinstance(mappings, dict):
        for col, mapping in mappings.items():
            if col not in df.columns or col in protected or not isinstance(mapping, dict):
                continue
            
            old_s = df[col]
            new_s = apply_mappings(old_s, mapping)
            
            if not old_s.equals(new_s):
                _audit_change(
                    audit,
                    column=col,
                    row_ids=row_ids,
                    old_s=old_s,
                    new_s=new_s,
                    reason="value_mapping",
                    policy_section="standardization.mappings",
                    clock=clk,
                )
                df[col] = new_s

    return df
