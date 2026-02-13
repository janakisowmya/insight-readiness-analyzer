from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Set

import pandas as pd
import numpy as np

from ira.reporting.audit import AuditLogger


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_row_id(df: pd.DataFrame) -> None:
    if "_row_id" not in df.columns:
        raise ValueError("DataFrame must contain a '_row_id' column for stable auditing.")


def _is_effectively_missing(x: Any) -> bool:
    if pd.isna(x):
        return True
    if isinstance(x, (int, float, np.number)):
        if np.isinf(x):
            return True
    s = str(x).strip().lower()
    return s in ("", "nan", "null", "none", "na", "n/a", "inf", "-inf", "infinity", "-infinity")


def drop_critical_missing(
    df: pd.DataFrame,
    policy: Dict[str, Any],
    audit: Optional[AuditLogger] = None,
    *,
    clock: Optional[Callable[[], str]] = None,
) -> pd.DataFrame:
    """
    READ/WRITE: returns a new df with rows removed if critical columns are missing.
    Uses roles.critical_columns and missing_data.drop_if_missing_critical
    """
    _require_row_id(df)
    
    missing_cfg = policy.get("missing_data", {})
    if not missing_cfg.get("enabled", True) or not missing_cfg.get("drop_if_missing_critical", True):
        return df

    roles = policy.get("roles", {})
    critical_cols = sorted(roles.get("critical_columns", []) or [])
    if not critical_cols:
        return df

    clk = clock or _utc_iso_now
    
    # Identify rows with any missing value in any critical column
    mask = pd.Series(False, index=df.index)
    for col in critical_cols:
        if col in df.columns:
            mask |= df[col].apply(_is_effectively_missing)

    if not mask.any():
        return df

    dropped_df = df[mask]
    if audit:
        ts = clk()
        for idx in sorted(dropped_df.index):
            rid = dropped_df.at[idx, "_row_id"]
            if isinstance(rid, np.generic):
                rid = rid.item()
            audit.log_value_change(
                event_type="row_dropped",
                row_id=rid,
                column="__row__",
                old_value="row_present",
                new_value=None,
                reason="missing_critical_column",
                policy_section="missing_data.drop_if_missing_critical",
                timestamp=ts,
            )

    return df[~mask].copy()


def apply_imputation(
    df: pd.DataFrame,
    policy: Dict[str, Any],
    audit: Optional[AuditLogger] = None,
    *,
    clock: Optional[Callable[[], str]] = None,
) -> pd.DataFrame:
    """
    Applies missing_data.imputation rules AFTER parsing.
    Must respect allow_if_missing_pct_leq thresholds.
    Must log every filled value (row_id, column, old, new, reason, timestamp).
    """
    _require_row_id(df)
    
    missing_cfg = policy.get("missing_data", {})
    if not missing_cfg.get("enabled", True):
        return df

    impute_cfg = missing_cfg.get("imputation", {})
    if not impute_cfg:
        return df

    # Hardening: avoid mutations on input df
    df = df.copy()
    clk = clock or _utc_iso_now
    row_ids = df["_row_id"]

    # Map column types from policy for easier lookup
    parsing_cfg = policy.get("parsing", {})
    col_types = parsing_cfg.get("column_types", {})
    roles = policy.get("roles", {})
    protected = set(roles.get("protected_columns", []) or [])

    # buckets definition
    numeric_types = ("numeric", "integer", "float")
    datetime_types = ("datetime", "date", "timestamp")
    boolean_types = ("boolean",)

    buckets_targets = {
        "numeric": numeric_types,
        "datetime": datetime_types,
        "categorical": None # defined as col_types[c] not in numeric/datetime/boolean
    }

    for bucket_name, type_tuple in buckets_targets.items():
        cfg = impute_cfg.get(bucket_name, {})
        if not cfg or (cfg.get("default", "none") == "none" and not cfg.get("constants")):
            continue

        default_strategy = cfg.get("default", "none")
        constants = cfg.get("constants", {})
        threshold = cfg.get("allow_if_missing_pct_leq", 1.0)
        
        # Determine which columns belong to this bucket
        bucket_cols = []
        for col in df.columns:
            if col in protected or col == "_row_id":
                continue
            
            typ = col_types.get(col)
            
            if typ:
                # Use policy-defined type
                if bucket_name == "numeric" and typ in numeric_types:
                    bucket_cols.append(col)
                elif bucket_name == "datetime" and typ in datetime_types:
                    bucket_cols.append(col)
                elif bucket_name == "categorical" and typ not in numeric_types and typ not in datetime_types and typ not in boolean_types:
                    bucket_cols.append(col)
            else:
                # Fallback to inference if not in policy
                if bucket_name == "numeric" and pd.api.types.is_numeric_dtype(df[col]):
                    bucket_cols.append(col)
                elif bucket_name == "datetime" and pd.api.types.is_datetime64_any_dtype(df[col]):
                    bucket_cols.append(col)
                elif bucket_name == "categorical" and not pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_datetime64_any_dtype(df[col]):
                    bucket_cols.append(col)

        bucket_cols = sorted(bucket_cols)
        for col in bucket_cols:
            s = df[col]
            missing_mask = s.apply(_is_effectively_missing)
            if not missing_mask.any():
                continue

            missing_pct = missing_mask.mean()
            if missing_pct > threshold:
                if audit:
                    audit.log({
                        "event_type": "imputation_skipped_threshold",
                        "column": col,
                        "reason": f"missing_pct_{missing_pct:.4f}_gt_{threshold:.4f}",
                        "policy_section": f"missing_data.imputation.{bucket_name}",
                        "timestamp": clk()
                    })
                continue

            # Determine strategy and fill_val
            strategy = default_strategy
            fill_val = None
            
            constant_val = constants.get(col)
            if constant_val is not None:
                strategy = "constant"
                fill_val = constant_val
            elif strategy == "mean" and pd.api.types.is_numeric_dtype(s):
                fill_val = s.mean()
            elif strategy == "median" and pd.api.types.is_numeric_dtype(s):
                fill_val = s.median()
            elif strategy == "mode" and bucket_name == "categorical":
                # Hardening: filter out effective missing so they don't become the mode
                non_missing = s[~missing_mask]
                modes = non_missing.mode()
                fill_val = modes.iloc[0] if not modes.empty else None
            elif strategy == "constant" and constant_val is None:
                 continue
            else:
                continue

            # Hardening: robust null check for fill_val
            if fill_val is None:
                continue
            try:
                if pd.isna(fill_val):
                    continue
            except Exception:
                pass

            # Apply fill and audit
            ts = clk()
            filled_indices = missing_mask[missing_mask].index

            # If target column is datetime and fill_val is a string, convert to Timestamp
            if pd.api.types.is_datetime64_any_dtype(df[col]) and isinstance(fill_val, str):
                try:
                    ts_val = pd.Timestamp(fill_val)
                    # Normalize tz-aware → tz-naive (UTC)
                    if ts_val.tzinfo is not None:
                        ts_val = ts_val.tz_convert("UTC").tz_localize(None)
                    fill_val = ts_val
                except Exception:
                    pass  # leave as string, will fail gracefully

            for idx in sorted(filled_indices):
                old_val = s.at[idx]
                df.at[idx, col] = fill_val
                
                if audit:
                    rid = row_ids.at[idx]
                    if isinstance(rid, np.generic):
                        rid = rid.item()
                    
                    audit.log_value_change(
                        event_type="imputation_fill",
                        row_id=rid,
                        column=col,
                        old_value=str(old_val) if pd.notna(old_val) else None,
                        new_value=str(fill_val),
                        reason=f"imputation_{strategy}",
                        policy_section=f"missing_data.imputation.{bucket_name}",
                        timestamp=ts,
                    )

    return df
