from __future__ import annotations

import collections
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set

import numpy as np
import pandas as pd

from ira.scoring.readiness import calculate_readiness_score


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


_EFFECTIVE_MISSING_TOKENS = {"", "nan", "null", "none", "na", "n/a"}


def _is_effectively_missing_profile(x: Any) -> bool:
    """Local version of missing check for profiling independence."""
    if pd.isna(x):
        return True
    if isinstance(x, str):
        s = x.strip().lower()
        return s in _EFFECTIVE_MISSING_TOKENS
    # Non-strings: keep only true nulls as missing
    return False


def _is_mixed_type(s: pd.Series, sample_size: int = 1000, threshold: float = 0.95) -> bool:
    """
    Detect if a series has mixed types among the first N non-missing values.
    Excludes effective-missing tokens before sampling.
    """
    # Filter out effective-missing values
    non_missing = s[~s.apply(_is_effectively_missing_profile)].head(sample_size)
    if non_missing.empty:
        return False

    type_counts = collections.Counter(non_missing.map(type))
    most_common_count = type_counts.most_common(1)[0][1]
    
    # Flag mixed if most common type covers less than threshold% of sampled values
    return (most_common_count / len(non_missing)) < threshold


def _count_parsing_failures(s: pd.Series, target_type: str, policy: Dict[str, Any]) -> int:
    """
    Count values that fail to parse to the target_type according to parsing policy.
    Does NOT modify data.
    """
    if s.empty:
        return 0
        
    parsing_cfg = policy.get("parsing", {}) or {}
    
    # We only care about non-missing values
    non_missing_mask = ~s.apply(_is_effectively_missing_profile)
    if not non_missing_mask.any():
        return 0
        
    vals = s[non_missing_mask]
    invalid_count = 0
    
    if target_type == "boolean":
        bool_cfg = parsing_cfg.get("boolean", {}) or {}
        true_vals = set(str(v).strip().lower() for v in bool_cfg.get("true_values", []))
        false_vals = set(str(v).strip().lower() for v in bool_cfg.get("false_values", []))
        for val in vals:
            sval = str(val).strip().lower()
            if sval not in true_vals and sval not in false_vals:
                invalid_count += 1
                
    elif target_type in ("numeric", "integer", "float"):
        num_cfg = parsing_cfg.get("numeric", {}) or {}
        allow_commas = bool(num_cfg.get("allow_commas", True))
        allow_currency = bool(num_cfg.get("allow_currency_symbols", True))
        allow_percent = bool(num_cfg.get("allow_percent_symbol", True))
        
        for val in vals:
            raw = str(val).strip()
            raw_normalized = " ".join(raw.split())
            
            # Percent detection must be endswith('%')
            is_pct = allow_percent and raw_normalized.endswith("%")
            if is_pct:
                raw_num = raw_normalized[:-1].strip()
            else:
                raw_num = raw_normalized
                
            # Strip currency and commas
            if allow_commas:
                raw_num = raw_num.replace(",", "")
            if allow_currency:
                for sym in ("$", "€", "£", "₹", "¥"):
                    raw_num = raw_num.replace(sym, "")
            
            try:
                float(raw_num)
            except (ValueError, TypeError):
                invalid_count += 1
                
    elif target_type in ("datetime", "date", "timestamp"):
        dt_cfg = parsing_cfg.get("datetime", {}) or {}
        dayfirst = bool(dt_cfg.get("dayfirst", False))
        yearfirst = bool(dt_cfg.get("yearfirst", False))
        allowed_formats = list(dt_cfg.get("allowed_formats", []) or [])
        
        for val in vals:
            text = str(val).strip()
            parsed = None
            if allowed_formats:
                for fmt in allowed_formats:
                    try:
                        parsed = pd.to_datetime(
                            text, format=fmt, errors="raise", 
                            dayfirst=dayfirst, yearfirst=yearfirst
                        )
                        break
                    except Exception:
                        continue
            
            if parsed is None:
                try:
                    parsed = pd.to_datetime(text, errors="raise", dayfirst=dayfirst, yearfirst=yearfirst)
                except Exception:
                    invalid_count += 1
                    
    return invalid_count


def _get_frequent_values(s: pd.Series, k: int = 5) -> List[Dict[str, Any]]:
    """Return top-k frequent values as list of {value, count}."""
    if s.empty:
        return []
    
    # Value counts drops NaNs by default
    vc = s.value_counts(dropna=False).head(k)
    return [{"value": str(val), "count": int(count)} for val, count in vc.items()]


def _parse_numeric_series(s: pd.Series, policy: Dict[str, Any]) -> pd.Series:
    """Attempt to parse a series to numeric, returning NaNs for failures."""
    parsing_cfg = policy.get("parsing", {}) or {}
    num_cfg = parsing_cfg.get("numeric", {}) or {}
    allow_commas = bool(num_cfg.get("allow_commas", True))
    allow_currency = bool(num_cfg.get("allow_currency_symbols", True))
    allow_percent = bool(num_cfg.get("allow_percent_symbol", True))
    
    def _clean(x):
        if pd.isna(x): return np.nan
        raw = str(x).strip()
        # Simple normalization: remove currency/commas
        if allow_percent and raw.endswith("%"):
            raw = raw[:-1].strip()
        if allow_currency:
            for sym in ("$", "€", "£", "₹", "¥"):
                raw = raw.replace(sym, "")
        if allow_commas:
            raw = raw.replace(",", "")
        try:
            return float(raw)
        except (ValueError, TypeError):
            return np.nan

    return s.apply(_clean)


def _parse_datetime_series(s: pd.Series, policy: Dict[str, Any]) -> pd.Series:
    """Attempt to parse a series to datetime, returning NaTs for failures."""
    parsing_cfg = policy.get("parsing", {}) or {}
    dt_cfg = parsing_cfg.get("datetime", {}) or {}
    allowed_formats = list(dt_cfg.get("allowed_formats", []) or [])
    dayfirst = bool(dt_cfg.get("dayfirst", False))
    yearfirst = bool(dt_cfg.get("yearfirst", False))
    
    # Try generic parsing first as it's robust
    # In a real heavy-load scenario, we'd optimize this loop
    def _parse(x):
        if pd.isna(x): return pd.NaT
        text = str(x).strip()
        if allowed_formats:
            for fmt in allowed_formats:
                try:
                    dt = pd.to_datetime(text, format=fmt, dayfirst=dayfirst, yearfirst=yearfirst)
                    return dt
                except: pass
        try:
            dt = pd.to_datetime(text, dayfirst=dayfirst, yearfirst=yearfirst)
            return dt
        except:
            return pd.NaT

    # Apply parsing
    parsed = s.apply(_parse)
    
    # Standardize timezone to UTC to avoid mixed-tz comparison errors
    # pd.to_datetime(..., utc=True) handles both naive (assumes UTC) and aware (converts to UTC)
    return pd.to_datetime(parsed, utc=True, errors='coerce')


def _get_column_stats(s: pd.Series, col_type: str, policy: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate type-specific statistics (min, max, mean, etc)."""
    stats = {}
    
    # Filter effective missing first
    mask = ~s.apply(_is_effectively_missing_profile)
    valid_s = s[mask]
    
    if valid_s.empty:
        return stats

    if col_type in ("numeric", "integer", "float", "number"):
        # Convert to numeric
        nums = _parse_numeric_series(valid_s, policy).dropna()
        if not nums.empty:
            stats["min"] = float(nums.min())
            stats["max"] = float(nums.max())
            stats["mean"] = float(nums.mean())
            stats["std"] = float(nums.std()) if len(nums) > 1 else 0.0
            stats["median"] = float(nums.median())

    elif col_type in ("datetime", "date", "timestamp"):
        # Convert to datetime
        dts = _parse_datetime_series(valid_s, policy).dropna()
        if not dts.empty:
            stats["min"] = dts.min().isoformat()
            stats["max"] = dts.max().isoformat()
            
    # String stats (calc on string representation)
    str_lens = valid_s.astype(str).str.len()
    if not str_lens.empty:
        stats["min_len"] = int(str_lens.min())
        stats["max_len"] = int(str_lens.max())
        stats["avg_len"] = float(str_lens.mean())
        
    return stats


def create_profile(
    df: pd.DataFrame,
    policy: Dict[str, Any],
    *,
    clock: Optional[Callable[[], str]] = None,
) -> Dict[str, Any]:
    """
    Generate a deterministic, read-only profile of the dataset.
    """
    row_count = len(df)
    col_count = len(df.columns)
    
    # 1. Metadata
    dataset_cfg = policy.get("dataset", {}) or {}
    clk = clock or _utc_iso_now
    timestamp = clk()
    
    profile = {
        "metadata": {
            "dataset_name": dataset_cfg.get("name", "unknown"),
            "policy_hash": policy.get("reproducibility", {}).get("policy_hash", ""),
            "timestamp": timestamp,
            "row_count": row_count,
            "col_count": col_count,
        },
        "duplicates": {},
        "columns": {},
    }
    
    # 2. Duplicates (excluding _row_id)
    cols_to_check = [c for c in df.columns if c != "_row_id"]
    if cols_to_check:
        exact_row_dupe_count = df.duplicated(subset=cols_to_check, keep="first").sum()
        profile["duplicates"]["exact_row_dupe_count"] = int(exact_row_dupe_count)
        
        pk_cols = dataset_cfg.get("primary_key", {}).get("columns", [])
        # Filter PK columns that exist and are not _row_id
        valid_pk_cols = [c for c in pk_cols if c in df.columns and c != "_row_id"]
        if valid_pk_cols:
            pk_dupes = df.duplicated(subset=valid_pk_cols, keep="first").sum()
            profile["duplicates"]["pk_dupe_count"] = int(pk_dupes)
        else:
            profile["duplicates"]["pk_dupe_count"] = None
    else:
        profile["duplicates"]["exact_row_dupe_count"] = 0
        profile["duplicates"]["pk_dupe_count"] = None
        
    # 3. Column Stats
    roles = policy.get("roles", {}) or {}
    parsing_cfg = policy.get("parsing", {}) or {}
    col_types = parsing_cfg.get("column_types", {}) or {}
    
    role_map = {}
    KNOWN_ROLES = ["critical_columns", "protected_columns", "standardize_columns", "fillable_columns", "droppable_columns"]
    for role in KNOWN_ROLES:
        cols = roles.get(role, [])
        role_label = role.replace("_columns", "")
        for c in cols:
            role_map[c] = role_label

    for col in df.columns:
        if col == "_row_id":
            continue
            
        series = df[col]
        
        # Missingness
        null_count = series.isna().sum()
        null_missing_pct = float(null_count / row_count) if row_count > 0 else 0.0
        
        effectively_missing_mask = series.apply(_is_effectively_missing_profile)
        effective_missing_count = effectively_missing_mask.sum()
        effective_missing_pct = float(effective_missing_count / row_count) if row_count > 0 else 0.0
        
        # Uniqueness
        unique_count = series.nunique()
        unique_pct = float(unique_count / row_count) if row_count > 0 else 0.0
        
        # Mixed Type
        is_mixed = _is_mixed_type(series)
        
        # Parseability (only for typed columns)
        target_type = col_types.get(col)
        is_parseable = None
        invalid_count = None
        invalid_type_pct = None
        
        if target_type:
            invalid_count = _count_parsing_failures(series, target_type, policy)
            is_parseable = (invalid_count == 0)
        
        if invalid_count is not None:
            invalid_type_pct = float(invalid_count / row_count) if row_count > 0 else 0.0
            
        profile["columns"][col] = {
            "role": role_map.get(col, "none"),
            "inferred_pandas_dtype": str(series.dtype),
            "null_missing_pct": round(null_missing_pct, 4),
            "effective_missing_pct": round(effective_missing_pct, 4),
            "unique_pct": round(unique_pct, 4),
            "is_mixed_type": is_mixed,
            "is_parseable": is_parseable,
            "invalid_count": invalid_count,
            "invalid_type_pct": round(invalid_type_pct, 4) if invalid_type_pct is not None else None,
            "frequent_values": _get_frequent_values(series),
            "stats": _get_column_stats(series, target_type or "unknown", policy),
        }
        
    # 4. Readiness Score
    profile["readiness"] = calculate_readiness_score(profile)
        
    return profile
