from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Set

import numpy as np
import pandas as pd

from ira.reporting.audit import AuditLogger


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_effectively_missing(x: Any) -> bool:
    """Check if value is effectively missing, including common null tokens."""
    if pd.isna(x):
        return True
    if isinstance(x, (int, float, np.number)):
        if np.isinf(x):
            return True
    s = str(x).strip().lower()
    return s in ("", "nan", "null", "none", "na", "n/a", "inf", "-inf", "infinity", "-infinity")


def _require_row_id(df: pd.DataFrame) -> None:
    if "_row_id" not in df.columns:
        raise ValueError("DataFrame must contain a '_row_id' column for stable auditing.")


def _audit_failure(
    audit: Optional[AuditLogger],
    *,
    clock: Callable[[], str],
    row_id: int,
    column: str,
    old_value: Any,
    reason: str,
    policy_section: str,
) -> None:
    if audit is None:
        return
    audit.log_value_change(
        event_type="parsing_failure",
        row_id=row_id,
        column=column,
        old_value=str(old_value),
        new_value=None,
        reason=reason,
        policy_section=policy_section,
        timestamp=clock(),
    )


def _strip_currency_and_commas(text: str, *, allow_commas: bool, allow_currency: bool) -> str:
    s = text
    if allow_commas:
        s = s.replace(",", "")
    if allow_currency:
        # Single-character currency symbols
        for sym in ("$", "€", "£", "₹", "¥"):
            s = s.replace(sym, "")
        # Multi-character currency codes (case-insensitive prefix/suffix)
        s_stripped = s.strip()
        s_upper = s_stripped.upper()
        for code in ("USD", "EUR", "GBP", "INR", "JPY", "AUD", "CAD", "CHF",
                      "CNY", "MXN", "BRL", "KRW", "SGD", "HKD", "NZD",
                      "RS.", "RS"):
            if s_upper.startswith(code):
                rest = s_stripped[len(code):].lstrip()
                if rest:  # Only strip if there's a number after the code
                    s = rest
                    break
            if s_upper.endswith(code):
                rest = s_stripped[:-len(code)].rstrip()
                if rest:
                    s = rest
                    break
    # Strip spaces used as thousands separators (e.g., "1 234 567.89")
    # Only strip if the pattern looks like a spaced number
    s = s.strip()
    return s


def _normalize_numeric_string(raw: str) -> str:
    """Pre-process a raw numeric string to handle edge cases before float() conversion.
    Handles: parenthesized negatives, en/em-dash minus, leading +, spaced thousands,
    and rejects NaN/Infinity tokens."""
    s = raw.strip()

    # Reject NaN/Infinity tokens — these should be treated as missing, not valid numbers
    s_lower = s.lower().replace(" ", "")
    if s_lower in ("nan", "-nan", "+nan", "inf", "-inf", "+inf",
                    "infinity", "-infinity", "+infinity"):
        raise ValueError("nan_or_infinity_token")

    # Parenthesized negatives: (1,234.00) → -1,234.00
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]

    # En-dash (U+2013) and em-dash (U+2014) as minus signs
    if s.startswith("\u2013") or s.startswith("\u2014"):
        s = "-" + s[1:]

    # Remove leading + sign (e.g., "+1234.56" → "1234.56")
    if s.startswith("+") and len(s) > 1 and s[1] != "+":
        s = s[1:]

    # Remove spaces between digits (thousands separator: "1 234 567.89" → "1234567.89")
    import re
    s = re.sub(r'(?<=\d) (?=\d)', '', s)

    return s


def _try_formats(value: Any, formats: list[str], *, dayfirst: bool, yearfirst: bool) -> Optional[pd.Timestamp]:
    text = str(value).strip()
    for fmt in formats:
        try:
            return pd.to_datetime(text, format=fmt, errors="raise", dayfirst=dayfirst, yearfirst=yearfirst)
        except Exception:
            continue
    return None


def apply_parsing(
    df: pd.DataFrame,
    policy: Dict[str, Any],
    audit: Optional[AuditLogger] = None,
    *,
    clock: Optional[Callable[[], str]] = None,
) -> pd.DataFrame:
    """
    Parsing rules (MVP):
      - Uses policy['parsing']['column_types'] to decide which columns to parse
      - Respects policy['roles']['protected_columns'] (never modified)
      - Handles on_failure: null / keep_raw / drop_row
      - drop_row is applied ONCE at end, after all columns are processed
      - Audits failures with stable row_id from '_row_id'
    """
    _require_row_id(df)
    clk = clock or _utc_iso_now

    roles = policy.get("roles", {}) or {}
    protected = set(roles.get("protected_columns", []) or [])

    parsing_cfg = policy.get("parsing", {}) or {}
    col_types: Dict[str, str] = dict(parsing_cfg.get("column_types", {}) or {})

    bool_cfg = parsing_cfg.get("boolean", {}) or {}
    num_cfg = parsing_cfg.get("numeric", {}) or {}
    dt_cfg = parsing_cfg.get("datetime", {}) or {}

    bool_on_failure = bool_cfg.get("on_failure", "null")
    num_on_failure = num_cfg.get("on_failure", "null")
    dt_on_failure = dt_cfg.get("on_failure", "null")

    rows_to_drop: Set[Any] = set()

    # Precompute row_id lookup
    row_id_by_index = df["_row_id"].to_dict()

    # -------- Boolean parsing --------
    true_vals = set(str(v).strip().lower() for v in bool_cfg.get("true_values", []))
    false_vals = set(str(v).strip().lower() for v in bool_cfg.get("false_values", []))

    bool_cols = sorted([c for c, t in col_types.items() if t == "boolean" and c in df.columns and c not in protected])
    for col in bool_cols:
        src = df[col]
        # Use object type to hold mixed boolean/None/Original during processing
        out: pd.Series = pd.Series([None] * len(src), index=src.index, dtype="object")

        for idx in sorted(src.index):
            val = src.at[idx]
            if _is_effectively_missing(val):
                out.at[idx] = None
                continue

            sval = str(val).strip().lower()
            if sval in true_vals:
                out.at[idx] = True
            elif sval in false_vals:
                out.at[idx] = False
            else:
                if bool_on_failure == "drop_row":
                    rows_to_drop.add(idx)
                elif bool_on_failure == "null":
                    out.at[idx] = None
                elif bool_on_failure == "keep_raw":
                    out.at[idx] = val
                else:
                    out.at[idx] = None

                _audit_failure(
                    audit,
                    clock=clk,
                    row_id=row_id_by_index[idx],
                    column=col,
                    old_value=val,
                    reason="invalid_boolean",
                    policy_section="parsing.boolean",
                )

        if bool_on_failure != "keep_raw":
            df[col] = out.astype("boolean")
        else:
            df[col] = out

    # -------- Numeric parsing --------
    allow_commas = bool(num_cfg.get("allow_commas", True))
    allow_currency = bool(num_cfg.get("allow_currency_symbols", True))
    allow_percent = bool(num_cfg.get("allow_percent_symbol", True))
    percent_scale = num_cfg.get("percent_scale", "auto")

    num_cols = sorted([c for c, t in col_types.items() if t in ("numeric", "integer", "float") and c in df.columns and c not in protected])
    for col in num_cols:
        src = df[col]
        keep_raw_mode = (num_on_failure == "keep_raw")
        
        # Initialize out buffer
        if keep_raw_mode:
            out = pd.Series([None] * len(src), index=src.index, dtype="object")
        else:
            out = pd.Series([np.nan] * len(src), index=src.index, dtype="float64")

        for idx in sorted(src.index):
            val = src.at[idx]
            if _is_effectively_missing(val):
                if keep_raw_mode:
                     out.at[idx] = val
                else:
                     out.at[idx] = np.nan
                continue

            raw = str(val).strip()
            # Normalize whitespace before checking for percent symbol
            raw_normalized = " ".join(raw.split())
            is_pct = allow_percent and "%" in raw_normalized
            if is_pct:
                raw_num = raw_normalized.replace("%", "").strip()
            else:
                raw_num = raw

            raw_num = _strip_currency_and_commas(raw_num, allow_commas=allow_commas, allow_currency=allow_currency)

            # Normalize: parenthesized negatives, en/em-dash minus, NaN/Infinity rejection
            try:
                raw_num = _normalize_numeric_string(raw_num)
            except ValueError:
                # NaN/Infinity tokens rejected — treat as parse failure
                pass

            try:
                num = float(raw_num)
                if np.isinf(num) or np.isnan(num):
                    reason = "infinite_or_nan_value"
                    raise ValueError(reason)
            except Exception as e:
                failure_reason = str(e) if "infinite" in str(e) or "nan" in str(e) else "invalid_numeric"
                if num_on_failure == "drop_row":
                    rows_to_drop.add(idx)
                    if keep_raw_mode:
                        out.at[idx] = val
                elif num_on_failure == "null":
                    out.at[idx] = np.nan
                elif num_on_failure == "keep_raw":
                    out.at[idx] = val
                else:
                    out.at[idx] = np.nan

                _audit_failure(
                    audit,
                    clock=clk,
                    row_id=row_id_by_index[idx],
                    column=col,
                    old_value=val,
                    reason=failure_reason,
                    policy_section="parsing.numeric",
                )
                continue

            if is_pct:
                if percent_scale == "0_1":
                    num = num / 100.0
                elif percent_scale == "0_100":
                    pass
                elif percent_scale == "auto":
                    num = num / 100.0

            out.at[idx] = num

        df[col] = out

    # -------- Datetime parsing --------
    dayfirst = bool(dt_cfg.get("dayfirst", False))
    yearfirst = bool(dt_cfg.get("yearfirst", False))
    allowed_formats = list(dt_cfg.get("allowed_formats", []) or [])

    dt_cols = sorted([c for c, t in col_types.items() if t in ("datetime", "date", "timestamp") and c in df.columns and c not in protected])
    for col in dt_cols:
        src = df[col]
        keep_raw_mode = (dt_on_failure == "keep_raw")
        if keep_raw_mode:
            out = pd.Series([None] * len(src), index=src.index, dtype="object")
        else:
            out = pd.Series([pd.NaT] * len(src), index=src.index, dtype="datetime64[ns]")

        for idx in sorted(src.index):
            val = src.at[idx]
            if _is_effectively_missing(val):
                if keep_raw_mode:
                    out.at[idx] = val
                else:
                    out.at[idx] = pd.NaT
                continue

            parsed: Optional[pd.Timestamp] = None
            if allowed_formats:
                parsed = _try_formats(val, allowed_formats, dayfirst=dayfirst, yearfirst=yearfirst)

            if parsed is None:
                parsed = pd.to_datetime(val, errors="coerce", dayfirst=dayfirst, yearfirst=yearfirst)

            if pd.isna(parsed):
                if dt_on_failure == "drop_row":
                    rows_to_drop.add(idx)
                    if keep_raw_mode:
                         out.at[idx] = val
                elif dt_on_failure == "null":
                    out.at[idx] = pd.NaT
                elif dt_on_failure == "keep_raw":
                    out.at[idx] = val
                else:
                    out.at[idx] = pd.NaT

                _audit_failure(
                    audit,
                    clock=clk,
                    row_id=row_id_by_index[idx],
                    column=col,
                    old_value=val,
                    reason="invalid_datetime",
                    policy_section="parsing.datetime",
                )
            else:
                # Normalize tz-aware → tz-naive (UTC) to avoid mixed-tz errors
                if hasattr(parsed, "tzinfo") and parsed.tzinfo is not None:
                    parsed = parsed.tz_convert("UTC").tz_localize(None)
                try:
                    out.at[idx] = parsed
                except (OverflowError, pd.errors.OutOfBoundsDatetime, AssertionError):
                    # Dates outside pandas nanosecond range (~1677-2262) overflow.
                    # Treat as parse failure.
                    out.at[idx] = pd.NaT
                    _audit_failure(
                        audit,
                        clock=clk,
                        row_id=row_id_by_index[idx],
                        column=col,
                        old_value=val,
                        reason="datetime_out_of_bounds",
                        policy_section="parsing.datetime",
                    )

        df[col] = out

    # Drop once at end
    if rows_to_drop:
        if audit:
            ts = clk()
            for idx in sorted(list(rows_to_drop)):
                audit.log_value_change(
                    event_type="row_dropped",
                    row_id=row_id_by_index[idx],
                    column="__row__",
                    old_value="row_present",
                    new_value=None,
                    reason="parsing_on_failure_drop_row",
                    policy_section="parsing.on_failure.drop_row",
                    timestamp=ts,
                )
        df = df.drop(index=sorted(list(rows_to_drop)))

    return df
