"""Auto-infer a policy YAML from CSV data by detecting column types.

This module scans column values and uses heuristics to classify each column
as numeric, datetime, boolean, or text.  It also detects currency symbols,
percentage patterns, and protected (PII-like) columns.

Usage (programmatic):
    from ira.profiling.infer_policy import infer_policy
    policy = infer_policy(df, dataset_name="my_dataset")

Usage (CLI):
    python -m ira.cli infer --input data.csv --out policy.yaml
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from ira.profiling.learned_patterns import LearningStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CURRENCY_SYMBOLS: List[str] = ["$", "€", "£", "₹", "¥"]
_CURRENCY_CODES: List[str] = [
    "USD", "EUR", "GBP", "INR", "JPY", "AUD", "CAD", "CHF",
    "CNY", "MXN", "BRL", "KRW", "SGD", "HKD", "NZD", "RS.", "RS", "R$",
]

_BOOL_TRUE: Set[str] = {
    "yes", "y", "true", "t", "1", "on", "active", "enabled",
    "si", "ja", "oui", "da", "affirmative", "yep", "yeah",
}
_BOOL_FALSE: Set[str] = {
    "no", "n", "false", "f", "0", "off", "inactive", "disabled",
    "non", "nein", "nah", "nope", "negative",
}
_BOOL_ALL: Set[str] = _BOOL_TRUE | _BOOL_FALSE

# Column name patterns that suggest PII / protected data
_PROTECTED_PATTERNS: List[str] = [
    # Names
    r"(?i)\bname\b", r"(?i)\bfirst[_\s]?name\b", r"(?i)\blast[_\s]?name\b",
    r"(?i)\bfull[_\s]?name\b", r"(?i)\bmiddle[_\s]?name\b", r"(?i)\bnickname\b",
    r"(?i)\bsurname\b", r"(?i)\bgiven[_\s]?name\b",
    # Contact
    r"(?i)\bemail\b", r"(?i)\be[_\-]?mail\b", r"(?i)\bphone\b", r"(?i)\bmobile\b",
    r"(?i)\bcell\b", r"(?i)\btelephone\b", r"(?i)\bfax\b", r"(?i)\bcontact\b",
    # Address / Location
    r"(?i)\baddress\b", r"(?i)\bstreet\b", r"(?i)\bcity\b", r"(?i)\bstate\b",
    r"(?i)\bzip\b", r"(?i)\bpostal\b", r"(?i)\bcountry\b", r"(?i)\bregion\b",
    r"(?i)\blocation\b", r"(?i)\bhome\b",
    # Identity documents
    r"(?i)\bssn\b", r"(?i)\bsocial[_\s]?security\b", r"(?i)\bpassport\b",
    r"(?i)\bdriver[_\s]?licen[sc]e\b", r"(?i)\bnational[_\s]?id\b", r"(?i)\btax[_\s]?id\b",
    # Financial identifiers
    r"(?i)\bbank\b", r"(?i)\baccount\b", r"(?i)\brouting\b", r"(?i)\biban\b",
    r"(?i)\bswift\b", r"(?i)\bcredit[_\s]?card\b", r"(?i)\bcard[_\s]?number\b",
    # HR / People
    r"(?i)\bemergency\b", r"(?i)\bmanager\b", r"(?i)\bsupervisor\b",
    r"(?i)\bdepartment\b", r"(?i)\bjob\b", r"(?i)\bposition\b", r"(?i)\brole\b",
    # Medical
    r"(?i)\bmedical\b", r"(?i)\bhealth\b", r"(?i)\bdiagnos\b", r"(?i)\ballerg\b",
    r"(?i)\bprescription\b", r"(?i)\binsurance\b",
    # Free text / dangerous to modify
    r"(?i)\breview\b", r"(?i)\bcomment\b", r"(?i)\bnote\b", r"(?i)\bmemo\b",
    r"(?i)\bdescription\b", r"(?i)\babout\b", r"(?i)\bcontent\b",
    r"(?i)\binternal\b", r"(?i)\bremark\b", r"(?i)\bfeedback\b",
    # URLs / media
    r"(?i)\blink\b", r"(?i)\burl\b", r"(?i)\bimg\b", r"(?i)\bimage\b",
    r"(?i)\bphoto\b", r"(?i)\bavatar\b", r"(?i)\bwebsite\b",
    # IDs referenced in protected patterns
    r"(?i)\buser[_\s]?id\b",
]

# Content patterns that suggest PII regardless of column name
_PII_CONTENT_PATTERNS: List[re.Pattern[str]] = [
    re.compile(r"^\+?\d[\d\-\s\(\)]{7,}$"),           # Phone numbers
    re.compile(r"^\d{3}-\d{2}-\d{4}$"),                 # SSN (US)
    re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),  # Email
    re.compile(r"^\*{2,}\d{3,}$"),                      # Masked values (****1234)
]

# Values that should be treated as missing/null (skip when sampling)
_NULL_TOKENS: Set[str] = {
    "", " ", "null", "none", "n/a", "na", "nan", "nil",
    "-", "--", ".", "?", "undefined", "missing", "unknown",
    "#n/a", "#ref!", "#value!", "#null!", "not available",
}

# Sample size for inference
_SAMPLE_SIZE = 200

# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------

def _is_null_token(val: Any) -> bool:
    """Check if a value is effectively null / should be skipped."""
    if pd.isna(val):
        return True
    s = str(val).strip().lower()
    return s in _NULL_TOKENS


def _strip_currency(s: str, symbols: List[str] = _CURRENCY_SYMBOLS, codes: List[str] = _CURRENCY_CODES) -> str:
    """Remove currency symbols and codes from a string."""
    result = s.strip()
    # Single-char symbols
    for sym in symbols:
        result = result.replace(sym, "")
    # Multi-char codes (prefix/suffix)
    r_upper = result.strip().upper()
    for code in codes:
        if r_upper.startswith(code):
            rest = result.strip()[len(code):].lstrip()
            if rest:
                result = rest
                break
        if r_upper.endswith(code):
            rest = result.strip()[:-len(code)].rstrip()
            if rest:
                result = rest
                break
    return result.strip()


def _try_float(s: str) -> bool:
    """Test if a cleaned string can parse as float."""
    try:
        v = float(s)
        import math
        return not (math.isinf(v) or math.isnan(v))
    except (ValueError, TypeError):
        return False


def _detect_numeric(
    values: List[str],
    symbols: List[str] = _CURRENCY_SYMBOLS,
    codes: List[str] = _CURRENCY_CODES
) -> Tuple[bool, bool, Set[str]]:
    """Detect if a column is numeric.

    Returns: (is_numeric, has_percent, detected_currency_symbols)
    """
    if not values:
        return False, False, set()

    parsed = 0
    pct_count = 0
    found_currencies: Set[str] = set()

    for raw in values:
        s = raw.strip()

        # Handle parenthesized negatives: (123.45) → -123.45
        if s.startswith("(") and s.endswith(")"):
            s = s[1:-1]

        # Detect percent
        has_pct = "%" in s
        if has_pct:
            pct_count += 1
            s = s.replace("%", "").strip()

        # Detect currency symbols
        for sym in symbols:
            if sym in raw:
                found_currencies.add(sym)
        for code in codes:
            if code.lower() in raw.strip().lower().split()[0:1] or \
               code.lower() in raw.strip().lower().split()[-1:]:
                found_currencies.add(code)

        # Strip currency and commas
        s = _strip_currency(s, symbols=symbols, codes=codes)
        s = s.replace(",", "")

        # Handle en/em-dash as negative
        if s and s[0] in ("–", "—"):
            s = "-" + s[1:]

        if _try_float(s):
            parsed += 1

    ratio = parsed / len(values)
    is_numeric = ratio >= 0.70
    has_percent = (pct_count / len(values)) >= 0.50 if values else False

    return is_numeric, has_percent, found_currencies


def _detect_datetime(values: List[str]) -> Tuple[bool, List[str]]:
    """Detect if a column is datetime.

    Returns: (is_datetime, detected_formats)
    """
    if not values:
        return False, []

    # Try pandas datetime parser (very flexible)
    parsed = 0
    for raw in values:
        s = raw.strip()
        try:
            pd.to_datetime(s, dayfirst=False)
            parsed += 1
        except Exception:
            try:
                pd.to_datetime(s, dayfirst=True)
                parsed += 1
            except Exception:
                pass

    ratio = parsed / len(values)
    is_dt = ratio >= 0.60

    # Detect common formats for the policy
    detected_formats: List[str] = []
    if is_dt:
        format_tests = [
            ("%Y-%m-%d", r"^\d{4}-\d{1,2}-\d{1,2}$"),
            ("%m/%d/%Y", r"^\d{1,2}/\d{1,2}/\d{4}$"),
            ("%d/%m/%Y", r"^\d{1,2}/\d{1,2}/\d{4}$"),
            ("%Y/%m/%d", r"^\d{4}/\d{1,2}/\d{1,2}$"),
            ("%d.%m.%Y", r"^\d{1,2}\.\d{1,2}\.\d{4}$"),
            ("%b %d, %Y", r"^[A-Za-z]{3} \d{1,2}, \d{4}$"),
            ("%B %d, %Y", r"^[A-Za-z]+ \d{1,2}, \d{4}$"),
            ("%d-%b-%y", r"^\d{1,2}-[A-Za-z]{3}-\d{2}$"),
        ]
        for fmt, pattern in format_tests:
            matches = sum(1 for v in values if re.match(pattern, v.strip()))
            if matches / len(values) >= 0.10:
                detected_formats.append(fmt)

        if not detected_formats:
            detected_formats = ["%Y-%m-%d"]

    return is_dt, detected_formats


def _detect_boolean(
    values: List[str],
    true_set: Set[str] = _BOOL_TRUE,
    false_set: Set[str] = _BOOL_FALSE,
) -> Tuple[bool, List[str], List[str]]:
    """Detect if a column is boolean.

    Returns: (is_boolean, true_values, false_values)
    """
    if not values:
        return False, [], []

    matched = 0
    found_true: Set[str] = set()
    found_false: Set[str] = set()

    for raw in values:
        s = raw.strip().lower()
        if s in true_set:
            matched += 1
            found_true.add(s)
        elif s in false_set:
            matched += 1
            found_false.add(s)

    ratio = matched / len(values)
    is_bool = ratio >= 0.80 and len(found_true) > 0 and len(found_false) > 0

    return is_bool, sorted(found_true), sorted(found_false)


def _detect_protected(col_name: str, patterns: List[str] = _PROTECTED_PATTERNS) -> bool:
    """Detect if a column name suggests protected/PII data.
    
    Normalizes underscores/hyphens to spaces so word-boundary patterns
    like \\bname\\b match inside 'emergency_contact_name'.
    """
    # Normalize separators so \b patterns work across underscored names
    normalized = col_name.replace("_", " ").replace("-", " ")
    for pattern in patterns:
        if re.search(pattern, normalized):
            return True
    return False


def _detect_id_column(col_name: str, series: pd.Series) -> bool:
    """Detect if a column is likely an ID column."""
    name_lower = col_name.lower()
    # Name-based check
    if any(kw in name_lower for kw in ["_id", "id", "code", "key", "ref"]):
        return True
    # High-cardinality string column (likely an ID)
    if series.dtype == object:
        nunique = series.nunique()
        if nunique / max(len(series), 1) > 0.90:
            # Check if values look like IDs (alphanumeric, short)
            sample = series.dropna().head(20)
            if all(len(str(v)) < 50 for v in sample):
                return True
    return False


# ---------------------------------------------------------------------------
# Main inference function
# ---------------------------------------------------------------------------

def infer_policy(
    df: pd.DataFrame,
    dataset_name: Optional[str] = None,
    store: Optional[LearningStore] = None,
) -> Dict[str, Any]:
    """Infer a policy dictionary from a DataFrame.

    Scans each column and classifies it by type using heuristic detectors.
    Merges learned patterns from the LearningStore (if available).
    Returns a policy dict ready to be serialized as YAML.
    """
    if dataset_name is None:
        dataset_name = "inferred_dataset"

    # Load learned patterns
    if store is None:
        store = LearningStore()

    # Merge learned currency symbols into detection
    learned_currencies = store.get_currency_symbols()
    extra_symbols = [s for s in learned_currencies if len(s) == 1]
    extra_codes = [s for s in learned_currencies if len(s) > 1]
    active_currency_symbols = _CURRENCY_SYMBOLS + extra_symbols
    active_currency_codes = _CURRENCY_CODES + extra_codes

    # Merge learned boolean values
    active_bool_true = _BOOL_TRUE | set(store.get_bool_true())
    active_bool_false = _BOOL_FALSE | set(store.get_bool_false())

    # Merge learned protected keywords into patterns
    learned_keywords = store.get_protected_keywords()
    extra_patterns = [rf"(?i)\b{re.escape(kw)}\b" for kw in learned_keywords]
    active_protected_patterns = _PROTECTED_PATTERNS + extra_patterns

    # Column type hints from past policies
    type_hints = store.get_column_type_hints()

    column_types: Dict[str, str] = {}
    boolean_columns: Dict[str, Any] = {}
    datetime_columns: Dict[str, Any] = {}
    protected_cols: List[str] = []
    critical_cols: List[str] = []
    all_currency_symbols: Set[str] = set()
    has_percent_cols: Set[str] = set()

    # Always treat _row_id as critical
    if "_row_id" in df.columns:
        critical_cols.append("_row_id")

    for col in df.columns:
        if col == "_row_id":
            continue

        series = df[col]

        # Sample non-null, non-missing values
        sample_values: List[str] = []
        for val in series:
            if not _is_null_token(val):
                sample_values.append(str(val).strip())
            if len(sample_values) >= _SAMPLE_SIZE:
                break

        # Skip if too few values to infer anything
        if len(sample_values) < 5:
            if _detect_protected(col, active_protected_patterns) or _detect_id_column(col, series):
                protected_cols.append(col)
            continue

        # 1. Check if ID column
        if _detect_id_column(col, series):
            critical_cols.append(col)
            protected_cols.append(col)
            continue

        # 2. Check if protected by name
        if _detect_protected(col, active_protected_patterns):
            protected_cols.append(col)
            continue

        # 3. Try boolean detection first (subset of text)
        is_bool, true_vals, false_vals = _detect_boolean(
            sample_values, true_set=active_bool_true, false_set=active_bool_false
        )
        if is_bool:
            boolean_columns[col] = {
                "true_values": true_vals,
                "false_values": false_vals,
            }
            continue

        # 4. Try numeric detection
        is_numeric, has_pct, currencies = _detect_numeric(
            sample_values, symbols=active_currency_symbols, codes=active_currency_codes
        )
        if is_numeric:
            column_types[col] = "float"
            all_currency_symbols.update(currencies)
            if has_pct:
                has_percent_cols.add(col)
            continue

        # 5. Try datetime detection
        is_dt, formats = _detect_datetime(sample_values)
        if is_dt:
            datetime_columns[col] = {
                "formats": formats,
                "on_failure": "null",
            }
            continue

        # 6. Content-based PII detection (phone numbers, SSNs, emails, etc.)
        pii_match_count = 0
        for v in sample_values[:50]:  # Check first 50 values
            for pattern in _PII_CONTENT_PATTERNS:
                if pattern.match(v):
                    pii_match_count += 1
                    break
        if pii_match_count / min(len(sample_values), 50) >= 0.30:
            protected_cols.append(col)
            continue

        # 7. Fallback: text — protect if high cardinality or long values
        avg_len = sum(len(v) for v in sample_values) / max(len(sample_values), 1)
        if avg_len > 20:
            protected_cols.append(col)

    # Build the policy dict
    policy: Dict[str, Any] = {
        "dataset": {"name": dataset_name},
        "reproducibility": {"policy_hash": f"{dataset_name}_auto_v1"},
        "roles": {
            "critical_columns": sorted(set(critical_cols)),
            "protected_columns": sorted(set(protected_cols)),
        },
        "standardization": {
            "unicode_normalization": "NFKC",
            "strip_whitespace": True,
            "case_mode": None,
        },
        "parsing": {
            "column_types": column_types,
        },
    }

    # Add currency symbols if detected
    if all_currency_symbols:
        policy["parsing"]["currency_symbols"] = sorted(all_currency_symbols)

    # Add percent handling if detected
    if has_percent_cols:
        policy["parsing"]["percent_handling"] = "strip_symbol"

    # Add thousands separator (always useful)
    policy["parsing"]["thousands_separator"] = ","

    # Add boolean columns if detected
    if boolean_columns:
        policy["parsing"]["boolean_columns"] = boolean_columns

    # Add datetime columns if detected
    if datetime_columns:
        policy["datetime"] = {"columns": datetime_columns}

    # Add imputation for numeric columns
    if column_types:
        policy["imputation"] = {
            "strategy": "median",
            "columns": sorted(column_types.keys()),
        }

    return policy
