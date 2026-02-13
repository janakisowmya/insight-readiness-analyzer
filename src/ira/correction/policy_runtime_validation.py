from __future__ import annotations
from typing import Any, Dict, List, Tuple
import pandas as pd

def validate_policy_against_df(policy: Dict[str, Any], df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Validates policy settings against the actual DataFrame content.
    Returns (errors, warnings).
    """
    errors = []
    warnings = []
    
    cols = set(df.columns)
    roles = policy.get("roles", {})
    critical = set(roles.get("critical_columns", []))
    protected = set(roles.get("protected_columns", []))
    
    # 1. Missing Column Checks
    # Error if critical column missing
    missing_critical = critical - cols
    if missing_critical:
        errors.append(f"Critical columns missing from dataset: {sorted(missing_critical)}")
        
    # Warning if other referenced columns missing
    parsing_types = policy.get("parsing", {}).get("column_types", {})
    missing_parsing = (set(parsing_types.keys()) - cols) - critical
    if missing_parsing:
        warnings.append(f"Columns in parsing.column_types missing from dataset: {sorted(missing_parsing)}")
        
    # 2. Imputation Thresholds
    impute_cfg = policy.get("missing_data", {}).get("imputation", {})
    for bucket in ["numeric", "categorical", "datetime"]:
        cfg = impute_cfg.get(bucket, {})
        thr = cfg.get("allow_if_missing_pct_leq")
        if thr is not None and not (0.0 <= thr <= 1.0):
            errors.append(f"imputation.{bucket}.allow_if_missing_pct_leq must be between 0 and 1 (got {thr})")
            
    # 3. Policy Alignment Warnings
    missing_data = policy.get("missing_data", {})
    if critical and not missing_data.get("drop_if_missing_critical", True):
        warnings.append("Critical columns defined but drop_if_missing_critical is False. Rows with missing IDs will remain.")
        
    # Primary Key check
    pk_cols = policy.get("dataset", {}).get("primary_key", {}).get("columns", [])
    missing_pk = set(pk_cols) - cols
    if missing_pk:
        warnings.append(f"Primary key columns missing from dataset: {sorted(missing_pk)}")
        
    return errors, warnings
