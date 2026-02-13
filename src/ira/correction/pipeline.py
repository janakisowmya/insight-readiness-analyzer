from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import pandas as pd

from ira.correction.standardize import apply_standardization
from ira.correction.parsing import apply_parsing
from ira.correction.missing import drop_critical_missing, apply_imputation
from ira.reporting.audit import AuditLogger


def run_correction_pipeline(
    df: pd.DataFrame,
    policy: Dict[str, Any],
    audit: Optional[AuditLogger] = None,
    *,
    clock: Optional[Callable[[], str]] = None,
) -> pd.DataFrame:
    """
    Coordination module for the full data correction pipeline.
    Runs phases in exact order:
    1. Standardization (Clean strings)
    2. Parsing (Convert types & flags failures)
    3. Drop Critical Missing (Remove rows missing critical fields)
    4. Imputation (Fill remaining missing values)

    Hardening: Copies the input DataFrame at the start to prevent side-effects.
    """
    def _require_df(obj: Any, phase: str) -> pd.DataFrame:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Phase '{phase}' must return a pandas DataFrame, got {type(obj)}")
        return obj

    # Defensive copy for safe profiling/reuse
    df = df.copy()

    # 1. Standardization
    df = _require_df(apply_standardization(df, policy, audit=audit, clock=clock), "standardization")

    # 2. Parsing
    df = _require_df(apply_parsing(df, policy, audit=audit, clock=clock), "parsing")

    # 2b. Validity
    from ira.correction.validity import apply_validity
    df = _require_df(apply_validity(df, policy, audit=audit, clock=clock), "validity")

    # 3. Drop Critical Missing
    df = _require_df(drop_critical_missing(df, policy, audit=audit, clock=clock), "drop_critical_missing")

    # 4. Imputation
    df = _require_df(apply_imputation(df, policy, audit=audit, clock=clock), "imputation")

    return df
