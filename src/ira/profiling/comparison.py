from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import pandas as pd

from ira.profiling.profile import create_profile
from ira.correction.pipeline import run_correction_pipeline


def create_profile_with_transformation(
    df: pd.DataFrame,
    policy: Dict[str, Any],
    *,
    clock: Optional[Callable[[], str]] = None,
) -> Dict[str, Any]:
    """
    Generate a deterministic profile with Before/After readiness comparison.
    This sequences the RAW profile, then runs the correction pipeline on a copy,
    then runs an OPTIMIZED profile to calculate the delta.
    """
    # 1. RAW
    raw_profile = create_profile(df, policy, clock=clock)

    # 2. OPTIMIZED (pipeline copies internally; still safe)
    df_after = run_correction_pipeline(df, policy, audit=None, clock=clock)
    optimized_profile = create_profile(df_after, policy, clock=clock)

    raw_readiness = raw_profile.get("readiness") or {}
    opt_readiness = optimized_profile.get("readiness") or {}

    raw_score = float(raw_readiness.get("score") or 0.0)
    opt_score = float(opt_readiness.get("score") or 0.0)

    # Harden mutable dicts and ensure canonical schema keys
    raw_bd = dict(raw_readiness.get("breakdown") or {})
    opt_bd = dict(opt_readiness.get("breakdown") or {})
    
    keys = ["completeness", "validity", "uniqueness", "consistency"]

    transformation = {
        "raw": {"score": raw_score, "breakdown": raw_bd},
        "optimized": {"score": opt_score, "breakdown": opt_bd},
        "delta": {
            "score": round(opt_score - raw_score, 2),
            "breakdown": {
                k: round(float(opt_bd.get(k, 0.0)) - float(raw_bd.get(k, 0.0)), 2) 
                for k in keys
            },
        },
        "meta": {
            "rows_raw": int(raw_profile.get("metadata", {}).get("row_count", 0) or 0),
            "rows_optimized": int(optimized_profile.get("metadata", {}).get("row_count", 0) or 0),
        },
    }

    # Final result uses the RAW profile details but adds the transformation comparison
    result = dict(raw_profile)
    result["readiness"] = {
        "score": raw_score,
        "breakdown": raw_bd,
        "transformation": transformation,
    }

    return result
