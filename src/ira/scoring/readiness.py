from __future__ import annotations

from typing import Any, Dict


def calculate_readiness_score(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate a deterministic Insight Readiness Score (0-100).
     Rubric:
    - Completeness (40%): Weighted by roles (critical columns = 2x).
    - Validity (30%): Success rate of parsing to policy-defined types.
    - Uniqueness (20%): 1 - dupe_pct (exact + PK).
    - Consistency (10%): Penalty for mixed columns.
    - Critical Penalty: -10 if any critical column has issues.
    """
    cols_stats = profile.get("columns", {})
    metadata = profile.get("metadata", {})
    row_count = metadata.get("row_count", 0)
    
    if row_count == 0:
        return {
            "score": 0.0,
            "breakdown": {
                "completeness": 0.0,
                "validity": 0.0,
                "uniqueness": 0.0,
                "consistency": 0.0
            }
        }

    # 1. Completeness (40%)
    # Average of (1 - effective_missing_pct) weighted by role
    comp_total_weight = 0.0
    comp_weighted_sum = 0.0
    for col, stats in cols_stats.items():
        weight = 2.0 if stats.get("role") == "critical" else 1.0
        success_rate = 1.0 - stats.get("effective_missing_pct", 0.0)
        comp_total_weight += weight
        comp_weighted_sum += success_rate * weight
    
    completeness_score = (comp_weighted_sum / comp_total_weight) if comp_total_weight > 0 else 1.0
    
    # 2. Validity (30%)
    # Average of (1 - invalid_type_pct) for columns with a target type
    valid_total_weight = 0
    valid_sum = 0.0
    for col, stats in cols_stats.items():
        if stats.get("invalid_type_pct") is not None:
            valid_total_weight += 1
            valid_sum += (1.0 - stats.get("invalid_type_pct", 0.0))
    
    validity_score = (valid_sum / valid_total_weight) if valid_total_weight > 0 else 1.0
    
    # 3. Uniqueness (20%)
    # 1 - max(exact_row_dupe_pct, pk_dupe_pct)
    dupes = profile.get("duplicates", {})
    exact_dupe_pct = float(dupes.get("exact_row_dupe_count", 0) / row_count)
    
    pk_dupes_count = dupes.get("pk_dupe_count")
    if pk_dupes_count is not None:
        pk_dupe_pct = float(pk_dupes_count / row_count)
        uniqueness_score = 1.0 - max(exact_dupe_pct, pk_dupe_pct)
    else:
        uniqueness_score = 1.0 - exact_dupe_pct
        
    # 4. Consistency (10%)
    # % of columns that are NOT mixed-type
    mixed_count = sum(1 for stats in cols_stats.values() if stats.get("is_mixed_type"))
    total_cols = len(cols_stats)
    consistency_score = (1.0 - (mixed_count / total_cols)) if total_cols > 0 else 1.0
    
    # Weighting
    final_score = (
        (completeness_score * 40.0) +
        (validity_score * 30.0) +
        (uniqueness_score * 20.0) +
        (consistency_score * 10.0)
    )
    
    # 5. Critical Penalty (-10)
    # If any critical column has effective_missing_pct > 0 or invalid_type_pct > 0
    has_critical_issue = False
    for col, stats in cols_stats.items():
        if stats.get("role") == "critical":
            if stats.get("effective_missing_pct", 0) > 0:
                has_critical_issue = True
                break
            if stats.get("invalid_type_pct", 0) and stats.get("invalid_type_pct", 0) > 0:
                has_critical_issue = True
                break
                
    if has_critical_issue:
        final_score -= 10.0
        
    # Bounded 0-100
    final_score = max(0.0, min(100.0, final_score))
    
    return {
        "score": round(final_score, 2),
        "breakdown": {
            "completeness": round(completeness_score * 40.0, 2),
            "validity": round(validity_score * 30.0, 2),
            "uniqueness": round(uniqueness_score * 20.0, 2),
            "consistency": round(consistency_score * 10.0, 2)
        }
    }
