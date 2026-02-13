from __future__ import annotations

import pytest
from ira.scoring.readiness import calculate_readiness_score


def test_perfect_score():
    """Verify that a perfect dataset gets a 100.0 score."""
    profile = {
        "metadata": {"row_count": 100},
        "duplicates": {"exact_row_dupe_count": 0, "pk_dupe_count": 0},
        "columns": {
            "col1": {
                "role": "critical",
                "effective_missing_pct": 0.0,
                "invalid_type_pct": 0.0,
                "is_mixed_type": False
            },
            "col2": {
                "role": "none",
                "effective_missing_pct": 0.0,
                "invalid_type_pct": 0.0,
                "is_mixed_type": False
            }
        }
    }
    result = calculate_readiness_score(profile)
    assert result["score"] == 100.0
    assert result["breakdown"]["completeness"] == 40.0
    assert result["breakdown"]["validity"] == 30.0
    assert result["breakdown"]["uniqueness"] == 20.0
    assert result["breakdown"]["consistency"] == 10.0


def test_completeness_weighting():
    """Verify critical columns have 2x weight in completeness."""
    # col1 (critical): 50% missing
    # col2 (none): 0% missing
    # Total weight = 2*1.0 + 1*1.0 = 3
    # Weighted success = 0.5 * 2 + 1.0 * 1 = 2.0
    # Completeness Score = 2.0 / 3.0 = 0.666...
    # Score component = 0.666... * 40 = 26.666...
    
    profile = {
        "metadata": {"row_count": 100},
        "duplicates": {"exact_row_dupe_count": 0},
        "columns": {
            "col1": {
                "role": "critical",
                "effective_missing_pct": 0.5,
                "invalid_type_pct": 0.0,
                "is_mixed_type": False
            },
            "col2": {
                "role": "none",
                "effective_missing_pct": 0.0,
                "invalid_type_pct": 0.0,
                "is_mixed_type": False
            }
        }
    }
    result = calculate_readiness_score(profile)
    # 26.67 (rounded) + 30 (validity) + 20 (uniqueness) + 10 (consistency) - 10 (penalty) = 76.67
    assert result["breakdown"]["completeness"] == 26.67
    assert result["score"] == 76.67


def test_critical_penalty():
    """Verify -10 penalty if critical column has any issue."""
    # Perfect except col1 (critical) has 1% missing
    profile = {
        "metadata": {"row_count": 100},
        "duplicates": {"exact_row_dupe_count": 0},
        "columns": {
            "col1": {
                "role": "critical",
                "effective_missing_pct": 0.01,
                "invalid_type_pct": 0.0,
                "is_mixed_type": False
            }
        }
    }
    result = calculate_readiness_score(profile)
    # Completeness: 0.99 * 40 = 39.6
    # Others: 30 + 20 + 10 = 60
    # Total: 99.6 - 10 = 89.6
    assert result["score"] == 89.6


def test_uniqueness_with_pk():
    """Verify uniqueness uses max of exact and pk dupes."""
    profile = {
        "metadata": {"row_count": 100},
        "duplicates": {
            "exact_row_dupe_count": 10, # 10%
            "pk_dupe_count": 20        # 20%
        },
        "columns": {"col1": {"role": "none", "effective_missing_pct": 0, "is_mixed_type": False}}
    }
    result = calculate_readiness_score(profile)
    # Uniqueness = (1 - 0.20) * 20 = 16.0
    assert result["breakdown"]["uniqueness"] == 16.0


def test_consistency_mixed_types():
    """Verify consistency penalty for mixed types."""
    profile = {
        "metadata": {"row_count": 100},
        "duplicates": {"exact_row_dupe_count": 0},
        "columns": {
            "col1": {"role": "none", "effective_missing_pct": 0, "is_mixed_type": True},
            "col2": {"role": "none", "effective_missing_pct": 0, "is_mixed_type": False}
        }
    }
    result = calculate_readiness_score(profile)
    # Consistency = 50% success * 10 = 5.0
    assert result["breakdown"]["consistency"] == 5.0
