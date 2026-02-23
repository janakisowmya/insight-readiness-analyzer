from __future__ import annotations

import random
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import numpy as np


class ReservoirSampler:
    """
    Maintains a representative sample of a stream of data using Algorithm R.
    Used for approximating median/quantiles on large datasets.
    """
    def __init__(self, size: int = 100000):
        self.size = size
        self.reservoir: List[float] = []
        self.n_seen = 0

    def update(self, values: Union[pd.Series, np.ndarray, List[Any]]) -> None:
        """Update reservoir with new values."""
        # Convert to list/array of valid numeric values
        if isinstance(values, pd.Series):
            values = values.dropna().values
        
        for v in values:
            if not isinstance(v, (int, float, np.number)) or np.isnan(v):
                continue
            
            self.n_seen += 1
            if len(self.reservoir) < self.size:
                self.reservoir.append(float(v))
            else:
                # Replace with probability size/n_seen
                r = random.randint(0, self.n_seen - 1)
                if r < self.size:
                    self.reservoir[r] = float(v)

    def get_median(self) -> Optional[float]:
        if not self.reservoir:
            return None
        return float(np.median(self.reservoir))


class StatsAccumulator:
    """
    Accumulates global statistics across data chunks.
    Tracks:
    - Numeric: Count, Sum, Min, Max, Mean (derived), Median (approx via reservoir)
    - Categorical: Mode (via value counts)
    """
    def __init__(self, numeric_cols: List[str], categorical_cols: List[str]):
        self.numeric_cols = numeric_cols
        self.categorical_cols = categorical_cols
        
        # Numeric State
        self.n_counts = {c: 0 for c in numeric_cols}
        self.sums = {c: 0.0 for c in numeric_cols}
        self.mins = {c: float('inf') for c in numeric_cols}
        self.maxs = {c: float('-inf') for c in numeric_cols}
        self.reservoirs = {c: ReservoirSampler(size=50000) for c in numeric_cols}
        
        # Categorical State
        # We limit the tracking to avoid memory issues with high cardinality.
        # However, for 'mode' imputation, we need the most frequent.
        # We'll use pandas value_counts per chunk and aggregate.
        self.value_counts: Dict[str, pd.Series] = {}

    def update(self, df: pd.DataFrame) -> None:
        # Numeric updates
        for col in self.numeric_cols:
            if col not in df.columns:
                continue
            
            s = pd.to_numeric(df[col], errors='coerce').dropna()
            if s.empty:
                continue
                
            count = len(s)
            self.n_counts[col] += count
            self.sums[col] += s.sum()
            self.mins[col] = min(self.mins[col], s.min())
            self.maxs[col] = max(self.maxs[col], s.max())
            self.reservoirs[col].update(s.values)

        # Categorical updates
        for col in self.categorical_cols:
            if col not in df.columns:
                continue
            
            # Count values in this chunk
            vc = df[col].value_counts()
            if col not in self.value_counts:
                self.value_counts[col] = vc
            else:
                # Add to existing counts
                self.value_counts[col] = self.value_counts[col].add(vc, fill_value=0)
                
                # Pruning: If table gets too big (> 10k unique), keep top 5000 to save RAM?
                # Risk: Early chunks might dominate. 
                # For safety against memory attacks/issues, let's prune if > 50k keys
                if len(self.value_counts[col]) > 50000:
                    self.value_counts[col] = self.value_counts[col].nlargest(25000)

    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns a dictionary suitable for passing to apply_imputation logic.
        Format: {col_name: {'mean': val, 'median': val, 'mode': val, ...}}
        """
        result = {}
        
        # Numeric Stats
        for col in self.numeric_cols:
            if self.n_counts[col] > 0:
                mean_val = self.sums[col] / self.n_counts[col]
                median_val = self.reservoirs[col].get_median()
                
                result[col] = {
                    "mean": mean_val,
                    "median": median_val,
                    "min": self.mins[col],
                    "max": self.maxs[col],
                    "count": self.n_counts[col]
                }
            else:
                result[col] = {"mean": None, "median": None}

        # Categorical Stats
        for col in self.categorical_cols:
            if col in self.value_counts and not self.value_counts[col].empty:
                # Mode is the top index
                mode_val = self.value_counts[col].idxmax()
                result[col] = {"mode": mode_val}
            else:
                result[col] = {"mode": None}
                
        return result
