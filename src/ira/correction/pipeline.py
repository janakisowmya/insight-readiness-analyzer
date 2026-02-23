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
    imputation_stats: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Coordination module for the full data correction pipeline.
    Runs phases in exact order:
    1. Standardization (Clean strings)
    2. Parsing (Convert types & flags failures)
    3. Drop Critical Missing (Remove rows missing critical fields)
    4. Imputation (Fill remaining missing values)
    
    Args:
        imputation_stats: Optional global stats (e.g. median/mode) for imputation.
                         Used when processing data in chunks.
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
    df = _require_df(apply_imputation(df, policy, audit=audit, clock=clock, precomputed_stats=imputation_stats), "imputation")

    # 5. Cleanup Artifacts
    if "_row_id" in df.columns:
        df = df.drop(columns=["_row_id"])

    return df


def run_chunked_correction(
    input_path: str,
    output_path: str,
    ChunkSize: int,
    policy: Dict[str, Any],
    audit: Optional[AuditLogger] = None,
    clock: Optional[Callable[[], str]] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> int:
    """
    Execute the correction pipeline in chunks (Two-Pass approach).
    Pass 1: Global Statistics Gathering (for imputation)
    Pass 2: Correction & Output Writing
    
    Args:
        progress_callback: Optional function(current_chunk, total_chunks_estimate) for UI updates.
    """
    import os
    from ira.profiling.accumulators import StatsAccumulator
    
    in_path = input_path
    
    # Identify columns for stats gathering
    parsing_cfg = policy.get("parsing", {})
    col_types = parsing_cfg.get("column_types", {})
    
    # --- Pass 1: Global Stats Gathering ---
    acc: Optional[StatsAccumulator] = None
    
    # Helper to read chunks
    def get_iterator():
        return pd.read_csv(
            in_path, 
            chunksize=ChunkSize, 
            engine="python", 
            on_bad_lines="warn",
            skipinitialspace=True,
            encoding="utf-8-sig"
        )
        
    try:
        pass_1_iterator = get_iterator()
    except Exception as e:
        raise ValueError(f"Failed to read input file: {e}")

    total_chunks_estimate = os.path.getsize(in_path) // (ChunkSize * 100) # Rough estimate, purely for UI feedback
    if total_chunks_estimate == 0: total_chunks_estimate = 1
    
    chunk_count = 0
    for i, chunk in enumerate(pass_1_iterator):
         chunk_count += 1
         # Basic column name cleaning
         chunk.columns = [str(c).lstrip('\ufeff').strip() for c in chunk.columns]
         
         if acc is None:
             # Initialize accumulator based on columns in first chunk + policy
             numeric_cols = []
             cat_cols = []
             for c in chunk.columns:
                 ctype = col_types.get(c)
                 if ctype in ("numeric", "integer", "float"):
                     numeric_cols.append(c)
                 elif ctype == "categorical":
                     cat_cols.append(c)
                 elif pd.api.types.is_numeric_dtype(chunk[c]):
                     numeric_cols.append(c)
                 else:
                     cat_cols.append(c)
             acc = StatsAccumulator(numeric_cols, cat_cols)
        
         acc.update(chunk)
         if progress_callback:
             progress_callback(i, total_chunks_estimate)

    if acc is None:
        raise ValueError("Error: Empty dataset or failed to read any chunks.")
        
    global_stats = acc.get_stats()
    
    # --- Pass 2: Correction & Writing ---
    pass_2_iterator = get_iterator()

    # Prepare Output: Write header only once
    import pathlib
    out_p = pathlib.Path(output_path)
    if out_p.exists():
        out_p.unlink()
        
    total_rows_out = 0
    
    # Suppress noisy date parsing warnings
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Parsing dates in .* format when dayfirst=False")
        
        write_header = True
        for i, chunk in enumerate(pass_2_iterator):
            # Clean cols
            chunk.columns = [str(c).lstrip('\ufeff').strip() for c in chunk.columns]
            
            # Missing ID generation
            if "_row_id" not in chunk.columns:
                offset = i * ChunkSize
                ids = range(offset + 1, offset + len(chunk) + 1)
                chunk["_row_id"] = pd.Series(ids, index=chunk.index).astype("Int64")
            
            processed_chunk = run_correction_pipeline(
                chunk, 
                policy, 
                audit=audit, 
                clock=clock, 
                imputation_stats=global_stats
            )
            
            processed_chunk.to_csv(output_path, mode='a', header=write_header, index=False)
            write_header = False
            total_rows_out += len(processed_chunk)
            
            if progress_callback:
                 progress_callback(chunk_count + i, total_chunks_estimate * 2)
                 
    return total_rows_out

