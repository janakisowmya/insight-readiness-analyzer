from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import yaml

from ira.correction.policy_schema import load_policy
from ira.profiling.profile import create_profile
from ira.profiling.comparison import create_profile_with_transformation
from ira.profiling.comparison import create_profile_with_transformation
from ira.profiling.infer_policy import infer_policy
from ira.profiling.learned_patterns import LearningStore
from ira.reporting.audit import AuditLogger
from ira.correction.pipeline import run_correction_pipeline
from ira.reporting.report import build_correction_report, generate_markdown_report
from ira.correction.policy_runtime_validation import validate_policy_against_df




def _read_dataset(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"ERROR: input file not found: {path}")
    try:
        if path.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(path, **kwargs)
        else:
            # Use Python engine for resilience against malformed rows
            # (e.g. spaces before quoted fields, extra/missing columns).
            # on_bad_lines="warn" keeps the pipeline running and logs issues.
            df = pd.read_csv(
                path,
                engine="python",
                on_bad_lines="warn",
                skipinitialspace=True,
                encoding="utf-8-sig",  # Handles BOM prefix
                **kwargs
            )
        
        # Strip any remaining BOM or whitespace from column names
        df.columns = [str(c).lstrip('\ufeff').strip() for c in df.columns]
        
        if "_row_id" in df.columns:
            # Coerce non-numeric _row_id values (e.g. 41.5, blanks) to NaN,
            # then floor fractional values before casting to nullable Int64.
            numeric_ids = pd.to_numeric(df["_row_id"], errors="coerce")
            import numpy as np
            df["_row_id"] = np.floor(numeric_ids).astype("Int64")
        else:
            # Auto-generate 1-based index if missing
            import numpy as np
            # Create as standard numpy int array first, then cast to nullable Int64 via Series assignment
            # (numpy arrays don't support "Int64" string alias directly in older versions or at all)
            ids = np.arange(1, len(df) + 1)
            df["_row_id"] = pd.Series(ids, index=df.index).astype("Int64")
        return df
    except Exception as e:
        raise SystemExit(f"ERROR: failed to read dataset: {path} ({e})") from e


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(
            json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    except Exception as e:
        raise SystemExit(f"ERROR: failed to write JSON: {path} ({e})") from e


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    if path.parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(path, index=False)
    except Exception as e:
        raise SystemExit(f"ERROR: failed to write CSV: {path} ({e})") from e


def _cmd_profile(args: argparse.Namespace) -> int:
    in_path = Path(args.input)
    out_path = Path(args.out)
    policy_path = Path(args.policy)

    df = _read_dataset(in_path)
    
    if "_row_id" not in df.columns:
        raise SystemExit("Error: Dataset must contain a '_row_id' column.")

    policy = load_policy(policy_path)

    # Runtime Validation
    errors, warnings = validate_policy_against_df(policy, df)
    if errors:
        msg = "\n".join([f"  - {e}" for e in errors])
        raise SystemExit(f"ERROR: Policy validation failed against dataset:\n{msg}")
    
    if warnings and not args.quiet:
        for w in warnings:
            print(f"WARNING: {w}")

    # Deterministic clock for tests if provided
    clock = (lambda: args.clock) if args.clock else None
    
    if args.compare:
        prof = create_profile_with_transformation(df, policy, clock=clock)
    else:
        prof = create_profile(df, policy, clock=clock)

    _write_json(out_path, prof)

    if not args.quiet:
        print(f"Wrote profile to {out_path}")

    return 0


def _cmd_infer(args: argparse.Namespace) -> int:
    """Infer a policy YAML from a CSV by auto-detecting column types."""
    in_path = Path(args.input)
    out_path = Path(args.out)

    # Large file support: Read only 100k rows for inference
    df = _read_dataset(in_path, nrows=100000)

    dataset_name = args.name or in_path.stem
    policy = infer_policy(df, dataset_name=dataset_name)

    # Write as YAML
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(policy, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    if not args.quiet:
        # Print a summary of what was detected
        col_types = policy.get("parsing", {}).get("column_types", {})
        bool_cols = policy.get("parsing", {}).get("boolean_columns", {})
        dt_cols = policy.get("datetime", {}).get("columns", {})
        protected = policy.get("roles", {}).get("protected_columns", [])
        critical = policy.get("roles", {}).get("critical_columns", [])

        print(f"Inferred policy from {in_path} ({len(df)} rows, {len(df.columns)} columns)")
        print(f"  Numeric columns:   {list(col_types.keys()) or '(none)'}")
        print(f"  Boolean columns:   {list(bool_cols.keys()) or '(none)'}")
        print(f"  Datetime columns:  {list(dt_cols.keys()) or '(none)'}")
        print(f"  Protected columns: {protected or '(none)'}")
        print(f"  Critical columns:  {critical or '(none)'}")
        currencies = policy.get("parsing", {}).get("currency_symbols", [])
        if currencies:
            print(f"  Currency symbols:  {currencies}")
        if policy.get("parsing", {}).get("percent_handling"):
            print(f"  Percent handling:  strip_symbol")
        print(f"Wrote policy to {out_path}")

    return 0


def _cmd_learn(args: argparse.Namespace) -> int:
    """Manage learned patterns."""
    store = LearningStore()
    
    if args.reset:
        store.reset()
        if not args.quiet:
            print("Reset all learned patterns.")
        return 0
        
    # Default: show summary
    print(store.summary())
    return 0



def _cmd_correct(args: argparse.Namespace) -> int:
    import warnings
    # Suppress specific pandas/dateutil warnings for cleaner CLI output
    warnings.filterwarnings("ignore", message="Parsing dates in .* format when dayfirst=False")

    in_path = Path(args.input)
    out_path = Path(args.out)
    audit_path = Path(args.audit)
    chunksize = int(args.chunksize) if args.chunksize else None

    # Load policy
    if args.auto_policy:
        if not args.quiet:
            print("Auto-inferring policy from data sample...")
        # Infer from sample (100k rows)
        df_sample = _read_dataset(in_path, nrows=100000)
        policy = infer_policy(df_sample, dataset_name=in_path.stem)
    elif args.policy:
        policy = load_policy(Path(args.policy))
    else:
        raise SystemExit("ERROR: Must provide --policy <path> or use --auto-policy")

    # Determine processing mode
    if chunksize is None:
        # Standard In-Memory Processing
        df = _read_dataset(in_path)
        
        # Runtime Validation (Full Data)
        errors, warnings = validate_policy_against_df(policy, df)
        if errors:
            msg = "\n".join([f"  - {e}" for e in errors])
            raise SystemExit(f"ERROR: Policy validation failed:\n{msg}")
        
        if warnings and not args.quiet:
            for w in warnings:
                print(f"WARNING: {w}")

        clock = (lambda: args.clock) if args.clock else None
        
        # Raw Profile for Report
        prof_raw = create_profile(df, policy, clock=clock)

        # Audit Setup
        detail = (args.audit_detail or "summary").strip().lower()
        if detail not in ("summary", "detailed"):
            raise SystemExit("ERROR: --audit-detail must be 'summary' or 'detailed'")

        logger = AuditLogger(audit_path, detail=detail)
        try:
            logger.set_context(
                dataset_name=policy.get("dataset", {}).get("name"),
                policy_hash=policy.get("reproducibility", {}).get("policy_hash"),
                rows_in=len(df),
                run_timestamp=clock() if clock else None
            )
            out_df = run_correction_pipeline(df, policy, audit=logger, clock=clock)
            logger.set_context(rows_out=len(out_df))
        finally:
            logger.close()

        _write_csv(out_path, out_df)
        
        # Post-process stats (for report/learning)
        final_df = out_df

    else:
        # Scalable Chunked Processing (Two-Pass)
        if not args.quiet:
            print(f"Running in chunked mode (chunksize={chunksize})...")

        from ira.profiling.accumulators import StatsAccumulator
        
        # Identify columns for stats gathering
        # We need to know which are numeric/categorical to init accumulator
        # We can use policy types if valid, or infer from first chunk
        parsing_cfg = policy.get("parsing", {})
        col_types = parsing_cfg.get("column_types", {})
        
        # Pass 1: Global Stats Gathering
        if not args.quiet:
            print("Pass 1: Gathering global statistics...")
            
        acc: Optional[StatsAccumulator] = None
        
        # Helper to read chunks
        # pandas read_csv(chunksize=N) returns iterator
        # read_excel does not support chunksize natively easily, we might need manual handling
        # For V1 scalability, we primarily target CSV. Excel is usually file-size limited anyway.
        if in_path.suffix.lower() in (".xlsx", ".xls"):
             if not args.quiet:
                 print("WARNING: Chunked processing not fully supported for Excel. Reading full file then chunking in memory (not true streaming).")
             # Fallback: Read full, then iterate df slices
             full_df = pd.read_excel(in_path) 
             # ... wait, that defeats the purpose. But users rarely have 2GB Excel files.
             # If they do, they should convert to CSV first.
             # We will just raise if they try chunked excel for now, or warn and process in-memory.
             # To be safe: Load full, then chunk loop.
             pass_1_iterator = (full_df.iloc[i:i+chunksize] for i in range(0, len(full_df), chunksize))
        else:
            pass_1_iterator = pd.read_csv(
                in_path, 
                chunksize=chunksize, 
                engine="python", 
                on_bad_lines="warn",
                skipinitialspace=True,
                encoding="utf-8-sig"
            )

        for i, chunk in enumerate(pass_1_iterator):
             # Basic column name cleaning on first chunk to match what _read_dataset does
             chunk.columns = [str(c).lstrip('\ufeff').strip() for c in chunk.columns]
             
             if acc is None:
                 # Initialize accumulator based on columns in first chunk + policy
                 # Simple heuristic: treat all columns as unknowns unless typed in policy
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
             if not args.quiet and i % 10 == 0:
                 print(f"  Scanned {i+1} chunks...", end="\r")


        if acc is None:
            raise SystemExit("Error: Empty dataset or failed to read any chunks.")
            
        global_stats = acc.get_stats()
        if not args.quiet:
            # Show a sample statistic for verification
            sample_col = next((c for c in global_stats if global_stats[c].get("median") is not None), None)
            if sample_col:
                print(f"  Stats gathered. Example ({sample_col} median): {global_stats[sample_col]['median']}")
            else:
                 print("  Stats gathered (no numeric columns found for median).")
        
        # Pass 2: Correction & Writing
        if not args.quiet:
            print("Pass 2: Applying correction pipeline...")
            
        # Re-open iterator
        if in_path.suffix.lower() in (".xlsx", ".xls"):
             pass_2_iterator = (full_df.iloc[i:i+chunksize] for i in range(0, len(full_df), chunksize))
        else:
            pass_2_iterator = pd.read_csv(
                in_path, 
                chunksize=chunksize, 
                engine="python", 
                on_bad_lines="warn",
                skipinitialspace=True,
                encoding="utf-8-sig"
            )

        # Setup Audit
        detail = (args.audit_detail or "summary").strip().lower()
        logger = AuditLogger(audit_path, detail=detail)
        clock = (lambda: args.clock) if args.clock else None
        
        # Prepare Output: Write header only once
        if out_path.exists():
            out_path.unlink() # clear existing
            
        # Audit Context
        logger.set_context(
            dataset_name=policy.get("dataset", {}).get("name"),
            run_timestamp=clock() if clock else None
        )
        
        total_rows_out = 0

        # Suppress noisy date parsing warnings from pandas/dateutil
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Parsing dates in .* format when dayfirst=False")
            
            try:
                write_header = True
                for i, chunk in enumerate(pass_2_iterator):
                    # Clean cols
                    chunk.columns = [str(c).lstrip('\ufeff').strip() for c in chunk.columns]
                    
                    # Missing ID generation (per chunk needs offset handling)
                    if "_row_id" not in chunk.columns:
                        # We need global offset logic
                        # chunk.index usually resets or matches CSV line number depending on pandas version/args
                        # Safest is to use a running counter
                        offset = i * chunksize
                        ids = range(offset + 1, offset + len(chunk) + 1)
                        chunk["_row_id"] = pd.Series(ids, index=chunk.index).astype("Int64")
                    
                    # Apply pipeline
                    # Note: validaton skipped per-chunk to save time/noise, or we can validate first chunk only?
                    # We'll rely on pipeline resilience.
                    
                    processed_chunk = run_correction_pipeline(
                        chunk, 
                        policy, 
                        audit=logger, 
                        clock=clock, 
                        imputation_stats=global_stats
                    )
                    
                    # Write
                    processed_chunk.to_csv(out_path, mode='a', header=write_header, index=False)
                    write_header = False
                    total_rows_out += len(processed_chunk)
                    
                    if not args.quiet and i % 10 == 0:
                         print(f"  Processed {i+1} chunks...", end="\r")
                         
                logger.set_context(rows_out=total_rows_out)
            finally:
                logger.close()

            
        # For report, we need a final df? 
        # Only feasible if it fits in memory. For scalability, we might skip the full profile/report
        # or compute it via streaming (Phase 11?).
        # For now, we set final_df = None and warn if report requested.
        final_df = None
        if args.report or args.report_md:
            if not args.quiet:
                print("WARNING: Full reporting not yet supported in chunked mode (skipped).")

    # Common Cleanup / Learning (only if in-memory df available)
    if args.policy and final_df is not None:
        try:
            store = LearningStore()
            n_learned = store.learn_from_policy(policy)
            if n_learned > 0 and not args.quiet:
                print(f"Learned {n_learned} new patterns from your policy.")
        except Exception as e:
            if not args.quiet:
                print(f"Warning: Failed to learn from policy: {e}")

    # Report Generation (In-Memory Only)
    if final_df is not None and (args.report or args.report_md):
         prof_opt = create_profile(final_df, policy, clock=clock)
         report = build_correction_report(prof_raw, prof_opt, audit_path, policy) # prof_raw from above
         
         if args.report:
             _write_json(Path(args.report), report)
             if not args.quiet:
                 print(f"Wrote report to {args.report}")
                 
         if args.report_md:
             md_path = Path(args.report_md)
             md_path.parent.mkdir(parents=True, exist_ok=True)
             md = generate_markdown_report(report)
             md_path.write_text(md, encoding="utf-8")
             if not args.quiet:
                 print(f"Wrote markdown report to {args.report_md}")

    if not args.quiet:
        print(f"\nDone. Wrote cleaned CSV to {out_path}")
        print(f"Wrote audit log to {audit_path}")

    return 0



def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ira", description="Insight Readiness Analyzer")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("profile", help="Profile a dataset (read-only) and output JSON summary.")
    sp.add_argument("--input", required=True, help="Path to input CSV")
    sp.add_argument("--policy", required=True, help="Path to policy YAML")
    sp.add_argument("--out", required=True, help="Path to output JSON (profile.json)")
    sp.add_argument("--compare", action="store_true", help="Include Before/After comparison in the readiness report")
    sp.add_argument("--clock", default=None, help="Fixed ISO-8601 timestamp for deterministic runs/tests")
    sp.add_argument("--quiet", action="store_true", help="Suppress stdout message")
    sp.set_defaults(func=_cmd_profile)

    sc = sub.add_parser("correct", help="Apply correction pipeline and output cleaned CSV + audit JSONL.")
    sc.add_argument("--input", required=True, help="Path to input CSV")
    policy_group = sc.add_mutually_exclusive_group(required=True)
    policy_group.add_argument("--policy", help="Path to policy YAML (user-defined)")
    policy_group.add_argument("--auto-policy", action="store_true", help="Auto-infer policy from data instead of providing one")
    sc.add_argument("--out", required=True, help="Path to output cleaned CSV")
    sc.add_argument("--audit", required=True, help="Path to output audit JSONL")
    sc.add_argument("--chunksize", type=int, default=None, help="Process in chunks of N rows (for large files)")
    sc.add_argument("--audit-detail", default="summary", help="summary (default) or detailed")
    sc.add_argument("--report", help="Path to output correction report JSON")
    sc.add_argument("--report-md", help="Path to output correction report Markdown")
    sc.add_argument("--clock", default=None, help="Fixed ISO-8601 timestamp for deterministic runs/tests")
    sc.add_argument("--quiet", action="store_true", help="Suppress stdout message")
    sc.set_defaults(func=_cmd_correct)

    # --- infer command (standalone policy generation) ---
    si = sub.add_parser("infer", help="Auto-infer a policy YAML from a CSV by detecting column types.")
    si.add_argument("--input", required=True, help="Path to input CSV")
    si.add_argument("--out", required=True, help="Path to output policy YAML")
    si.add_argument("--name", default=None, help="Dataset name for the policy (default: filename stem)")
    si.add_argument("--quiet", action="store_true", help="Suppress stdout messages")
    si.set_defaults(func=_cmd_infer)

    # --- learn command (manage learned patterns) ---
    sl = sub.add_parser("learn", help="View or reset learned policy patterns.")
    sl.add_argument("--reset", action="store_true", help="Reset all learned patterns")
    sl.add_argument("--quiet", action="store_true", help="Suppress stdout messages")
    sl.set_defaults(func=_cmd_learn)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    import sys
    sys.exit(main())
