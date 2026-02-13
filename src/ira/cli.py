from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

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


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"ERROR: input file not found: {path}")
    try:
        # Use Python engine for resilience against malformed rows
        # (e.g. spaces before quoted fields, extra/missing columns).
        # on_bad_lines="warn" keeps the pipeline running and logs issues.
        df = pd.read_csv(
            path,
            engine="python",
            on_bad_lines="warn",
            skipinitialspace=True,
            encoding="utf-8-sig",  # Handles BOM prefix
        )
        # Strip any remaining BOM or whitespace from column names
        df.columns = [c.lstrip('\ufeff').strip() for c in df.columns]
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
        raise SystemExit(f"ERROR: failed to read CSV: {path} ({e})") from e


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

    df = _read_csv(in_path)
    
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

    df = _read_csv(in_path)

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
    in_path = Path(args.input)
    out_path = Path(args.out)
    audit_path = Path(args.audit)

    df = _read_csv(in_path)

    if "_row_id" not in df.columns:
        raise SystemExit("Error: Dataset must contain a '_row_id' column.")

    # Policy: user-provided (default) or auto-inferred (optional)
    if args.auto_policy:
        if not args.quiet:
            print("Auto-inferring policy from data...")
        policy = infer_policy(df, dataset_name=in_path.stem)
    elif args.policy:
        policy = load_policy(Path(args.policy))
    else:
        raise SystemExit("ERROR: Must provide --policy <path> or use --auto-policy")

    # Runtime Validation
    errors, warnings = validate_policy_against_df(policy, df)
    if errors:
        msg = "\n".join([f"  - {e}" for e in errors])
        raise SystemExit(f"ERROR: Policy validation failed against dataset:\n{msg}")
    
    if warnings and not args.quiet:
        for w in warnings:
            print(f"WARNING: {w}")
            
    clock = (lambda: args.clock) if args.clock else None

    # For reporting, we need a raw profile first
    prof_raw = create_profile(df, policy, clock=clock)

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

    # Opportunity to learn from the policy (if provided by user)
    if args.policy:
        try:
            store = LearningStore()
            n_learned = store.learn_from_policy(policy)
            if n_learned > 0 and not args.quiet:
                print(f"Learned {n_learned} new patterns from your policy.")
        except Exception as e:
            # Learning should never crash the mainly pipeline
            if not args.quiet:
                print(f"Warning: Failed to learn from policy: {e}")

    # For reporting, we need an optimized profile of the result
    prof_opt = create_profile(out_df, policy, clock=clock)

    if args.report or args.report_md:
        report = build_correction_report(prof_raw, prof_opt, audit_path, policy)
        
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
        print(f"Wrote cleaned CSV to {out_path}")
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
