from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _read_audit_jsonl(audit_path: str | Path) -> List[Dict[str, Any]]:
    path = Path(audit_path)
    if not path.exists():
        return []
    events: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except Exception:
            continue
    return events


def build_correction_report(
    profile_raw: Dict[str, Any],
    profile_optimized: Dict[str, Any],
    audit_path: str | Path,
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    events = _read_audit_jsonl(audit_path)

    # ---- Readiness ----
    raw_r = (profile_raw.get("readiness") or {})
    opt_r = (profile_optimized.get("readiness") or {})

    readiness = {
        "raw": raw_r,
        "optimized": opt_r,
        "delta": {
            "score": round(float(opt_r.get("score", 0.0)) - float(raw_r.get("score", 0.0)), 2),
            "breakdown": {
                k: round(float((opt_r.get("breakdown") or {}).get(k, 0.0)) - float((raw_r.get("breakdown") or {}).get(k, 0.0)), 2)
                for k in sorted(set((raw_r.get("breakdown") or {}).keys()) | set((opt_r.get("breakdown") or {}).keys()))
            },
        },
    }

    # ---- Actions from audit ----
    # Works for BOTH detailed events + summary aggregates
    action_counts = Counter()
    column_counts = Counter()
    row_drop_reasons = Counter()

    for e in events:
        rec_type = e.get("type")
        if rec_type == "job_summary":
            continue

        is_summary = (e.get("type") == "summary_aggregate")
        mult = int(e.get("count", 1)) if is_summary else 1

        et = e.get("event_type") or e.get("type") or "unknown"
        action_counts[et] += mult

        col = e.get("column")
        if isinstance(col, str) and col and col != "__row__":
            column_counts[col] += mult

        if e.get("event_type") == "row_dropped":
            row_drop_reasons[e.get("reason", "unknown")] += mult

    actions = {
        "event_counts": dict(action_counts),
        "top_impacted_columns": [
            {"column": c, "count": int(n)} for c, n in column_counts.most_common(10)
        ],
        "row_drops": {
            "total": int(sum(row_drop_reasons.values())),
            "by_reason": dict(row_drop_reasons),
        },
    }

    # ---- Compliance Engine ----
    roles = policy.get("roles", {}) or {}
    protected = set(roles.get("protected_columns", []) or [])

    protected_hits = []
    for e in events:
        if e.get("type") == "job_summary":
            continue
        col = e.get("column")
        if col in protected:
            protected_hits.append({"event_type": e.get("event_type"), "column": col, "reason": e.get("reason")})

    # Identify skipped columns due to imputation threshold
    # Primary source: audit events (logged in Phase 8.1+)
    # Fallback: Inference from policy + raw profile stats
    skipped_events = [e for e in events if e.get("event_type") == "imputation_skipped_threshold"]
    
    if skipped_events:
        skipped = []
        for e in skipped_events:
            # Parse reason if possible to get missing_pct and threshold
            # "missing_pct_0.4000_gt_0.3"
            reason = e.get("reason", "")
            miss_pct = 0.0
            thr = 0.0
            try:
                if "_gt_" in reason:
                    parts = reason.replace("missing_pct_", "").split("_gt_")
                    miss_pct = float(parts[0])
                    thr = float(parts[1])
            except Exception:
                pass
                
            skipped.append({
                "column": e.get("column"),
                "bucket": (e.get("policy_section") or "").split(".")[-1],
                "missing_pct": miss_pct,
                "threshold": thr
            })
    else:
        skipped = []
        missing_cfg = (policy.get("missing_data") or {})
        impute_cfg = (missing_cfg.get("imputation") or {})

        raw_cols = (profile_raw.get("columns") or {})
        parsing_types = ((policy.get("parsing") or {}).get("column_types") or {})

        def bucket_for(col: str) -> Optional[str]:
            t = parsing_types.get(col)
            if not t:
                return None
            if t in ("numeric", "integer", "float"):
                return "numeric"
            if t in ("datetime", "date", "timestamp"):
                return "datetime"
            if t in ("boolean",):
                return None
            return "categorical"

        for col, stats in raw_cols.items():
            b = bucket_for(col)
            if not b:
                continue
            cfg = impute_cfg.get(b) or {}
            thr = cfg.get("allow_if_missing_pct_leq", None)
            if thr is None:
                continue

            miss_pct = float(stats.get("effective_missing_pct", 0.0) or 0.0)
            if miss_pct > float(thr):
                skipped.append({"column": col, "bucket": b, "missing_pct": miss_pct, "threshold": float(thr)})

    compliance = {
        "protected_columns": {
            "status": "pass" if not protected_hits else "fail",
            "protected_count": len(protected),
            "violations": protected_hits[:50],
        },
        "skipped_imputation_columns": skipped,
        "critical_row_drops": actions["row_drops"],
    }

    # ---- Metadata ----
    meta_raw = profile_raw.get("metadata") or {}
    meta_opt = profile_optimized.get("metadata") or {}
    metadata = {
        "dataset_name": meta_raw.get("dataset_name"),
        "policy_hash": meta_raw.get("policy_hash"),
        "timestamp_raw": meta_raw.get("timestamp"),
        "timestamp_optimized": meta_opt.get("timestamp"),
        "rows_raw": meta_raw.get("row_count"),
        "rows_optimized": meta_opt.get("row_count"),
    }

    return {
        "metadata": metadata,
        "readiness": readiness,
        "actions": actions,
        "compliance": compliance,
    }


def generate_markdown_report(report: Dict[str, Any]) -> str:
    md: List[str] = []
    md.append("# Correction Report")
    md.append("")
    md.append("## Job Summary")
    md.append("")

    meta = report.get("metadata", {}) or {}
    md.append(f"- Dataset: **{meta.get('dataset_name', 'unknown')}**")
    md.append(f"- Policy hash: `{meta.get('policy_hash', '')}`")
    md.append(f"- Rows (raw → optimized): **{meta.get('rows_raw')} → {meta.get('rows_optimized')}**")
    md.append("")

    rd = report.get("readiness", {}) or {}
    md.append("## Readiness Impact")
    md.append("")
    md.append(f"- Raw score: **{(rd.get('raw') or {}).get('score')}**")
    md.append(f"- Optimized score: **{(rd.get('optimized') or {}).get('score')}**")
    md.append(f"- Delta: **{((rd.get('delta') or {}).get('score'))}**")
    md.append("")

    actions = report.get("actions", {}) or {}
    md.append("## Actions")
    md.append("")
    md.append("### Event Counts")
    md.append("")
    for k, v in sorted((actions.get("event_counts") or {}).items()):
        md.append(f"- {k}: **{v}**")
    md.append("")

    md.append("### Top Impacted Columns")
    md.append("")
    for item in actions.get("top_impacted_columns", []) or []:
        md.append(f"- {item.get('column')}: **{item.get('count')}**")
    md.append("")

    md.append("### Critical Row Drops")
    md.append("")
    rdrops = actions.get("row_drops", {}) or {}
    md.append(f"- Total rows dropped: **{rdrops.get('total', 0)}**")
    for reason, count in sorted((rdrops.get("by_reason", {}) or {}).items()):
        md.append(f"- {reason}: **{count}**")
    md.append("")

    comp = report.get("compliance", {}) or {}
    md.append("## Policy Compliance")
    md.append("")
    pc = comp.get("protected_columns", {}) or {}
    md.append(f"- Protected columns status: **{pc.get('status')}**")
    if pc.get("status") == "fail":
        md.append("")
        md.append("### Protected Column Violations (sample)")
        md.append("")
        for v in (pc.get("violations") or [])[:10]:
            md.append(f"- column={v.get('column')} event={v.get('event_type')} reason={v.get('reason')}")
    md.append("")

    md.append("### Skipped Imputation Columns")
    md.append("")
    skipped = report.get("compliance", {}) or {}
    skipped_cols = skipped.get("skipped_imputation_columns", []) or []
    if not skipped_cols:
        md.append("- None")
    else:
        for s in skipped_cols[:20]:
            md.append(f"- {s.get('column')} ({s.get('bucket')}): missing_pct={s.get('missing_pct')} > threshold={s.get('threshold')}")
    md.append("")

    return "\n".join(md)
