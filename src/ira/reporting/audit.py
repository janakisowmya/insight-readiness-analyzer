from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, TextIO

AuditDetail = Literal["summary", "detailed"]


class AuditLogger:
    def __init__(self, path: str | Path, detail: AuditDetail = "summary"):
        self.path = Path(path)
        self.detail = detail
        self.file: Optional[TextIO] = None
        self._opened = False
        
        # Summary mode state
        # Key: (event_type, column, reason) -> count
        self._summary_counts: Dict[tuple, int] = defaultdict(int)
        # Key: (event_type, column, reason) -> list of sample events
        self._summary_samples: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
        self._max_samples_per_group = 25
        
        # Job Context
        self._context: Dict[str, Any] = {}
        self._timestamp_start: Optional[str] = None
        self._timestamp_end: Optional[str] = None

        self._ensure_file_open()

    def _ensure_file_open(self):
        if not self._opened:
            # Ensure parent exists
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.file = open(self.path, "w", encoding="utf-8")
            self._opened = True

    def log(self, event: Dict[str, Any]) -> None:
        """
        Log an event.
        Required fields: timestamp, event_type, reason, policy_section.
        """
        required = {"timestamp", "event_type", "reason", "policy_section"}
        missing = required - event.keys()
        if missing:
            raise ValueError(f"Missing required audit fields: {missing}")

        # Always aggregate so we can emit a footer in both modes
        if self._timestamp_start is None:
            self._timestamp_start = event.get("timestamp")
        self._timestamp_end = event.get("timestamp")
        
        self._aggregate(event)

        # Detailed mode also writes each event line-by-line
        if self.detail == "detailed":
            self._write_line(event)

    def log_value_change(
        self,
        event_type: str,
        row_id: Any,
        column: str,
        old_value: Any,
        new_value: Any,
        reason: str,
        policy_section: str,
        severity: Optional[str] = None,
        timestamp: Optional[str] = None,
    ) -> None:
        """
        Helper to log a value change.
        Note: timestamp is optional here and should be provided by caller or added if missing.
        However, per requirements, caller/tests inject timestamp for determinism.
        If timestamp is missing in the underlying log call, it raises ValueError.
        So we expect the caller to pass it or we'd need a clock strategy. 
        Given the requirement "stable timestamps in unit tests: tests should inject a fixed timestamp",
        we will require timestamp to be passed or present in the dict.
        """
        # Ideally we'd have a clock injection, but for now we'll assume the caller handles the timestamp
        # or we could add a default if not provided (but determinism requires control).
        # We will assume constraints mean explicit control.
        if timestamp is None:
             # In a real app we might default to now(), but for strict determinism we fail if not provided?
             # Or we use a safe default but warn.
             # The instructions say "tests should inject a fixed timestamp".
             # We'll allow None and let log() validation fail if it's truly required and missing.
             # Actually, let's leave it to the caller to provide valid dicts.
             pass

        event = {
            "event_type": event_type,
            "row_id": row_id,
            "column": column,
            "old_value": old_value,
            "new_value": new_value,
            "reason": reason,
            "policy_section": policy_section,
        }
        if severity:
            event["severity"] = severity
        if timestamp:
            event["timestamp"] = timestamp
            
        self.log(event)

    def set_context(self, **kwargs: Any) -> None:
        """Sets metadata for the job summary footer (JSON-safe)."""
        for k, v in kwargs.items():
            if hasattr(v, "item"):  # numpy scalars
                v = v.item()
            if isinstance(v, Path):
                v = str(v)
            self._context[k] = v

    @staticmethod
    def _json_safe(obj: Any) -> Any:
        """Recursively convert pandas/numpy types to JSON-serializable Python types."""
        import pandas as _pd
        import numpy as _np

        if isinstance(obj, dict):
            return {k: AuditLogger._json_safe(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [AuditLogger._json_safe(v) for v in obj]
        # pd.NA, pd.NaT, numpy NaN → None
        if obj is _pd.NA or obj is _pd.NaT:
            return None
        if isinstance(obj, float) and _np.isnan(obj):
            return None
        # numpy scalar → Python scalar
        if hasattr(obj, "item"):
            return obj.item()
        return obj

    def _write_line(self, data: Dict[str, Any]) -> None:
        if self.file and not self.file.closed:
            # Sanitize pandas/numpy types, then deterministic serialization
            safe = self._json_safe(data)
            line = json.dumps(safe, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            self.file.write(line + "\n")
            self.file.flush()

    def _aggregate(self, event: Dict[str, Any]) -> None:
        # Keying by event_type, column, reason
        # We handle None for column if it's missing (though log_value_change has it)
        etype = event.get("event_type", "unknown")
        col = event.get("column", "")  # Empty string if global/row-level
        reason = event.get("reason", "unknown")
        
        key = (etype, col, reason)
        self._summary_counts[key] += 1
        
        if len(self._summary_samples[key]) < self._max_samples_per_group:
            self._summary_samples[key].append(event)

    def close(self) -> None:
        if not self._opened:
            return

        if self.file and not self.file.closed:
            # In summary mode, keep existing behavior: write per-group aggregates
            if self.detail == "summary":
                sorted_keys = sorted(self._summary_counts.keys())
                for key in sorted_keys:
                    etype, col, reason = key
                    count = self._summary_counts[key]
                    samples = self._summary_samples[key]

                    summary_record = {
                        "type": "summary_aggregate",
                        "event_type": etype,
                        "column": col,
                        "reason": reason,
                        "count": count,
                        "samples": samples,
                    }
                    self._write_line(summary_record)

            # Always write a final footer/job summary (both summary + detailed)
            total_events = 0
            by_event_type: Dict[str, int] = {}
            touched_columns: Set[str] = set()

            for key, count in self._summary_counts.items():
                etype, col, _reason = key
                total_events += int(count)
                by_event_type[etype] = by_event_type.get(etype, 0) + int(count)
                if col not in (None, "", "__row__"):
                    touched_columns.add(str(col))

            footer = {
                "type": "job_summary",
                "detail": self.detail,
                "total_events": total_events,
                "event_type_counts": dict(sorted(by_event_type.items())),
                "unique_columns_touched": len(touched_columns),
                "timestamp_start": self._timestamp_start,
                "timestamp_end": self._timestamp_end,
                "job_context": {k: self._context[k] for k in sorted(self._context.keys())},
            }
            
            self._write_line(footer)

        if self.file:
            self.file.close()
        self._opened = False
