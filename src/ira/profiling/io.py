from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict
import pandas as pd


def load_dataframe(path: Path | str) -> pd.DataFrame:
    """Load dataset from CSV. Supports only CSV in MVP."""
    return pd.read_csv(path)


def write_json(path: Path | str, obj: Dict[str, Any]) -> None:
    """Write object to JSON with deterministic settings."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=False)
