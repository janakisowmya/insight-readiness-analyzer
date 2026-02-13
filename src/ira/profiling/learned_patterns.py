"""Persistent learning store for auto-policy inference.

When users provide manual policies, the system extracts new patterns
(currency symbols, protected keywords, boolean values, etc.) and stores
them in ``~/.ira/learned_patterns.json`` for future auto-inference runs.

Usage:
    store = LearningStore()          # loads from disk
    n = store.learn_from_policy(policy_dict)  # extract & save
    patterns = store.get_learned()   # retrieve for inference
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set

# Default store location
_DEFAULT_STORE_DIR = Path.home() / ".ira"
_DEFAULT_STORE_FILE = _DEFAULT_STORE_DIR / "learned_patterns.json"

# Built-in values (so we only store what's truly NEW)
_BUILTIN_CURRENCY_SYMBOLS: Set[str] = {"$", "€", "£", "₹", "¥"}
_BUILTIN_CURRENCY_CODES: Set[str] = {
    "USD", "EUR", "GBP", "INR", "JPY", "AUD", "CAD", "CHF",
    "CNY", "MXN", "BRL", "KRW", "SGD", "HKD", "NZD", "RS.", "RS", "R$",
}
_BUILTIN_BOOL_TRUE: Set[str] = {
    "yes", "y", "true", "t", "1", "on", "active", "enabled",
    "si", "ja", "oui", "da", "affirmative", "yep", "yeah",
}
_BUILTIN_BOOL_FALSE: Set[str] = {
    "no", "n", "false", "f", "0", "off", "inactive", "disabled",
    "non", "nein", "nah", "nope", "negative",
}


class LearningStore:
    """Manages learned patterns on disk as a simple JSON file."""

    def __init__(self, store_path: Path | None = None) -> None:
        self.path = store_path or _DEFAULT_STORE_FILE
        self._data: Dict[str, Any] = self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> Dict[str, Any]:
        """Load learned patterns from disk, or return empty structure."""
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return self._validate(data)
            except (json.JSONDecodeError, TypeError, KeyError):
                return self._empty()
        return self._empty()

    def _save(self) -> None:
        """Persist learned patterns to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _empty() -> Dict[str, Any]:
        return {
            "version": 1,
            "currency_symbols": [],
            "protected_keywords": [],
            "bool_true_values": [],
            "bool_false_values": [],
            "column_type_hints": {},       # col_name -> "float"/"datetime"/...
            "imputation_preference": None, # "median" / "mean" / "mode"
            "learn_count": 0,
        }

    @staticmethod
    def _validate(data: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure all expected keys exist."""
        empty = LearningStore._empty()
        for key, default in empty.items():
            if key not in data:
                data[key] = default
        return data

    # ------------------------------------------------------------------
    # Learning
    # ------------------------------------------------------------------

    def learn_from_policy(self, policy: Dict[str, Any]) -> int:
        """Extract new patterns from a user-provided policy.

        Returns the number of *new* patterns learned.
        """
        learned = 0

        # 1. Currency symbols
        user_currencies = set(policy.get("parsing", {}).get("currency_symbols", []))
        all_builtin = _BUILTIN_CURRENCY_SYMBOLS | _BUILTIN_CURRENCY_CODES
        existing_learned = set(self._data["currency_symbols"])
        new_currencies = user_currencies - all_builtin - existing_learned
        if new_currencies:
            self._data["currency_symbols"].extend(sorted(new_currencies))
            learned += len(new_currencies)

        # 2. Protected column keywords
        # Extract meaningful keywords from protected column names
        user_protected = set(policy.get("roles", {}).get("protected_columns", []))
        existing_keywords = set(self._data["protected_keywords"])
        for col_name in user_protected:
            # Split on underscores/spaces/hyphens to get keywords
            parts = col_name.replace("_", " ").replace("-", " ").lower().split()
            for part in parts:
                # Only learn words of 3+ chars that aren't too generic
                if (len(part) >= 3
                        and part not in existing_keywords
                        and part not in {"the", "and", "for", "with", "from", "col",
                                         "column", "field", "data", "raw", "new", "old",
                                         "row", "num", "str", "int", "val", "value"}):
                    self._data["protected_keywords"].append(part)
                    existing_keywords.add(part)
                    learned += 1

        # 3. Boolean values
        user_booleans = policy.get("parsing", {}).get("boolean_columns", {})
        existing_true = set(self._data["bool_true_values"])
        existing_false = set(self._data["bool_false_values"])
        for _col, cfg in user_booleans.items():
            if isinstance(cfg, dict):
                for tv in cfg.get("true_values", []):
                    tv_lower = str(tv).lower()
                    if tv_lower not in _BUILTIN_BOOL_TRUE and tv_lower not in existing_true:
                        self._data["bool_true_values"].append(tv_lower)
                        existing_true.add(tv_lower)
                        learned += 1
                for fv in cfg.get("false_values", []):
                    fv_lower = str(fv).lower()
                    if fv_lower not in _BUILTIN_BOOL_FALSE and fv_lower not in existing_false:
                        self._data["bool_false_values"].append(fv_lower)
                        existing_false.add(fv_lower)
                        learned += 1

        # 4. Column type hints (remember specific column->type mappings)
        user_types = policy.get("parsing", {}).get("column_types", {})
        for col_name, col_type in user_types.items():
            if col_name not in self._data["column_type_hints"]:
                self._data["column_type_hints"][col_name] = col_type
                learned += 1

        # 5. Imputation preference
        user_strategy = policy.get("imputation", {}).get("strategy")
        if user_strategy and self._data["imputation_preference"] is None:
            self._data["imputation_preference"] = user_strategy
            learned += 1

        if learned > 0:
            self._data["learn_count"] = self._data.get("learn_count", 0) + 1
            self._save()

        return learned

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def get_learned(self) -> Dict[str, Any]:
        """Return all learned patterns as a dict."""
        return dict(self._data)

    def get_currency_symbols(self) -> List[str]:
        return list(self._data["currency_symbols"])

    def get_protected_keywords(self) -> List[str]:
        return list(self._data["protected_keywords"])

    def get_bool_true(self) -> List[str]:
        return list(self._data["bool_true_values"])

    def get_bool_false(self) -> List[str]:
        return list(self._data["bool_false_values"])

    def get_column_type_hints(self) -> Dict[str, str]:
        return dict(self._data["column_type_hints"])

    def get_imputation_preference(self) -> str | None:
        return self._data.get("imputation_preference")

    # ------------------------------------------------------------------
    # Management
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Clear all learned patterns."""
        self._data = self._empty()
        self._save()

    def summary(self) -> str:
        """Return a human-readable summary of learned patterns."""
        lines: List[str] = []
        n = self._data.get("learn_count", 0)
        lines.append(f"Learning sessions: {n}")
        lines.append(f"Store location: {self.path}")
        lines.append("")

        cs = self._data["currency_symbols"]
        lines.append(f"Currency symbols ({len(cs)}): {cs or '(none)'}")

        pk = self._data["protected_keywords"]
        lines.append(f"Protected keywords ({len(pk)}): {pk or '(none)'}")

        bt = self._data["bool_true_values"]
        lines.append(f"Boolean TRUE values ({len(bt)}): {bt or '(none)'}")

        bf = self._data["bool_false_values"]
        lines.append(f"Boolean FALSE values ({len(bf)}): {bf or '(none)'}")

        ct = self._data["column_type_hints"]
        lines.append(f"Column type hints ({len(ct)}): {dict(ct) or '(none)'}")

        ip = self._data.get("imputation_preference")
        lines.append(f"Imputation preference: {ip or '(not set)'}")

        return "\n".join(lines)
