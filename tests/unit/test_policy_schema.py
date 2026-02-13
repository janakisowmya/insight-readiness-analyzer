from pathlib import Path
import pytest

from ira.correction.policy_schema import load_policy, PolicyLoadError


def _write(tmpdir: Path, text: str) -> Path:
    p = tmpdir / "policy.yaml"
    p.write_text(text, encoding="utf-8")
    return p


def test_load_minimal_policy_applies_defaults(tmp_path: Path):
    p = _write(tmp_path, "version: '1.0'\nroles: {}\n")
    pol = load_policy(p)
    assert pol["version"] == "1.0"
    assert "parsing" in pol
    assert pol["reproducibility"]["policy_hash"]


def test_protected_cannot_be_standardized(tmp_path: Path):
    y = """
version: "1.0"
roles:
  protected_columns: ["customer_id"]
  standardize_columns: ["customer_id"]
"""
    p = _write(tmp_path, y)
    with pytest.raises(PolicyLoadError):
        load_policy(p)


def test_dedupe_keep_max_requires_order_by(tmp_path: Path):
    y = """
version: "1.0"
deduplication:
  enabled: true
  keys: ["order_id"]
  strategy: "keep_max"
"""
    p = _write(tmp_path, y)
    with pytest.raises(PolicyLoadError):
        load_policy(p)


def test_time_use_only_requires_time_column(tmp_path: Path):
    y = """
version: "1.0"
dataset:
  time_column:
    mode: "use_only"
"""
    p = _write(tmp_path, y)
    with pytest.raises(PolicyLoadError):
        load_policy(p)
