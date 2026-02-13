from __future__ import annotations
import json
import subprocess
import tempfile
from pathlib import Path
import pandas as pd
import yaml
import pytest

import sys

def test_cli_profile_happy_path():
    """Verify ira profile happy path with deterministic output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        csv_path = tmp_path / "data.csv"
        yaml_path = tmp_path / "policy.yaml"
        out_path = tmp_path / "profile.json"

        # 1. Create Input CSV
        df = pd.DataFrame({
            "_row_id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [25, 30]
        })
        df.to_csv(csv_path, index=False)

        # 2. Create Policy YAML
        policy = {
            "dataset": {"name": "test_cli"},
            "parsing": {"column_types": {"age": "integer"}}
        }
        with open(yaml_path, "w") as f:
            yaml.dump(policy, f)

        # 3. Run CLI
        cmd = [
            sys.executable, "src/ira/cli.py", "profile",
            "--input", str(csv_path),
            "--policy", str(yaml_path),
            "--out", str(out_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        assert result.returncode == 0
        assert "Wrote profile to" in result.stdout
        assert out_path.exists()

        # 4. Verify Deterministic Output (sort_keys=True)
        with open(out_path, "r") as f:
            raw_json = f.read()
            profile = json.loads(raw_json)
        
        # Check sort_keys by verifying indentation and key order in raw string 
        # (Though json.loads loses order, we can check if it's valid JSON first)
        assert profile["metadata"]["dataset_name"] == "test_cli"
        assert "age" in profile["columns"]
        
        # Re-parse and check if keys are sorted in the first level
        data = json.loads(raw_json)
        keys = list(data.keys())
        assert keys == sorted(keys)


def test_cli_profile_missing_row_id():
    """Verify error when _row_id is missing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        csv_path = tmp_path / "bad.csv"
        yaml_path = tmp_path / "policy.yaml"
        out_path = tmp_path / "profile.json"

        df = pd.DataFrame({"name": ["Alice"]}) # Missing _row_id
        df.to_csv(csv_path, index=False)
        
        with open(yaml_path, "w") as f:
            yaml.dump({"dataset": {"name": "bad"}}, f)

        cmd = [
            sys.executable, "src/ira/cli.py", "profile",

            "--input", str(csv_path),
            "--policy", str(yaml_path),
            "--out", str(out_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        assert result.returncode != 0
        assert "Error: Dataset must contain a '_row_id' column." in result.stderr


def test_cli_determinism_through_direct_call():
    """Verify create_profile determinism with clock callable."""
    from ira.profiling.profile import create_profile
    df = pd.DataFrame({"_row_id": [1], "v": [1]})
    policy = {"dataset": {"name": "test"}}
    
    fixed_time = "2025-01-01T12:00:00+00:00"
    p1 = create_profile(df, policy, clock=lambda: fixed_time)
    p2 = create_profile(df, policy, clock=lambda: fixed_time)
    
    assert p1["metadata"]["timestamp"] == p2["metadata"]["timestamp"]
    assert p1 == p2


def test_cli_profile_output_formatting():
    """Verify JSON output has sort_keys and indent."""
    from ira.profiling.io import write_json
    import tempfile
    
    with tempfile.NamedTemporaryFile(suffix=".json") as tmp:
        data = {"b": 1, "a": 2}
        write_json(tmp.name, data)
        with open(tmp.name, "r") as f:
            content = f.read()
            # Key 'a' should come before 'b'
            assert content.index('"a": 2') < content.index('"b": 1')
            assert "  " in content # Indent check
