from __future__ import annotations
import json
from pathlib import Path
from ira.cli import main

def test_determinism_correct_outputs(tmp_path: Path):
    in_csv = tmp_path / "input.csv"
    policy_yaml = tmp_path / "policy.yaml"
    
    in_csv.write_text("_row_id,val\n1, 10 \n2,20\n", encoding="utf-8")
    policy_yaml.write_text("dataset: {name: test}\nstandardization: {global_trim_whitespace: true}\n", encoding="utf-8")
    
    def run_cli(out_folder: Path):
        audit = out_folder / "audit.jsonl"
        report = out_folder / "report.json"
        report_md = out_folder / "report.md"
        out_csv = out_folder / "out.csv"
        
        main([
            "correct",
            "--input", str(in_csv),
            "--policy", str(policy_yaml),
            "--out", str(out_csv),
            "--audit", str(audit),
            "--report", str(report),
            "--report-md", str(report_md),
            "--clock", "2023-01-01T10:00:00Z",
            "--quiet"
        ])
        return {
            "csv": out_csv.read_text(),
            "audit": audit.read_text(),
            "report": report.read_text(),
            "report_md": report_md.read_text()
        }

    run1 = run_cli(tmp_path / "run1")
    run2 = run_cli(tmp_path / "run2")
    
    assert run1["csv"] == run2["csv"]
    assert run1["audit"] == run2["audit"]
    assert run1["report"] == run2["report"]
    assert run1["report_md"] == run2["report_md"]
