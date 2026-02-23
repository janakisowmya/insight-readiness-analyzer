import subprocess
import sys
from pathlib import Path

datasets = [
    ("robustness_challenge.csv", "robustness_challenge_policy.yaml"),
    ("adversarial_logic.csv", "adversarial_logic_policy.yaml"),
    ("walmart_retail_sales.csv", "walmart_retail_policy.yaml"),
    ("hospital_billing.csv", "hospital_billing_policy.yaml"),
    ("insurance_claims.csv", "insurance_claims_policy.yaml"),
    ("global_freelancers_raw.csv", "freelancers_policy.yaml"),
    ("complete_chaos.csv", "validity_policy.yaml"), # Add the chaos one too
]

def run_test(csv_file, policy_file):
    if not Path(csv_file).exists():
        print(f"SKIP: {csv_file} not found")
        return True # Skip
    if not Path(policy_file).exists():
        print(f"SKIP: {policy_file} not found for {csv_file}")
        return True # Skip

    out_file = f"regression_{csv_file}"
    audit_file = f"regression_{csv_file}.jsonl"
    
    cmd = [
        sys.executable, "-m", "ira.cli", "correct",
        "--input", csv_file,
        "--policy", policy_file,
        "--out", out_file,
        "--audit", audit_file
    ]
    
    print(f"Running {csv_file}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"FAIL: {csv_file}")
        print(result.stderr)
        return False
    
    print(f"PASS: {csv_file}")
    return True

def main():
    failures = 0
    for csv, policy in datasets:
        if not run_test(csv, policy):
            failures += 1
            
    if failures > 0:
        print(f"\nFAILED: {failures} datasets failed processing.")
        sys.exit(1)
    else:
        print("\nSUCCESS: All datasets processed successfully.")
        sys.exit(0)

if __name__ == "__main__":
    main()
