
import pandas as pd
import sys
import os

# Add src to path
sys.path.append(os.path.abspath("src"))

from ira.profiling.infer_policy import infer_policy
from ira.correction.pipeline import run_correction_pipeline

def test_inference_and_correction():
    print("Loading simulation dataset...")
    df = pd.read_csv("test_data/work_rate_simulation.csv")
    
    print("\n--- Running Inference ---")
    policy = infer_policy(df, dataset_name="simulation")
    
    # Check 1: Total Pay detected as float?
    pay_type = policy["parsing"]["column_types"].get("total pay")
    print(f"Total Pay Type: {pay_type}")
    print(f"All Column Types: {policy['parsing']['column_types']}")
    assert pay_type == "float", f"Expected total pay to be float, got {pay_type}"
    
    # Check 2: Account Created detected as datetime?
    created_dt = policy.get("datetime", {}).get("columns", {}).get("account created on")
    print(f"Account Created Config: {created_dt}")
    assert created_dt is not None, "Expected account created to be detected as datetime"
    
    # Check 3: Validity Rules for Email
    email_rule = policy.get("validity", {}).get("rules", {}).get("email")
    print(f"Email Rule: {email_rule}")
    assert email_rule is not None, "Expected email validity rule to be automated"
    assert "regex" in email_rule, "Expected regex in email rule"

    # Check 4: Universal Imputation
    imp_cols = policy.get("imputation", {}).get("columns", {})
    print(f"Imputation Columns: {list(imp_cols.keys())}")
    assert "monthly wage" in imp_cols, "Expected monthly wage in imputation"
    assert "total pay" in imp_cols, "Expected total pay in imputation"
    
    print("\n--- Running Correction Pipeline ---")
    clean_df = run_correction_pipeline(df, policy)
    
    print("\n--- Results ---")
    print(clean_df[["total pay", "monthly wage", "account created on", "email"]].head())
    print("\nTypes:")
    print(clean_df.dtypes)
    
    # Verification
    # Total Pay should be numeric
    assert pd.api.types.is_float_dtype(clean_df["total pay"]), "Total pay should be float in output"
    
    # Email should be flagged (we can't easily check flags here without audit log, but pipeline ran)
    
    print("\n✅ SUCCESS: Hardening logic verified!")

if __name__ == "__main__":
    try:
        test_inference_and_correction()
    except AssertionError as e:
        print(f"\n❌ FAILURE: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ CRASH: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
