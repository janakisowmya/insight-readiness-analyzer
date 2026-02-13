import pandas as pd
import numpy as np

def clean_whitespace(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().where(s.notna(), s)

def debug():
    df = pd.DataFrame({
        "_row_id": [10, 20, 30],
        "name": ["  Alice  ", "BOB", "Charlie"],
        "city": ["New York", "london", "Paris\x00"],
    })
    
    print("Dtypes:\n", df.dtypes)
    col = "name"
    old_s = df[col]
    new_s = clean_whitespace(old_s)
    
    print("\nOld:\n", old_s)
    print("\nNew:\n", new_s)
    
    print("\nEquals:", old_s.equals(new_s))
    
    if not old_s.equals(new_s):
        print("UPDATING DF")
        df[col] = new_s
        
    print("\nDF after update:\n", df)

if __name__ == "__main__":
    debug()
