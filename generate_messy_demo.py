import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_messy_test_data(num_rows=150):
    """
    Generates an intentionally messy dataset designed to showcase all the 
    capabilities of the Insight Readiness Analyzer pipeline.
    
    Includes:
    - Missing values (Nulls, NaNs, "N/A", "Unknown", etc.)
    - Numeric problems: Strings mixed with numbers ("$5,000", "12k")
    - Date problems: Mixed formats (MM/DD/YYYY, YYYY-MM-DD), invalid dates
    - Semantic problems: Zip codes requiring leading zeros, messy city names with accents
    - Outliers / Validity issues: Negative salaries, ages > 120
    - Whitespace issues
    """
    np.random.seed(42)
    random.seed(42)
    
    cities = [
        ("São Paulo", "SP", "01001000"),
        ("Rio de Janeiro", "RJ", "20040002"),
        ("Brasília", "DF", "70040010"),
        ("New York", "NY", "10001"),
        ("Los Angeles", "CA", "90001"),
    ]
    
    data = []
    base_date = datetime(2020, 1, 1)
    
    for i in range(num_rows):
        city, state, zip_str = random.choice(cities)
        
        # 1. Messy City (Mixed case, accents, whitespace)
        case_choice = random.choice(["lower", "upper", "title", "mixed"])
        if case_choice == "lower": city_dirty = city.lower()
        elif case_choice == "upper": city_dirty = city.upper()
        elif case_choice == "title": city_dirty = city.title()
        else: city_dirty = city
        
        # Add random leading/trailing whitespace
        if random.random() < 0.2:
            city_dirty = f"   {city_dirty}  "
            
        # 2. Messy Zip Code
        # Simulating pandas dropping leading zeros by casting to int sometimes
        if random.random() < 0.3 and zip_str.startswith("0"):
            zip_dirty = int(zip_str)
        else:
            zip_dirty = zip_str
            
        # 3. Messy Dates
        date_obj = base_date + timedelta(days=random.randint(0, 1000))
        if random.random() < 0.2: # Invalid/Missing looking date
            date_dirty = random.choice(["Unknown", "N/A", "99/99/9999", ""])
        elif random.random() < 0.3: # Different format
            date_dirty = date_obj.strftime("%d-%m-%Y")
        else: # Standard format
            date_dirty = date_obj.strftime("%Y/%m/%d")
            
        # 4. Messy Numerical / Currency (Salary)
        base_salary = random.randint(30000, 150000)
        if random.random() < 0.1: # Outlier / Invalid (Negative)
            salary_dirty = -5000
        elif random.random() < 0.2: # Currency formatting
            salary_dirty = f"${base_salary:,.2f}"
        elif random.random() < 0.15: # Suffixes
            salary_dirty = f"{base_salary/1000}k"
        elif random.random() < 0.1: # Missing
            salary_dirty = np.nan
        else:
            salary_dirty = base_salary
            
        # 5. Age (With outliers)
        if random.random() < 0.05:
            age = random.randint(150, 999) # Impossible age
        elif random.random() < 0.1:
            age = -1 # Invalid
        elif random.random() < 0.1:
            age = "nan"
        else:
            age = random.randint(18, 80)
            
        # 6. ID parsing
        cust_id = f"CUST_{i:04d}" if random.random() < 0.95 else f" CUST_{i:04d} "
            
        data.append({
            "customer_id": cust_id,
            "account_created_date": date_dirty,
            "geolocation_city": city_dirty,
            "geolocation_state": state,
            "geolocation_zip_code": zip_dirty,
            "annual_salary": salary_dirty,
            "customer_age": age,
            "is_active": random.choice(["Yes", "No", "Y", "N", "1", "0", "True", "False", "", np.nan])
        })
        
    df = pd.DataFrame(data)
    
    # Introduce row completely missing data
    df.loc[10, :] = np.nan
    df.loc[11, :] = "N/A"
    
    return df

if __name__ == "__main__":
    df = generate_messy_test_data(200)
    output_path = "messy_demo_dataset.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Generated highly uncleaned dataset at: {output_path}")
    print("Features included:")
    print("- Zip codes missing leading zeros")
    print("- Cities with accents and mixed casing")
    print("- Salaries with '$', ',', and 'k' suffixes")
    print("- Mixed date formats and invalid '99/99/9999' dates")
    print("- Impossible ages (outliers like 150+)")
    print("- Boolean columns with Yes/No/1/0/True/False")
    print("- Empty rows and 'N/A' strings")
