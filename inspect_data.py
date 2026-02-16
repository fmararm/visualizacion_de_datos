
import pandas as pd
import numpy as np

try:
    df = pd.read_csv('distribucion-renta-canarias.csv')
    print("Shape:", df.shape)
    print("\nColumns and their null counts:")
    print(df.isnull().sum())
    print("\nEmpty string counts:")
    for col in df.columns:
        if df[col].dtype == object:
            print(f"{col}: {len(df[df[col] == ''])}")
            
    print("\nDescribe:")
    print(df.describe(include='all'))
    
    print("\nCheck if columns are full duplicates:")
    # Check if TIME_PERIOD#es equals TIME_PERIOD_CODE (if both are essentially the same info)
    # But first, let's just see column names again.
    print(df.columns.tolist())

except Exception as e:
    print(f"Error: {e}")
