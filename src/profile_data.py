import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")

for file in DATA_DIR.glob("*.csv"):
    df = pd.read_csv(file, low_memory=False)

    print(f"\nFILE: {file.name}")

    print(f"Rows: {df.shape[0]:,}")
    print(f"Columns: {df.shape[1]}")
    print("Missing values:")
    print(df.isnull().sum()[df.isnull().sum() > 0])
    print(f"Duplicate rows: {df.duplicated().sum():,}")