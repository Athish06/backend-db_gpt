import os
import time
import pandas as pd
from typing import List, Dict

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def save_to_parquet(cache_id: str, rows: List[Dict]) -> str:
    """Saves raw data to a parquet file and returns the file path."""
    filepath = os.path.join(CACHE_DIR, f"{cache_id}.parquet")
    
    if not rows:
        df = pd.DataFrame()
    else:
        # Convert data to DataFrame
        # Flatten nested structures or cast to string to ensure parquet compatibility
        df = pd.DataFrame(rows)
        for col in df.columns:
            if df[col].apply(type).eq(dict).any() or df[col].apply(type).eq(list).any():
                df[col] = df[col].astype(str)

    df.to_parquet(filepath, engine="pyarrow")
    return filepath

def get_parquet_filepath(cache_id: str) -> str:
    return os.path.join(CACHE_DIR, f"{cache_id}.parquet")

def cleanup_old_parquets(max_age_seconds: int = 3600):
    """Deletes parquet files older than max_age_seconds."""
    now = time.time()
    for filename in os.listdir(CACHE_DIR):
        if filename.endswith(".parquet"):
            filepath = os.path.join(CACHE_DIR, filename)
            if os.path.isfile(filepath):
                file_age = now - os.path.getmtime(filepath)
                if file_age > max_age_seconds:
                    try:
                        os.remove(filepath)
                    except Exception as e:
                        print(f"Failed to delete old cache file {filepath}: {e}")
